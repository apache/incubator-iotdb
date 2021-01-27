/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.dataset.groupby;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.measurementorderoptimizer.MeasurementOrderOptimizer;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.GroupByQueryRecord;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupByWithoutValueFilterDataSet extends GroupByEngineDataSet {

  private static final Logger logger = LoggerFactory
      .getLogger(GroupByWithoutValueFilterDataSet.class);

  private Map<PartialPath, GroupByExecutor> pathExecutors = new HashMap<>();

  /**
   * path -> result index for each aggregation
   * <p>
   * e.g.,
   * <p>
   * deduplicated paths : s1, s2, s1 deduplicated aggregations : count, count, sum
   * <p>
   * s1 -> 0, 2 s2 -> 1
   */
  private Map<PartialPath, List<Integer>> resultIndexes = new HashMap<>();

  public GroupByWithoutValueFilterDataSet() {
  }

  /**
   * constructor.
   */
  public GroupByWithoutValueFilterDataSet(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {
    super(context, groupByTimePlan);

    initGroupBy(context, groupByTimePlan);
  }

  protected void initGroupBy(QueryContext context, GroupByTimePlan groupByTimePlan)
      throws StorageEngineException, QueryProcessException {
    IExpression expression = groupByTimePlan.getExpression();

    Filter timeFilter = null;
    if (expression != null) {
      timeFilter = ((GlobalTimeExpression) expression).getFilter();
    }

    List<StorageGroupProcessor> list = StorageEngine.getInstance()
        .mergeLock(paths.stream().map(p -> (PartialPath) p).collect(Collectors.toList()));
    try {
      // init resultIndexes, group result indexes by path
      WorkloadManager manager = WorkloadManager.getInstance();
      Map<String, List<Integer>> deviceQueryIdxMap = new HashMap<>();
      for (int i = 0; i < paths.size(); i++) {
        PartialPath path = (PartialPath) paths.get(i);
        if (!pathExecutors.containsKey(path)) {
          //init GroupByExecutor
          pathExecutors.put(path,
              getGroupByExecutor(path, groupByTimePlan.getAllMeasurementsInDevice(path.getDevice()),
                  dataTypes.get(i), context, timeFilter, null, groupByTimePlan.isAscending()));
          resultIndexes.put(path, new ArrayList<>());
        }
        resultIndexes.get(path).add(i);
        AggregateResult aggrResult = AggregateResultFactory
            .getAggrResultByName(groupByTimePlan.getDeduplicatedAggregations().get(i),
                dataTypes.get(i), ascending);
        pathExecutors.get(path).addAggregateResult(aggrResult);
        // Map the device id to the corresponding query indexes
        if (deviceQueryIdxMap.containsKey(path.getDevice())) {
          deviceQueryIdxMap.get(path.getDevice()).add(i);
        } else {
          deviceQueryIdxMap.put(path.getDevice(), new ArrayList<>());
          deviceQueryIdxMap.get(path.getDevice()).add(i);
        }
      }

      // Add the query records to the workload manager
      for(String device: deviceQueryIdxMap.keySet()) {
        List<Integer> pathIndexes = deviceQueryIdxMap.get(device);
        List<String> sensors = new ArrayList<>();
        List<String> ops = new ArrayList<>();
        for(int idx: pathIndexes) {
          PartialPath path = (PartialPath) paths.get(idx);
          sensors.add(path.getMeasurement());
          ops.add(groupByTimePlan.getDeduplicatedAggregations().get(idx));
        }
        QueryRecord record = new GroupByQueryRecord(device, sensors, ops, groupByTimePlan.getStartTime(),
                groupByTimePlan.getEndTime(), groupByTimePlan.getInterval(), groupByTimePlan.getSlidingStep());
        manager.addRecord(record);
      }
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  @Override
  protected RowRecord nextWithoutConstraint() throws IOException {
    if (!hasCachedTimeInterval) {
      throw new IOException("need to call hasNext() before calling next() "
          + "in GroupByWithoutValueFilterDataSet.");
    }
    hasCachedTimeInterval = false;
    RowRecord record;
    if (leftCRightO) {
      record = new RowRecord(curStartTime);
    } else {
      record = new RowRecord(curEndTime - 1);
    }

    AggregateResult[] fields = new AggregateResult[paths.size()];

    try {
      // Reorder the order of queries to be consistent with the physical order of the files
      // deviceId -> Set<Measurement to be query>
      HashMap<String, Set<String>> deviceMeasurementMap = new HashMap<>();
      // deviceId -> measurement -> partial path
      HashMap<String, Map<String, PartialPath>> partialPathMap = new HashMap<>();
      for (Entry<PartialPath, GroupByExecutor> pathToExecutorEntry : pathExecutors.entrySet()) {
        if (!deviceMeasurementMap.containsKey(pathToExecutorEntry.getKey().getDevice())) {
          deviceMeasurementMap.put(pathToExecutorEntry.getKey().getDevice(), new HashSet<>());
          partialPathMap.put(pathToExecutorEntry.getKey().getDevice(), new HashMap<>());
        }
        deviceMeasurementMap.get(pathToExecutorEntry.getKey().getDevice()).
                add(pathToExecutorEntry.getKey().getMeasurement());
        partialPathMap.get(pathToExecutorEntry.getKey().getDevice()).
                put(pathToExecutorEntry.getKey().getMeasurement(), pathToExecutorEntry.getKey());
      }

      Map<String, List<PartialPath>> measurementQueryOrder = new HashMap<>();
      for(Entry<String,Set<String>> measurementToBeQueried : deviceMeasurementMap.entrySet()) {
        String deviceId = measurementToBeQueried.getKey();
        List<String> physicalMeasurementOrder = MeasurementOrderOptimizer.getInstance().getMeasurementsOrder(deviceId);
        Set<String> measurements = deviceMeasurementMap.get(deviceId);
        measurementQueryOrder.put(deviceId, new LinkedList<>());
        for(String measurement : physicalMeasurementOrder) {
          if (measurements.contains(measurement)) {
            measurementQueryOrder.get(deviceId).add(partialPathMap.get(deviceId).get(measurement));
          }
        }
      }

      // query in the physical order
      for(String device : measurementQueryOrder.keySet()) {
        List<PartialPath> measurementPaths = measurementQueryOrder.get(device);
        for(PartialPath path : measurementPaths) {
          GroupByExecutor executor = pathExecutors.get(path);
          List<AggregateResult> aggregations = executor.calcResult(curStartTime, curEndTime);
          for (int i = 0; i < aggregations.size(); i++) {
            int resultIndex = resultIndexes.get(path).get(i);
            fields[resultIndex] = aggregations.get(i);
          }
        }
      }
    } catch (QueryProcessException e) {
      logger.error("GroupByWithoutValueFilterDataSet execute has error", e);
      throw new IOException(e.getMessage(), e);
    }

    for (AggregateResult res : fields) {
      if (res == null) {
        record.addField(null);
        continue;
      }
      record.addField(res.getResult(), res.getResultDataType());
    }
    return record;
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(Path path, int i) throws IOException {
    Pair<Long, Object> result = null;
    long nextStartTime = curStartTime;
    long nextEndTime;
    do {
      nextStartTime -= slidingStep;
      if (nextStartTime >= startTime) {
        nextEndTime = Math.min(nextStartTime + interval, endTime);
      } else {
        return null;
      }
      result = pathExecutors.get(path).peekNextNotNullValue(nextStartTime, nextEndTime);
    } while (result == null);
    return result;
  }

  protected GroupByExecutor getGroupByExecutor(PartialPath path, Set<String> allSensors,
      TSDataType dataType,
      QueryContext context, Filter timeFilter, TsFileFilter fileFilter, boolean ascending)
      throws StorageEngineException, QueryProcessException {
    return new LocalGroupByExecutor(path, allSensors, dataType, context, timeFilter, fileFilter,
        ascending);
  }
}