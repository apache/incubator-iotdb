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
package org.apache.iotdb.db.cq;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.crud.GroupByClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectComponent;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContinuousQueryTask extends WrappedRunnable {

  private static final int FETCH_SIZE = 10000;
  private static final int BATCH_SIZE = 10000;

  private static final Logger logger = LoggerFactory.getLogger(ContinuousQueryTask.class);

  // To execute the query plan
  private static PlanExecutor planExecutor;

  static {
    try {
      planExecutor = new PlanExecutor();
    } catch (QueryProcessException e) {
      logger.error(e.getMessage());
    }
  }

  // To save the continuous query info
  private final CreateContinuousQueryPlan plan;
  // To transform query operator to query plan
  private static final Planner planner = new Planner();
  // Next timestamp to execute a query
  private long windowEndTimestamp;

  private static final Pattern pattern = Pattern.compile("\\$\\{\\w+}");

  public ContinuousQueryTask(CreateContinuousQueryPlan plan, long windowEndTimestamp) {
    this.plan = plan;
    this.windowEndTimestamp = windowEndTimestamp;
  }

  @Override
  public void runMayThrow()
      throws QueryProcessException, StorageEngineException, IOException, InterruptedException,
          QueryFilterOptimizationException, MetadataException {

    GroupByTimePlan queryPlan = generateQueryPlan();

    if (queryPlan.getDeduplicatedPaths().isEmpty()) {
      logger.info(plan.getContinuousQueryName() + ": deduplicated paths empty");
      return;
    }

    QueryDataSet result = doQuery(queryPlan);

    if (result == null || result.getPaths().size() == 0) {
      logger.info(plan.getContinuousQueryName() + ": query result empty");
      return;
    }

    doInsert(result, queryPlan);
  }

  public void onRejection() {
    logger.warn("Continuous Query Task {} rejected", plan.getContinuousQueryName());
  }

  private GroupByTimePlan generateQueryPlan() throws QueryProcessException {

    QueryOperator queryOperator = plan.getQueryOperator();

    // To handle the time series meta changes in different queries, i.e. creation & deletion,
    // we need to apply concatenation optimization to SelectComponent before every query.
    // Since the concatenation optimization will change resultColumns information of
    // SelectComponent,
    // we need to save one copy of the original SelectComponent.
    SelectComponent selectComponentCopy = new SelectComponent(queryOperator.getSelectComponent());

    GroupByTimePlan queryPlan = planner.cqQueryOperatorToGroupByTimePlan(queryOperator);

    queryOperator.setSelectComponent(selectComponentCopy);

    queryPlan.setStartTime(windowEndTimestamp - plan.getForInterval());
    queryPlan.setEndTime(windowEndTimestamp);

    return queryPlan;
  }

  private QueryDataSet doQuery(GroupByTimePlan queryPlan)
      throws StorageEngineException, QueryFilterOptimizationException, MetadataException,
          IOException, InterruptedException, QueryProcessException {
    long queryId = QueryResourceManager.getInstance().assignQueryId(true);

    try {
      return planExecutor.processQuery(queryPlan, new QueryContext(queryId));
    } finally {
      QueryResourceManager.getInstance().endQuery(queryId);
    }
  }

  private void doInsert(QueryDataSet result, GroupByTimePlan queryPlan)
      throws QueryProcessException, IOException, IllegalPathException {

    int columnSize = result.getDataTypes().size();
    TSDataType dataType =
        TypeInferenceUtils.getAggrDataType(
            queryPlan.getAggregations().get(0), queryPlan.getDataTypes().get(0));

    InsertTabletPlan[] insertTabletPlans = generateInsertTabletPlans(columnSize, result, dataType);

    int batchSize =
        (int)
            Math.min(
                BATCH_SIZE,
                Math.ceil(
                    (float) plan.getForInterval()
                        / ((GroupByClauseComponent)
                                plan.getQueryOperator().getSpecialClauseComponent())
                            .getUnit()));

    Object[][] columns = constructColumns(columnSize, batchSize, dataType);
    long[][] timestamps = new long[columnSize][batchSize];
    int[] rowNums = new int[columnSize];

    boolean hasNext = true;

    while (hasNext) {
      int rowNum = 0;

      while (++rowNum <= batchSize) {
        if (!result.hasNextWithoutConstraint()) {
          hasNext = false;
          break;
        }
        RowRecord record = result.nextWithoutConstraint();
        fillColumns(columns, dataType, record, rowNums, timestamps);
      }

      for (int i = 0; i < columnSize; i++) {
        if (rowNums[i] > 0) {
          insertTabletPlans[i].setTimes(timestamps[i]);
          insertTabletPlans[i].setColumns(columns[i]);
          insertTabletPlans[i].setRowCount(rowNums[i]);
          planExecutor.insertTablet(insertTabletPlans[i]);
        }
      }
    }
  }

  private InsertTabletPlan[] generateInsertTabletPlans(
      int columnSize, QueryDataSet result, TSDataType dataType) throws IllegalPathException {
    List<PartialPath> targetPaths = generateTargetPaths(result.getPaths());
    InsertTabletPlan[] insertTabletPlans = new InsertTabletPlan[columnSize];
    String[] measurements = new String[] {targetPaths.get(0).getMeasurement()};
    List<Integer> dataTypes = Collections.singletonList(dataType.ordinal());

    for (int i = 0; i < columnSize; i++) {
      insertTabletPlans[i] =
          new InsertTabletPlan(
              new PartialPath(targetPaths.get(i).getDevice()), measurements, dataTypes);
    }

    return insertTabletPlans;
  }

  private Object[][] constructColumns(int columnSize, int fetchSize, TSDataType dataType) {
    Object[][] columns = new Object[columnSize][1];
    for (int i = 0; i < columnSize; i++) {
      switch (dataType) {
        case DOUBLE:
          columns[i][0] = new double[fetchSize];
          break;
        case INT64:
          columns[i][0] = new long[fetchSize];
          break;
        case INT32:
          columns[i][0] = new int[fetchSize];
          break;
        case FLOAT:
          columns[i][0] = new float[fetchSize];
          break;
        default:
          break;
      }
    }
    return columns;
  }

  private void fillColumns(
      Object[][] columns,
      TSDataType dataType,
      RowRecord record,
      int[] rowNums,
      long[][] timestamps) {
    List<Field> fields = record.getFields();
    long ts = record.getTimestamp();

    for (int i = 0; i < columns.length; i++) {
      Field field = fields.get(i);
      if (field != null) {
        timestamps[i][rowNums[i]] = ts;
        switch (dataType) {
          case DOUBLE:
            ((double[]) columns[i][0])[rowNums[i]] = field.getDoubleV();
            break;
          case INT64:
            ((long[]) columns[i][0])[rowNums[i]] = field.getLongV();
            break;
          case INT32:
            ((int[]) columns[i][0])[rowNums[i]] = field.getIntV();
            break;
          case FLOAT:
            ((float[]) columns[i][0])[rowNums[i]] = field.getFloatV();
            break;
          default:
        }

        rowNums[i]++;
      }
    }
  }

  private List<PartialPath> generateTargetPaths(List<Path> rawPaths) throws IllegalPathException {
    List<PartialPath> targetPaths = new ArrayList<>(rawPaths.size());
    for (Path rawPath : rawPaths) {
      targetPaths.add(new PartialPath(fillTargetPathTemplate((PartialPath) rawPath)));
    }
    return targetPaths;
  }

  private String fillTargetPathTemplate(PartialPath rawPath) {
    String[] nodes = rawPath.getNodes();
    int indexOfLeftBracket = nodes[0].indexOf("(");
    if (indexOfLeftBracket != -1) {
      nodes[0] = nodes[0].substring(indexOfLeftBracket + 1);
    }
    int indexOfRightBracket = nodes[nodes.length - 1].indexOf(")");
    if (indexOfRightBracket != -1) {
      nodes[nodes.length - 1] = nodes[nodes.length - 1].substring(0, indexOfRightBracket);
    }
    StringBuffer sb = new StringBuffer();
    Matcher m = pattern.matcher(this.plan.getTargetPath().getFullPath());
    while (m.find()) {
      String param = m.group();
      String value = nodes[Integer.parseInt(param.substring(2, param.length() - 1).trim())];
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  public CreateContinuousQueryPlan getCreateContinuousQueryPlan() {
    return plan;
  }
}
