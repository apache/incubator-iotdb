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

package org.apache.iotdb.db.query.executor;


import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.executor.fill.LastPointReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

public class LastQueryExecutor {

  private List<PartialPath> selectedSeries;
  private List<TSDataType> dataTypes;
  private IExpression expression;
  private static boolean lastCacheEnabled =
          IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled();

  public LastQueryExecutor(LastQueryPlan lastQueryPlan) {
    this.selectedSeries = lastQueryPlan.getDeduplicatedPaths();
    this.dataTypes = lastQueryPlan.getDeduplicatedDataTypes();
    this.expression = lastQueryPlan.getExpression();
  }

  public LastQueryExecutor(List<PartialPath> selectedSeries, List<TSDataType> dataTypes) {
    this.selectedSeries = selectedSeries;
    this.dataTypes = dataTypes;
  }

  /**
   * execute last function
   *
   * @param context query context
   */
  public QueryDataSet execute(QueryContext context, LastQueryPlan lastQueryPlan)
      throws StorageEngineException, IOException, QueryProcessException {

    ListDataSet dataSet = new ListDataSet(
        Arrays.asList(new PartialPath(COLUMN_TIMESERIES, false), new PartialPath(COLUMN_VALUE, false)),
        Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));

    for (int i = 0; i < selectedSeries.size(); i++) {
      TimeValuePair lastTimeValuePair;
      lastTimeValuePair = calculateLastPairForOneSeries(
              selectedSeries.get(i), dataTypes.get(i), context,
              lastQueryPlan.getAllMeasurementsInDevice(selectedSeries.get(i).getDevice()));
      if (lastTimeValuePair.getValue() != null) {
        RowRecord resultRecord = new RowRecord(lastTimeValuePair.getTimestamp());
        Field pathField = new Field(TSDataType.TEXT);
        if (selectedSeries.get(i).getTsAlias() != null) {
          pathField.setBinaryV(new Binary(selectedSeries.get(i).getTsAlias()));
        } else {
          if (selectedSeries.get(i).getMeasurementAlias() != null) {
            pathField.setBinaryV(new Binary(selectedSeries.get(i).getFullPathWithAlias()));
          } else {
            pathField.setBinaryV(new Binary(selectedSeries.get(i).getFullPath()));
          }
        }
        resultRecord.addField(pathField);

        Field valueField = new Field(TSDataType.TEXT);
        valueField.setBinaryV(new Binary(lastTimeValuePair.getValue().getStringValue()));
        resultRecord.addField(valueField);

        dataSet.putRecord(resultRecord);
      }
    }

    if (!lastQueryPlan.isAscending()) {
      dataSet.sortByTime();
    }
    return dataSet;
  }

  protected TimeValuePair calculateLastPairForOneSeries(
      PartialPath seriesPath, TSDataType tsDataType, QueryContext context, Set<String> deviceMeasurements)
      throws IOException, QueryProcessException, StorageEngineException {
    return calculateLastPairForOneSeriesLocally(seriesPath, tsDataType, context,
        expression, deviceMeasurements);
  }

  /**
   * get last result for one series
   *
   * @param context query context
   * @return TimeValuePair
   */
  public static TimeValuePair calculateLastPairForOneSeriesLocally(
      PartialPath seriesPath, TSDataType tsDataType, QueryContext context,
      IExpression expression, Set<String> deviceMeasurements)
      throws IOException, QueryProcessException, StorageEngineException {

    // Retrieve last value from MNode
    MeasurementMNode node = null;
    Filter filter = null;
    if (lastCacheEnabled) {
      if (expression != null) {
        filter = ((GlobalTimeExpression) expression).getFilter();
      }
      try {
        node = (MeasurementMNode) IoTDB.metaManager.getNodeByPath(seriesPath);
      } catch (MetadataException e) {
        TimeValuePair timeValuePair = IoTDB.metaManager.getLastCache(seriesPath);
        if (timeValuePair != null && satisfyFilter(filter, timeValuePair)) {
          return timeValuePair;
        }
      }

      if (node != null && node.getCachedLast() != null) {
        TimeValuePair timeValuePair =  node.getCachedLast();
        if (timeValuePair != null && satisfyFilter(filter, timeValuePair)) {
          return timeValuePair;
        }
      }
    }

    return calculateLastPairByScanningTsFiles(
        seriesPath, tsDataType, context, filter, deviceMeasurements, node);
  }

  private static TimeValuePair calculateLastPairByScanningTsFiles(
          PartialPath seriesPath, TSDataType tsDataType, QueryContext context,
          Filter filter, Set<String> deviceMeasurements, MeasurementMNode node)
      throws QueryProcessException, StorageEngineException, IOException {

    QueryDataSource dataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, filter);

    LastPointReader lastReader = new LastPointReader(
        seriesPath, tsDataType, deviceMeasurements, context, dataSource, Long.MAX_VALUE, filter);
    TimeValuePair resultPair = lastReader.readLastPoint();

    // Update cached last value with low priority unless "FROM" expression exists
    if (lastCacheEnabled && filter == null) {
      IoTDB.metaManager.updateLastCache(
          seriesPath, resultPair, false, Long.MIN_VALUE, node);
    }
    return resultPair;
  }

  private static boolean satisfyFilter(Filter filter, TimeValuePair tvPair) {
    if (filter == null ||
        filter.satisfy(tvPair.getTimestamp(), tvPair.getValue().getValue())) {
      return true;
    }
    return false;
  }
}
