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

package org.apache.iotdb.cluster.query.groupby;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MergeGroupByExecutorTest extends BaseQueryTest {

  @Test
  public void testNoTimeFilter()
      throws QueryProcessException, IOException, IllegalPathException, StorageEngineException {
    PartialPath path = new PartialPath(TestUtils.getTestSeries(0, 0));
    TSDataType dataType = TSDataType.DOUBLE;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      Filter timeFilter = null;
      Set<String> deviceMeasurements = new HashSet<>();
      deviceMeasurements.add(path.getMeasurement());

      MergeGroupByExecutor groupByExecutor =
          new MergeGroupByExecutor(
              path, deviceMeasurements, dataType, context, timeFilter, testMetaMember, true);
      AggregationType[] types = AggregationType.values();
      for (AggregationType type : types) {
        groupByExecutor.addAggregateResult(
            AggregateResultFactory.getAggrResultByType(type, TSDataType.DOUBLE, true));
      }
      Object[] answers;
      List<AggregateResult> aggregateResults;

      answers = new Object[] {5.0, 2.0, 10.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0, 4.0};
      aggregateResults = groupByExecutor.calcResult(0, 5);
      checkAggregations(aggregateResults, answers);

      answers = new Object[] {5.0, 7.0, 35.0, 5.0, 9.0, 9.0, 5.0, 9.0, 5.0, 9.0};
      aggregateResults = groupByExecutor.calcResult(5, 10);
      checkAggregations(aggregateResults, answers);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testTimeFilter()
      throws QueryProcessException, IOException, IllegalPathException, StorageEngineException {
    PartialPath path = new PartialPath(TestUtils.getTestSeries(0, 0));
    TSDataType dataType = TSDataType.DOUBLE;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      Filter timeFilter = TimeFilter.gtEq(3);
      Set<String> deviceMeasurements = new HashSet<>();
      deviceMeasurements.add(path.getMeasurement());

      MergeGroupByExecutor groupByExecutor =
          new MergeGroupByExecutor(
              path, deviceMeasurements, dataType, context, timeFilter, testMetaMember, true);
      AggregationType[] types = AggregationType.values();
      for (AggregationType type : types) {
        groupByExecutor.addAggregateResult(
            AggregateResultFactory.getAggrResultByType(type, TSDataType.DOUBLE, true));
      }

      Object[] answers;
      List<AggregateResult> aggregateResults;
      answers = new Object[] {2.0, 3.5, 7.0, 3.0, 4.0, 4.0, 3.0, 4.0, 3.0, 4.0};
      aggregateResults = groupByExecutor.calcResult(0, 5);
      checkAggregations(aggregateResults, answers);

      answers = new Object[] {5.0, 7.0, 35.0, 5.0, 9.0, 9.0, 5.0, 9.0, 5.0, 9.0};
      aggregateResults = groupByExecutor.calcResult(5, 10);
      checkAggregations(aggregateResults, answers);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
