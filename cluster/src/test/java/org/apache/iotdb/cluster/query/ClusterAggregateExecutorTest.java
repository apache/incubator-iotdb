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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.aggregate.ClusterAggregateExecutor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterAggregateExecutorTest extends BaseQueryTest {

  private ClusterAggregateExecutor executor;

  @Test
  public void testNoFilter()
      throws QueryProcessException, StorageEngineException, IOException, IllegalPathException {
    AggregationPlan plan = new AggregationPlan();
    List<PartialPath> paths =
        Arrays.asList(
            new PartialPath(TestUtils.getTestSeries(0, 0)),
            new PartialPath(TestUtils.getTestSeries(0, 1)),
            new PartialPath(TestUtils.getTestSeries(0, 2)),
            new PartialPath(TestUtils.getTestSeries(0, 3)),
            new PartialPath(TestUtils.getTestSeries(0, 4)));
    List<TSDataType> dataTypes =
        Arrays.asList(
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE);
    List<String> aggregations =
        Arrays.asList(
            SQLConstant.MIN_TIME,
            SQLConstant.MAX_VALUE,
            SQLConstant.AVG,
            SQLConstant.COUNT,
            SQLConstant.SUM);
    plan.setPaths(paths);
    plan.setDeduplicatedPaths(paths);
    plan.setDataTypes(dataTypes);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setAggregations(aggregations);
    plan.setDeduplicatedAggregations(aggregations);

    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      executor = new ClusterAggregateExecutor(plan, testMetaMember);
      QueryDataSet queryDataSet = executor.executeWithoutValueFilter(context, plan);
      assertTrue(queryDataSet.hasNext());
      RowRecord record = queryDataSet.next();
      List<Field> fields = record.getFields();
      assertEquals(5, fields.size());
      Object[] answers = new Object[] {0.0, 19.0, 9.5, 20.0, 190.0};
      for (int i = 0; i < 5; i++) {
        assertEquals((double) answers[i], Double.parseDouble(fields.get(i).toString()), 0.00001);
      }
      assertFalse(queryDataSet.hasNext());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFilter()
      throws StorageEngineException, IOException, QueryProcessException, IllegalPathException {
    AggregationPlan plan = new AggregationPlan();
    List<PartialPath> paths =
        Arrays.asList(
            new PartialPath(TestUtils.getTestSeries(0, 0)),
            new PartialPath(TestUtils.getTestSeries(0, 1)),
            new PartialPath(TestUtils.getTestSeries(0, 2)),
            new PartialPath(TestUtils.getTestSeries(0, 3)),
            new PartialPath(TestUtils.getTestSeries(0, 4)));
    List<TSDataType> dataTypes =
        Arrays.asList(
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE,
            TSDataType.DOUBLE);
    List<String> aggregations =
        Arrays.asList(
            SQLConstant.MIN_TIME,
            SQLConstant.MAX_VALUE,
            SQLConstant.AVG,
            SQLConstant.COUNT,
            SQLConstant.SUM);
    plan.setPaths(paths);
    plan.setDeduplicatedPaths(paths);
    plan.setDataTypes(dataTypes);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setAggregations(aggregations);
    plan.setDeduplicatedAggregations(aggregations);
    plan.setExpression(
        BinaryExpression.and(
            new SingleSeriesExpression(
                new PartialPath(TestUtils.getTestSeries(0, 0)), ValueFilter.ltEq(8.0)),
            new SingleSeriesExpression(
                new PartialPath(TestUtils.getTestSeries(0, 0)), TimeFilter.gtEq(3))));

    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      executor = new ClusterAggregateExecutor(plan, testMetaMember);
      QueryDataSet queryDataSet = executor.executeWithValueFilter(context, plan);
      assertTrue(queryDataSet.hasNext());
      RowRecord record = queryDataSet.next();
      List<Field> fields = record.getFields();
      assertEquals(5, fields.size());
      Object[] answers = new Object[] {3.0, 8.0, 5.5, 6.0, 33.0};
      for (int i = 0; i < 5; i++) {
        assertEquals((double) answers[i], Double.parseDouble(fields.get(i).toString()), 0.00001);
      }
      assertFalse(queryDataSet.hasNext());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
