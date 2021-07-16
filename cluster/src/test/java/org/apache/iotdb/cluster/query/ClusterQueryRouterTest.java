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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;

public class ClusterQueryRouterTest extends BaseQueryTest {

  private ClusterQueryRouter clusterQueryRouter;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clusterQueryRouter = new ClusterQueryRouter(testMetaMember);
  }

  @Test
  public void test() throws StorageEngineException, IOException, QueryProcessException {
    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedPathsAndUpdate(pathList);
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));

    try {
      QueryDataSet dataSet = clusterQueryRouter.rawDataQuery(queryPlan, context);
      checkSequentialDataset(dataSet, 0, 20);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testAggregation()
      throws StorageEngineException, IOException, QueryProcessException,
          QueryFilterOptimizationException, IllegalPathException {
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
    plan.setDeduplicatedPathsAndUpdate(paths);
    plan.setDataTypes(dataTypes);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setAggregations(aggregations);
    plan.setDeduplicatedAggregations(aggregations);

    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));
    try {
      QueryDataSet queryDataSet = clusterQueryRouter.aggregate(plan, context);
      checkDoubleDataset(queryDataSet, new Object[] {0.0, 19.0, 9.5, 20.0, 190.0});
      assertFalse(queryDataSet.hasNext());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testPreviousFill()
      throws QueryProcessException, StorageEngineException, IOException, IllegalPathException {
    FillQueryPlan plan = new FillQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(
        Collections.singletonList(new PartialPath(TestUtils.getTestSeries(0, 10))));
    plan.setDeduplicatedDataTypes(Collections.singletonList(TSDataType.DOUBLE));
    plan.setPaths(plan.getDeduplicatedPaths());
    plan.setDataTypes(plan.getDeduplicatedDataTypes());
    long defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    Map<TSDataType, IFill> tsDataTypeIFillMap =
        Collections.singletonMap(
            TSDataType.DOUBLE, new PreviousFill(TSDataType.DOUBLE, 0, defaultFillInterval));
    plan.setFillType(tsDataTypeIFillMap);
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));

    try {
      QueryDataSet queryDataSet;
      long[] queryTimes = new long[] {-1, 0, 5, 10, 20};
      Object[][] answers =
          new Object[][] {
            new Object[] {null},
            new Object[] {0.0},
            new Object[] {0.0},
            new Object[] {10.0},
            new Object[] {10.0},
          };
      for (int i = 0; i < queryTimes.length; i++) {
        plan.setQueryTime(queryTimes[i]);
        queryDataSet = clusterQueryRouter.fill(plan, context);
        checkDoubleDataset(queryDataSet, answers[i]);
        assertFalse(queryDataSet.hasNext());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testLinearFill()
      throws QueryProcessException, StorageEngineException, IOException, IllegalPathException {
    FillQueryPlan plan = new FillQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(
        Collections.singletonList(new PartialPath(TestUtils.getTestSeries(0, 10))));
    plan.setDeduplicatedDataTypes(Collections.singletonList(TSDataType.DOUBLE));
    plan.setPaths(plan.getDeduplicatedPaths());
    plan.setDataTypes(plan.getDeduplicatedDataTypes());
    long defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    Map<TSDataType, IFill> tsDataTypeIFillMap =
        Collections.singletonMap(
            TSDataType.DOUBLE,
            new LinearFill(TSDataType.DOUBLE, 0, defaultFillInterval, defaultFillInterval));
    plan.setFillType(tsDataTypeIFillMap);

    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));

    try {
      QueryDataSet queryDataSet;
      long[] queryTimes = new long[] {-1, 0, 5, 10, 20};
      Object[][] answers =
          new Object[][] {
            new Object[] {null},
            new Object[] {0.0},
            new Object[] {5.0},
            new Object[] {10.0},
            new Object[] {null},
          };
      for (int i = 0; i < queryTimes.length; i++) {
        plan.setQueryTime(queryTimes[i]);
        queryDataSet = clusterQueryRouter.fill(plan, context);
        checkDoubleDataset(queryDataSet, answers[i]);
        assertFalse(queryDataSet.hasNext());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testVFilterGroupBy()
      throws IOException, StorageEngineException, QueryFilterOptimizationException,
          QueryProcessException, IllegalPathException {
    QueryContext queryContext =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));
    try {
      GroupByTimePlan groupByPlan = new GroupByTimePlan();
      List<PartialPath> pathList = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      List<String> aggregations = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        pathList.add(new PartialPath(TestUtils.getTestSeries(i, 0)));
        dataTypes.add(TSDataType.DOUBLE);
        aggregations.add(SQLConstant.COUNT);
      }
      groupByPlan.setPaths(pathList);
      groupByPlan.setDeduplicatedPathsAndUpdate(pathList);
      groupByPlan.setDataTypes(dataTypes);
      groupByPlan.setDeduplicatedDataTypes(dataTypes);
      groupByPlan.setAggregations(aggregations);
      groupByPlan.setDeduplicatedAggregations(aggregations);

      groupByPlan.setStartTime(0);
      groupByPlan.setEndTime(20);
      groupByPlan.setSlidingStep(5);
      groupByPlan.setInterval(5);

      IExpression expression =
          BinaryExpression.and(
              new SingleSeriesExpression(
                  new PartialPath(TestUtils.getTestSeries(0, 0)), ValueFilter.gtEq(5.0)),
              new SingleSeriesExpression(
                  new PartialPath(TestUtils.getTestSeries(5, 0)), TimeFilter.ltEq(15)));
      groupByPlan.setExpression(expression);
      QueryDataSet queryDataSet = clusterQueryRouter.groupBy(groupByPlan, queryContext);

      Object[][] answers =
          new Object[][] {
            new Object[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0},
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
            new Object[] {1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
          };
      for (Object[] answer : answers) {
        checkDoubleDataset(queryDataSet, answer);
      }
      assertFalse(queryDataSet.hasNext());
    } finally {
      QueryResourceManager.getInstance().endQuery(queryContext.getQueryId());
    }
  }

  @Test
  public void testNoVFilterGroupBy()
      throws StorageEngineException, IOException, QueryFilterOptimizationException,
          QueryProcessException, IllegalPathException {
    QueryContext queryContext =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));
    try {
      GroupByTimePlan groupByPlan = new GroupByTimePlan();
      List<PartialPath> pathList = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      List<String> aggregations = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        pathList.add(new PartialPath(TestUtils.getTestSeries(i, 0)));
        dataTypes.add(TSDataType.DOUBLE);
        aggregations.add(SQLConstant.COUNT);
      }
      groupByPlan.setPaths(pathList);
      groupByPlan.setDeduplicatedPathsAndUpdate(pathList);
      groupByPlan.setDataTypes(dataTypes);
      groupByPlan.setDeduplicatedDataTypes(dataTypes);
      groupByPlan.setAggregations(aggregations);
      groupByPlan.setDeduplicatedAggregations(aggregations);

      groupByPlan.setStartTime(0);
      groupByPlan.setEndTime(20);
      groupByPlan.setSlidingStep(5);
      groupByPlan.setInterval(5);

      QueryDataSet dataSet = clusterQueryRouter.groupBy(groupByPlan, queryContext);

      Object[][] answers =
          new Object[][] {
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
          };
      for (Object[] answer : answers) {
        checkDoubleDataset(dataSet, answer);
      }
      assertFalse(dataSet.hasNext());
    } finally {
      QueryResourceManager.getInstance().endQuery(queryContext.getQueryId());
    }
  }

  @Test
  public void testUDTFQuery() throws QueryProcessException, StorageEngineException {
    ClusterPlanner processor = new ClusterPlanner();
    String sqlStr = "select sin(s0) from root.*";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr, ZoneId.systemDefault(), 1024);
    UDTFPlan udtfPlan = (UDTFPlan) plan;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));
    try {
      QueryDataSet queryDataSet = clusterQueryRouter.udtfQuery(udtfPlan, context);
      for (int i = 0; i < 20; i++) {
        TestCase.assertTrue(queryDataSet.hasNext());
        RowRecord record = queryDataSet.next();
        assertEquals(i, record.getTimestamp());
        assertEquals(10, record.getFields().size());
        for (int j = 0; j < 10; j++) {
          assertEquals(Math.sin(i * 1.0), record.getFields().get(j).getDoubleV(), 0.00001);
        }
      }
      TestCase.assertFalse(queryDataSet.hasNext());
    } catch (StorageEngineException | IOException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
