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
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClusterDataQueryExecutorTest extends BaseQueryTest {

  private ClusterDataQueryExecutor queryExecutor;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testNoFilter() throws IOException, StorageEngineException {
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList);
    plan.setDeduplicatedDataTypes(dataTypes);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(context);
      checkSequentialDataset(dataSet, 0, 20);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFilter()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {
    IExpression expression =
        new SingleSeriesExpression(
            new PartialPath(TestUtils.getTestSeries(0, 0)), ValueFilter.gtEq(5.0));
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setExpression(expression);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithValueFilter(context);
      checkSequentialDataset(dataSet, 5, 15);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testNoFilterWithRedirect() throws StorageEngineException {
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setEnableRedirect(true);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(context);
      assertNull(dataSet.getEndPoint());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFilterWithValueFilterRedirect()
      throws StorageEngineException, QueryProcessException, IllegalPathException {
    IExpression expression =
        new SingleSeriesExpression(
            new PartialPath(TestUtils.getTestSeries(0, 0)), ValueFilter.gtEq(5.0));
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList);
    plan.setDeduplicatedDataTypes(dataTypes);
    plan.setExpression(expression);
    plan.setEnableRedirect(true);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithValueFilter(context);
      assertNull(dataSet.getEndPoint());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFilterWithTimeFilterRedirect()
      throws StorageEngineException, QueryProcessException {
    IExpression expression =
        new GlobalTimeExpression(new AndFilter(TimeFilter.gtEq(5), TimeFilter.ltEq(10)));
    RawDataQueryPlan plan = new RawDataQueryPlan();
    plan.setDeduplicatedPathsAndUpdate(pathList.subList(0, 1));
    plan.setDeduplicatedDataTypes(dataTypes.subList(0, 1));
    plan.setExpression(expression);
    plan.setEnableRedirect(true);
    queryExecutor = new ClusterDataQueryExecutor(plan, testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      QueryDataSet dataSet = queryExecutor.executeWithoutValueFilter(context);
      assertEquals("ip:port=0.0.0.0:6667", dataSet.getEndPoint().toString());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
