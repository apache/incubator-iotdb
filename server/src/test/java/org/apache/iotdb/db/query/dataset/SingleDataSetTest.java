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
package org.apache.iotdb.db.query.dataset;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SingleDataSetTest {
  private final IPlanExecutor queryExecutor = new PlanExecutor();
  private final Planner processor = new Planner();

  private final String[] sqls = {
      "SET STORAGE GROUP TO root.vehicle",
      "SET STORAGE GROUP TO root.test",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.test.d0.s1 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.test.d1.\"s3.xy\" WITH DATATYPE=TEXT, ENCODING=PLAIN"};

  public SingleDataSetTest() throws QueryProcessException {
  }

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    for (String sql : sqls) {
      queryExecutor.processNonQuery(processor.parseSQLToPhysicalPlan(sql));
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void countDevice()
      throws QueryProcessException, TException, StorageEngineException, QueryFilterOptimizationException, MetadataException, IOException, InterruptedException, SQLException, IllegalAccessException, IntrospectionException, InvocationTargetException {
    PhysicalPlan plan = processor
        .parseSQLToPhysicalPlan(
            "count devices");
    QueryDataSet dataSet = queryExecutor
        .processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof SingleDataSet);
    Assert.assertEquals("[devices]", dataSet.getPaths().toString());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals("0\t3", record.toString());
    }
  }

  @Test
  public void countTimeSeries()
      throws QueryProcessException, TException, StorageEngineException, QueryFilterOptimizationException, MetadataException, IOException, InterruptedException, SQLException, IllegalAccessException, IntrospectionException, InvocationTargetException {
    PhysicalPlan plan = processor
        .parseSQLToPhysicalPlan(
            "count TimeSeries");
    QueryDataSet dataSet = queryExecutor
        .processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof SingleDataSet);
    Assert.assertEquals("[count]", dataSet.getPaths().toString());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals("0\t5", record.toString());
    }
  }

  @Test
  public void countStorageGroup()
      throws TException, StorageEngineException, QueryFilterOptimizationException, MetadataException, IOException, InterruptedException, SQLException, QueryProcessException, IllegalAccessException, IntrospectionException, InvocationTargetException {
    PhysicalPlan plan = processor
        .parseSQLToPhysicalPlan(
            "count storage group");
    QueryDataSet dataSet = queryExecutor
        .processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof SingleDataSet);
    Assert.assertEquals("[storage group]", dataSet.getPaths().toString());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals("0\t2", record.toString());
    }
  }

  @Test
  public void countNodes()
      throws QueryProcessException, TException, StorageEngineException, QueryFilterOptimizationException, MetadataException, IOException, InterruptedException, SQLException, IllegalAccessException, IntrospectionException, InvocationTargetException {
    PhysicalPlan plan = processor
        .parseSQLToPhysicalPlan(
            "count nodes root.test level=2");
    QueryDataSet dataSet = queryExecutor
        .processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof SingleDataSet);
    Assert.assertEquals("[count]", dataSet.getPaths().toString());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals("0\t2", record.toString());
    }
  }

  @Test
  public void countClient()
      throws QueryProcessException, IOException, InvocationTargetException, QueryFilterOptimizationException, SQLException, InterruptedException, IllegalAccessException, MetadataException, StorageEngineException, TException {
    PhysicalPlan plan = processor
        .parseSQLToPhysicalPlan(
            "count clients");
    QueryDataSet dataSet = queryExecutor
        .processQuery(plan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertTrue(dataSet instanceof SingleDataSet);
    Assert.assertEquals("[count]", dataSet.getPaths().toString());
  }

}
