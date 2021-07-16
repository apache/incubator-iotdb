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

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.ZoneId;

import static junit.framework.TestCase.assertEquals;

public class ClusterUDTFQueryExecutorTest extends BaseQueryTest {

  private ClusterUDTFQueryExecutor executor;

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
  public void testWithoutValueFilterAlignByTime()
      throws QueryProcessException, StorageEngineException {
    ClusterPlanner processor = new ClusterPlanner();
    String sqlStr = "select sin(s0) from root.*";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr, ZoneId.systemDefault(), 1024);
    UDTFPlan udtfPlan = (UDTFPlan) plan;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));
    try {
      executor = new ClusterUDTFQueryExecutor(udtfPlan, testMetaMember);
      QueryDataSet queryDataSet = executor.executeWithoutValueFilterAlignByTime(context);
      checkSequentialDatasetWithMathFunction(queryDataSet, 0, 20, Math::sin);
    } catch (StorageEngineException | IOException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testWithValueFilterAlignByTime()
      throws IOException, StorageEngineException, QueryProcessException {
    ClusterPlanner processor = new ClusterPlanner();
    String sqlStr = "select sin(s0) from root.* where time >= 5";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr, ZoneId.systemDefault(), 1024);
    UDTFPlan udtfPlan = (UDTFPlan) plan;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, -1));
    try {
      executor = new ClusterUDTFQueryExecutor(udtfPlan, testMetaMember);
      QueryDataSet queryDataSet = executor.executeWithoutValueFilterAlignByTime(context);
      checkSequentialDatasetWithMathFunction(queryDataSet, 5, 15, Math::sin);
    } catch (QueryProcessException | InterruptedException e) {
      e.printStackTrace();
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  private interface MathFunctionProxy {
    double invoke(double x);
  }

  void checkSequentialDatasetWithMathFunction(
      QueryDataSet dataSet, int offset, int size, MathFunctionProxy functionProxy)
      throws IOException {
    for (int i = offset; i < offset + size; i++) {
      TestCase.assertTrue(dataSet.hasNext());
      RowRecord record = dataSet.next();
      assertEquals(i, record.getTimestamp());
      assertEquals(10, record.getFields().size());
      for (int j = 0; j < 10; j++) {
        assertEquals(
            functionProxy.invoke(i * 1.0), record.getFields().get(j).getDoubleV(), 0.00001);
      }
    }
    TestCase.assertFalse(dataSet.hasNext());
  }
}
