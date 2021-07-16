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
package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;

import org.antlr.v4.runtime.RecognitionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/** test the correctness of {@linkplain ConcatPathOptimizer ConcatPathOptimizer} */
public class ConcatOptimizerTest {

  private Planner processor;

  @Before
  public void before() throws MetadataException {
    processor = new Planner();
    IoTDB.metaManager.init();
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.laptop"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.laptop.d1.s1"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.laptop.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.laptop.d2.s1"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.laptop.d2.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.laptop.d3.s1"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.laptop.d3.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  @After
  public void after() throws IOException {
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testConcat1() throws QueryProcessException, RecognitionException {
    String inputSQL = "select s1 from root.laptop.d1";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
    assertEquals("root.laptop.d1.s1", plan.getPaths().get(0).getFullPath());
  }

  @Test
  public void testConcat2() throws QueryProcessException, RecognitionException {
    String inputSQL = "select s1 from root.laptop.*";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
    assertEquals("root.laptop.d1.s1", plan.getPaths().get(0).getFullPath());
    assertEquals("root.laptop.d2.s1", plan.getPaths().get(1).getFullPath());
    assertEquals("root.laptop.d3.s1", plan.getPaths().get(2).getFullPath());
  }

  @Test
  public void testConcat3()
      throws QueryProcessException, RecognitionException, IllegalPathException {
    String inputSQL = "select s1 from root.laptop.d1 where s1 < 10";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
    SingleSeriesExpression seriesExpression =
        new SingleSeriesExpression(new PartialPath("root.laptop.d1", "s1"), ValueFilter.lt(10));
    assertEquals(seriesExpression.toString(), ((RawDataQueryPlan) plan).getExpression().toString());
  }

  @Test
  public void testConcatMultipleDeviceInFilter()
      throws QueryProcessException, IllegalPathException {
    String inputSQL = "select s1 from root.laptop.* where s1 < 10";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(inputSQL);
    IExpression expression =
        BinaryExpression.and(
            BinaryExpression.and(
                new SingleSeriesExpression(
                    new PartialPath("root.laptop.d1", "s1"), ValueFilter.lt(10)),
                new SingleSeriesExpression(
                    new PartialPath("root.laptop.d2", "s1"), ValueFilter.lt(10))),
            new SingleSeriesExpression(
                new PartialPath("root.laptop.d3", "s1"), ValueFilter.lt(10)));
    assertEquals(expression.toString(), ((RawDataQueryPlan) plan).getExpression().toString());
  }
}
