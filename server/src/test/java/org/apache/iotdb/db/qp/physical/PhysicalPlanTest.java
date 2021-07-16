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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.DropFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowContinuousQueriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowFunctionsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.physical.sys.ShowTriggersPlan;
import org.apache.iotdb.db.qp.physical.sys.StartTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StopTriggerPlan;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PhysicalPlanTest {

  private final Planner processor = new Planner();

  @Before
  public void before() throws MetadataException {
    EnvironmentUtils.envSetUp();
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d2.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d3.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d4.s1"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  @After
  public void clean() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testMetadata() throws QueryProcessException {
    String metadata = "create timeseries root.vehicle.d1.s2 with datatype=INT32,encoding=RLE";
    Planner processor = new Planner();
    CreateTimeSeriesPlan plan = (CreateTimeSeriesPlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        "seriesPath: PartialPath{fullPath=root.vehicle.d1.s2, majorVersion=0, minorVersion=0}, resultDataType: INT32, encoding: RLE, compression: SNAPPY, tagOffset: -1",
        plan.toString());
  }

  @Test
  public void testMetadata2() throws QueryProcessException {
    String metadata = "create timeseries root.vehicle.d1.s2 with datatype=int32,encoding=rle";
    Planner processor = new Planner();
    CreateTimeSeriesPlan plan = (CreateTimeSeriesPlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        "seriesPath: PartialPath{fullPath=root.vehicle.d1.s2, majorVersion=0, minorVersion=0}, resultDataType: INT32, encoding: RLE, compression: SNAPPY, tagOffset: -1",
        plan.toString());
  }

  @Test
  public void testMetadata3() throws QueryProcessException {
    String metadata =
        "create timeseries root.vehicle.d1.s2(温度) with datatype=int32,encoding=rle, compression=SNAPPY "
            + "tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)";
    System.out.println(metadata.length());
    Planner processor = new Planner();
    CreateTimeSeriesPlan plan = (CreateTimeSeriesPlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        "seriesPath: PartialPath{fullPath=root.vehicle.d1.s2, majorVersion=0, minorVersion=0}, resultDataType: INT32, encoding: RLE, compression: SNAPPY, tagOffset: -1",
        plan.toString());
  }

  // TODO @Steve SU
  //  @Test
  //  public void testMetadata4() throws QueryProcessException {
  //    String metadata =
  //        "create aligned timeseries root.vehicle.d1.(s1 INT32, s2 FLOAT) with encoding=(RLE, RLE)
  // compression=SNAPPY";
  //    Planner processor = new Planner();
  //    CreateAlignedTimeSeriesPlan plan =
  //        (CreateAlignedTimeSeriesPlan) processor.parseSQLToPhysicalPlan(metadata);
  //    assertEquals(
  //        "devicePath: root.vehicle.d1, measurements: [s1, s2], dataTypes: [INT32, FLOAT],
  // encoding: [RLE, RLE], compression: SNAPPY",
  //        plan.toString());
  //  }

  @Test
  public void testAuthor() throws QueryProcessException {
    String sql =
        "grant role xm privileges 'SET_STORAGE_GROUP','DELETE_TIMESERIES' on root.vehicle.d1.s1";
    Planner processor = new Planner();
    AuthorPlan plan = (AuthorPlan) processor.parseSQLToPhysicalPlan(sql);
    assertEquals(
        "userName: null\n"
            + "roleName: xm\n"
            + "password: null\n"
            + "newPassword: null\n"
            + "permissions: [0, 5]\n"
            + "nodeName: PartialPath{fullPath=root.vehicle.d1.s1, majorVersion=0, minorVersion=0}\n"
            + "authorType: GRANT_ROLE",
        plan.toString());
  }

  @Test
  public void testAggregation() throws QueryProcessException {
    String sqlStr =
        "select sum(d1.s1) "
            + "from root.vehicle "
            + "where time <= 51 or !(time != 100 and time < 460)";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    if (!plan.isQuery()) {
      fail();
    }
    AggregationPlan mergePlan = (AggregationPlan) plan;
    assertEquals("sum", mergePlan.getAggregations().get(0));
  }

  @Test
  public void testGroupBy1() throws QueryProcessException {
    String sqlStr =
        "select count(s1) "
            + "from root.vehicle.d1 "
            + "where s1 < 20 and time <= now() "
            + "group by([8,737), 3ms)";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    if (!plan.isQuery()) {
      fail();
    }
    GroupByTimePlan mergePlan = (GroupByTimePlan) plan;
    assertEquals(3L, mergePlan.getInterval());
    assertEquals(3L, mergePlan.getSlidingStep());
    assertEquals(8L, mergePlan.getStartTime());
    assertEquals(737L, mergePlan.getEndTime());
  }

  @Test
  public void testGroupBy2() throws QueryProcessException {
    String sqlStr =
        "select count(s1) "
            + "from root.vehicle.d1 "
            + "where s1 < 20 and time <= now() "
            + "group by([123,2017-6-2T12:00:12+07:00), 111ms)";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    if (!plan.isQuery()) {
      fail();
    }
    GroupByTimePlan mergePlan = (GroupByTimePlan) plan;
    assertEquals(111, mergePlan.getInterval());
  }

  @Test
  public void testGroupBy3() throws QueryProcessException {
    String sqlStr =
        "select count(s1) "
            + "from root.vehicle.d1 "
            + "where s1 < 20 and time <= now() "
            + "group by([2017-6-2T12:00:12+07:00,2017-6-12T12:00:12+07:00), 3h, 24h)";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    if (!plan.isQuery()) {
      fail();
    }
    GroupByTimePlan mergePlan = (GroupByTimePlan) plan;
    assertEquals(3 * 60 * 60 * 1000, mergePlan.getInterval());
    assertEquals(24 * 60 * 60 * 1000, mergePlan.getSlidingStep());
    assertEquals(1496379612000L, mergePlan.getStartTime());
    assertEquals(1497243612000L, mergePlan.getEndTime());
  }

  @Test
  public void testFill1() throws QueryProcessException {
    String sqlStr =
        "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear, 5m, 5m], boolean[previous, 5m])";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    if (!plan.isQuery()) {
      fail();
    }
    FillQueryPlan mergePlan = (FillQueryPlan) plan;
    assertEquals(5000, mergePlan.getQueryTime());
    assertEquals(
        300000, ((LinearFill) mergePlan.getFillType().get(TSDataType.INT32)).getBeforeRange());
    assertEquals(
        300000, ((LinearFill) mergePlan.getFillType().get(TSDataType.INT32)).getAfterRange());
    assertEquals(
        300000, ((PreviousFill) mergePlan.getFillType().get(TSDataType.BOOLEAN)).getBeforeRange());
  }

  @Test
  public void testFill2() throws QueryProcessException {
    String sqlStr =
        "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear], boolean[previous])";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    if (!plan.isQuery()) {
      fail();
    }
    int defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
    FillQueryPlan mergePlan = (FillQueryPlan) plan;
    assertEquals(5000, mergePlan.getQueryTime());
    assertEquals(
        defaultFillInterval,
        ((LinearFill) mergePlan.getFillType().get(TSDataType.INT32)).getBeforeRange());
    assertEquals(
        defaultFillInterval,
        ((LinearFill) mergePlan.getFillType().get(TSDataType.INT32)).getAfterRange());
    assertEquals(
        defaultFillInterval,
        ((PreviousFill) mergePlan.getFillType().get(TSDataType.BOOLEAN)).getBeforeRange());
  }

  @Test
  public void testFill3() {
    String sqlStr =
        "SELECT s1 FROM root.vehicle.d1 WHERE time = 5000 Fill(int32[linear, 5m], boolean[previous])";
    try {
      processor.parseSQLToPhysicalPlan(sqlStr);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void testFill4() {
    String sqlStr =
        "SELECT s1 FROM root.vehicle.d1 WHERE time > 5000 Fill(int32[linear], boolean[previous])";
    try {
      processor.parseSQLToPhysicalPlan(sqlStr);
      fail();
    } catch (Exception e) {
      assertEquals("Only \"=\" can be used in fill function", e.getMessage());
    }
  }

  @Test
  public void testGroupByFill1() {
    String sqlStr =
        "select last_value(s1) "
            + " from root.vehicle.d1 "
            + "group by([8,737), 3ms) fill(int32[previous])";
    try {
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
      if (!plan.isQuery()) {
        fail();
      }
      if (!(plan instanceof GroupByTimeFillPlan)) {
        fail();
      }
      GroupByTimeFillPlan groupByFillPlan = (GroupByTimeFillPlan) plan;
      assertEquals(3L, groupByFillPlan.getInterval());
      assertEquals(3L, groupByFillPlan.getSlidingStep());
      assertEquals(8L, groupByFillPlan.getStartTime());
      assertEquals(737L, groupByFillPlan.getEndTime());
      assertEquals(1, groupByFillPlan.getFillType().size());
      assertTrue(groupByFillPlan.getFillType().containsKey(TSDataType.INT32));
      assertTrue(groupByFillPlan.getFillType().get(TSDataType.INT32) instanceof PreviousFill);
      PreviousFill previousFill =
          (PreviousFill) groupByFillPlan.getFillType().get(TSDataType.INT32);
      assertFalse(previousFill.isUntilLast());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGroupByFill2() {
    String sqlStr =
        "select last_value(s1) "
            + " from root.vehicle.d1 "
            + "group by([8,737), 3ms) fill(ALL[previousuntillast])";
    try {
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
      if (!plan.isQuery()) {
        fail();
      }
      if (!(plan instanceof GroupByTimeFillPlan)) {
        fail();
      }
      GroupByTimeFillPlan groupByFillPlan = (GroupByTimeFillPlan) plan;
      assertEquals(3L, groupByFillPlan.getInterval());
      assertEquals(3L, groupByFillPlan.getSlidingStep());
      assertEquals(8L, groupByFillPlan.getStartTime());
      assertEquals(737L, groupByFillPlan.getEndTime());
      assertEquals(TSDataType.values().length, groupByFillPlan.getFillType().size());
      for (TSDataType tsDataType : TSDataType.values()) {
        assertTrue(groupByFillPlan.getFillType().containsKey(tsDataType));
        assertTrue(groupByFillPlan.getFillType().get(tsDataType) instanceof PreviousFill);
        PreviousFill previousFill = (PreviousFill) groupByFillPlan.getFillType().get(tsDataType);
        assertTrue(previousFill.isUntilLast());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGroupByFill3() {
    String sqlStr =
        "select last_value(d1.s1), last_value(d2.s1)"
            + " from root.vehicle "
            + "group by([8,737), 3ms) fill(int32[previousuntillast], int64[previous])";
    try {
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
      if (!plan.isQuery()) {
        fail();
      }
      if (!(plan instanceof GroupByTimeFillPlan)) {
        fail();
      }
      GroupByTimeFillPlan groupByFillPlan = (GroupByTimeFillPlan) plan;
      assertEquals(3L, groupByFillPlan.getInterval());
      assertEquals(3L, groupByFillPlan.getSlidingStep());
      assertEquals(8L, groupByFillPlan.getStartTime());
      assertEquals(737L, groupByFillPlan.getEndTime());
      assertEquals(2, groupByFillPlan.getDeduplicatedPaths().size());
      assertEquals(2, groupByFillPlan.getFillType().size());

      assertTrue(groupByFillPlan.getFillType().containsKey(TSDataType.INT32));
      assertTrue(groupByFillPlan.getFillType().get(TSDataType.INT32) instanceof PreviousFill);
      PreviousFill previousFill =
          (PreviousFill) groupByFillPlan.getFillType().get(TSDataType.INT32);
      assertTrue(previousFill.isUntilLast());

      assertTrue(groupByFillPlan.getFillType().containsKey(TSDataType.INT64));
      assertTrue(groupByFillPlan.getFillType().get(TSDataType.INT64) instanceof PreviousFill);
      previousFill = (PreviousFill) groupByFillPlan.getFillType().get(TSDataType.INT64);
      assertFalse(previousFill.isUntilLast());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGroupByFill4() {
    String sqlStr =
        "select last_value(d1.s1), last_value(d2.s1)"
            + " from root.vehicle "
            + "group by([8,737), 3ms) fill(int32[linear])";
    try {
      processor.parseSQLToPhysicalPlan(sqlStr);
      fail();
    } catch (SQLParserException e) {
      assertEquals("group by fill doesn't support linear fill", e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGroupByFill5() {
    String sqlStr =
        "select last_value(d1.s1), count(d2.s1)"
            + " from root.vehicle "
            + "group by([8,737), 3ms) fill(int32[previous])";
    try {
      processor.parseSQLToPhysicalPlan(sqlStr);
      fail();
    } catch (QueryProcessException e) {
      assertEquals("Group By Fill only support last_value function", e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGroupByFill6() {
    String sqlStr =
        "select count(s1)"
            + "from root.vehicle.d1 "
            + "group by([8,737), 3ms, 5ms) fill(int32[previous])";
    try {
      processor.parseSQLToPhysicalPlan(sqlStr);
      fail();
    } catch (ParseCancellationException e) {
      assertTrue(e.getMessage().contains("mismatched input 'fill'"));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testGroupByFill7() {
    String sqlStr =
        "select last_value(d1.s1), last_value(d2.s1)"
            + " from root.vehicle "
            + "group by([8,737), 3ms) fill(int32[previousuntillast,10ms], int64[previous,10ms])";
    try {
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
      if (!plan.isQuery()) {
        fail();
      }
      if (!(plan instanceof GroupByTimeFillPlan)) {
        fail();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCreateFunctionPlan1() { // create function
    try {
      PhysicalPlan plan =
          processor.parseSQLToPhysicalPlan(
              "create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      if (plan.isQuery() || !(plan instanceof CreateFunctionPlan)) {
        fail();
      }
      CreateFunctionPlan createFunctionPlan = (CreateFunctionPlan) plan;
      assertEquals("udf", createFunctionPlan.getUdfName());
      assertEquals(
          "org.apache.iotdb.db.query.udf.example.Adder", createFunctionPlan.getClassName());
      assertFalse(createFunctionPlan.isTemporary());
    } catch (QueryProcessException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testCreateFunctionPlan2() { // create temporary function
    try {
      PhysicalPlan plan =
          processor.parseSQLToPhysicalPlan(
              "create temporary function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      if (plan.isQuery() || !(plan instanceof CreateFunctionPlan)) {
        fail();
      }
      CreateFunctionPlan createFunctionPlan = (CreateFunctionPlan) plan;
      assertEquals("udf", createFunctionPlan.getUdfName());
      assertEquals(
          "org.apache.iotdb.db.query.udf.example.Adder", createFunctionPlan.getClassName());
      assertTrue(createFunctionPlan.isTemporary());
    } catch (QueryProcessException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testUDTFQuery1() {
    try {
      CreateFunctionPlan createFunctionPlan =
          (CreateFunctionPlan)
              processor.parseSQLToPhysicalPlan(
                  "create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      UDFRegistrationService.getInstance()
          .register(
              createFunctionPlan.getUdfName(),
              createFunctionPlan.getClassName(),
              createFunctionPlan.isTemporary(),
              true);

      String sqlStr =
          "select udf(d2.s1, d1.s1), udf(d1.s1, d2.s1), d1.s1, d2.s1, udf(d1.s1, d2.s1), udf(d2.s1, d1.s1), d1.s1, d2.s1 from root.vehicle";
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);

      UDFRegistrationService.getInstance().deregister(createFunctionPlan.getUdfName());

      if (!(plan instanceof UDTFPlan)) {
        fail();
      }

      UDTFPlan udtfPlan = (UDTFPlan) plan;

      assertTrue(udtfPlan.isAlignByTime());

      assertEquals(8, udtfPlan.getPaths().size());
      assertEquals(8, udtfPlan.getDataTypes().size());

      assertEquals(2, udtfPlan.getDeduplicatedPaths().size());
      assertEquals(2, udtfPlan.getDeduplicatedDataTypes().size());

      assertEquals(4, udtfPlan.getPathToIndex().size());
      assertTrue(
          udtfPlan.getPathToIndex().containsKey("udf(root.vehicle.d2.s1, root.vehicle.d1.s1)"));
      assertTrue(
          udtfPlan.getPathToIndex().containsKey("udf(root.vehicle.d1.s1, root.vehicle.d2.s1)"));
      assertTrue(udtfPlan.getPathToIndex().containsKey("root.vehicle.d1.s1"));
      assertTrue(udtfPlan.getPathToIndex().containsKey("root.vehicle.d2.s1"));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  @Test
  public void testUDTFQuery2() {
    try {
      CreateFunctionPlan createFunctionPlan =
          (CreateFunctionPlan)
              processor.parseSQLToPhysicalPlan(
                  "create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      UDFRegistrationService.getInstance()
          .register(
              createFunctionPlan.getUdfName(),
              createFunctionPlan.getClassName(),
              createFunctionPlan.isTemporary(),
              true);

      String sqlStr =
          "select udf(d2.s1, d1.s1, \"addend\"=\"100\"), udf(d1.s1, d2.s1), d1.s1, d2.s1, udf(d2.s1, d1.s1) from root.vehicle";
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);

      UDFRegistrationService.getInstance().deregister(createFunctionPlan.getUdfName());
      if (!(plan instanceof UDTFPlan)) {
        fail();
      }

      UDTFPlan udtfPlan = (UDTFPlan) plan;

      assertTrue(udtfPlan.isAlignByTime());

      assertEquals(5, udtfPlan.getPaths().size());
      assertEquals(5, udtfPlan.getDataTypes().size());

      assertEquals(2, udtfPlan.getDeduplicatedPaths().size());
      assertEquals(2, udtfPlan.getDeduplicatedDataTypes().size());

      assertEquals(5, udtfPlan.getPathToIndex().size());
      assertTrue(
          udtfPlan
              .getPathToIndex()
              .containsKey("udf(root.vehicle.d2.s1, root.vehicle.d1.s1, \"addend\"=\"100\")"));
      assertTrue(
          udtfPlan.getPathToIndex().containsKey("udf(root.vehicle.d2.s1, root.vehicle.d1.s1)"));
      assertTrue(
          udtfPlan.getPathToIndex().containsKey("udf(root.vehicle.d1.s1, root.vehicle.d2.s1)"));
      assertTrue(udtfPlan.getPathToIndex().containsKey("root.vehicle.d1.s1"));
      assertTrue(udtfPlan.getPathToIndex().containsKey("root.vehicle.d2.s1"));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  @Test
  public void testUDTFQuery3() {
    try {
      CreateFunctionPlan createFunctionPlan =
          (CreateFunctionPlan)
              processor.parseSQLToPhysicalPlan(
                  "create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      UDFRegistrationService.getInstance()
          .register(
              createFunctionPlan.getUdfName(),
              createFunctionPlan.getClassName(),
              createFunctionPlan.isTemporary(),
              true);

      String sqlStr = "select *, udf(*, *), *, udf(*, *), * from root.vehicle";
      PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);

      UDFRegistrationService.getInstance().deregister(createFunctionPlan.getUdfName());
      if (!(plan instanceof UDTFPlan)) {
        fail();
      }

      UDTFPlan udtfPlan = (UDTFPlan) plan;

      assertTrue(udtfPlan.isAlignByTime());

      assertEquals(44, udtfPlan.getPaths().size());
      assertEquals(44, udtfPlan.getDataTypes().size());

      assertEquals(4, udtfPlan.getDeduplicatedPaths().size());
      assertEquals(4, udtfPlan.getDeduplicatedDataTypes().size());

      assertEquals(20, udtfPlan.getPathToIndex().size());
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  @Test
  public void testDropFunctionPlan() { // drop function
    try {
      DropFunctionPlan dropFunctionPlan =
          (DropFunctionPlan) processor.parseSQLToPhysicalPlan("drop function udf");
      assertEquals("udf", dropFunctionPlan.getUdfName());
    } catch (QueryProcessException e) {
      fail(e.toString());
    }
  }

  @Test
  public void testQuery1() throws QueryProcessException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 5000";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect = new GlobalTimeExpression(TimeFilter.gt(5000L));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQuery2() throws QueryProcessException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 50 and time <= 100";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new GlobalTimeExpression(FilterFactory.and(TimeFilter.gt(50L), TimeFilter.ltEq(100L)));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQuery3() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 50 and time <= 100 or s1 < 10";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new GlobalTimeExpression(FilterFactory.and(TimeFilter.gt(50L), TimeFilter.ltEq(100L)));
    expect =
        BinaryExpression.or(
            expect,
            new SingleSeriesExpression(
                new PartialPath("root.vehicle.d1", "s1"), ValueFilter.lt(10.0)));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQuery4() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 50 and time <= 100 and s1 < 10";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();

    IExpression expect =
        BinaryExpression.and(
            new SingleSeriesExpression(
                new PartialPath("root.vehicle.d1", "s1"), ValueFilter.lt(10.0)),
            new GlobalTimeExpression(FilterFactory.and(TimeFilter.gt(50L), TimeFilter.ltEq(100L))));

    assertEquals(expect.toString(), queryFilter.toString());

    PartialPath path = new PartialPath("root.vehicle.d1.s1");
    assertEquals(path, plan.getPaths().get(0));
  }

  @Test
  public void testQuery5() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 20 or s1 < 10";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"),
            FilterFactory.or(ValueFilter.gt(20.0), ValueFilter.lt(10.0)));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQuery6() throws QueryProcessException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE time > 20 or time < 10";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new GlobalTimeExpression(FilterFactory.or(TimeFilter.gt(20L), TimeFilter.lt(10L)));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQuery7() throws QueryProcessException {
    String sqlStr =
        "SELECT s1 FROM root.vehicle.d1 WHERE time > 2019-10-16 10:59:00+08:00 - 1d5h or time < 10";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new GlobalTimeExpression(
            FilterFactory.or(TimeFilter.gt(1571090340000L), TimeFilter.lt(10L)));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testLimitOffset() throws QueryProcessException {
    String sqlStr =
        "SELECT s1 FROM root.vehicle.d1,root.vehicle.d2 WHERE time < 10 "
            + "limit 100 offset 10 slimit 1 soffset 1";
    QueryPlan plan = (QueryPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals(100, plan.getRowLimit());
    assertEquals(10, plan.getRowOffset());
    // NOTE that the parameters of the SLIMIT clause is not stored in the physicalPlan,
    // because the SLIMIT clause takes effect before the physicalPlan is finally generated.
  }

  @Test
  public void testOffsetLimit() throws QueryProcessException {
    String sqlStr =
        "SELECT s1 FROM root.vehicle.d1,root.vehicle.d2 WHERE time < 10 "
            + "offset 10 limit 100 soffset 1 slimit 1";
    QueryPlan plan = (QueryPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals(100, plan.getRowLimit());
    assertEquals(10, plan.getRowOffset());
    // NOTE that the parameters of the SLIMIT clause is not stored in the physicalPlan,
    // because the SLIMIT clause takes effect before the physicalPlan is finally generated.
  }

  @Test
  public void testQueryFloat1() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 20.5e3";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(20.5e3));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat2() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 20.5E-3";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(20.5e-3));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat3() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 2.5";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(2.5));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat4() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 2.5";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(2.5));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat5() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > -2.5";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(-2.5));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat6() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > -2.5E-1";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(-2.5e-1));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat7() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 2.5E2";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(2.5e+2));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat8() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > .2e2";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(0.2e+2));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat9() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > .2";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(0.2));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat10() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 2.";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(2.0));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat11() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > 2.";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(2.0));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat12() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > -2.";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(-2.0));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat13() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > -.2";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(-0.2));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testQueryFloat14() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 > -.2e2";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    IExpression expect =
        new SingleSeriesExpression(new PartialPath("root.vehicle.d1", "s1"), ValueFilter.gt(-20.0));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testInOperator() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 in (25, 30, 40)";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    Set<Float> values = new HashSet<>();
    values.add(25.0f);
    values.add(30.0f);
    values.add(40.0f);
    IExpression expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"), ValueFilter.in(values, false));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testNotInOperator() throws QueryProcessException, IllegalPathException {
    String sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE s1 not in (25, 30, 40)";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    IExpression queryFilter = ((RawDataQueryPlan) plan).getExpression();
    Set<Float> values = new HashSet<>();
    values.add(25.0f);
    values.add(30.0f);
    values.add(40.0f);
    IExpression expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"), ValueFilter.in(values, true));
    assertEquals(expect.toString(), queryFilter.toString());

    sqlStr = "SELECT s1 FROM root.vehicle.d1 WHERE not(s1 not in (25, 30, 40))";
    plan = processor.parseSQLToPhysicalPlan(sqlStr);
    queryFilter = ((RawDataQueryPlan) plan).getExpression();
    expect =
        new SingleSeriesExpression(
            new PartialPath("root.vehicle.d1", "s1"), ValueFilter.in(values, false));
    assertEquals(expect.toString(), queryFilter.toString());
  }

  @Test
  public void testGrantWatermarkEmbedding() throws QueryProcessException {
    String sqlStr = "GRANT WATERMARK_EMBEDDING to a,b";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    DataAuthPlan dataAuthPlan = (DataAuthPlan) plan;
    Assert.assertEquals(2, dataAuthPlan.getUsers().size());
    Assert.assertEquals(OperatorType.GRANT_WATERMARK_EMBEDDING, dataAuthPlan.getOperatorType());
  }

  @Test
  public void testRevokeWatermarkEmbedding() throws QueryProcessException {
    String sqlStr = "REVOKE WATERMARK_EMBEDDING from a,b";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    DataAuthPlan dataAuthPlan = (DataAuthPlan) plan;
    Assert.assertEquals(2, dataAuthPlan.getUsers().size());
    Assert.assertEquals(OperatorType.REVOKE_WATERMARK_EMBEDDING, dataAuthPlan.getOperatorType());
  }

  @Test
  public void testConfiguration() throws QueryProcessException {
    String metadata = "load configuration";
    Planner processor = new Planner();
    LoadConfigurationPlan plan = (LoadConfigurationPlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals("LOAD_CONFIGURATION", plan.toString());
  }

  @Test
  public void testShowFlushInfo() throws QueryProcessException {
    String metadata = "show flush task info";
    Planner processor = new Planner();
    ShowPlan plan = (ShowPlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals("SHOW FLUSH_TASK_INFO", plan.toString());
  }

  @Test
  public void testLoadFiles() throws QueryProcessException {
    String filePath = "data" + File.separator + "213213441243-1-2.tsfile";
    String metadata = String.format("load \"%s\"", filePath);
    Planner processor = new Planner();
    OperateFilePlan plan = (OperateFilePlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        String.format(
            "OperateFilePlan{file=%s, targetDir=null, autoCreateSchema=true, sgLevel=1, operatorType=LOAD_FILES}",
            filePath),
        plan.toString());

    metadata = String.format("load \"%s\" true", filePath);
    processor = new Planner();
    plan = (OperateFilePlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        String.format(
            "OperateFilePlan{file=%s, targetDir=null, autoCreateSchema=true, sgLevel=1, operatorType=LOAD_FILES}",
            filePath),
        plan.toString());

    metadata = String.format("load \"%s\" false", filePath);
    processor = new Planner();
    plan = (OperateFilePlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        String.format(
            "OperateFilePlan{file=%s, targetDir=null, autoCreateSchema=false, sgLevel=1, operatorType=LOAD_FILES}",
            filePath),
        plan.toString());

    metadata = String.format("load \"%s\" true 3", filePath);
    processor = new Planner();
    plan = (OperateFilePlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        String.format(
            "OperateFilePlan{file=%s, targetDir=null, autoCreateSchema=true, sgLevel=3, operatorType=LOAD_FILES}",
            filePath),
        plan.toString());
  }

  @Test
  public void testRemoveFile() throws QueryProcessException {
    String filePath = "data" + File.separator + "213213441243-1-2.tsfile";
    String metadata = String.format("remove \"%s\"", filePath);
    Planner processor = new Planner();
    OperateFilePlan plan = (OperateFilePlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        String.format(
            "OperateFilePlan{file=%s, targetDir=null, autoCreateSchema=false, sgLevel=0, operatorType=REMOVE_FILE}",
            filePath),
        plan.toString());
  }

  @Test
  public void testMoveFile() throws QueryProcessException {
    String filePath = "data" + File.separator + "213213441243-1-2.tsfile";
    String targetDir = "user" + File.separator + "backup";
    String metadata = String.format("move \"%s\" \"%s\"", filePath, targetDir);
    Planner processor = new Planner();
    OperateFilePlan plan = (OperateFilePlan) processor.parseSQLToPhysicalPlan(metadata);
    assertEquals(
        String.format(
            "OperateFilePlan{file=%s, targetDir=%s, autoCreateSchema=false, sgLevel=0, operatorType=MOVE_FILE}",
            filePath, targetDir),
        plan.toString());
  }

  @Test
  public void testDeduplicatedPath() throws Exception {
    String sqlStr = "select * from root.vehicle.d1,root.vehicle.d1,root.vehicle.d1";
    RawDataQueryPlan plan = (RawDataQueryPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    Assert.assertEquals(1, plan.getDeduplicatedPaths().size());
    Assert.assertEquals(1, plan.getDeduplicatedDataTypes().size());
    Assert.assertEquals(new Path("root.vehicle.d1", "s1"), plan.getDeduplicatedPaths().get(0));

    sqlStr = "select count(*) from root.vehicle.d1,root.vehicle.d1,root.vehicle.d1";
    plan = (RawDataQueryPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    Assert.assertEquals(1, plan.getDeduplicatedPaths().size());
    Assert.assertEquals(1, plan.getDeduplicatedDataTypes().size());
    Assert.assertEquals(new Path("root.vehicle.d1", "s1"), plan.getDeduplicatedPaths().get(0));
  }

  @Test
  public void testLastPlanPaths() throws QueryProcessException {
    String sqlStr1 = "SELECT last s1 FROM root.vehicle.d1";
    String sqlStr2 = "SELECT last s1 FROM root.vehicle.d1, root.vehicle.d2";
    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr1);
    PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr2);
    Path path1 = new Path("root.vehicle.d1", "s1");
    Path path2 = new Path("root.vehicle.d2", "s1");
    assertEquals(1, plan1.getPaths().size());
    assertEquals(path1.toString(), plan1.getPaths().get(0).getFullPath());
    assertEquals(2, plan2.getPaths().size());
    assertEquals(path1.toString(), plan2.getPaths().get(0).getFullPath());
    assertEquals(path2.toString(), plan2.getPaths().get(1).getFullPath());
  }

  @Test
  public void testLastPlanDataTypes() throws QueryProcessException {
    String sqlStr1 = "SELECT last s1 FROM root.vehicle.d1";
    String sqlStr2 = "SELECT last s1 FROM root.vehicle.d2, root.vehicle.d3, root.vehicle.d4";

    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr1);
    PhysicalPlan plan2 = processor.parseSQLToPhysicalPlan(sqlStr2);

    assertEquals(1, ((LastQueryPlan) plan1).getDataTypes().size());
    TSDataType dataType = ((LastQueryPlan) plan1).getDataTypes().get(0);
    assertEquals(TSDataType.FLOAT, dataType);

    assertEquals(3, ((LastQueryPlan) plan2).getDataTypes().size());
    for (TSDataType dt : ((LastQueryPlan) plan2).getDataTypes()) {
      assertEquals(TSDataType.FLOAT, dt);
    }
  }

  @Test
  public void testDelete1() throws QueryProcessException, IllegalPathException {
    PartialPath path = new PartialPath("root.vehicle.d1.s1");
    List<PartialPath> pathList = new ArrayList<>(Collections.singletonList(path));
    String sqlStr = "delete FROM root.vehicle.d1.s1 WHERE time < 5000";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals(OperatorType.DELETE, plan.getOperatorType());
    assertEquals(pathList, plan.getPaths());
  }

  @Test
  public void testDelete2() throws QueryProcessException, IllegalPathException {
    PartialPath path1 = new PartialPath("root.vehicle.d1.s1");
    PartialPath path2 = new PartialPath("root.vehicle.d1.s2");
    List<PartialPath> pathList = new ArrayList<>(Arrays.asList(path1, path2));
    String sqlStr = "delete FROM root.vehicle.d1.s1,root.vehicle.d1.s2 WHERE time < 5000";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals(OperatorType.DELETE, plan.getOperatorType());
    assertEquals(pathList, plan.getPaths());
  }

  @Test
  public void testDelete3() throws QueryProcessException, IllegalPathException {
    PartialPath path1 = new PartialPath("root.vehicle.d1.s1");
    PartialPath path2 = new PartialPath("root.vehicle.d2.s3");
    List<PartialPath> pathList = new ArrayList<>(Arrays.asList(path1, path2));
    String sqlStr = "delete FROM root.vehicle.d1.s1,root.vehicle.d2.s3 WHERE time < 5000";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals(OperatorType.DELETE, plan.getOperatorType());
    assertEquals(pathList, plan.getPaths());
  }

  @Test
  public void testSpecialCharacters() throws QueryProcessException {
    String sqlStr1 =
        "create timeseries root.3e-3.-1.1/2.SNAPPY.RLE.81+12.+2.s/io.in[jack].hel[jjj.s[1].desc with "
            + "datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2)"
            + " attributes(attr1=v1, attr2=v2)";
    PhysicalPlan plan1 = processor.parseSQLToPhysicalPlan(sqlStr1);
    Assert.assertEquals(OperatorType.CREATE_TIMESERIES, plan1.getOperatorType());
  }

  @Test
  public void testTimeRangeDelete() throws QueryProcessException, IllegalPathException {
    String sqlStr1 = "DELETE FROM root.vehicle.d1 where time >= 1 and time <= 2";

    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr1);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals(
        plan.getPaths(), Collections.singletonList(new PartialPath("root.vehicle.d1")));
    Assert.assertEquals(1, ((DeletePlan) plan).getDeleteStartTime());
    Assert.assertEquals(2, ((DeletePlan) plan).getDeleteEndTime());
  }

  @Test
  public void testCreateTrigger1() throws QueryProcessException {
    String sql =
        "CREATE TRIGGER trigger1 BEFORE INSERT ON root.sg1.d1.s1 AS 'org.apache.iotdb.engine.trigger.Example'";

    CreateTriggerPlan plan = (CreateTriggerPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("trigger1", plan.getTriggerName());
    Assert.assertEquals(TriggerEvent.BEFORE_INSERT, plan.getEvent());
    Assert.assertEquals("root.sg1.d1.s1", plan.getFullPath().getFullPath());
    Assert.assertEquals("org.apache.iotdb.engine.trigger.Example", plan.getClassName());
  }

  @Test
  public void testCreateTrigger2() throws QueryProcessException {
    String sql =
        "CREATE TRIGGER trigger2 AFTER INSERT ON root.sg1.d1.s2 AS 'org.apache.iotdb.engine.trigger.Example'"
            + "WITH ('key1'='value1', 'key2'='value2')";

    CreateTriggerPlan plan = (CreateTriggerPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("trigger2", plan.getTriggerName());
    Assert.assertEquals(TriggerEvent.AFTER_INSERT, plan.getEvent());
    Assert.assertEquals("root.sg1.d1.s2", plan.getFullPath().getFullPath());
    Assert.assertEquals("org.apache.iotdb.engine.trigger.Example", plan.getClassName());
    Map<String, String> expectedAttributes = new HashMap<>();
    expectedAttributes.put("key1", "value1");
    expectedAttributes.put("key2", "value2");
    Assert.assertEquals(expectedAttributes, plan.getAttributes());
  }

  @Test
  public void testDropTrigger() throws QueryProcessException {
    String sql = "DROP TRIGGER trigger1";

    DropTriggerPlan plan = (DropTriggerPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("trigger1", plan.getTriggerName());
  }

  @Test
  public void testStartTrigger() throws QueryProcessException {
    String sql = "START TRIGGER my-trigger";

    StartTriggerPlan plan = (StartTriggerPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("my-trigger", plan.getTriggerName());
  }

  @Test
  public void testStopTrigger() throws QueryProcessException {
    String sql = "STOP TRIGGER my-trigger";

    StopTriggerPlan plan = (StopTriggerPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("my-trigger", plan.getTriggerName());
  }

  @Test
  public void testShowTriggers() throws QueryProcessException {
    String sql = "SHOW TRIGGERS";

    ShowTriggersPlan plan = (ShowTriggersPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertTrue(plan.isQuery());
    Assert.assertEquals(ShowContentType.TRIGGERS, plan.getShowContentType());
  }

  @Test
  public void testCreateCQ1() throws QueryProcessException {
    String sql =
        "CREATE CONTINUOUS QUERY cq1 BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* GROUP BY time(10s) END";

    CreateContinuousQueryPlan plan =
        (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("cq1", plan.getContinuousQueryName());
    Assert.assertEquals(10000, plan.getEveryInterval());
    Assert.assertEquals(10000, plan.getForInterval());
    Assert.assertEquals(
        "root.${1}.${2}.${3}.${4}.temperature_max", plan.getTargetPath().getFullPath());
    Assert.assertEquals(
        "select max_value(temperature) from root.ln.*.*.* group by ([now() - 10s, now()), 10s)",
        plan.getQuerySql());
  }

  @Test
  public void testCreateCQ2() throws QueryProcessException {
    String sql =
        "CREATE CONTINUOUS QUERY cq1 BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";

    CreateContinuousQueryPlan plan =
        (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("cq1", plan.getContinuousQueryName());
    Assert.assertEquals(10000, plan.getEveryInterval());
    Assert.assertEquals(10000, plan.getForInterval());
    Assert.assertEquals("root.${1}.${2}.${3}.temperature_max", plan.getTargetPath().getFullPath());
    Assert.assertEquals(
        "select max_value(temperature) from root.ln.*.*.* group by ([now() - 10s, now()), 10s), level = 3",
        plan.getQuerySql());
  }

  @Test
  public void testCreateCQ3() throws QueryProcessException {
    String sql =
        "CREATE CONTINUOUS QUERY cq1 RESAMPLE EVERY 20s BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";

    CreateContinuousQueryPlan plan =
        (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("cq1", plan.getContinuousQueryName());
    Assert.assertEquals(20000, plan.getEveryInterval());
    Assert.assertEquals(10000, plan.getForInterval());
    Assert.assertEquals("root.${1}.${2}.${3}.temperature_max", plan.getTargetPath().getFullPath());
    Assert.assertEquals(
        "select max_value(temperature) from root.ln.*.*.* group by ([now() - 10s, now()), 10s), level = 3",
        plan.getQuerySql());
  }

  @Test
  public void testCreateCQ4() throws QueryProcessException {
    String sql =
        "CREATE CONTINUOUS QUERY cq1 RESAMPLE EVERY 20s FOR 10s BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";

    CreateContinuousQueryPlan plan =
        (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("cq1", plan.getContinuousQueryName());
    Assert.assertEquals(20000, plan.getEveryInterval());
    Assert.assertEquals(10000, plan.getForInterval());
    Assert.assertEquals("root.${1}.${2}.${3}.temperature_max", plan.getTargetPath().getFullPath());
    Assert.assertEquals(
        "select max_value(temperature) from root.ln.*.*.* group by ([now() - 10s, now()), 10s), level = 3",
        plan.getQuerySql());
  }

  @Test
  public void testCreateCQ5() throws QueryProcessException {
    String sql =
        "CREATE CONTINUOUS QUERY cq1 RESAMPLE FOR 20s BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";

    CreateContinuousQueryPlan plan =
        (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("cq1", plan.getContinuousQueryName());
    Assert.assertEquals(10000, plan.getEveryInterval());
    Assert.assertEquals(20000, plan.getForInterval());
    Assert.assertEquals("root.${1}.${2}.${3}.temperature_max", plan.getTargetPath().getFullPath());
    Assert.assertEquals(
        "select max_value(temperature) from root.ln.*.*.* group by ([now() - 20s, now()), 10s), level = 3",
        plan.getQuerySql());
  }

  @Test
  public void testCreateCQ6() throws QueryProcessException {

    String sql =
        "CREATE CQ cq1 RESAMPLE FOR 20s BEGIN SELECT max_value(temperature) INTO root.${a4}.${3}.temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";
    try {
      CreateContinuousQueryPlan plan =
          (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
      fail();
    } catch (SQLParserException e) {
      assertEquals("CQ: x of ${x} should be an integer.", e.getMessage());
    }
  }

  @Test
  public void testCreateCQ7() throws QueryProcessException {

    String sql =
        "CREATE CQ cq1 RESAMPLE FOR 20s BEGIN SELECT max_value(temperature) INTO root.${4}.${3}.temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";
    try {
      CreateContinuousQueryPlan plan =
          (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
      fail();
    } catch (SQLParserException e) {
      assertEquals(
          "CQ: x of ${x} should be greater than 0 and equal to or less than <level> or the length of queried path prefix.",
          e.getMessage());
    }
  }

  @Test
  public void testCreateCQ8() throws QueryProcessException {
    String sql =
        "CREATE CONTINUOUS QUERY cq1 BEGIN SELECT max_value(temperature) INTO root.${1}_cq.${2}.${3}.temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";

    CreateContinuousQueryPlan plan =
        (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("cq1", plan.getContinuousQueryName());
    Assert.assertEquals(10000, plan.getEveryInterval());
    Assert.assertEquals(10000, plan.getForInterval());
    Assert.assertEquals(
        "root.${1}_cq.${2}.${3}.temperature_max", plan.getTargetPath().getFullPath());
    Assert.assertEquals(
        "select max_value(temperature) from root.ln.*.*.* group by ([now() - 10s, now()), 10s), level = 3",
        plan.getQuerySql());
  }

  @Test
  public void testCreateCQ9() throws QueryProcessException {
    String sql =
        "CREATE CONTINUOUS QUERY cq1 BEGIN SELECT max_value(temperature) INTO ${1}.${0}.${2}.${3}.temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";
    try {
      CreateContinuousQueryPlan plan =
          (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
      fail();
    } catch (ParseCancellationException e) {
      assertTrue(e.getMessage().contains("mismatched input '.' expecting FROM"));
    }
  }

  @Test
  public void testCreateCQ10() throws QueryProcessException {

    String sql =
        "CREATE CQ cq1 RESAMPLE FOR 20s BEGIN SELECT max_value(temperature) INTO root.${0}.${3}.temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";
    try {
      CreateContinuousQueryPlan plan =
          (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
      fail();
    } catch (SQLParserException e) {
      assertEquals(
          "CQ: x of ${x} should be greater than 0 and equal to or less than <level> or the length of queried path prefix.",
          e.getMessage());
    }
  }

  @Test
  public void testCreateCQ11() throws QueryProcessException {

    String sql =
        "CREATE CQ cq1 RESAMPLE FOR 20s BEGIN SELECT max_value(temperature), avg(temperature) INTO root.${0}.${3}.temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END";
    try {
      CreateContinuousQueryPlan plan =
          (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
      fail();
    } catch (SQLParserException e) {
      assertEquals("CQ: CQ currently does not support multiple result columns.", e.getMessage());
    }
  }

  @Test
  public void testCreateCQ12() throws QueryProcessException {
    long minEveryInterval =
        IoTDBDescriptor.getInstance().getConfig().getContinuousQueryMinimumEveryInterval();
    long everyInterval = minEveryInterval / 2;
    String sql =
        String.format(
            "CREATE CQ cq1 RESAMPLE EVERY %dms FOR 20s BEGIN SELECT max_value(temperature) INTO temperature_max FROM root.ln.*.*.* GROUP BY time(10s), level = 3 END",
            everyInterval);
    try {
      CreateContinuousQueryPlan plan =
          (CreateContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
      fail();
    } catch (SQLParserException e) {
      assertEquals(
          "CQ: every interval should not be lower than the minimum value you configured.",
          e.getMessage());
    }
  }

  @Test
  public void testDropCQ() throws QueryProcessException {
    String sql = "DROP CONTINUOUS QUERY cq1";

    DropContinuousQueryPlan plan = (DropContinuousQueryPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertFalse(plan.isQuery());
    Assert.assertEquals("cq1", plan.getContinuousQueryName());
  }

  @Test
  public void testShowCQs() throws QueryProcessException {
    String sql = "SHOW CONTINUOUS QUERIES";

    ShowContinuousQueriesPlan plan =
        (ShowContinuousQueriesPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertTrue(plan.isQuery());
  }

  @Test
  public void testShowFunction() throws QueryProcessException {
    String sql = "SHOW FUNCTIONS";

    ShowFunctionsPlan plan = (ShowFunctionsPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertTrue(plan.isQuery());
    Assert.assertEquals(ShowContentType.FUNCTIONS, plan.getShowContentType());
  }
}
