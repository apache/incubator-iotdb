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
package org.apache.iotdb.db.qp.logical;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.crud.DeleteDataOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.SetStorageGroupOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.service.IoTDB;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;

public class LogicalPlanSmallTest {

  private LogicalGenerator logicalGenerator;

  @Before
  public void before() {
    logicalGenerator = new LogicalGenerator();
  }

  @Test
  public void testLimit() {
    String sqlStr = "select * from root.vehicle.d1 limit 10";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(10, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test
  public void testOffset() {
    String sqlStr = "select * from root.vehicle.d1 limit 10 offset 20";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(10, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(20, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test
  public void testSlimit() {
    String sqlStr = "select * from root.vehicle.d1 limit 10 slimit 1";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(10, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(1, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test
  public void testSOffset() {
    String sqlStr =
        "select * from root.vehicle.d1 where s1 < 20 and time <= now() limit 50 slimit 10 soffset 100";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(50, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(10, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(100, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test
  public void testSOffsetTimestamp() {
    String sqlStr =
        "select * from root.vehicle.d1 where s1 < 20 and timestamp <= now() limit 50 slimit 10 soffset 100";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertEquals(50, ((QueryOperator) operator).getRowLimit());
    Assert.assertEquals(0, ((QueryOperator) operator).getRowOffset());
    Assert.assertEquals(10, ((QueryOperator) operator).getSeriesLimit());
    Assert.assertEquals(100, ((QueryOperator) operator).getSeriesOffset());
  }

  @Test(expected = SQLParserException.class)
  public void testLimitOutOfRange() {
    String sqlStr =
        "select * from root.vehicle.d1 where s1 < 20 and time <= now() limit 1111111111111111111111";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    // expected to throw SQLParserException: Out of range. LIMIT <N>: N should be Int32.
  }

  @Test(expected = SQLParserException.class)
  public void testLimitNotPositive() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() limit 0";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    // expected to throw SQLParserException: LIMIT <N>: N should be greater than 0.
  }

  @Test(expected = SQLParserException.class)
  public void testOffsetOutOfRange() {
    String sqlStr =
        "select * from root.vehicle.d1 where s1 < 20 and time <= now() "
            + "limit 1 offset 1111111111111111111111";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    // expected to throw SQLParserException: Out of range. OFFSET <OFFSETValue>: OFFSETValue should
    // be Int32.
  }

  @Test(expected = ParseCancellationException.class)
  public void testOffsetNotPositive() {
    String sqlStr =
        "select * from root.vehicle.d1 where s1 < 20 and time <= now() limit 1 offset -1";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    // expected to throw SQLParserException: OFFSET <OFFSETValue>: OFFSETValue should >= 0.
  }

  @Test(expected = SQLParserException.class)
  public void testSlimitOutOfRange() {
    String sqlStr =
        "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1111111111111111111111";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    // expected to throw SQLParserException: Out of range. SLIMIT <SN>: SN should be Int32.
  }

  @Test(expected = SQLParserException.class)
  public void testSlimitNotPositive() {
    String sqlStr = "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 0";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    // expected to throw SQLParserException: SLIMIT <SN>: SN should be greater than 0.
  }

  @Test(expected = SQLParserException.class)
  public void testSoffsetOutOfRange() {
    String sqlStr =
        "select * from root.vehicle.d1 where s1 < 20 and time <= now() "
            + "slimit 1 soffset 1111111111111111111111";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    // expected to throw SQLParserException: Out of range. SOFFSET <SOFFSETValue>: SOFFSETValue
    // should be Int32.
  }

  @Test
  public void testSoffsetNotPositive() {
    String sqlStr =
        "select * from root.vehicle.d1 where s1 < 20 and time <= now() slimit 1 soffset 1";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(1, ((QueryOperator) operator).getSeriesOffset());
    Assert.assertEquals(1, ((QueryOperator) operator).getSeriesLimit());
  }

  @Test(expected = LogicalOptimizeException.class)
  public void testSoffsetExceedColumnNum() throws QueryProcessException {
    String sqlStr =
        "select s1 from root.vehicle.d1 where s1 < 20 and time <= now() slimit 2 soffset 1";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    IoTDB.metaManager.init();
    ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer();
    concatPathOptimizer.transform(operator);
    IoTDB.metaManager.clear();
    // expected to throw LogicalOptimizeException: The value of SOFFSET (%d) is equal to or exceeds
    // the number of sequences (%d) that can actually be returned.
  }

  @Test
  public void testDeleteStorageGroup() throws IllegalPathException {
    String sqlStr = "delete storage group root.vehicle.d1";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(DeleteStorageGroupOperator.class, operator.getClass());
    PartialPath path = new PartialPath("root.vehicle.d1");
    Assert.assertEquals(path, ((DeleteStorageGroupOperator) operator).getDeletePathList().get(0));
  }

  @Test
  public void testDisableAlign() {
    String sqlStr = "select * from root.vehicle disable align";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertFalse(((QueryOperator) operator).isAlignByTime());
  }

  @Test
  public void testNotDisableAlign() {
    String sqlStr = "select * from root.vehicle";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    Assert.assertTrue(((QueryOperator) operator).isAlignByTime());
  }

  @Test(expected = ParseCancellationException.class)
  public void testDisableAlignConflictAlignByDevice() {
    String sqlStr = "select * from root.vehicle disable align align by device";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr, ZoneId.systemDefault());
  }

  @Test
  public void testChineseCharacter() throws IllegalPathException {
    String sqlStr1 = "set storage group to root.一级";
    RootOperator operator =
        (RootOperator) logicalGenerator.generate(sqlStr1, ZoneId.systemDefault());
    Assert.assertEquals(SetStorageGroupOperator.class, operator.getClass());
    Assert.assertEquals(new PartialPath("root.一级"), ((SetStorageGroupOperator) operator).getPath());

    String sqlStr2 = "select * from root.一级.设备1 limit 10 offset 20";
    operator = (RootOperator) logicalGenerator.generate(sqlStr2, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, operator.getClass());
    ArrayList<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath("*"));
    Assert.assertEquals(paths, ((QueryOperator) operator).getSelectedPaths());
  }

  @Test
  public void testKeyWordSQL() {
    try {
      String sql =
          "delete from ROOT.CREATE.INSERT.UPDATE.DELETE.SELECT.SHOW.GRANT.INTO.SET.WHERE.FROM.TO.BY.DEVICE."
              + "CONFIGURATION.DESCRIBE.SLIMIT.LIMIT.UNLINK.OFFSET.SOFFSET.FILL.LINEAR.PREVIOUS.PREVIOUSUNTILLAST."
              + "METADATA.TIMESERIES.TIMESTAMP.PROPERTY.WITH.DATATYPE.COMPRESSOR.STORAGE.GROUP.LABEL.ADD."
              + "UPSERT.VALUES.NOW.LINK.INDEX.USING.ON.DROP.MERGE.LIST.USER.PRIVILEGES.ROLE.ALL.OF."
              + "ALTER.PASSWORD.REVOKE.LOAD.WATERMARK_EMBEDDING.UNSET.TTL.FLUSH.TASK.INFO.VERSION."
              + "REMOVE.MOVE.CHILD.PATHS.DEVICES.COUNT.NODES.LEVEL.MIN_TIME.MAX_TIME.MIN_VALUE.MAX_VALUE.AVG."
              + "FIRST_VALUE.SUM.LAST_VALUE.LAST.DISABLE.ALIGN.COMPRESSION.TIME.ATTRIBUTES.TAGS.RENAME.FULL.CLEAR.CACHE."
              + "SNAPSHOT.FOR.SCHEMA.TRACING.OFF where time>=1 and time < 3";

      Operator op = logicalGenerator.generate(sql, ZoneId.systemDefault());
      Assert.assertEquals(DeleteDataOperator.class, op.getClass());
    } catch (ParseCancellationException ignored) {
    }
  }

  @Test
  public void testRangeDelete() throws IllegalPathException {
    String sql1 = "delete from root.d1.s1 where time>=1 and time < 3";
    Operator op = logicalGenerator.generate(sql1, ZoneId.systemDefault());
    Assert.assertEquals(DeleteDataOperator.class, op.getClass());
    ArrayList<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath("root.d1.s1"));
    Assert.assertEquals(paths, ((DeleteDataOperator) op).getSelectedPaths());
    Assert.assertEquals(1, ((DeleteDataOperator) op).getStartTime());
    Assert.assertEquals(2, ((DeleteDataOperator) op).getEndTime());

    String sql2 = "delete from root.d1.s1 where time>=1";
    op = logicalGenerator.generate(sql2, ZoneId.systemDefault());
    Assert.assertEquals(paths, ((DeleteDataOperator) op).getSelectedPaths());
    Assert.assertEquals(1, ((DeleteDataOperator) op).getStartTime());
    Assert.assertEquals(Long.MAX_VALUE, ((DeleteDataOperator) op).getEndTime());

    String sql3 = "delete from root.d1.s1 where time>1";
    op = logicalGenerator.generate(sql3, ZoneId.systemDefault());
    Assert.assertEquals(paths, ((DeleteDataOperator) op).getSelectedPaths());
    Assert.assertEquals(2, ((DeleteDataOperator) op).getStartTime());
    Assert.assertEquals(Long.MAX_VALUE, ((DeleteDataOperator) op).getEndTime());

    String sql4 = "delete from root.d1.s1 where time <= 1";
    op = logicalGenerator.generate(sql4, ZoneId.systemDefault());
    Assert.assertEquals(paths, ((DeleteDataOperator) op).getSelectedPaths());
    Assert.assertEquals(Long.MIN_VALUE, ((DeleteDataOperator) op).getStartTime());
    Assert.assertEquals(1, ((DeleteDataOperator) op).getEndTime());

    String sql5 = "delete from root.d1.s1 where time<1";
    op = logicalGenerator.generate(sql5, ZoneId.systemDefault());
    Assert.assertEquals(paths, ((DeleteDataOperator) op).getSelectedPaths());
    Assert.assertEquals(Long.MIN_VALUE, ((DeleteDataOperator) op).getStartTime());
    Assert.assertEquals(0, ((DeleteDataOperator) op).getEndTime());

    String sql6 = "delete from root.d1.s1 where time = 3";
    op = logicalGenerator.generate(sql6, ZoneId.systemDefault());
    Assert.assertEquals(paths, ((DeleteDataOperator) op).getSelectedPaths());
    Assert.assertEquals(3, ((DeleteDataOperator) op).getStartTime());
    Assert.assertEquals(3, ((DeleteDataOperator) op).getEndTime());

    String sql7 = "delete from root.d1.s1 where time > 5 and time >= 2";
    op = logicalGenerator.generate(sql7, ZoneId.systemDefault());
    Assert.assertEquals(paths, ((DeleteDataOperator) op).getSelectedPaths());
    Assert.assertEquals(6, ((DeleteDataOperator) op).getStartTime());
    Assert.assertEquals(Long.MAX_VALUE, ((DeleteDataOperator) op).getEndTime());
  }

  @Test
  public void testErrorDeleteRange() {
    String sql = "delete from root.d1.s1 where time>=1 and time < 3 or time >1";
    String errorMsg = null;
    try {
      logicalGenerator.generate(sql, ZoneId.systemDefault());
    } catch (SQLParserException e) {
      errorMsg = e.getMessage();
    }
    Assert.assertEquals(
        "For delete statement, where clause can only contain atomic expressions like : "
            + "time > XXX, time <= XXX, or two atomic expressions connected by 'AND'",
        errorMsg);

    sql = "delete from root.d1.s1 where time>=1 or time < 3";
    errorMsg = null;
    try {
      logicalGenerator.generate(sql, ZoneId.systemDefault());
    } catch (SQLParserException e) {
      errorMsg = e.getMessage();
    }
    Assert.assertEquals(
        "For delete statement, where clause can only contain atomic expressions like : "
            + "time > XXX, time <= XXX, or two atomic expressions connected by 'AND'",
        errorMsg);

    String sql7 = "delete from root.d1.s1 where time = 1 and time < -1";
    errorMsg = null;
    try {
      logicalGenerator.generate(sql7, ZoneId.systemDefault());
    } catch (RuntimeException e) {
      errorMsg = e.getMessage();
    }
    Assert.assertEquals("Invalid delete range: [1, -2]", errorMsg);

    sql = "delete from root.d1.s1 where time > 5 and time <= 0";
    errorMsg = null;
    try {
      logicalGenerator.generate(sql, ZoneId.systemDefault());
    } catch (SQLParserException e) {
      errorMsg = e.getMessage();
    }
    Assert.assertEquals("Invalid delete range: [6, 0]", errorMsg);
  }
}
