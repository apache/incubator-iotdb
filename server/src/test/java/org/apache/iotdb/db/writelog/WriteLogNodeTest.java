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
package org.apache.iotdb.db.writelog;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WriteLogNodeTest {

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private boolean enableWal;

  @Before
  public void setUp() throws Exception {
    enableWal = config.isEnableWal();
    config.setEnableWal(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    config.setEnableWal(enableWal);
  }

  @Test
  public void testWriteLogAndSync() throws IOException, IllegalPathException {
    // this test uses a dummy insert log node to insert a few logs and flushes them
    // then reads the logs from file
    String identifier = "root.logTestDevice";

    WriteLogNode logNode = new ExclusiveWriteLogNode(identifier);

    InsertRowPlan bwInsertPlan = new InsertRowPlan(new PartialPath(identifier), 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new TSDataType[]{TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 50, new PartialPath(identifier + ".s1"));

    long[] times = new long[]{110L, 111L, 112L, 113L};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.DOUBLE.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    Object[] columns = new Object[4];
    columns[0] = new double[4];
    columns[1] = new long[4];
    columns[2] = new Binary[4];
    columns[3] = new boolean[4];

    for (int r = 0; r < 4; r++) {
      ((double[]) columns[0])[r] = 1.0;
      ((long[]) columns[1])[r] = 1;
      ((Binary[]) columns[2])[r] = new Binary("hh" + r);
      ((boolean[]) columns[3])[r] = false;
    }

    InsertTabletPlan tabletPlan = new InsertTabletPlan(new PartialPath(identifier),
      new String[]{"s1", "s2", "s3", "s4"}, dataTypes);
    tabletPlan.setTimes(times);
    tabletPlan.setColumns(columns);
    tabletPlan.setRowCount(times.length);
    tabletPlan.setStart(0);
    tabletPlan.setEnd(4);

    tabletPlan.markFailedMeasurementInsertion(1, new Exception());

    logNode.write(bwInsertPlan, false);
    logNode.write(deletePlan, false);
    logNode.write(tabletPlan, false);

    logNode.close();

    File walFile = new File(
        config.getWalDir() + File.separator + identifier + File.separator + "wal1");
    assertTrue(walFile.exists());

    ILogReader reader = logNode.getLogReader();
    assertEquals(bwInsertPlan, reader.next());
    assertEquals(deletePlan, reader.next());
    InsertTabletPlan newPlan = (InsertTabletPlan) reader.next();
    assertEquals(newPlan.getMeasurements().length, 3);
    reader.close();

    logNode.delete();
  }

  @Test
  public void testNotifyFlush() throws IOException, IllegalPathException {
    // this test writes a few logs and sync them
    // then calls notifyStartFlush() and notifyEndFlush() to delete old file
    String identifier = "root.logTestDevice";

    WriteLogNode logNode = new ExclusiveWriteLogNode(identifier);

    InsertRowPlan bwInsertPlan = new InsertRowPlan(new PartialPath(identifier), 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new TSDataType[]{TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 50, new PartialPath(identifier + ".s1"));

    logNode.write(bwInsertPlan, false);
    logNode.notifyStartFlush();
    logNode.write(deletePlan, false);
    logNode.notifyStartFlush();

    ILogReader logReader = logNode.getLogReader();
    assertEquals(bwInsertPlan, logReader.next());
    assertEquals(deletePlan, logReader.next());
    logReader.close();

    logNode.notifyEndFlush();
    logReader = logNode.getLogReader();
    assertEquals(deletePlan, logReader.next());
    logReader.close();

    logNode.notifyEndFlush();
    logReader = logNode.getLogReader();
    assertFalse(logReader.hasNext());
    logReader.close();

    logNode.delete();
  }

  @Test
  public void testSyncThreshold() throws IOException, IllegalPathException {
    // this test checks that if more logs than threshold are written, a sync will be triggered.
    int flushWalThreshold = config.getFlushWalThreshold();
    config.setFlushWalThreshold(2);

    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice");

    InsertRowPlan bwInsertPlan = new InsertRowPlan(new PartialPath("root.logTestDevice"), 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new TSDataType[]{TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 50, new PartialPath("root.logTestDevice.s1"));

    logNode.write(bwInsertPlan, false);

    File walFile = new File(
        config.getWalDir() + File.separator + "root.logTestDevice" + File.separator + "wal1");
    assertFalse(walFile.exists());

    logNode.write(deletePlan, false);
    System.out.println("Waiting for wal file to be created");
    while (!walFile.exists()) {

    }
    assertTrue(walFile.exists());

    logNode.delete();
    config.setFlushWalThreshold(flushWalThreshold);
  }

  @Test
  public void testDelete() throws IOException, IllegalPathException {
    // this test uses a dummy insert log node to insert a few logs and flushes them
    // then deletes the node

    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice");

    InsertRowPlan bwInsertPlan = new InsertRowPlan(new PartialPath("logTestDevice"), 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new TSDataType[]{TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 50, new PartialPath("root.logTestDevice.s1"));

    logNode.write(bwInsertPlan, false);
    logNode.write(deletePlan, false);

    logNode.forceSync();

    File walFile = new File(
        config.getWalDir() + File.separator + "root.logTestDevice" + File.separator + "wal1");
    System.out.println("Waiting for wal to be created");
    while (!walFile.exists()) {

    }

    assertTrue(new File(logNode.getLogDirectory()).exists());
    logNode.delete();
    assertTrue(!new File(logNode.getLogDirectory()).exists());
  }

  @Test
  public void testOverSizedWAL() throws IOException, IllegalPathException {
    // this test uses a dummy insert log node to insert an over-sized log and assert exception caught
    WriteLogNode logNode = new ExclusiveWriteLogNode("root.logTestDevice.oversize");

    InsertRowPlan bwInsertPlan = new InsertRowPlan(new PartialPath("root.logTestDevice.oversize"), 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new TSDataType[]{TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN},
        new String[]{"1.0", "15", new String(new char[65 * 1024 * 1024]), "false"});

    boolean caught = false;
    try {
      logNode.write(bwInsertPlan, false);
    } catch (IOException e) {
      caught = true;
    }
    assertTrue(caught);

    logNode.delete();
  }
}
