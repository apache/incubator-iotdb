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
import static junit.framework.TestCase.assertNotSame;
import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.manager.WriteLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WriteLogNodeManagerTest {

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
  public void testGetAndDelete() throws IOException {
    String identifier = "testLogNode";
    WriteLogNodeManager manager = MultiFileLogNodeManager.getInstance();
    WriteLogNode logNode = manager.getNode(identifier);
    assertEquals(identifier, logNode.getIdentifier());

    WriteLogNode theSameNode = manager.getNode(identifier);
    assertSame(logNode, theSameNode);

    manager.deleteNode(identifier);
    WriteLogNode anotherNode = manager.getNode(identifier);
    assertNotSame(logNode, anotherNode);
  }

  @Test
  public void testAutoSync() throws IOException, InterruptedException, IllegalPathException {
    // this test check that nodes in a manager will sync periodically.
    int flushWalPeriod = config.getFlushWalThreshold();
    config.setForceWalPeriodInMs(10000);
    File tempRestore = File.createTempFile("managerTest", "restore");
    File tempProcessorStore = File.createTempFile("managerTest", "processorStore");

    WriteLogNodeManager manager = MultiFileLogNodeManager.getInstance();
    WriteLogNode logNode = manager
        .getNode("root.managerTest");

    InsertRowPlan bwInsertPlan = new InsertRowPlan(new PartialPath("logTestDevice"), 100,
        new String[]{"s1", "s2", "s3", "s4"},
        new TSDataType[]{TSDataType.DOUBLE, TSDataType.INT64, TSDataType.TEXT, TSDataType.BOOLEAN},
        new String[]{"1.0", "15", "str", "false"});
    DeletePlan deletePlan = new DeletePlan(Long.MIN_VALUE, 50, new PartialPath("root.logTestDevice.s1"));

    File walFile = new File(logNode.getLogDirectory() + File.separator + "wal1");
    assertTrue(!walFile.exists());

    logNode.write(bwInsertPlan, false);
    logNode.write(deletePlan, false);

    Thread.sleep(config.getForceWalPeriodInMs() + 1000);
    assertTrue(walFile.exists());

    logNode.delete();
    config.setForceWalPeriodInMs(flushWalPeriod);
    tempRestore.delete();
    tempProcessorStore.delete();
    tempRestore.getParentFile().delete();
  }
}
