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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.SetDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MManagerBasicTest {

  private CompressionType compressionType;

  @Before
  public void setUp() {
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAddPathAndExist() throws IllegalPathException {
    MManager manager = IoTDB.metaManager;
    assertTrue(manager.isPathExist(new PartialPath("root")));

    assertFalse(manager.isPathExist(new PartialPath("root.laptop")));

    try {
      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.1")));

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          "some children of root.laptop have already been set to storage group", e.getMessage());
    }

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertTrue(manager.isPathExist(new PartialPath("root.laptop")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));
    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.1_2"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.\"1.2.3\""),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.1.2.3"),
          TSDataType.INT32,
          TSEncoding.RLE,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());

      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));
      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.1_2")));
      assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.\"1.2.3\"")));
      assertTrue(manager.isPathExist(new PartialPath("root.1.2")));
      assertTrue(manager.isPathExist(new PartialPath("root.1.2.3")));
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s1")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop")));
    assertTrue(manager.isPathExist(new PartialPath("root")));

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
    } catch (MetadataException e1) {
      e1.printStackTrace();
      fail(e1.getMessage());
    }

    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d2")));
    assertFalse(manager.checkStorageGroupByPath(new PartialPath("root.laptop.d2")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s1"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.setStorageGroup(new PartialPath("root.laptop1"));
    } catch (MetadataException e) {
      Assert.assertEquals(
          String.format(
              "The seriesPath of %s already exist, it can't be set to the storage group",
              "root.laptop1"),
          e.getMessage());
    }

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.1_2"));
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.\"1.2.3\""));
      manager.deleteTimeseries(new PartialPath("root.1.2.3"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.1_2")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.\"1.2.3\"")));
    assertFalse(manager.isPathExist(new PartialPath("root.1.2.3")));
    assertFalse(manager.isPathExist(new PartialPath("root.1.2")));
    assertTrue(manager.isPathExist(new PartialPath("root.1")));

    try {
      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.1")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.1")));
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Test
  public void testCreateAlignedTimeseries() throws IllegalPathException {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.vector"),
          Arrays.asList("s1", "s2", "s3"),
          Arrays.asList(
              TSDataType.valueOf("INT32"),
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          compressionType);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.laptop")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.vector")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s2")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s3")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.vector.s2"));
    } catch (MetadataException e) {
      assertEquals(
          "Not support deleting part of aligned timeseies! (Path: root.laptop.d1.vector.s2)",
          e.getMessage());
    }

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.vector"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.vector")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s1")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s2")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s3")));

    try {
      manager.deleteTimeseries(new PartialPath("root.laptop.d1.s0"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertFalse(manager.isPathExist(new PartialPath("root.laptop.d1.s0")));

    try {
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.vector"),
          Arrays.asList("s0", "s2", "s4"),
          Arrays.asList(
              TSDataType.valueOf("INT32"),
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          compressionType);
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.vector")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s0")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s2")));
    assertTrue(manager.isPathExist(new PartialPath("root.laptop.d1.vector.s4")));
  }

  @Test
  @SuppressWarnings("squid:S5783")
  public void testGetAllTimeseriesCount() {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s2.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*")), 6);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.*")), 5);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.*.t1")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.*.s1")), 2);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1")), 3);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.*")), 3);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d2.s1")), 1);
      assertEquals(manager.getAllTimeseriesCount(new PartialPath("root.laptop.d2")), 2);

      try {
        manager.getAllTimeseriesCount(new PartialPath("root.laptop.d3.s1"));
        fail("Expected exception");
      } catch (MetadataException e) {
        assertEquals("Path [root.laptop.d3.s1] does not exist", e.getMessage());
      }
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testSetStorageGroupAndExist() {

    MManager manager = IoTDB.metaManager;

    try {
      assertFalse(manager.isStorageGroup(new PartialPath("root")));
      assertFalse(manager.isStorageGroup(new PartialPath("root1.laptop.d2")));

      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d1.s1")));

      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testRecover() {

    MManager manager = IoTDB.metaManager;

    try {

      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      Set<String> devices =
          new TreeSet<String>() {
            {
              add("root.laptop.d1");
              add("root.laptop.d2");
            }
          };
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.laptop.d2")));
      assertTrue(manager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(manager.isStorageGroup(new PartialPath("root.laptop")));
      devices.remove("root.laptop.d2");
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      MManager recoverManager = new MManager();
      recoverManager.init();

      assertTrue(recoverManager.isStorageGroup(new PartialPath("root.laptop.d1")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d2")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop.d3")));
      assertFalse(recoverManager.isStorageGroup(new PartialPath("root.laptop")));
      // prefix with *
      assertEquals(
          devices,
          recoverManager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));

      recoverManager.clear();
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetAllFileNamesByPath() {

    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop.d1"));
      manager.setStorageGroup(new PartialPath("root.laptop.d2"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      List<String> list = new ArrayList<>();

      list.add("root.laptop.d1");
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop.d1.s1")));
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop.d1")));
      list.add("root.laptop.d2");
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root.laptop")));
      assertEquals(list, manager.getStorageGroupByPath(new PartialPath("root")));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckStorageExistOfPath() {
    MManager manager = IoTDB.metaManager;

    try {
      assertTrue(manager.getAllTimeseriesPath(new PartialPath("root")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle.device")).isEmpty());
      assertTrue(
          manager.getStorageGroupByPath(new PartialPath("root.vehicle.device.sensor")).isEmpty());

      manager.setStorageGroup(new PartialPath("root.vehicle"));
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle.device")).isEmpty());
      assertFalse(
          manager.getStorageGroupByPath(new PartialPath("root.vehicle.device.sensor")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device")).isEmpty());

      manager.setStorageGroup(new PartialPath("root.vehicle1.device"));
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device1")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device2")).isEmpty());
      assertTrue(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device3")).isEmpty());
      assertFalse(manager.getStorageGroupByPath(new PartialPath("root.vehicle1.device")).isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowChildNodesWithGivenPrefix() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      Set<String> nodes = new HashSet<>(Arrays.asList("s1", "s2"));
      Set<String> nodes2 = new HashSet<>(Arrays.asList("laptop"));
      Set<String> nodes3 = new HashSet<>(Arrays.asList("d1", "d2"));
      Set<String> nexLevelNodes1 =
          manager.getChildNodeInNextLevel(new PartialPath("root.laptop.d1"));
      Set<String> nexLevelNodes2 = manager.getChildNodeInNextLevel(new PartialPath("root"));
      Set<String> nexLevelNodes3 = manager.getChildNodeInNextLevel(new PartialPath("root.laptop"));
      // usual condition
      assertEquals(nodes, nexLevelNodes1);
      assertEquals(nodes2, nexLevelNodes2);
      assertEquals(nodes3, nexLevelNodes3);

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetStorageGroupNameByAutoLevel() {
    int level = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();

    try {
      assertEquals(
          "root.laptop",
          MetaUtils.getStorageGroupPathByLevel(new PartialPath("root.laptop.d1.s1"), level)
              .getFullPath());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    boolean caughtException = false;
    try {
      MetaUtils.getStorageGroupPathByLevel(new PartialPath("root1.laptop.d1.s1"), level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root1.laptop.d1.s1 is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);

    caughtException = false;
    try {
      MetaUtils.getStorageGroupPathByLevel(new PartialPath("root"), level);
    } catch (MetadataException e) {
      caughtException = true;
      assertEquals("root is not a legal path", e.getMessage());
    }
    assertTrue(caughtException);
  }

  @Test
  public void testSetStorageGroupWithIllegalName() {
    MManager manager = IoTDB.metaManager;
    try {
      PartialPath path1 = new PartialPath("root.laptop\n");
      try {
        manager.setStorageGroup(path1);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
    try {
      PartialPath path2 = new PartialPath("root.laptop\t");
      try {
        manager.setStorageGroup(path2);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
  }

  @Test
  public void testCreateTimeseriesWithIllegalName() {
    MManager manager = IoTDB.metaManager;
    try {
      PartialPath path1 = new PartialPath("root.laptop.d1\n.s1");
      try {
        manager.createTimeseries(
            path1, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
    try {
      PartialPath path2 = new PartialPath("root.laptop.d1\t.s1");
      try {
        manager.createTimeseries(
            path2, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.SNAPPY, null);
        fail();
      } catch (MetadataException e) {
      }
    } catch (IllegalPathException e1) {
      fail();
    }
  }

  @Test
  public void testGetDevicesWithGivenPrefix() {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      Set<String> devices = new TreeSet<>();
      devices.add("root.laptop.d1");
      devices.add("root.laptop.d2");
      // usual condition
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.laptop")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
      manager.setStorageGroup(new PartialPath("root.vehicle"));
      manager.createTimeseries(
          new PartialPath("root.vehicle.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      devices.add("root.vehicle.d1");
      // prefix with *
      assertEquals(
          devices,
          manager.getDevices(new PartialPath("root.*")).stream()
              .map(PartialPath::getFullPath)
              .collect(Collectors.toSet()));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testGetChildNodePathInNextLevel() {
    MManager manager = IoTDB.metaManager;
    String[] res =
        new String[] {
          "[root.laptop, root.vehicle]",
          "[root.laptop.b1, root.laptop.b2]",
          "[root.laptop.b1.d1, root.laptop.b1.d2]",
          "[root.laptop.b1, root.laptop.b2, root.vehicle.b1, root.vehicle.b2]",
          "[root.laptop.b1.d1, root.laptop.b1.d2, root.vehicle.b1.d0, root.vehicle.b1.d2, root.vehicle.b1.d3]",
          "[root.laptop.b1.d1, root.laptop.b1.d2]",
          "[root.vehicle.b1.d0, root.vehicle.b1.d2, root.vehicle.b1.d3, root.vehicle.b2.d0]",
          "[root.laptop.b1.d1.s0, root.laptop.b1.d1.s1, root.laptop.b1.d2.s0, root.laptop.b2.d1.s1, root.laptop.b2.d1.s3, root.laptop.b2.d2.s2]",
          "[]"
        };

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.setStorageGroup(new PartialPath("root.vehicle"));

      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b1.d2.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.b2.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d0.s0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b1.d3.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.vehicle.b2.d0.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(res[0], manager.getChildNodePathInNextLevel(new PartialPath("root")).toString());
      assertEquals(
          res[1], manager.getChildNodePathInNextLevel(new PartialPath("root.laptop")).toString());
      assertEquals(
          res[2],
          manager.getChildNodePathInNextLevel(new PartialPath("root.laptop.b1")).toString());
      assertEquals(
          res[3], manager.getChildNodePathInNextLevel(new PartialPath("root.*")).toString());
      assertEquals(
          res[4], manager.getChildNodePathInNextLevel(new PartialPath("root.*.b1")).toString());
      assertEquals(
          res[5], manager.getChildNodePathInNextLevel(new PartialPath("root.l*.b1")).toString());
      assertEquals(
          res[6], manager.getChildNodePathInNextLevel(new PartialPath("root.v*.*")).toString());
      assertEquals(
          res[7], manager.getChildNodePathInNextLevel(new PartialPath("root.l*.b*.*")).toString());
      assertEquals(
          res[8], manager.getChildNodePathInNextLevel(new PartialPath("root.laptopp")).toString());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testTemplate() throws MetadataException {
    CreateTemplatePlan plan = getCreateTemplatePlan();

    MManager manager = IoTDB.metaManager;
    manager.createDeviceTemplate(plan);

    // set device template
    SetDeviceTemplatePlan setDeviceTemplatePlan =
        new SetDeviceTemplatePlan("template1", "root.sg1.d1");

    manager.setDeviceTemplate(setDeviceTemplatePlan);

    MNode node = manager.getDeviceNode(new PartialPath("root.sg1.d1"));
    node.setUseTemplate(true);

    MeasurementSchema s11 =
        new MeasurementSchema("s11", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    assertNotNull(node.getDeviceTemplate());
    assertEquals(node.getDeviceTemplate().getSchemaMap().get("s11"), s11);

    Set<IMeasurementSchema> allSchema =
        new HashSet<>(node.getDeviceTemplate().getSchemaMap().values());
    for (IMeasurementSchema schema :
        manager.getAllMeasurementByDevicePath(new PartialPath("root.sg1.d1"))) {
      allSchema.remove(schema);
    }

    assertTrue(allSchema.isEmpty());

    MeasurementMNode[] mNodes =
        manager.getMNodes(new PartialPath("root.sg1.d1"), new String[] {"s11"});
    assertNotNull(mNodes[0]);
    assertEquals(mNodes[0].getSchema(), s11);

    mNodes = manager.getMNodes(new PartialPath("root.sg1.d1"), new String[] {"s100"});
    assertNull(mNodes[0]);
  }

  private CreateTemplatePlan getCreateTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");
    schemaNames.add("vector");

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  @Test
  public void testTemplateCompatibility() throws MetadataException {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");
    schemaNames.add("test_vector");

    CreateTemplatePlan plan1 =
        new CreateTemplatePlan(
            "template1",
            new ArrayList<>(schemaNames),
            new ArrayList<>(measurementList),
            new ArrayList<>(dataTypeList),
            new ArrayList<>(encodingList),
            new ArrayList<>(compressionTypes));

    measurementList.add(Collections.singletonList("s12"));
    schemaNames.add("s12");
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    compressionTypes.add(CompressionType.SNAPPY);

    CreateTemplatePlan plan2 =
        new CreateTemplatePlan(
            "template2",
            new ArrayList<>(schemaNames),
            new ArrayList<>(measurementList),
            new ArrayList<>(dataTypeList),
            new ArrayList<>(encodingList),
            new ArrayList<>(compressionTypes));

    MManager manager = IoTDB.metaManager;

    assertTrue(manager.isTemplateCompatible(new Template(plan1), new Template(plan2)));
    assertFalse(manager.isTemplateCompatible(new Template(plan2), new Template(plan1)));

    System.out.println(measurementList);
    measurementList.get(1).add("s13");
    dataTypeList.get(1).add(TSDataType.INT64);
    encodingList.get(1).add(TSEncoding.RLE);

    CreateTemplatePlan plan3 =
        new CreateTemplatePlan(
            "template3",
            new ArrayList<>(schemaNames),
            new ArrayList<>(measurementList),
            new ArrayList<>(dataTypeList),
            new ArrayList<>(encodingList),
            new ArrayList<>(compressionTypes));

    assertTrue(manager.isTemplateCompatible(new Template(plan1), new Template(plan3)));

    List<String> vectorList = new ArrayList<>(measurementList.get(1));
    vectorList.remove(0);
    List<TSDataType> vectorDataTypesList = new ArrayList<>(dataTypeList.get(1));
    vectorDataTypesList.remove(0);
    List<TSEncoding> vectorEncodingsList = new ArrayList<>(encodingList.get(1));
    vectorEncodingsList.remove(0);

    measurementList.set(1, vectorList);
    dataTypeList.set(1, vectorDataTypesList);
    encodingList.set(1, vectorEncodingsList);

    CreateTemplatePlan plan4 =
        new CreateTemplatePlan(
            "template4",
            new ArrayList<>(schemaNames),
            new ArrayList<>(measurementList),
            new ArrayList<>(dataTypeList),
            new ArrayList<>(encodingList),
            new ArrayList<>(compressionTypes));

    assertFalse(manager.isTemplateCompatible(new Template(plan1), new Template(plan4)));

    // test manager
    manager.createDeviceTemplate(plan1);
    manager.createDeviceTemplate(plan2);
    manager.createDeviceTemplate(plan4);

    manager.setDeviceTemplate(new SetDeviceTemplatePlan("template1", "root.sg1.d1"));
    try {
      manager.setDeviceTemplate(new SetDeviceTemplatePlan("template4", "root.sg1.d1.d2"));
      fail("These two templates are incompatible");
    } catch (MetadataException e) {
      assertEquals("Incompatible template", e.getMessage());
    }

    manager.setDeviceTemplate(new SetDeviceTemplatePlan("template2", "root.sg1.d1.d2"));
  }

  @Test
  public void testTemplateAndTimeSeriesCompatibility() throws MetadataException {
    CreateTemplatePlan plan = getCreateTemplatePlan();
    MManager manager = IoTDB.metaManager;
    manager.createDeviceTemplate(plan);

    // set device template
    SetDeviceTemplatePlan setDeviceTemplatePlan =
        new SetDeviceTemplatePlan("template1", "root.sg1.d1");

    manager.setDeviceTemplate(setDeviceTemplatePlan);

    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg1.d1.s20"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);

    manager.createTimeseries(createTimeSeriesPlan);

    CreateTimeSeriesPlan createTimeSeriesPlan2 =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg1.d1.vector.s1"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);

    try {
      manager.createTimeseries(createTimeSeriesPlan2);
      fail();
    } catch (Exception e) {
      assertEquals(
          "Path [root.sg1.d1.vector.s1 ( which is incompatible with template )] already exist",
          e.getMessage());
    }
  }

  @Test
  public void testTemplateAndNodePathCompatibility() throws MetadataException {
    MManager manager = IoTDB.metaManager;
    CreateTemplatePlan plan = getCreateTemplatePlan();
    manager.createDeviceTemplate(plan);

    // set device template
    SetDeviceTemplatePlan setDeviceTemplatePlan =
        new SetDeviceTemplatePlan("template1", "root.sg1.d1");

    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg1.d1.s11"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);

    manager.createTimeseries(createTimeSeriesPlan);

    try {
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      fail();
    } catch (PathAlreadyExistException e) {
      assertEquals("Path [root.sg1.d1.s11] already exist", e.getMessage());
    }

    manager.deleteTimeseries(new PartialPath("root.sg1.d1.s11"));

    CreateTimeSeriesPlan createTimeSeriesPlan2 =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg1.d1.vector.s1"),
            TSDataType.INT32,
            TSEncoding.PLAIN,
            CompressionType.GZIP,
            null,
            null,
            null,
            null);
    manager.createTimeseries(createTimeSeriesPlan2);

    try {
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      fail();
    } catch (PathAlreadyExistException e) {
      assertEquals("Path [root.sg1.d1.vector] already exist", e.getMessage());
    }
  }

  @Test
  public void testShowTimeseries() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.vector"),
          Arrays.asList("s1", "s2", "s3"),
          Arrays.asList(
              TSDataType.valueOf("INT32"),
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          compressionType);

      // show timeseries root.laptop.d1.s0
      ShowTimeSeriesPlan showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.s0"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> result =
          manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s0", result.get(0).getName());

      // show timeseries
      //      showTimeSeriesPlan =
      //          new ShowTimeSeriesPlan(new PartialPath("root"), false, null, null, 0, 0, false);
      //      result = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      //      assertEquals(4, result.size());

      // show timeseries root.laptop.d1.vector
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.vector"), false, null, null, 0, 0, false);
      result = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(3, result.size());
      for (int i = 0; i < result.size(); i++) {
        assertEquals("root.laptop.d1.vector.s" + (i + 1), result.get(i).getName());
      }

      // show timeseries root.laptop.d1.vector.s1
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.vector.s1"), false, null, null, 0, 0, false);
      result = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.vector.s1", result.get(0).getName());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // show timeseries root.laptop.d1.(s0,s1)
    try {
      ShowTimeSeriesPlan showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.(s0,s1)"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> result =
          manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
    } catch (MetadataException e) {
      assertEquals(
          "Cannot get node of children in different aligned timeseries (Path: (s0,s1))",
          e.getMessage());
    }
  }

  @Test
  public void testShowTimeseriesWithTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));
    List<String> measurements = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      measurements.add("s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.INT32);
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      compressionTypes.add(compressionType);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s0");
    schemaNames.add("vector");

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);
    MManager manager = IoTDB.metaManager;
    try {
      manager.createDeviceTemplate(plan);

      // set device template
      SetDeviceTemplatePlan setDeviceTemplatePlan =
          new SetDeviceTemplatePlan("template1", "root.laptop.d1");
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      manager.getDeviceNode(new PartialPath("root.laptop.d1")).setUseTemplate(true);

      // show timeseries root.laptop.d1.s0
      ShowTimeSeriesPlan showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.s0"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> result =
          manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.s0", result.get(0).getName());

      // show timeseries root.laptop.d1.vector.s1
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.vector.s1"), false, null, null, 0, 0, false);
      result = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(1, result.size());
      assertEquals("root.laptop.d1.vector.s1", result.get(0).getName());

      // show timeseries root.laptop.d1.(s1,s2,s3)
      showTimeSeriesPlan =
          new ShowTimeSeriesPlan(new PartialPath("root"), false, null, null, 0, 0, false);
      result = manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
      assertEquals(4, result.size());
      Set<String> set = new HashSet<>();
      for (int i = 1; i < result.size(); i++) {
        set.add("root.laptop.d1.vector.s" + i);
      }
      set.add("root.laptop.d1.s0");

      for (int i = 0; i < result.size(); i++) {
        set.remove(result.get(i).getName());
      }

      assertTrue(set.isEmpty());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    // show timeseries root.laptop.d1.(s0,s1)
    try {
      ShowTimeSeriesPlan showTimeSeriesPlan =
          new ShowTimeSeriesPlan(
              new PartialPath("root.laptop.d1.(s0,s1)"), false, null, null, 0, 0, false);
      List<ShowTimeSeriesResult> result =
          manager.showTimeseries(showTimeSeriesPlan, new QueryContext());
    } catch (MetadataException e) {
      assertEquals(
          "Cannot get node of children in different aligned timeseries (Path: (s0,s1))",
          e.getMessage());
    }
  }

  @Test
  public void testCountTimeseriesWithTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));
    measurementList.add(Collections.singletonList("s1"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Collections.singletonList(TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      compressionTypes.add(compressionType);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s0");
    schemaNames.add("s1");

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);
    MManager manager = IoTDB.metaManager;
    try {
      manager.createDeviceTemplate(plan);

      // set device template
      SetDeviceTemplatePlan setDeviceTemplatePlan =
          new SetDeviceTemplatePlan("template1", "root.laptop.d1");
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      manager.getDeviceNode(new PartialPath("root.laptop.d1")).setUseTemplate(true);

      manager.createTimeseries(
          new PartialPath("root.computer.d1.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      setDeviceTemplatePlan = new SetDeviceTemplatePlan("template1", "root.computer");
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      manager.getDeviceNode(new PartialPath("root.computer.d1")).setUseTemplate(true);

      Assert.assertEquals(2, manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.laptop.d1.s1")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1.s1")));
      Assert.assertEquals(1, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1.s2")));
      Assert.assertEquals(3, manager.getAllTimeseriesCount(new PartialPath("root.computer.d1")));
      Assert.assertEquals(3, manager.getAllTimeseriesCount(new PartialPath("root.computer")));
      Assert.assertEquals(5, manager.getAllTimeseriesCount(new PartialPath("root")));

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCountDeviceWithTemplate() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s0"));
    measurementList.add(Collections.singletonList("s1"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT32));
    dataTypeList.add(Collections.singletonList(TSDataType.FLOAT));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.RLE));

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      compressionTypes.add(compressionType);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s0");
    schemaNames.add("s1");

    CreateTemplatePlan plan =
        new CreateTemplatePlan(
            "template1",
            schemaNames,
            measurementList,
            dataTypeList,
            encodingList,
            compressionTypes);
    MManager manager = IoTDB.metaManager;

    try {
      manager.createDeviceTemplate(plan);
      // set device template
      SetDeviceTemplatePlan setDeviceTemplatePlan =
          new SetDeviceTemplatePlan("template1", "root.laptop.d1");
      manager.setDeviceTemplate(setDeviceTemplatePlan);
      manager.getDeviceNode(new PartialPath("root.laptop.d1")).setUseTemplate(true);

      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      Assert.assertEquals(1, manager.getDevicesNum(new PartialPath("root.laptop.d1")));
      Assert.assertEquals(1, manager.getDevicesNum(new PartialPath("root.laptop.d2")));
      Assert.assertEquals(2, manager.getDevicesNum(new PartialPath("root.laptop")));

    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testTotalSeriesNumber() throws Exception {
    MManager manager = IoTDB.metaManager;

    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d0"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s2.t1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s3"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s1"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);
      manager.createTimeseries(
          new PartialPath("root.laptop.d2.s2"),
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.GZIP,
          null);

      assertEquals(6, manager.getTotalSeriesNumber());
      EnvironmentUtils.restartDaemon();
      assertEquals(6, manager.getTotalSeriesNumber());
      manager.deleteTimeseries(new PartialPath("root.laptop.d2.s1"));
      assertEquals(5, manager.getTotalSeriesNumber());
      manager.deleteStorageGroups(Collections.singletonList(new PartialPath("root.laptop")));
      assertEquals(0, manager.getTotalSeriesNumber());
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testStorageGroupNameWithHyphen() throws IllegalPathException {
    MManager manager = IoTDB.metaManager;
    assertTrue(manager.isPathExist(new PartialPath("root")));

    assertFalse(manager.isPathExist(new PartialPath("root.group-with-hyphen")));

    try {
      manager.setStorageGroup(new PartialPath("root.group-with-hyphen"));
    } catch (MetadataException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    assertTrue(manager.isPathExist(new PartialPath("root.group-with-hyphen")));
  }

  @Test
  public void testCreateAlignedTimeseriesAndInsertWithMismatchDataType() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.vector"),
          Arrays.asList("s1", "s2", "s3"),
          Arrays.asList(
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT64"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          compressionType);

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes =
          new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.INT32};

      String[] columns = new String[3];
      columns[0] = 2.0 + "";
      columns[1] = 10000 + "";
      columns[2] = 100 + "";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.vector"),
              time,
              new String[] {"s1", "s2", "s3"},
              dataTypes,
              columns,
              true);
      insertRowPlan.setMeasurementMNodes(
          new MeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      MNode mNode = manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertEquals(3, manager.getAllTimeseriesCount(mNode.getPartialPath()));
      assertEquals(1, mNode.getMeasurementMNodeCount());
      assertNull(insertRowPlan.getMeasurementMNodes()[0]);
      assertNull(insertRowPlan.getMeasurementMNodes()[1]);
      assertNull(insertRowPlan.getMeasurementMNodes()[2]);
      assertEquals(3, insertRowPlan.getFailedMeasurementNumber());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateAlignedTimeseriesAndInsertWithNotAlignedData() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createAlignedTimeSeries(
          new PartialPath("root.laptop.d1.vector"),
          Arrays.asList("s1", "s2", "s3"),
          Arrays.asList(
              TSDataType.valueOf("FLOAT"),
              TSDataType.valueOf("INT64"),
              TSDataType.valueOf("INT32")),
          Arrays.asList(
              TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE"), TSEncoding.valueOf("RLE")),
          compressionType);

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes =
          new TSDataType[] {TSDataType.FLOAT, TSDataType.INT64, TSDataType.INT32};

      String[] columns = new String[3];
      columns[0] = "1.0";
      columns[1] = "2";
      columns[2] = "3";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.vector"),
              time,
              new String[] {"s1", "s2", "s3"},
              dataTypes,
              columns,
              false);
      insertRowPlan.setMeasurementMNodes(
          new MeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(
          "Path [root.laptop.d1.vector] is an aligned timeseries, please set InsertPlan.isAligned() = true",
          e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsertWithMismatchDataType() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.s0"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT};

      String[] columns = new String[1];
      columns[0] = 2.0 + "";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1"), time, new String[] {"s0"}, dataTypes, columns);
      insertRowPlan.setMeasurementMNodes(
          new MeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      MNode mNode = manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
      assertEquals(1, mNode.getMeasurementMNodeCount());
      assertNull(insertRowPlan.getMeasurementMNodes()[0]);
      assertEquals(1, insertRowPlan.getFailedMeasurementNumber());

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testCreateTimeseriesAndInsertWithAlignedData() {
    MManager manager = IoTDB.metaManager;
    try {
      manager.setStorageGroup(new PartialPath("root.laptop"));
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.vector.s1"),
          TSDataType.valueOf("INT32"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());
      manager.createTimeseries(
          new PartialPath("root.laptop.d1.vector.s2"),
          TSDataType.valueOf("INT64"),
          TSEncoding.valueOf("RLE"),
          compressionType,
          Collections.emptyMap());

      // construct an insertRowPlan with mismatched data type
      long time = 1L;
      TSDataType[] dataTypes = new TSDataType[] {TSDataType.INT32, TSDataType.INT64};

      String[] columns = new String[2];
      columns[0] = "1";
      columns[1] = "2";

      InsertRowPlan insertRowPlan =
          new InsertRowPlan(
              new PartialPath("root.laptop.d1.vector"),
              time,
              new String[] {"s1", "s2"},
              dataTypes,
              columns,
              true);
      insertRowPlan.setMeasurementMNodes(
          new MeasurementMNode[insertRowPlan.getMeasurements().length]);

      // call getSeriesSchemasAndReadLockDevice
      manager.getSeriesSchemasAndReadLockDevice(insertRowPlan);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(
          "Path [root.laptop.d1.vector] is not an aligned timeseries, please set InsertPlan.isAligned() = false",
          e.getMessage());
    }
  }

  @Test
  public void testGetStorageGroupNodeByPath() {
    MManager manager = IoTDB.metaManager;
    PartialPath partialPath = null;

    try {
      partialPath = new PartialPath("root.ln.sg1");
    } catch (IllegalPathException e) {
      fail(e.getMessage());
    }

    try {
      manager.setStorageGroup(partialPath);
    } catch (MetadataException e) {
      fail(e.getMessage());
    }

    try {
      partialPath = new PartialPath("root.ln.sg2.device1.sensor1");
    } catch (IllegalPathException e) {
      fail(e.getMessage());
    }

    try {
      manager.getStorageGroupNodeByPath(partialPath);
    } catch (StorageGroupNotSetException e) {
      Assert.assertEquals(
          "Storage group is not set for current seriesPath: [root.ln.sg2.device1.sensor1]",
          e.getMessage());
    } catch (MetadataException e) {
      fail(e.getMessage());
    }
  }
}
