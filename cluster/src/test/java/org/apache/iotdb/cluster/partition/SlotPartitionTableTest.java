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
package org.apache.iotdb.cluster.partition;

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.slot.SlotNodeRemovalResult;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.query.ClusterPlanRouter;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.utils.Constants;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CountPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore // need maintenance
@SuppressWarnings({"java:S2699"})
public class SlotPartitionTableTest {

  Logger logger = LoggerFactory.getLogger(SlotPartitionTableTest.class);
  SlotPartitionTable localTable;
  Node localNode;
  int replica_size = 5;
  int raftId = 0;
  MManager[] mManager;

  SlotPartitionTable[] tables; // The PartitionTable on each node.
  List<Node> nodes;

  private int prevReplicaNum;
  private boolean prevEnablePartition;
  private long prevPartitionInterval;

  @Before
  public void setUp() throws MetadataException {
    prevEnablePartition = StorageEngine.isEnablePartition();
    prevPartitionInterval = StorageEngine.getTimePartitionInterval();
    StorageEngine.setEnablePartition(true);

    IoTDB.metaManager.init();
    StorageEngine.setTimePartitionInterval(7 * 24 * 3600 * 1000L);
    nodes = new ArrayList<>();
    IntStream.range(0, 20).forEach(i -> nodes.add(getNode(i)));
    localNode = nodes.get(3);
    prevReplicaNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(replica_size);
    tables = new SlotPartitionTable[20];

    // suppose there are 40 storage groups and each node maintains two of them.
    String[] storageNames = new String[40];
    List<String>[] nodeSGs = new ArrayList[20];
    for (int i = 0; i < 20; i++) {
      nodeSGs[i] = new ArrayList<>();
      tables[i] = new SlotPartitionTable(nodes, nodes.get(i));
    }
    localTable = tables[3];
    // thisNode hold No. 1500 to 1999 slot.

    for (int i = 0; i < 20; i++) {
      storageNames[i] = String.format("root.sg.l2.l3.%d", i);
      // determine which node the sg belongs to
      RaftNode node = localTable.routeToHeaderByTime(storageNames[i], 0);
      nodeSGs[node.getNode().getMetaPort() - 30000].add(storageNames[i]);
      storageNames[i + 20] = String.format("root.sg.l2.l3.l4.%d", i + 20);
      node = localTable.routeToHeaderByTime(storageNames[i + 20], 0);
      nodeSGs[node.getNode().getMetaPort() - 30000].add(storageNames[i + 20]);
    }
  }

  private void initMockMManager(
      int id, MManager mmanager, String[] storageGroups, List<String> ownedSGs)
      throws MetadataException {
    for (String sg : storageGroups) {
      mmanager.setStorageGroup(new PartialPath(sg));
    }
    for (String sg : ownedSGs) {
      // register 4 series;
      for (int i = 0; i < 4; i++) {
        try {
          mmanager.createTimeseries(
              new PartialPath(String.format(sg + ".ld.l1.d%d.s%d", i / 2, i % 2)),
              TSDataType.INT32,
              TSEncoding.RLE,
              CompressionType.SNAPPY,
              Collections.EMPTY_MAP);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(prevReplicaNum);
    if (mManager != null) {
      for (MManager manager : mManager) {
        manager.clear();
      }
    }
    EnvironmentUtils.cleanEnv();
    File[] files = new File("target/schemas").listFiles();
    if (files != null) {
      for (File file : files) {
        try {
          Files.delete(file.toPath());
        } catch (IOException e) {
          logger.error("{} can not be deleted.", file, e);
        }
      }
    }
    StorageEngine.setEnablePartition(prevEnablePartition);
    StorageEngine.setTimePartitionInterval(prevPartitionInterval);
  }

  @Test
  public void testGetHeaderGroup() {
    Arrays.stream(new int[] {10, 15, 19})
        .forEach(
            i -> {
              int last = (i + replica_size - 1) % 20;
              assertGetHeaderGroup(i, last);
            });
  }

  private void assertGetHeaderGroup(int start, int last) {
    PartitionGroup group =
        localTable.getHeaderGroup(
            new RaftNode(
                new Node(
                    "localhost",
                    30000 + start,
                    start,
                    40000 + start,
                    Constants.RPC_PORT + start,
                    "localhost"),
                0));
    assertEquals(replica_size, group.size());
    assertEquals(
        new Node(
            "localhost",
            30000 + start,
            start,
            40000 + start,
            Constants.RPC_PORT + start,
            "localhost"),
        group.getHeader().getNode());

    assertEquals(
        new Node(
            "localhost", 30000 + last, last, 40000 + last, Constants.RPC_PORT + last, "localhost"),
        group.get(replica_size - 1));
  }

  private void assertPartitionGroup(PartitionGroup group, int... nodeIds) {
    for (int i = 0; i < nodeIds.length; i++) {
      assertEquals(nodeIds[i], group.get(i).nodeIdentifier);
    }
  }

  @Test
  public void testRoute() {
    PartitionGroup group1 = localTable.route("root.sg1", 1);
    PartitionGroup group2 = localTable.route("root.sg1", 2);
    PartitionGroup group3 = localTable.route("root.sg2", 2);
    PartitionGroup group4 =
        localTable.route("root.sg1", 2 + StorageEngine.getTimePartitionInterval());

    assertEquals(group1, group2);
    assertNotEquals(group2, group3);
    assertNotEquals(group3, group4);

    PartitionGroup group = localTable.route(ClusterConstant.SLOT_NUM + 1);
    assertNull(group);
    // thisNode hold No. 1500 to 1999 slot.
    group1 = localTable.route(1501);
    group2 = localTable.route(1502);
    group3 = localTable.route(2501);
    assertEquals(group1, group2);
    assertNotEquals(group2, group3);
  }

  @Test
  public void routeToHeader() {
    RaftNode node1 = localTable.routeToHeaderByTime("root.sg.l2.l3.l4.28", 0);
    RaftNode node2 = localTable.routeToHeaderByTime("root.sg.l2.l3.l4.28", 1);
    RaftNode node3 =
        localTable.routeToHeaderByTime(
            "root.sg.l2.l3.l4.28", 1 + StorageEngine.getTimePartitionInterval());
    assertEquals(node1, node2);
    assertNotEquals(node2, node3);
  }

  @Test
  public void addNode() {
    // TODO do it when delete node is finished.
  }

  @Test
  public void getLocalGroups() {
    List<PartitionGroup> groups = localTable.getLocalGroups();
    int[][] nodeIds = new int[replica_size][replica_size];
    // we write them clearly to help people understand how the replica is assigned.
    nodeIds[0] = new int[] {3, 4, 5, 6, 7};
    nodeIds[1] = new int[] {2, 3, 4, 5, 6};
    nodeIds[2] = new int[] {1, 2, 3, 4, 5};
    nodeIds[3] = new int[] {0, 1, 2, 3, 4};
    nodeIds[4] = new int[] {19, 0, 1, 2, 3};
    for (int i = 0; i < nodeIds.length; i++) {
      assertPartitionGroup(groups.get(i), nodeIds[i]);
    }
  }

  @Test
  public void serializeAndDeserialize() {
    ByteBuffer buffer = localTable.serialize();
    SlotPartitionTable tmpTable = new SlotPartitionTable(new Node());
    tmpTable.deserialize(buffer);
    assertEquals(localTable, tmpTable);
  }

  @Test
  public void getAllNodes() {
    assertEquals(20, localTable.getAllNodes().size());
  }

  @Test
  public void getPreviousNodeMap() {
    // before adding or deleting node, it should be null
    assertNull(localTable.getPreviousNodeMap(new RaftNode(localNode, 0)));
    // TODO after adding or deleting node, it has data
  }

  @Test
  public void getNodeSlots() {
    // TODO only meaningful when nodelist changes
  }

  @Test
  public void getAllNodeSlots() {
    // TODO only meaningful when nodelist changes
  }

  @Test
  public void getTotalSlotNumbers() {
    assertEquals(ClusterConstant.SLOT_NUM, localTable.getTotalSlotNumbers());
  }

  @Test
  public void testPhysicalPlan() throws QueryProcessException, IllegalPathException {
    PhysicalPlan deletePlan = new DeletePlan();
    assertTrue(PartitionUtils.isGlobalDataPlan(deletePlan));

    try {
      PhysicalPlan authorPlan =
          new AuthorPlan(
              AuthorType.CREATE_ROLE,
              "test",
              "test",
              "test",
              "test",
              new String[] {},
              new PartialPath("root.sg.l2.l3.l4.28.ld.l1.d0"));
      assertTrue(PartitionUtils.isGlobalMetaPlan(authorPlan));
    } catch (AuthException | IllegalPathException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    PhysicalPlan deleteStorageGroup = new DeleteStorageGroupPlan(Collections.emptyList());
    assertTrue(PartitionUtils.isGlobalMetaPlan(deleteStorageGroup));
    PhysicalPlan globalLoadConfigPlan =
        new LoadConfigurationPlan(LoadConfigurationPlanType.GLOBAL, new Properties[2]);
    assertTrue(PartitionUtils.isGlobalMetaPlan(globalLoadConfigPlan));
    PhysicalPlan localLoadConfigPlan = new LoadConfigurationPlan(LoadConfigurationPlanType.LOCAL);
    assertFalse(PartitionUtils.isGlobalMetaPlan(localLoadConfigPlan));
    PhysicalPlan operateFilePlan = new OperateFilePlan(new File(""), OperatorType.LOAD_FILES);
    assertTrue(PartitionUtils.isLocalNonQueryPlan(operateFilePlan));

    PhysicalPlan setStorageGroupPlan = new SetStorageGroupPlan();
    assertTrue(PartitionUtils.isGlobalMetaPlan(setStorageGroupPlan));
    PhysicalPlan setTTLPlan = new SetTTLPlan(new PartialPath("root.group"));
    assertTrue(PartitionUtils.isGlobalMetaPlan(setTTLPlan));
  }

  // @Test
  public void testInsertPlan() throws IllegalPathException {
    PhysicalPlan insertPlan1 =
        new InsertRowPlan(
            new PartialPath("root.sg.l2.l3.l4.28.ld.l1.d0"),
            1,
            new String[] {"s0", "s1"},
            new String[] {"0", "1"});
    PhysicalPlan insertPlan2 =
        new InsertRowPlan(
            new PartialPath("root.sg.l2.l3.l4.28.ld.l1.d0"),
            1 + StorageEngine.getTimePartitionInterval(),
            new String[] {"s0", "s1"},
            new String[] {"0", "1"});
    PartitionGroup group1, group2;
    assertFalse(insertPlan1.canBeSplit());
    ClusterPlanRouter router = new ClusterPlanRouter(localTable);
    try {
      group1 = router.routePlan(insertPlan1);
      group2 = router.routePlan(insertPlan2);
      assertNotEquals(group1, group2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // @Test
  public void testCreateTimeSeriesPlan() throws IllegalPathException {
    PhysicalPlan createTimeSeriesPlan1 =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg.l2.l3.l4.28.ld" + ".l1.d1"),
            TSDataType.BOOLEAN,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null);
    PhysicalPlan createTimeSeriesPlan2 =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg.l2.l3.l4.28.ld" + ".l1.d2"),
            TSDataType.BOOLEAN,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null);
    PhysicalPlan createTimeSeriesPlan3 =
        new CreateTimeSeriesPlan(
            new PartialPath("root.sg.l2.l3.l4.29.ld" + ".l1.d2"),
            TSDataType.BOOLEAN,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null);
    assertFalse(createTimeSeriesPlan1.canBeSplit());
    ClusterPlanRouter router = new ClusterPlanRouter(localTable);

    try {
      PartitionGroup group1 = router.routePlan(createTimeSeriesPlan1);
      PartitionGroup group2 = router.routePlan(createTimeSeriesPlan2);
      PartitionGroup group3 = router.routePlan(createTimeSeriesPlan3);
      assertEquals(group1, group2);
      assertNotEquals(group2, group3);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // @Test
  public void testInsertTabletPlan() throws IllegalPathException {
    PhysicalPlan batchInertPlan =
        new InsertTabletPlan(
            new PartialPath("root.sg.l2.l3.l4.28.ld.l1" + ".d0"),
            new String[] {"s0", "s1"},
            Arrays.asList(0, 1));
    assertTrue(batchInertPlan.canBeSplit());
    // (String deviceId, String[] measurements, List<Integer> dataTypes)
    long[] times = new long[9];
    Object[] values = new Object[2];
    values[0] = new boolean[9];
    values[1] = new int[9];

    for (int i = 0; i < 3; i++) {
      times[i] = Math.abs(new Random().nextLong()) % StorageEngine.getTimePartitionInterval();
      ((boolean[]) values[0])[i] = new Random().nextBoolean();
      ((int[]) values[1])[i] = new Random().nextInt();
    }
    for (int i = 3; i < 6; i++) {
      times[i] =
          StorageEngine.getTimePartitionInterval()
              + Math.abs(new Random().nextLong()) % StorageEngine.getTimePartitionInterval();
      ((boolean[]) values[0])[i] = new Random().nextBoolean();
      ((int[]) values[1])[i] = new Random().nextInt();
    }
    for (int i = 6; i < 9; i++) {
      times[i] =
          StorageEngine.getTimePartitionInterval() * 10
              + Math.abs(new Random().nextLong()) % StorageEngine.getTimePartitionInterval();
      ((boolean[]) values[0])[i] = new Random().nextBoolean();
      ((int[]) values[1])[i] = new Random().nextInt();
    }
    ((InsertTabletPlan) batchInertPlan).setTimes(times);
    ((InsertTabletPlan) batchInertPlan).setColumns(values);
    ((InsertTabletPlan) batchInertPlan).setRowCount(9);
    try {
      ClusterPlanRouter router = new ClusterPlanRouter(localTable);
      Map<PhysicalPlan, PartitionGroup> result = router.splitAndRoutePlan(batchInertPlan);
      assertEquals(3, result.size());
      result.forEach(
          (key, value) -> {
            assertEquals(3, ((InsertTabletPlan) key).getRowCount());
            long[] subtimes = ((InsertTabletPlan) key).getTimes();
            assertEquals(3, subtimes.length);
            assertEquals(
                subtimes[0] / StorageEngine.getTimePartitionInterval(),
                subtimes[2] / StorageEngine.getTimePartitionInterval());
          });
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // @Test
  public void testCountPlan() throws IllegalPathException {
    PhysicalPlan countPlan1 =
        new CountPlan(
            ShowContentType.COUNT_TIMESERIES, new PartialPath("root.sg.*.l3" + ".l4.28.*"));
    PhysicalPlan countPlan2 =
        new CountPlan(ShowContentType.COUNT_TIMESERIES, new PartialPath("root.sg.*.l3" + ".*"));
    PhysicalPlan countPlan3 =
        new CountPlan(
            ShowContentType.COUNT_NODE_TIMESERIES, new PartialPath("root.sg" + ".l2.l3.l4.28"));
    PhysicalPlan countPlan4 =
        new CountPlan(
            ShowContentType.COUNT_NODE_TIMESERIES, new PartialPath("root.sg" + ".l2.l3"), 6);
    PhysicalPlan countPlan5 =
        new CountPlan(ShowContentType.COUNT_NODES, new PartialPath("root.sg.l2.l3"), 5);
    try {
      ClusterPlanRouter router = new ClusterPlanRouter(localTable);
      assertTrue(countPlan1.canBeSplit());
      Map<PhysicalPlan, PartitionGroup> result1 = router.splitAndRoutePlan(countPlan1);
      assertEquals(1, result1.size());
      Map<PhysicalPlan, PartitionGroup> result2 = router.splitAndRoutePlan(countPlan2);
      assertEquals(40, result2.size());
      Map<PhysicalPlan, PartitionGroup> result3 = router.splitAndRoutePlan(countPlan3);
      assertEquals(1, result3.size());
      Map<PhysicalPlan, PartitionGroup> result4 = router.splitAndRoutePlan(countPlan4);
      // TODO this case can be optimized
      assertEquals(40, result4.size());
      Map<PhysicalPlan, PartitionGroup> result5 = router.splitAndRoutePlan(countPlan5);
      // TODO this case can be optimized
      assertEquals(40, result5.size());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // @Test
  public void testShowChildPathsPlan() throws IllegalPathException {
    PhysicalPlan showChildPathsPlan1 =
        new ShowChildPathsPlan(ShowContentType.CHILD_PATH, new PartialPath("root.sg.l2.l3.l4.28"));
    PhysicalPlan showChildPathsPlan2 =
        new ShowChildPathsPlan(ShowContentType.CHILD_PATH, new PartialPath("root.sg.l2.l3.l4"));
    try {
      assertFalse(showChildPathsPlan1.canBeSplit());
      ClusterPlanRouter router = new ClusterPlanRouter(localTable);
      PartitionGroup group1 = router.routePlan(showChildPathsPlan1);
      PartitionGroup group2 = router.routePlan(showChildPathsPlan2);
      assertNotEquals(group1, group2);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testDataAuthPlan() {
    List<String> users = new ArrayList(Arrays.asList("user1", "user2"));
    PhysicalPlan dataAuthPlan = new DataAuthPlan(OperatorType.GRANT_WATERMARK_EMBEDDING, users);
    Assert.assertTrue(PartitionUtils.isGlobalMetaPlan(dataAuthPlan));
  }

  private Node getNode(int i) {
    return new Node("localhost", 30000 + i, i, 40000 + i, Constants.RPC_PORT + i, "localhost");
  }

  private RaftNode getRaftNode(int i, int raftId) {
    return new RaftNode(getNode(i), raftId);
  }

  @Test
  public void testRemoveNode() {
    List<Integer> nodeSlots = localTable.getNodeSlots(getRaftNode(0, raftId));
    localTable.removeNode(getNode(0));
    NodeRemovalResult nodeRemovalResult = localTable.getNodeRemovalResult();
    assertFalse(localTable.getAllNodes().contains(getNode(0)));
    PartitionGroup removedGroup = nodeRemovalResult.getRemovedGroup(0);
    for (int i = 0; i < 5; i++) {
      assertTrue(removedGroup.contains(getNode(i)));
    }
    // the slots owned by the removed one should be redistributed to other nodes
    Map<RaftNode, List<Integer>> newSlotOwners =
        ((SlotNodeRemovalResult) nodeRemovalResult).getNewSlotOwners();
    for (List<Integer> slots : newSlotOwners.values()) {
      assertTrue(nodeSlots.containsAll(slots));
      nodeSlots.removeAll(slots);
    }
    assertTrue(nodeSlots.isEmpty());
  }
}
