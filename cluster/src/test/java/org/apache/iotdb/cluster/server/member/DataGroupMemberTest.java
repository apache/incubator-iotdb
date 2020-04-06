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

package org.apache.iotdb.cluster.server.member;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.common.TestDataClient;
import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.ReaderNotFoundException;
import org.apache.iotdb.cluster.exception.SnapshotApplicationException;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.RemoteFileSnapshot;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullSnapshotHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullTimeseriesSchemaHandler;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataGroupMemberTest extends MemberTest {

  private DataGroupMember dataGroupMember;
  private Map<Integer, FileSnapshot> snapshotMap;
  private Map<Integer, RemoteFileSnapshot> receivedSnapshots;
  private Set<Integer> pulledSnapshots;
  private boolean hasInitialSnapshots;
  private boolean enableSyncLeader;
  private int prevReplicationNum;

  @Before
  public void setUp() throws Exception {
    prevReplicationNum = ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();
    ClusterDescriptor.getINSTANCE().getConfig().setReplicationNum(3);
    super.setUp();
    dataGroupMember = getDataGroupMember(TestUtils.getNode(0));
    snapshotMap = new HashMap<>();
    for (int i = 0; i < ClusterConstant.SLOT_NUM; i++) {
      FileSnapshot fileSnapshot = new FileSnapshot();
      fileSnapshot.setTimeseriesSchemas(Collections.singleton(TestUtils.getTestSchema(1, i)));
      snapshotMap.put(i, fileSnapshot);
    }
    receivedSnapshots = new HashMap<>();
    pulledSnapshots = new HashSet<>();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    ClusterDescriptor.getINSTANCE().getConfig().setReplicationNum(prevReplicationNum);
  }

  private PartitionedSnapshotLogManager getLogManager(PartitionGroup partitionGroup) {
    return new TestPartitionedLogManager(new DataLogApplier(testMetaMember),
        testMetaMember.getPartitionTable(), partitionGroup.getHeader(), FileSnapshot::new) {
      @Override
      public Snapshot getSnapshot() {
        PartitionedSnapshot<FileSnapshot> snapshot = new PartitionedSnapshot<>(FileSnapshot::new);
        if (hasInitialSnapshots) {
          for (int i = 0; i < 100; i++) {
            snapshot.putSnapshot(i, snapshotMap.get(i));
          }
        }
        return snapshot;
      }

      @Override
      public void setSnapshot(Snapshot snapshot, int slot) {
        receivedSnapshots.put(slot, (RemoteFileSnapshot) snapshot);
      }
    };
  }

  DataGroupMember getDataGroupMember(Node node) {
    PartitionGroup nodes = partitionTable.getHeaderGroup(node);
    return getDataGroupMember(node, nodes);
  }

  private DataGroupMember getDataGroupMember(Node node, PartitionGroup nodes) {
    return new DataGroupMember(new TCompactProtocol.Factory(), nodes, node, getLogManager(nodes),
        testMetaMember) {
      @Override
      public boolean syncLeader() {
        return true;
      }

      @Override
      public AsyncClient connectNode(Node node) {
        try {
          return new TestDataClient(node, dataGroupMemberMap) {

            @Override
            public void pullSnapshot(PullSnapshotRequest request,
                AsyncMethodCallback<PullSnapshotResp> resultHandler) {
              new Thread(() -> {
                PullSnapshotResp resp = new PullSnapshotResp();
                Map<Integer, ByteBuffer> snapshotBufferMap = new HashMap<>();
                for (Integer requiredSlot : request.getRequiredSlots()) {
                  FileSnapshot fileSnapshot = snapshotMap.get(requiredSlot);
                  if (fileSnapshot != null) {
                    snapshotBufferMap.put(requiredSlot, fileSnapshot.serialize());
                  }
                  synchronized (dataGroupMember) {
                    pulledSnapshots.add(requiredSlot);
                  }
                }
                resp.setSnapshotBytes(snapshotBufferMap);
                resultHandler.onComplete(resp);
              }).start();
            }

            @Override
            public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                if (enableSyncLeader) {
                  resultHandler.onComplete(-1L);
                } else {
                  resultHandler.onError(new TestException());
                }
              }).start();
            }
          };
        } catch (IOException e) {
          return null;
        }
      }
    };
  }

  @Test
  public void testGetHeader() {
    assertEquals(TestUtils.getNode(0), dataGroupMember.getHeader());
  }

  @Test
  public void testAddNode() {
    PartitionGroup partitionGroup = new PartitionGroup(TestUtils.getNode(0),
        TestUtils.getNode(50), TestUtils.getNode(90));
    DataGroupMember firstMember = getDataGroupMember(TestUtils.getNode(0),
        new PartitionGroup(partitionGroup));
    DataGroupMember midMember = getDataGroupMember(TestUtils.getNode(50), new PartitionGroup(partitionGroup));
    DataGroupMember lastMember = getDataGroupMember(TestUtils.getNode(90), new PartitionGroup(partitionGroup));

    Node newNodeBeforeGroup = TestUtils.getNode(-5);
    assertFalse(firstMember.addNode(newNodeBeforeGroup));
    assertFalse(midMember.addNode(newNodeBeforeGroup));
    assertFalse(lastMember.addNode(newNodeBeforeGroup));

    Node newNodeInGroup = TestUtils.getNode(66);
    assertFalse(firstMember.addNode(newNodeInGroup));
    assertFalse(midMember.addNode(newNodeInGroup));
    assertTrue(lastMember.addNode(newNodeInGroup));

    Node newNodeAfterGroup = TestUtils.getNode(101);
    assertFalse(firstMember.addNode(newNodeAfterGroup));
    assertFalse(midMember.addNode(newNodeAfterGroup));
  }

  @Test
  public void testProcessElectionRequest() {
    dataGroupMember.getLogManager().setLastLogId(10);
    dataGroupMember.getLogManager().setLastLogTerm(10);
    dataGroupMember.getTerm().set(10);
    testMetaMember.getTerm().set(10);
    metaLogManager.setLastLogId(5);
    metaLogManager.setLastLogTerm(5);

    // a valid request
    ElectionRequest electionRequest = new ElectionRequest();
    electionRequest.setTerm(11);
    electionRequest.setLastLogIndex(100);
    electionRequest.setLastLogTerm(100);
    electionRequest.setDataLogLastTerm(100);
    electionRequest.setDataLogLastIndex(100);
    assertEquals(Response.RESPONSE_AGREE, dataGroupMember.processElectionRequest(electionRequest));

    // a request with too small term
    electionRequest.setTerm(1);
    assertEquals(11, dataGroupMember.processElectionRequest(electionRequest));

    // a request with stale meta log
    electionRequest.setTerm(11);
    electionRequest.setLastLogIndex(1);
    electionRequest.setLastLogTerm(1);
    assertEquals(Response.RESPONSE_META_LOG_STALE,
        dataGroupMember.processElectionRequest(electionRequest));

    // a request with stale data log
    electionRequest.setTerm(12);
    electionRequest.setLastLogIndex(100);
    electionRequest.setLastLogTerm(100);
    electionRequest.setDataLogLastTerm(1);
    electionRequest.setDataLogLastIndex(1);
    assertEquals(Response.RESPONSE_LOG_MISMATCH,
        dataGroupMember.processElectionRequest(electionRequest));
  }

  @Test
  public void testSendSnapshot() {
    PartitionedSnapshot<FileSnapshot> partitionedSnapshot =
        new PartitionedSnapshot<>(FileSnapshot::new);
    partitionedSnapshot.setLastLogId(100);
    partitionedSnapshot.setLastLogTerm(100);

    for (int i = 0; i < 3; i++) {
      FileSnapshot fileSnapshot = new FileSnapshot();
      partitionedSnapshot.putSnapshot(i, fileSnapshot);
    }
    ByteBuffer serialize = partitionedSnapshot.serialize();

    SendSnapshotRequest request = new SendSnapshotRequest();
    request.setSnapshotBytes(serialize);
    AtomicBoolean callbackCalled = new AtomicBoolean(false);
    dataGroupMember.sendSnapshot(request, new AsyncMethodCallback() {
      @Override
      public void onComplete(Object o) {
        callbackCalled.set(true);
      }

      @Override
      public void onError(Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    });

    assertTrue(callbackCalled.get());
    assertEquals(100, dataGroupMember.getLogManager().getLastLogIndex());
    assertEquals(100, dataGroupMember.getLogManager().getLastLogTerm());
  }

  @Test
  public void testApplySnapshot()
      throws StorageEngineException, IOException, WriteProcessException, SnapshotApplicationException {
    FileSnapshot snapshot = new FileSnapshot();
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      schemaList.add(TestUtils.getTestSchema(0, i));
    }
    snapshot.setTimeseriesSchemas(schemaList);

    // resource1 exists locally, resource2 does not exist locally and without modification,
    // resource2 does not exist locally and with modification
    snapshot.addFile(prepareResource(0, false), TestUtils.getNode(0));
    snapshot.addFile(prepareResource(1, false), TestUtils.getNode(0));
    snapshot.addFile(prepareResource(2, true), TestUtils.getNode(0));

    // create a local resource1
    StorageGroupProcessor processor = StorageEngine.getInstance()
        .getProcessor(TestUtils.getTestSg(0));
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setTime(0);
    insertPlan.setMeasurements(new String[]{"s0"});
    insertPlan.setSchemas(new MeasurementSchema[] {TestUtils.getTestSchema(0, 0)});
    insertPlan.setValues(new String[]{"1.0"});
    processor.insert(insertPlan);
    processor.asyncCloseAllWorkingTsFileProcessors();

    dataGroupMember.applySnapshot(snapshot);
    assertEquals(3, processor.getSequenceFileTreeSet().size());
    assertEquals(0, processor.getUnSequenceFileList().size());
    Deletion deletion = new Deletion(new Path(TestUtils.getTestSg(0)), 0, 0);
    assertTrue(processor.getSequenceFileTreeSet().get(2).getModFile().getModifications()
        .contains(deletion));
  }

  @Test
  public void testForwardPullSnapshot() throws InterruptedException {
    dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    PullSnapshotRequest request = new PullSnapshotRequest();
    List<Integer> requiredSlots = Arrays.asList(1, 3, 5, 7, 9);
    request.setRequiredSlots(requiredSlots);
    AtomicReference<Map<Integer, FileSnapshot>> reference = new AtomicReference<>();
    PullSnapshotHandler<FileSnapshot> handler = new PullSnapshotHandler<>(reference,
        TestUtils.getNode(1), request.getRequiredSlots(), FileSnapshot::new);
    synchronized (reference) {
      dataGroupMember.pullSnapshot(request, handler);
      reference.wait(500);
    }
    assertEquals(requiredSlots.size(), reference.get().size());
    for (Integer requiredSlot : requiredSlots) {
      assertEquals(snapshotMap.get(requiredSlot), reference.get().get(requiredSlot));
    }
  }

  @Test
  public void testPullSnapshot() throws InterruptedException {
    hasInitialSnapshots = true;
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    PullSnapshotRequest request = new PullSnapshotRequest();
    List<Integer> requiredSlots = Arrays.asList(1, 3, 5, 7, 9, 11, 101);
    request.setRequiredSlots(requiredSlots);
    AtomicReference<Map<Integer, FileSnapshot>> reference = new AtomicReference<>();
    PullSnapshotHandler<FileSnapshot> handler = new PullSnapshotHandler<>(reference,
        TestUtils.getNode(1), request.getRequiredSlots(), FileSnapshot::new);
    synchronized (reference) {
      dataGroupMember.pullSnapshot(request, handler);
      reference.wait( 500);
    }
    assertEquals(requiredSlots.size() - 1, reference.get().size());
    for (int i = 0; i < requiredSlots.size() - 1; i++) {
      Integer requiredSlot = requiredSlots.get(i);
      assertEquals(snapshotMap.get(requiredSlot), reference.get().get(requiredSlot));
    }
  }

  @Test
  public void testPullRemoteSnapshot() throws TTransportException {
    dataGroupMember.start();
    try {
      hasInitialSnapshots = false;
      partitionTable.addNode(TestUtils.getNode(100));
      List<Integer> requiredSlots =
          new ArrayList<>(partitionTable.getPreviousNodeMap(TestUtils.getNode(100)).keySet());
      dataGroupMember.pullNodeAdditionSnapshots(requiredSlots, TestUtils.getNode(100));
      assertEquals(requiredSlots.size(), receivedSnapshots.size());
      for (Integer requiredSlot : requiredSlots) {
        receivedSnapshots.get(requiredSlot).getRemoteSnapshot();
        assertTrue(MManager.getInstance().isPathExist(TestUtils.getTestSeries(1, requiredSlot)));
      }
    } finally {
      dataGroupMember.stop();
    }
  }

  @Test
  public void testFollowerExecuteNonQuery() {
    dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    MeasurementSchema measurementSchema = TestUtils.getTestSchema(20, 0);
    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(new Path(measurementSchema.getMeasurementId()),
            measurementSchema.getType(), measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(), measurementSchema.getProps());
    assertEquals(200, dataGroupMember.executeNonQuery(createTimeSeriesPlan).code);
    assertTrue(MManager.getInstance().isPathExist(measurementSchema.getMeasurementId()));
  }

  @Test
  public void testLeaderExecuteNonQuery() {
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    MeasurementSchema measurementSchema = TestUtils.getTestSchema(2, 0);
    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(new Path(measurementSchema.getMeasurementId()),
            measurementSchema.getType(), measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(), measurementSchema.getProps());
    assertEquals(200, dataGroupMember.executeNonQuery(createTimeSeriesPlan).code);
    assertTrue(MManager.getInstance().isPathExist(measurementSchema.getMeasurementId()));
  }

  @Test
  public void testPullTimeseries() throws InterruptedException {
    int prevTimeOut = RaftServer.connectionTimeoutInMS;
    int prevMaxWait = RaftServer.syncLeaderMaxWaitMs;
    RaftServer.connectionTimeoutInMS = 20;
    RaftServer.syncLeaderMaxWaitMs = 200;
    try {
      // sync with leader is temporarily disabled, the request should be forward to the leader
      dataGroupMember.setLeader(TestUtils.getNode(1));
      dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
      enableSyncLeader = false;

      PullSchemaRequest request = new PullSchemaRequest();
      request.setPrefixPaths(Collections.singletonList(TestUtils.getTestSg(0)));
      AtomicReference<List<MeasurementSchema>> result = new AtomicReference<>();
      PullTimeseriesSchemaHandler handler = new PullTimeseriesSchemaHandler(TestUtils.getNode(1),
          request.getPrefixPaths(), result);
      synchronized (result) {
        dataGroupMember.pullTimeSeriesSchema(request, handler);
        result.wait(500);
      }
      for (int i = 0; i < 10; i++) {
        assertEquals(TestUtils.getTestSchema(0, i), result.get().get(i));
      }

      // the member is a leader itself
      dataGroupMember.setCharacter(NodeCharacter.LEADER);
      result.set(null);
      handler = new PullTimeseriesSchemaHandler(TestUtils.getNode(1),
          request.getPrefixPaths(), result);
      synchronized (result) {
        dataGroupMember.pullTimeSeriesSchema(request, handler);
        result.wait(500);
      }
      for (int i = 0; i < 10; i++) {
        assertEquals(TestUtils.getTestSchema(0, i), result.get().get(i));
      }
    } finally {
      RaftServer.connectionTimeoutInMS = prevTimeOut;
      RaftServer.syncLeaderMaxWaitMs = prevMaxWait;
    }
  }

  @Test
  public void testQuerySingleSeries() throws QueryProcessException, InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setSchemas(new MeasurementSchema[] {TestUtils.getTestSchema(0, 0)});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[] {String.valueOf(i)});
      PlanExecutor PlanExecutor = new PlanExecutor();
      PlanExecutor.processNonQuery(insertPlan);
    }

    // node1 manages the data above
    dataGroupMember.setThisNode(TestUtils.getNode(10));
    dataGroupMember.setAllNodes(partitionTable.getHeaderGroup(TestUtils.getNode(10)));
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setRequester(TestUtils.getNode(1));
    request.setQueryId(0);
    Filter filter = TimeFilter.gtEq(5);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    request.setTimeFilterBytes(byteArrayOutputStream.toByteArray());

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      dataGroupMember.querySingleSeries(request, handler);
      result.wait(200);
    }
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0),
        dataResult);
    synchronized (dataResult) {
      dataGroupMember.fetchSingleSeries(TestUtils.getNode(0), readerId, dataHandler);
      dataResult.wait(200);
    }
    ByteBuffer dataBuffer = dataResult.get();
    BatchData batchData = SerializeUtils.deserializeBatchData(dataBuffer);
    for (int i = 5; i < 10; i++) {
      assertTrue(batchData.hasCurrent());
      assertEquals(i, batchData.currentTime());
      assertEquals(i * 1.0, batchData.getDouble(), 0.00001);
      batchData.next();
    }
    assertFalse(batchData.hasCurrent());

    dataGroupMember.endQuery(TestUtils.getNode(0), TestUtils.getNode(1), 0,
        new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesWithValueFilter() throws QueryProcessException,
      InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setSchemas(new MeasurementSchema[] {TestUtils.getTestSchema(0, 0)});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[] {String.valueOf(i)});
      PlanExecutor PlanExecutor = new PlanExecutor();
      PlanExecutor.processNonQuery(insertPlan);
    }

    // node1 manages the data above
    dataGroupMember.setThisNode(TestUtils.getNode(10));
    dataGroupMember.setAllNodes(partitionTable.getHeaderGroup(TestUtils.getNode(10)));
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setRequester(TestUtils.getNode(1));
    request.setQueryId(0);
    Filter filter = new AndFilter(TimeFilter.gtEq(5), ValueFilter.ltEq(8.0));
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    request.setTimeFilterBytes(byteArrayOutputStream.toByteArray());

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      dataGroupMember.querySingleSeries(request, handler);
      result.wait(200);
    }
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0),
        dataResult);
    synchronized (dataResult) {
      dataGroupMember.fetchSingleSeries(TestUtils.getNode(0), readerId, dataHandler);
      dataResult.wait(200);
    }
    ByteBuffer dataBuffer = dataResult.get();
    BatchData batchData = SerializeUtils.deserializeBatchData(dataBuffer);
    for (int i = 5; i < 9; i++) {
      assertTrue(batchData.hasCurrent());
      assertEquals(i, batchData.currentTime());
      assertEquals(i * 1.0, batchData.getDouble(), 0.00001);
      batchData.next();
    }
    assertFalse(batchData.hasCurrent());

    dataGroupMember.endQuery(TestUtils.getNode(0), TestUtils.getNode(1), 0,
        new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesByTimestamp() throws QueryProcessException, InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setSchemas(new MeasurementSchema[] {TestUtils.getTestSchema(0, 0)});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[] {String.valueOf(i)});
      PlanExecutor PlanExecutor = new PlanExecutor();
      PlanExecutor.processNonQuery(insertPlan);
    }

    // node1 manages the data above
    dataGroupMember.setThisNode(TestUtils.getNode(10));
    dataGroupMember.setAllNodes(partitionTable.getHeaderGroup(TestUtils.getNode(10)));
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setRequester(TestUtils.getNode(1));
    request.setQueryId(0);
    Filter filter = TimeFilter.gtEq(5);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    request.setTimeFilterBytes(byteArrayOutputStream.toByteArray());

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      dataGroupMember.querySingleSeriesByTimestamp(request, handler);
      result.wait(200);
    }
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0),
        dataResult);

    for (int i = 5; i < 10; i++) {
      synchronized (dataResult) {
        dataGroupMember.fetchSingleSeriesByTimestamp(TestUtils.getNode(0), readerId, i,
            dataHandler);
        dataResult.wait(200);
      }
      Object value =  SerializeUtils.deserializeObject(dataResult.get());
      assertEquals(i * 1.0, (Double) value, 0.00001);
    }

    dataGroupMember.endQuery(TestUtils.getNode(0), TestUtils.getNode(1), 0,
        new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesByTimestampWithValueFilter() throws QueryProcessException,
      InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setSchemas(new MeasurementSchema[] {TestUtils.getTestSchema(0, 0)});
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[] {String.valueOf(i)});
      PlanExecutor PlanExecutor = new PlanExecutor();
      PlanExecutor.processNonQuery(insertPlan);
    }

    // node1 manages the data above
    dataGroupMember.setThisNode(TestUtils.getNode(10));
    dataGroupMember.setAllNodes(partitionTable.getHeaderGroup(TestUtils.getNode(10)));
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setRequester(TestUtils.getNode(10));
    request.setQueryId(0);
    Filter filter = new AndFilter(TimeFilter.gtEq(5), ValueFilter.ltEq(8.0));
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    filter.serialize(dataOutputStream);
    request.setTimeFilterBytes(byteArrayOutputStream.toByteArray());

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    synchronized (result) {
      dataGroupMember.querySingleSeriesByTimestamp(request, handler);
      result.wait(200);
    }
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0),
        dataResult);
    for (int i = 5; i < 9; i++) {
      synchronized (dataResult) {
        dataGroupMember.fetchSingleSeriesByTimestamp(TestUtils.getNode(0), readerId, i,
            dataHandler);
        dataResult.wait(200);
      }
      Object value =  SerializeUtils.deserializeObject(dataResult.get());
      assertEquals(i * 1.0, (Double) value, 0.00001);
    }


    dataGroupMember.endQuery(TestUtils.getNode(0), TestUtils.getNode(1), 0,
        new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testGetPaths() throws InterruptedException {
    String path = TestUtils.getTestSg(0);
    AtomicReference<List<String>> pathResult = new AtomicReference<>();
    GenericHandler<List<String>> handler = new GenericHandler<>(TestUtils.getNode(0), pathResult);
    synchronized (pathResult) {
      dataGroupMember.getAllPaths(TestUtils.getNode(0), Collections.singletonList(path), handler);
      pathResult.wait(200);
    }
    List<String> result = pathResult.get();
    assertEquals(20, result.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(TestUtils.getTestSeries(0, i), result.get(i));
    }
  }

  @Test
  public void testFetchWithoutQuery() throws InterruptedException {
    AtomicReference<Exception> result = new AtomicReference<>();
    synchronized (result) {
      dataGroupMember.fetchSingleSeriesByTimestamp(TestUtils.getNode(0), 0, 0,
          new AsyncMethodCallback<ByteBuffer>() {
            @Override
            public void onComplete(ByteBuffer buffer) {
              new Thread(() -> {
                synchronized (result) {
                  result.notifyAll();
                }
              }).start();
            }

            @Override
            public void onError(Exception e) {
              new Thread(() -> {
                synchronized (result) {
                  result.set(e);
                  result.notifyAll();
                }
              }).start();
            }
          });
      result.wait(200);
    }
    Exception exception = result.get();
    assertTrue(exception instanceof ReaderNotFoundException);
    assertEquals("The requested reader 0 is not found", exception.getMessage());

    synchronized (result) {
      dataGroupMember.fetchSingleSeries(TestUtils.getNode(0), 0,
          new AsyncMethodCallback<ByteBuffer>() {
            @Override
            public void onComplete(ByteBuffer buffer) {
              new Thread(() -> {
                synchronized (result) {
                  result.notifyAll();
                }
              }).start();
            }

            @Override
            public void onError(Exception e) {
              new Thread(() -> {
                synchronized (result) {
                  result.set(e);
                  result.notifyAll();
                }
              }).start();
            }
          });
      result.wait(200);
    }
    exception = result.get();
    assertTrue(exception instanceof ReaderNotFoundException);
    assertEquals("The requested reader 0 is not found", exception.getMessage());
  }

  private TsFileResource prepareResource(int serialNum, boolean withModification)
      throws IOException {
    TsFileResource resource = new RemoteTsFileResource();
    File file = new File("target" + File.separator + TestUtils.getTestSg(0) + File.separator + "0",
        "0-" + (serialNum + 101L) + "-0.tsfile");
    file.getParentFile().mkdirs();
    file.createNewFile();

    resource.setFile(file);
    resource.setHistoricalVersions(Collections.singleton(serialNum + 101L));
    resource.updateStartTime(TestUtils.getTestSg(0), serialNum * 100);
    resource.updateEndTime(TestUtils.getTestSg(0), (serialNum + 1) * 100 - 1);
    if (withModification) {
      Deletion deletion = new Deletion(new Path(TestUtils.getTestSg(0)), 0, 0);
      resource.getModFile().write(deletion);
      resource.getModFile().close();
    }
    return resource;
  }

  @Test
  public void testRemoveLeader() throws TTransportException, InterruptedException {
    Node nodeToRemove = TestUtils.getNode(10);
    NodeRemovalResult nodeRemovalResult = testMetaMember.getPartitionTable()
        .removeNode(nodeToRemove);
    dataGroupMember.setLeader(nodeToRemove);
    dataGroupMember.start();

    try {
      synchronized (dataGroupMember) {
        dataGroupMember.removeNode(nodeToRemove, nodeRemovalResult);
        dataGroupMember.wait(500);
      }

      assertEquals(NodeCharacter.ELECTOR, dataGroupMember.getCharacter());
      assertEquals(Long.MIN_VALUE, dataGroupMember.getLastHeartbeatReceivedTime());
      assertTrue(dataGroupMember.getAllNodes().contains(TestUtils.getNode(30)));
      assertFalse(dataGroupMember.getAllNodes().contains(nodeToRemove));
      List<Integer> newSlots = nodeRemovalResult.getNewSlotOwners().get(TestUtils.getNode(0));
      assertEquals(newSlots.size(), pulledSnapshots.size());
      for (Integer newSlot : newSlots) {
        assertTrue(pulledSnapshots.contains(newSlot));
      }
    } finally {
      dataGroupMember.stop();
    }
  }

  @Test
  public void testRemoveNonLeader() throws TTransportException, InterruptedException {
    Node nodeToRemove = TestUtils.getNode(10);
    NodeRemovalResult nodeRemovalResult = testMetaMember.getPartitionTable()
        .removeNode(nodeToRemove);
    dataGroupMember.setLeader(TestUtils.getNode(20));
    dataGroupMember.start();

    try {
      synchronized (dataGroupMember) {
        dataGroupMember.removeNode(nodeToRemove, nodeRemovalResult);
        dataGroupMember.wait(500);
      }

      assertEquals(0, dataGroupMember.getLastHeartbeatReceivedTime());
      assertTrue(dataGroupMember.getAllNodes().contains(TestUtils.getNode(30)));
      assertFalse(dataGroupMember.getAllNodes().contains(nodeToRemove));
      List<Integer> newSlots = nodeRemovalResult.getNewSlotOwners().get(TestUtils.getNode(0));
      assertEquals(newSlots.size(), pulledSnapshots.size());
      for (Integer newSlot : newSlots) {
        assertTrue(pulledSnapshots.contains(newSlot));
      }
    } finally {
      dataGroupMember.stop();
    }
  }

  @Test
  public void testGroupBy() throws QueryProcessException {
    TestUtils.prepareData();

    GroupByRequest request = new GroupByRequest();
    request.setPath(TestUtils.getTestSeries(0, 0));
    List<Integer> aggregationTypes = new ArrayList<>();
    for (AggregationType value : AggregationType.values()) {
      aggregationTypes.add(value.ordinal());
    }
    request.setAggregationTypeOrdinals(aggregationTypes);
    Filter timeFilter = TimeFilter.gtEq(5);
    request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    QueryContext queryContext =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    request.setQueryId(queryContext.getQueryId());
    request.setRequestor(TestUtils.getNode(0));
    request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
    request.setDeviceMeasurements(Collections.singleton(TestUtils.getTestMeasurement(0)));

    DataGroupMember dataGroupMember;
    AtomicReference<Long> resultRef;
    GenericHandler<Long> handler;
    Long executorId;
    AtomicReference<List<ByteBuffer>> aggrResultRef;
    GenericHandler<List<ByteBuffer>> aggrResultHandler;
    List<ByteBuffer> byteBuffers;
    List<AggregateResult> aggregateResults;
    Object[] answers;
    // get an executor from a node holding this timeseries
    request.setHeader(TestUtils.getNode(10));
    dataGroupMember = getDataGroupMember(TestUtils.getNode(10));
    resultRef = new AtomicReference<>();
    handler = new GenericHandler<>(TestUtils.getNode(0), resultRef);
    dataGroupMember.getGroupByExecutor(request, handler);
    executorId = resultRef.get();
    assertEquals(1L, (long) executorId);

    // fetch result
    aggrResultRef = new AtomicReference<>();
    aggrResultHandler = new GenericHandler<>(TestUtils.getNode(0), aggrResultRef);
    dataGroupMember.getGroupByResult(TestUtils.getNode(10), executorId, 0, 20, aggrResultHandler);

    byteBuffers = aggrResultRef.get();
    assertNotNull(byteBuffers);
    aggregateResults = new ArrayList<>();
    for (ByteBuffer byteBuffer : byteBuffers) {
      aggregateResults.add(AggregateResult.deserializeFrom(byteBuffer));
    }
    answers = new Object[] {15.0, 12.0, 180.0, 5.0, 19.0, 19.0, 5.0, 19.0, 5.0};
    checkAggregates(answers, aggregateResults);

    // get an executor from a node not holding this timeseries
    request.setHeader(TestUtils.getNode(30));
    dataGroupMember = getDataGroupMember(TestUtils.getNode(30));
    resultRef = new AtomicReference<>();
    handler = new GenericHandler<>(TestUtils.getNode(0), resultRef);
    request.timeFilterBytes.position(0);
    dataGroupMember.getGroupByExecutor(request, handler);
    executorId = resultRef.get();
    assertEquals(-1L, (long) executorId);

    // fetch result
    aggrResultRef = new AtomicReference<>();
    aggrResultHandler = new GenericHandler<>(TestUtils.getNode(0), aggrResultRef);
    dataGroupMember.getGroupByResult(TestUtils.getNode(30), executorId, 0, 20, aggrResultHandler);

    byteBuffers = aggrResultRef.get();
    assertNull(byteBuffers);
  }

  private void checkAggregates(Object[] answers, List<AggregateResult> aggregateResults) {
    assertEquals(answers.length, aggregateResults.size());
    for (int i = 0; i < aggregateResults.size(); i++) {
      if (answers[i] != null) {
        assertEquals((double) answers[i],
            Double.parseDouble(aggregateResults.get(i).getResult().toString()), 0.000001);
      } else {
        assertNull(aggregateResults.get(i).getResult());
      }
    }
  }
}