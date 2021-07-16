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

import org.apache.iotdb.cluster.RemoteTsFileResource;
import org.apache.iotdb.cluster.common.TestAsyncDataClient;
import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.ReaderNotFoundException;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotNodeAdditionResult;
import org.apache.iotdb.cluster.partition.slot.SlotNodeRemovalResult;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.GetAllPathsResult;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RequestCommitIndexResponse;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullMeasurementSchemaHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullSnapshotHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullTimeseriesSchemaHandler;
import org.apache.iotdb.cluster.server.service.DataAsyncService;
import org.apache.iotdb.cluster.utils.Constants;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataGroupMemberTest extends BaseMember {

  private DataGroupMember dataGroupMember;
  private Map<Integer, FileSnapshot> snapshotMap;
  private Set<Integer> pulledSnapshots;
  private boolean hasInitialSnapshots;
  private boolean enableSyncLeader;
  private int prevReplicationNum;

  @Override
  @Before
  public void setUp() throws Exception {
    prevReplicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(3);
    super.setUp();
    dataGroupMember = getDataGroupMember(TestUtils.getNode(0));
    snapshotMap = new HashMap<>();
    for (int i = 0; i < ClusterConstant.SLOT_NUM; i++) {
      FileSnapshot fileSnapshot = new FileSnapshot();
      fileSnapshot.setTimeseriesSchemas(
          Collections.singletonList(TestUtils.getTestTimeSeriesSchema(0, i)));
      snapshotMap.put(i, fileSnapshot);
    }
    pulledSnapshots = new ConcurrentSkipListSet<>();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    dataGroupMember.stop();
    super.tearDown();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(prevReplicationNum);
  }

  private PartitionedSnapshotLogManager getLogManager(
      PartitionGroup partitionGroup, DataGroupMember dataGroupMember) {
    return new TestPartitionedLogManager(
        new DataLogApplier(testMetaMember, dataGroupMember),
        testMetaMember.getPartitionTable(),
        partitionGroup.getHeader(),
        FileSnapshot.Factory.INSTANCE) {
      @Override
      public Snapshot getSnapshot(long minIndex) {
        PartitionedSnapshot<FileSnapshot> snapshot =
            new PartitionedSnapshot<>(FileSnapshot.Factory.INSTANCE);
        if (hasInitialSnapshots) {
          for (int i = 0; i < 100; i++) {
            snapshot.putSnapshot(i, snapshotMap.get(i));
          }
        }
        return snapshot;
      }
    };
  }

  @Override
  DataGroupMember getDataGroupMember(Node node) {
    PartitionGroup nodes = partitionTable.getHeaderGroup(node);
    return dataGroupMemberMap.computeIfAbsent(node, n -> getDataGroupMember(n, nodes));
  }

  private DataGroupMember getDataGroupMember(Node node, PartitionGroup nodes) {
    DataGroupMember dataGroupMember =
        new DataGroupMember(new Factory(), nodes, node, testMetaMember) {
          @Override
          public boolean syncLeader(CheckConsistency checkConsistency) {
            return true;
          }

          @Override
          public void updateHardState(long currentTerm, Node leader) {}

          @Override
          public AsyncClient getSendLogAsyncClient(Node node) {
            return getAsyncClient(node);
          }

          @Override
          public AsyncClient getAsyncClient(Node node, boolean activatedOnly) {
            return getAsyncClient(node);
          }

          @Override
          public AsyncClient getAsyncClient(Node node) {
            try {
              return new TestAsyncDataClient(node, dataGroupMemberMap) {

                @Override
                public void pullMeasurementSchema(
                    PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
                  dataGroupMemberMap.get(request.getHeader()).setCharacter(NodeCharacter.LEADER);
                  new DataAsyncService(dataGroupMemberMap.get(request.getHeader()))
                      .pullMeasurementSchema(request, resultHandler);
                }

                @Override
                public void pullTimeSeriesSchema(
                    PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
                  dataGroupMemberMap.get(request.getHeader()).setCharacter(NodeCharacter.LEADER);
                  new DataAsyncService(dataGroupMemberMap.get(request.getHeader()))
                      .pullTimeSeriesSchema(request, resultHandler);
                }

                @Override
                public void pullSnapshot(
                    PullSnapshotRequest request,
                    AsyncMethodCallback<PullSnapshotResp> resultHandler) {
                  PullSnapshotResp resp = new PullSnapshotResp();
                  Map<Integer, ByteBuffer> snapshotBufferMap = new HashMap<>();
                  for (Integer requiredSlot : request.getRequiredSlots()) {
                    FileSnapshot fileSnapshot = snapshotMap.get(requiredSlot);
                    if (fileSnapshot != null) {
                      snapshotBufferMap.put(requiredSlot, fileSnapshot.serialize());
                    }
                    synchronized (DataGroupMemberTest.this.dataGroupMember) {
                      pulledSnapshots.add(requiredSlot);
                    }
                  }
                  resp.setSnapshotBytes(snapshotBufferMap);
                  resultHandler.onComplete(resp);
                }

                @Override
                public void requestCommitIndex(
                    Node header, AsyncMethodCallback<RequestCommitIndexResponse> resultHandler) {
                  new Thread(
                          () -> {
                            if (enableSyncLeader) {
                              resultHandler.onComplete(new RequestCommitIndexResponse());
                            } else {
                              resultHandler.onError(new TestException());
                            }
                          })
                      .start();
                }

                @Override
                public void removeHardLink(
                    String hardLinkPath, AsyncMethodCallback<Void> resultHandler) {
                  new Thread(
                          () -> {
                            try {
                              Files.deleteIfExists(new File(hardLinkPath).toPath());
                            } catch (IOException e) {
                              // ignore
                            }
                          })
                      .start();
                }
              };
            } catch (IOException e) {
              return null;
            }
          }
        };
    PartitionedSnapshotLogManager logManager = getLogManager(nodes, dataGroupMember);
    dataGroupMember.setLogManager(logManager);
    dataGroupMember.setLeader(node);
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    dataGroupMember.setAppendLogThreadPool(testThreadPool);
    return dataGroupMember;
  }

  @Test
  public void testGetHeader() {
    System.out.println("Start testGetHeader()");
    assertEquals(TestUtils.getNode(0), dataGroupMember.getHeader());
  }

  @Test
  public void testAddNode() {
    System.out.println("Start testAddNode()");
    PartitionGroup partitionGroup =
        new PartitionGroup(TestUtils.getNode(0), TestUtils.getNode(50), TestUtils.getNode(90));
    DataGroupMember firstMember =
        getDataGroupMember(TestUtils.getNode(0), new PartitionGroup(partitionGroup));
    DataGroupMember midMember =
        getDataGroupMember(TestUtils.getNode(50), new PartitionGroup(partitionGroup));
    DataGroupMember lastMember =
        getDataGroupMember(TestUtils.getNode(90), new PartitionGroup(partitionGroup));
    SlotNodeAdditionResult result = new SlotNodeAdditionResult();
    result.setLostSlots(new HashMap<>());

    try {
      Node newNodeBeforeGroup = TestUtils.getNode(-5);
      assertFalse(firstMember.addNode(newNodeBeforeGroup, result));
      assertFalse(midMember.addNode(newNodeBeforeGroup, result));
      assertFalse(lastMember.addNode(newNodeBeforeGroup, result));

      Node newNodeInGroup = TestUtils.getNode(66);
      assertFalse(firstMember.addNode(newNodeInGroup, result));
      assertFalse(midMember.addNode(newNodeInGroup, result));
      assertTrue(lastMember.addNode(newNodeInGroup, result));

      Node newNodeAfterGroup = TestUtils.getNode(101);
      assertFalse(firstMember.addNode(newNodeAfterGroup, result));
      assertFalse(midMember.addNode(newNodeAfterGroup, result));
    } finally {
      firstMember.closeLogManager();
      midMember.closeLogManager();
      lastMember.closeLogManager();
    }
  }

  @Test
  public void testStartElection() {
    System.out.println("Start testStartElection()");
    class TestHandler implements AsyncMethodCallback<Long> {

      private long response;

      @Override
      public void onComplete(Long resp) {
        response = resp;
      }

      @Override
      public void onError(Exception e) {}

      public long getResponse() {
        return response;
      }
    }
    List<Log> dataLogs = TestUtils.prepareTestLogs(11);
    dataGroupMember.getLogManager().append(dataLogs);
    dataGroupMember.getTerm().set(10);
    testMetaMember.getTerm().set(10);
    List<Log> metaLogs = TestUtils.prepareTestLogs(6);
    metaLogManager.append(metaLogs);
    Node voteFor = new Node("127.0.0.1", 30000, 0, 40000, Constants.RPC_PORT, "127.0.0.1");
    Node elector = new Node("127.0.0.1", 30001, 1, 40001, Constants.RPC_PORT + 1, "127.0.0.1");

    // a request with smaller term
    ElectionRequest electionRequest = new ElectionRequest();
    electionRequest.setTerm(1);
    electionRequest.setLastLogIndex(100);
    electionRequest.setLastLogTerm(100);
    electionRequest.setDataLogLastTerm(100);
    electionRequest.setDataLogLastIndex(100);
    TestHandler handler = new TestHandler();
    new DataAsyncService(dataGroupMember).startElection(electionRequest, handler);
    assertEquals(10, handler.getResponse());

    // a valid request with same term and voteFor is empty
    electionRequest.setTerm(10);
    handler = new TestHandler();
    new DataAsyncService(dataGroupMember).startElection(electionRequest, handler);
    assertEquals(Response.RESPONSE_AGREE, handler.getResponse());

    dataGroupMember.setVoteFor(voteFor);

    // a request with same term and voteFor is not empty and elector is not same to voteFor
    // should reject election
    electionRequest.setTerm(10);
    electionRequest.setElector(elector);
    handler = new TestHandler();
    new DataAsyncService(dataGroupMember).startElection(electionRequest, handler);
    assertEquals(Response.RESPONSE_REJECT, handler.getResponse());

    // a valid request with same term and voteFor is not empty and elector is same to voteFor
    electionRequest.setTerm(10);
    electionRequest.setElector(voteFor);
    handler = new TestHandler();
    new DataAsyncService(dataGroupMember).startElection(electionRequest, handler);
    assertEquals(Response.RESPONSE_AGREE, handler.getResponse());

    // a request with larger term and stale meta log
    // should reject election but update term
    electionRequest.setTerm(13);
    electionRequest.setLastLogIndex(1);
    electionRequest.setLastLogTerm(1);
    handler = new TestHandler();
    new DataAsyncService(dataGroupMember).startElection(electionRequest, handler);
    assertEquals(Response.RESPONSE_META_LOG_STALE, handler.getResponse());
    assertEquals(13, dataGroupMember.getTerm().get());

    // a request with with larger term and stale data log
    // should reject election but update term
    electionRequest.setTerm(14);
    electionRequest.setLastLogIndex(100);
    electionRequest.setLastLogTerm(100);
    electionRequest.setDataLogLastTerm(1);
    electionRequest.setDataLogLastIndex(1);
    new DataAsyncService(dataGroupMember).startElection(electionRequest, handler);
    assertEquals(Response.RESPONSE_LOG_MISMATCH, handler.getResponse());
    assertEquals(14, dataGroupMember.getTerm().get());

    // a valid request with with larger term
    electionRequest.setTerm(15);
    electionRequest.setDataLogLastTerm(100);
    electionRequest.setDataLogLastIndex(100);
    new DataAsyncService(dataGroupMember).startElection(electionRequest, handler);
    assertEquals(Response.RESPONSE_AGREE, handler.getResponse());
    assertEquals(15, dataGroupMember.getTerm().get());
  }

  @Test
  public void testSendSnapshot() {
    System.out.println("Start testSendSnapshot()");
    PartitionedSnapshot<FileSnapshot> partitionedSnapshot =
        new PartitionedSnapshot<>(FileSnapshot.Factory.INSTANCE);
    partitionedSnapshot.setLastLogIndex(100);
    partitionedSnapshot.setLastLogTerm(100);

    for (int i = 0; i < 3; i++) {
      FileSnapshot fileSnapshot = new FileSnapshot();
      partitionedSnapshot.putSnapshot(i, fileSnapshot);
    }
    ByteBuffer serialize = partitionedSnapshot.serialize();

    SendSnapshotRequest request = new SendSnapshotRequest();
    request.setSnapshotBytes(serialize);
    AtomicBoolean callbackCalled = new AtomicBoolean(false);
    new DataAsyncService(dataGroupMember)
        .sendSnapshot(
            request,
            new AsyncMethodCallback() {
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
      throws IOException, WriteProcessException, SnapshotInstallationException,
          QueryProcessException, IllegalPathException {
    System.out.println("Start testApplySnapshot()");
    FileSnapshot snapshot = new FileSnapshot();
    List<TimeseriesSchema> schemaList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      schemaList.add(TestUtils.getTestTimeSeriesSchema(0, i));
    }
    snapshot.setTimeseriesSchemas(schemaList);

    // resource1, resource1 exists locally, resource2 is closed but resource1 is not
    // resource3 does not exist locally and without modification,
    // resource4 does not exist locally and with modification
    snapshot.addFile(prepareResource(1, false, true), TestUtils.getNode(0));
    snapshot.addFile(prepareResource(2, false, true), TestUtils.getNode(0));
    snapshot.addFile(prepareResource(3, false, true), TestUtils.getNode(0));
    snapshot.addFile(prepareResource(4, true, true), TestUtils.getNode(0));
    // resource5 is the merge result of 3,4,5
    TsFileResource tsFileResource = prepareResource(5, true, true);
    tsFileResource.updateStartTime(TestUtils.getTestSg(0), 300);
    tsFileResource.updateEndTime(TestUtils.getTestSg(0), 599);
    tsFileResource.setMinPlanIndex(3);
    tsFileResource.setMaxPlanIndex(5);
    snapshot.addFile(tsFileResource, TestUtils.getNode(0), true);

    // create a local resource1
    StorageGroupProcessor processor;
    while (true) {
      try {
        processor =
            StorageEngine.getInstance().getProcessor(new PartialPath(TestUtils.getTestSg(0)));
        break;
      } catch (StorageEngineException e) {
        // ignore
      }
    }

    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setDeviceId(new PartialPath(TestUtils.getTestSg(0)));
    insertPlan.setTime(0);
    insertPlan.setMeasurements(new String[] {"s0"});
    insertPlan.setNeedInferType(true);
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    insertPlan.setValues(new Object[] {"1.0"});
    insertPlan.setMeasurementMNodes(new MeasurementMNode[] {TestUtils.getTestMeasurementMNode(0)});
    insertPlan.transferType();
    processor.insert(insertPlan);
    processor.syncCloseAllWorkingTsFileProcessors();

    // create a local resource2
    insertPlan.setTime(101);
    processor.insert(insertPlan);

    snapshot.getDefaultInstaller(dataGroupMember).install(snapshot, 0);
    assertEquals(2, processor.getSequenceFileTreeSet().size());
    assertEquals(1, processor.getUnSequenceFileList().size());
    Deletion deletion = new Deletion(new PartialPath(TestUtils.getTestSg(0)), 0, 0);
    assertTrue(
        processor
            .getUnSequenceFileList()
            .get(0)
            .getModFile()
            .getModifications()
            .contains(deletion));
  }

  @Test
  public void testForwardPullSnapshot() {
    System.out.println("Start testForwardPullSnapshot()");
    dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    PullSnapshotRequest request = new PullSnapshotRequest();
    List<Integer> requiredSlots = Arrays.asList(1, 3, 5, 7, 9);
    request.setRequiredSlots(requiredSlots);
    AtomicReference<Map<Integer, FileSnapshot>> reference = new AtomicReference<>();
    PullSnapshotHandler<FileSnapshot> handler =
        new PullSnapshotHandler<>(
            reference,
            TestUtils.getNode(1),
            request.getRequiredSlots(),
            FileSnapshot.Factory.INSTANCE);
    new DataAsyncService(dataGroupMember).pullSnapshot(request, handler);
    assertEquals(requiredSlots.size(), reference.get().size());
    for (Integer requiredSlot : requiredSlots) {
      assertEquals(snapshotMap.get(requiredSlot), reference.get().get(requiredSlot));
    }
  }

  @Test
  public void testPullSnapshot() {
    System.out.println("Start testPullSnapshot()");
    hasInitialSnapshots = true;
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    PullSnapshotRequest request = new PullSnapshotRequest();
    List<Integer> requiredSlots = Arrays.asList(1, 3, 5, 7, 9, 11, 101);
    request.setRequiredSlots(requiredSlots);
    AtomicReference<Map<Integer, FileSnapshot>> reference = new AtomicReference<>();
    PullSnapshotHandler<FileSnapshot> handler =
        new PullSnapshotHandler<>(
            reference,
            TestUtils.getNode(1),
            request.getRequiredSlots(),
            FileSnapshot.Factory.INSTANCE);
    new DataAsyncService(dataGroupMember).pullSnapshot(request, handler);
    assertEquals(requiredSlots.size() - 1, reference.get().size());
    for (int i = 0; i < requiredSlots.size() - 1; i++) {
      Integer requiredSlot = requiredSlots.get(i);
      assertEquals(snapshotMap.get(requiredSlot), reference.get().get(requiredSlot));
    }
  }

  @Test
  public void testFollowerExecuteNonQuery() throws IllegalPathException {
    System.out.println("Start testFollowerExecuteNonQuery()");
    dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    TimeseriesSchema timeseriesSchema = TestUtils.getTestTimeSeriesSchema(0, 100);
    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath(timeseriesSchema.getFullPath()),
            timeseriesSchema.getType(),
            timeseriesSchema.getEncodingType(),
            timeseriesSchema.getCompressor(),
            timeseriesSchema.getProps(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null);
    assertEquals(200, dataGroupMember.executeNonQueryPlan(createTimeSeriesPlan).code);
    assertTrue(IoTDB.metaManager.isPathExist(new PartialPath(timeseriesSchema.getFullPath())));
  }

  @Test
  public void testLeaderExecuteNonQuery() throws QueryProcessException, IllegalPathException {
    System.out.println("Start testLeaderExecuteNonQuery()");
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    dataGroupMember.setLeader(TestUtils.getNode(1));
    ExecutorService testThreadPool = Executors.newFixedThreadPool(4);
    dataGroupMember.setAppendLogThreadPool(testThreadPool);

    TimeseriesSchema timeseriesSchema = TestUtils.getTestTimeSeriesSchema(0, 100);
    CreateTimeSeriesPlan createTimeSeriesPlan =
        new CreateTimeSeriesPlan(
            new PartialPath(timeseriesSchema.getFullPath()),
            timeseriesSchema.getType(),
            timeseriesSchema.getEncodingType(),
            timeseriesSchema.getCompressor(),
            timeseriesSchema.getProps(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null);
    testMetaMember = super.getMetaGroupMember(TestUtils.getNode(0));
    testMetaMember.setPartitionTable(partitionTable);
    dataGroupMember.setLogManager(
        getLogManager(partitionTable.getHeaderGroup(TestUtils.getNode(0)), dataGroupMember));
    assertEquals(200, dataGroupMember.executeNonQueryPlan(createTimeSeriesPlan).code);
    assertTrue(IoTDB.metaManager.isPathExist(new PartialPath(timeseriesSchema.getFullPath())));
    testThreadPool.shutdownNow();
  }

  @Test
  public void testPullTimeseriesSchema() {
    System.out.println("Start testPullTimeseriesSchema()");
    int prevTimeOut = RaftServer.getConnectionTimeoutInMS();
    int prevMaxWait = RaftServer.getSyncLeaderMaxWaitMs();
    RaftServer.setConnectionTimeoutInMS(20);
    RaftServer.setSyncLeaderMaxWaitMs(200);
    try {
      // sync with leader is temporarily disabled, the request should be forward to the leader
      dataGroupMember.setLeader(TestUtils.getNode(0));
      dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
      enableSyncLeader = false;

      PullSchemaRequest request = new PullSchemaRequest();
      request.setPrefixPaths(Collections.singletonList(TestUtils.getTestSg(0)));
      request.setHeader(TestUtils.getNode(0));
      AtomicReference<List<TimeseriesSchema>> result = new AtomicReference<>();
      PullTimeseriesSchemaHandler handler =
          new PullTimeseriesSchemaHandler(TestUtils.getNode(1), request.getPrefixPaths(), result);
      new DataAsyncService(dataGroupMember).pullTimeSeriesSchema(request, handler);
      for (int i = 0; i < 10; i++) {
        assertTrue(result.get().contains(TestUtils.getTestTimeSeriesSchema(0, i)));
      }

      // the member is a leader itself
      dataGroupMember.setCharacter(NodeCharacter.LEADER);
      result.set(null);
      handler =
          new PullTimeseriesSchemaHandler(TestUtils.getNode(1), request.getPrefixPaths(), result);
      new DataAsyncService(dataGroupMember).pullTimeSeriesSchema(request, handler);
      for (int i = 0; i < 10; i++) {
        assertTrue(result.get().contains(TestUtils.getTestTimeSeriesSchema(0, i)));
      }
    } finally {
      RaftServer.setConnectionTimeoutInMS(prevTimeOut);
      RaftServer.setSyncLeaderMaxWaitMs(prevMaxWait);
    }
  }

  @Test
  public void testPullMeasurementSchema() {
    System.out.println("Start testPullMeasurementSchema()");
    int prevTimeOut = RaftServer.getConnectionTimeoutInMS();
    int prevMaxWait = RaftServer.getSyncLeaderMaxWaitMs();
    RaftServer.setConnectionTimeoutInMS(20);
    RaftServer.setSyncLeaderMaxWaitMs(200);
    try {
      // sync with leader is temporarily disabled, the request should be forward to the leader
      dataGroupMember.setLeader(TestUtils.getNode(0));
      dataGroupMember.setCharacter(NodeCharacter.FOLLOWER);
      enableSyncLeader = false;

      PullSchemaRequest request = new PullSchemaRequest();
      request.setPrefixPaths(Collections.singletonList(TestUtils.getTestSg(0)));
      request.setHeader(TestUtils.getNode(0));
      AtomicReference<List<MeasurementSchema>> result = new AtomicReference<>();
      PullMeasurementSchemaHandler handler =
          new PullMeasurementSchemaHandler(TestUtils.getNode(1), request.getPrefixPaths(), result);
      new DataAsyncService(dataGroupMember).pullMeasurementSchema(request, handler);
      for (int i = 0; i < 10; i++) {
        assertTrue(result.get().contains(TestUtils.getTestMeasurementSchema(i)));
      }

      // the member is a leader itself
      dataGroupMember.setCharacter(NodeCharacter.LEADER);
      result.set(null);
      handler =
          new PullMeasurementSchemaHandler(TestUtils.getNode(1), request.getPrefixPaths(), result);
      new DataAsyncService(dataGroupMember).pullMeasurementSchema(request, handler);
      for (int i = 0; i < 10; i++) {
        assertTrue(result.get().contains(TestUtils.getTestMeasurementSchema(i)));
      }
    } finally {
      RaftServer.setConnectionTimeoutInMS(prevTimeOut);
      RaftServer.setSyncLeaderMaxWaitMs(prevMaxWait);
    }
  }

  @Test
  public void testQuerySingleSeries()
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException,
          IllegalPathException {
    System.out.println("Start testQuerySingleSeries()");
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setDeviceId(new PartialPath(TestUtils.getTestSg(0)));
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new Object[] {String.valueOf(i)});
      insertPlan.setMeasurementMNodes(
          new MeasurementMNode[] {TestUtils.getTestMeasurementMNode(0)});
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
    new DataAsyncService(dataGroupMember).querySingleSeries(request, handler);
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0), dataResult);
    new DataAsyncService(dataGroupMember)
        .fetchSingleSeries(TestUtils.getNode(0), readerId, dataHandler);
    ByteBuffer dataBuffer = dataResult.get();
    BatchData batchData = SerializeUtils.deserializeBatchData(dataBuffer);
    for (int i = 5; i < 10; i++) {
      assertTrue(batchData.hasCurrent());
      assertEquals(i, batchData.currentTime());
      assertEquals(i * 1.0, batchData.getDouble(), 0.00001);
      batchData.next();
    }
    assertFalse(batchData.hasCurrent());

    new DataAsyncService(dataGroupMember)
        .endQuery(
            TestUtils.getNode(0),
            TestUtils.getNode(1),
            0,
            new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesWithValueFilter()
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException,
          IllegalPathException {
    System.out.println("Start testQuerySingleSeriesWithValueFilter()");
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setDeviceId(new PartialPath(TestUtils.getTestSg(0)));
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new Object[] {String.valueOf(i)});
      insertPlan.setMeasurementMNodes(
          new MeasurementMNode[] {TestUtils.getTestMeasurementMNode(0)});
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
    new DataAsyncService(dataGroupMember).querySingleSeries(request, handler);
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0), dataResult);
    new DataAsyncService(dataGroupMember)
        .fetchSingleSeries(TestUtils.getNode(0), readerId, dataHandler);
    ByteBuffer dataBuffer = dataResult.get();
    BatchData batchData = SerializeUtils.deserializeBatchData(dataBuffer);
    for (int i = 5; i < 9; i++) {
      assertTrue(batchData.hasCurrent());
      assertEquals(i, batchData.currentTime());
      assertEquals(i * 1.0, batchData.getDouble(), 0.00001);
      batchData.next();
    }
    assertFalse(batchData.hasCurrent());

    new DataAsyncService(dataGroupMember)
        .endQuery(
            TestUtils.getNode(0),
            TestUtils.getNode(1),
            0,
            new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesByTimestamp()
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException,
          IllegalPathException {
    System.out.println("Start testQuerySingleSeriesByTimestamp()");
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setDeviceId(new PartialPath(TestUtils.getTestSg(0)));
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new Object[] {String.valueOf(i)});
      insertPlan.setMeasurementMNodes(
          new MeasurementMNode[] {TestUtils.getTestMeasurementMNode(0)});
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
    request.setAscending(true);

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    new DataAsyncService(dataGroupMember).querySingleSeriesByTimestamp(request, handler);
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0), dataResult);

    List<Long> timestamps = new ArrayList<>(5);
    for (int i = 5; i < 10; i++) {
      timestamps.add((long) i);
    }
    new DataAsyncService(dataGroupMember)
        .fetchSingleSeriesByTimestamps(TestUtils.getNode(0), readerId, timestamps, dataHandler);
    Object[] values = SerializeUtils.deserializeObjects(dataResult.get());
    for (int i = 5; i < 10; i++) {
      assertEquals(i * 1.0, (Double) values[i - 5], 0.00001);
    }

    new DataAsyncService(dataGroupMember)
        .endQuery(
            TestUtils.getNode(0),
            TestUtils.getNode(1),
            0,
            new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testQuerySingleSeriesByTimestampWithValueFilter()
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException,
          IllegalPathException {
    System.out.println("Start testQuerySingleSeriesByTimestampWithValueFilter()");
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setDeviceId(new PartialPath(TestUtils.getTestSg(0)));
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new Object[] {String.valueOf(i)});
      insertPlan.setMeasurementMNodes(
          new MeasurementMNode[] {TestUtils.getTestMeasurementMNode(0)});
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
    request.setAscending(true);

    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    new DataAsyncService(dataGroupMember).querySingleSeriesByTimestamp(request, handler);
    long readerId = result.get();
    assertEquals(1, readerId);

    AtomicReference<ByteBuffer> dataResult = new AtomicReference<>();
    GenericHandler<ByteBuffer> dataHandler = new GenericHandler<>(TestUtils.getNode(0), dataResult);
    List<Long> timestamps = new ArrayList<>(4);
    for (int i = 5; i < 9; i++) {
      timestamps.add((long) i);
    }
    new DataAsyncService(dataGroupMember)
        .fetchSingleSeriesByTimestamps(TestUtils.getNode(0), readerId, timestamps, dataHandler);
    Object[] values = SerializeUtils.deserializeObjects(dataResult.get());
    for (int i = 5; i < 9; i++) {
      assertEquals(i * 1.0, (Double) values[i - 5], 0.00001);
    }

    new DataAsyncService(dataGroupMember)
        .endQuery(
            TestUtils.getNode(0),
            TestUtils.getNode(1),
            0,
            new GenericHandler<>(TestUtils.getNode(0), null));
  }

  @Test
  public void testGetPaths() {
    System.out.println("Start testGetPaths()");
    String path = TestUtils.getTestSg(0);
    AtomicReference<GetAllPathsResult> pathResult = new AtomicReference<>();
    GenericHandler<GetAllPathsResult> handler =
        new GenericHandler<>(TestUtils.getNode(0), pathResult);
    new DataAsyncService(dataGroupMember)
        .getAllPaths(TestUtils.getNode(0), Collections.singletonList(path), false, handler);
    List<String> result = pathResult.get().paths;
    assertEquals(20, result.size());
    for (int i = 0; i < 10; i++) {
      assertTrue(result.contains(TestUtils.getTestSeries(0, i)));
    }
  }

  @Test
  public void testFetchWithoutQuery() {
    System.out.println("Start testFetchWithoutQuery()");
    AtomicReference<Exception> result = new AtomicReference<>();
    List<Long> timestamps = new ArrayList<>(1);
    timestamps.add((long) 0);
    new DataAsyncService(dataGroupMember)
        .fetchSingleSeriesByTimestamps(
            TestUtils.getNode(0),
            0,
            timestamps,
            new AsyncMethodCallback<ByteBuffer>() {
              @Override
              public void onComplete(ByteBuffer buffer) {}

              @Override
              public void onError(Exception e) {
                result.set(e);
              }
            });
    Exception exception = result.get();
    assertTrue(exception instanceof ReaderNotFoundException);
    assertEquals("The requested reader 0 is not found", exception.getMessage());

    new DataAsyncService(dataGroupMember)
        .fetchSingleSeries(
            TestUtils.getNode(0),
            0,
            new AsyncMethodCallback<ByteBuffer>() {
              @Override
              public void onComplete(ByteBuffer buffer) {}

              @Override
              public void onError(Exception e) {
                result.set(e);
              }
            });
    exception = result.get();
    assertTrue(exception instanceof ReaderNotFoundException);
    assertEquals("The requested reader 0 is not found", exception.getMessage());
  }

  private TsFileResource prepareResource(
      long serialNum, boolean withModification, boolean asHardLink)
      throws IOException, IllegalPathException {
    TsFileResource resource = new RemoteTsFileResource();
    String fileName =
        "target"
            + File.separator
            + TestUtils.getTestSg(0)
            + File.separator
            + "0"
            + File.separator
            + "0"
            + File.separator
            + "0-"
            + serialNum
            + "-0-0.tsfile";
    if (asHardLink) {
      fileName = fileName + ".0_0";
    }
    File file = new File(fileName);
    file.getParentFile().mkdirs();
    file.createNewFile();

    resource.setFile(file);
    resource.setMaxPlanIndex(serialNum);
    resource.setMaxPlanIndex(serialNum);
    resource.updateStartTime(TestUtils.getTestSg(0), serialNum * 100);
    resource.updateEndTime(TestUtils.getTestSg(0), (serialNum + 1) * 100 - 1);
    if (withModification) {
      Deletion deletion = new Deletion(new PartialPath(TestUtils.getTestSg(0)), 0, 0);
      resource.getModFile().write(deletion);
      resource.getModFile().close();
    }
    return resource;
  }

  @Test
  public void testRemoveLeader() {
    System.out.println("Start testRemoveLeader()");
    Node nodeToRemove = TestUtils.getNode(10);
    SlotNodeRemovalResult nodeRemovalResult =
        (SlotNodeRemovalResult) testMetaMember.getPartitionTable().removeNode(nodeToRemove);
    dataGroupMember.setLeader(nodeToRemove);
    dataGroupMember.start();

    try {
      dataGroupMember.removeNode(nodeToRemove, nodeRemovalResult);

      assertEquals(NodeCharacter.ELECTOR, dataGroupMember.getCharacter());
      assertEquals(Long.MIN_VALUE, dataGroupMember.getLastHeartbeatReceivedTime());
      assertTrue(dataGroupMember.getAllNodes().contains(TestUtils.getNode(30)));
      assertFalse(dataGroupMember.getAllNodes().contains(nodeToRemove));
      List<Integer> newSlots = nodeRemovalResult.getNewSlotOwners().get(TestUtils.getNode(0));
      while (newSlots.size() != pulledSnapshots.size()) {}

      for (Integer newSlot : newSlots) {
        assertTrue(pulledSnapshots.contains(newSlot));
      }
    } finally {
      dataGroupMember.stop();
    }
  }

  @Test
  public void testRemoveNonLeader() {
    System.out.println("Start testRemoveNonLeader()");
    Node nodeToRemove = TestUtils.getNode(10);
    NodeRemovalResult nodeRemovalResult =
        testMetaMember.getPartitionTable().removeNode(nodeToRemove);
    dataGroupMember.setLeader(TestUtils.getNode(20));
    dataGroupMember.start();

    try {
      dataGroupMember.removeNode(nodeToRemove, nodeRemovalResult);

      assertEquals(0, dataGroupMember.getLastHeartbeatReceivedTime());
      assertTrue(dataGroupMember.getAllNodes().contains(TestUtils.getNode(30)));
      assertFalse(dataGroupMember.getAllNodes().contains(nodeToRemove));
      List<Integer> newSlots =
          ((SlotNodeRemovalResult) nodeRemovalResult).getNewSlotOwners().get(TestUtils.getNode(0));
      while (newSlots.size() != pulledSnapshots.size()) {}

      for (Integer newSlot : newSlots) {
        assertTrue(pulledSnapshots.contains(newSlot));
      }
    } finally {
      dataGroupMember.stop();
    }
  }

  @Test
  public void testGroupBy()
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException,
          IllegalPathException {
    System.out.println("Start testGroupBy()");
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
    try {
      request.setQueryId(queryContext.getQueryId());
      request.setRequestor(TestUtils.getNode(0));
      request.setDataTypeOrdinal(TSDataType.DOUBLE.ordinal());
      request.setDeviceMeasurements(Collections.singleton(TestUtils.getTestMeasurement(0)));
      request.setAscending(true);

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
      try {
        resultRef = new AtomicReference<>();
        handler = new GenericHandler<>(TestUtils.getNode(0), resultRef);
        new DataAsyncService(dataGroupMember).getGroupByExecutor(request, handler);
        executorId = resultRef.get();
        assertEquals(1L, (long) executorId);

        // fetch result
        aggrResultRef = new AtomicReference<>();
        aggrResultHandler = new GenericHandler<>(TestUtils.getNode(0), aggrResultRef);
        new DataAsyncService(dataGroupMember)
            .getGroupByResult(TestUtils.getNode(10), executorId, 0, 20, aggrResultHandler);

        byteBuffers = aggrResultRef.get();
        assertNotNull(byteBuffers);
        aggregateResults = new ArrayList<>();
        for (ByteBuffer byteBuffer : byteBuffers) {
          aggregateResults.add(AggregateResult.deserializeFrom(byteBuffer));
        }
        answers = new Object[] {15.0, 12.0, 180.0, 5.0, 19.0, 19.0, 5.0, 19.0, 5.0};
        checkAggregates(answers, aggregateResults);
      } finally {
        dataGroupMember.closeLogManager();
      }

      // get an executor from a node not holding this timeseries
      request.setHeader(TestUtils.getNode(30));
      dataGroupMember = getDataGroupMember(TestUtils.getNode(30));
      try {
        resultRef = new AtomicReference<>();
        handler = new GenericHandler<>(TestUtils.getNode(0), resultRef);
        request.timeFilterBytes.position(0);
        new DataAsyncService(dataGroupMember).getGroupByExecutor(request, handler);
        executorId = resultRef.get();
        assertEquals(-1L, (long) executorId);

        // fetch result
        aggrResultRef = new AtomicReference<>();
        aggrResultHandler = new GenericHandler<>(TestUtils.getNode(0), aggrResultRef);
        new DataAsyncService(dataGroupMember)
            .getGroupByResult(TestUtils.getNode(30), executorId, 0, 20, aggrResultHandler);

        byteBuffers = aggrResultRef.get();
        assertNull(byteBuffers);
      } finally {
        dataGroupMember.closeLogManager();
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(queryContext.getQueryId());
    }
  }

  private void checkAggregates(Object[] answers, List<AggregateResult> aggregateResults) {
    assertEquals(answers.length, aggregateResults.size());
    for (int i = 0; i < aggregateResults.size(); i++) {
      if (answers[i] != null) {
        assertEquals(
            (double) answers[i],
            Double.parseDouble(aggregateResults.get(i).getResult().toString()),
            0.000001);
      } else {
        assertNull(aggregateResults.get(i).getResult());
      }
    }
  }
}
