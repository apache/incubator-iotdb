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

import static org.apache.iotdb.cluster.server.NodeCharacter.ELECTOR;
import static org.apache.iotdb.cluster.server.NodeCharacter.FOLLOWER;
import static org.apache.iotdb.cluster.server.NodeCharacter.LEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.TestDataClient;
import org.apache.iotdb.cluster.common.TestMetaClient;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.DataClusterServer;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetaGroupMemberTest extends MemberTest {

  private DataClusterServer dataClusterServer;

  private boolean mockDataClusterServer;
  private Node exiledNode;

  private int prevReplicaNum;

  @Override
  @After
  public void tearDown() throws Exception {
    dataClusterServer.stop();
    super.tearDown();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(prevReplicaNum);
  }

  @Before
  public void setUp() throws Exception {
    prevReplicaNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(2);
    super.setUp();
    dummyResponse.set(Response.RESPONSE_AGREE);
    testMetaMember.setAllNodes(allNodes);

    dataClusterServer = new DataClusterServer(TestUtils.getNode(0),
        new DataGroupMember.Factory(null, testMetaMember) {
          @Override
          public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode) {
            return getDataGroupMember(partitionGroup, thisNode);
          }
        });
    buildDataGroups(dataClusterServer);
    testMetaMember.getThisNode().setNodeIdentifier(0);
    mockDataClusterServer = false;
    QueryCoordinator.getINSTANCE().setMetaGroupMember(testMetaMember);
    exiledNode = null;
    System.out.println("Init term of metaGroupMember: " + testMetaMember.getTerm().get());
  }

  private DataGroupMember getDataGroupMember(PartitionGroup group, Node node) {
    DataGroupMember dataGroupMember = new DataGroupMember(null, group, node,
        testMetaMember) {
      @Override
      public boolean syncLeader() {
        return true;
      }

      @Override
      TSStatus executeNonQuery(PhysicalPlan plan) {
        try {
          planExecutor.processNonQuery(plan);
          return StatusUtils.OK;
        } catch (QueryProcessException e) {
          TSStatus status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
          status.setMessage(e.getMessage());
          return status;
        }
      }

      @Override
      TSStatus forwardPlan(PhysicalPlan plan, Node node, Node header) {
        return executeNonQuery(plan);
      }

      @Override
      public AsyncClient connectNode(Node node) {
        return null;
      }

      @Override
      public void pullTimeSeriesSchema(PullSchemaRequest request,
          AsyncMethodCallback<PullSchemaResp> resultHandler) {
        mockedPullTimeSeriesSchema(request, resultHandler);
      }
    };
    dataGroupMember.setLogManager(new TestPartitionedLogManager(null,
        partitionTable, group.getHeader(), TestSnapshot::new));
    return dataGroupMember;
  }

  private void mockedPullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    new Thread(() -> {
      try {
        List<MeasurementSchema> schemas = new ArrayList<>();
        List<String> prefixPaths = request.getPrefixPaths();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        for (String prefixPath : prefixPaths) {
          if (!prefixPath.equals(TestUtils.getTestSeries(10, 0))) {
            MManager.getInstance().collectSeries(prefixPath, schemas);
            dataOutputStream.writeInt(schemas.size());
            for (MeasurementSchema schema : schemas) {
              schema.serializeTo(dataOutputStream);
            }
          } else {
            dataOutputStream.writeInt(1);
            TestUtils.getTestSchema(10, 0).serializeTo(dataOutputStream);
          }
        }
        PullSchemaResp resp = new PullSchemaResp();
        resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
        resultHandler.onComplete(resp);
      } catch (IOException e) {
        resultHandler.onError(e);
      }
    }).start();
  }


  protected MetaGroupMember getMetaGroupMember(Node node) throws QueryProcessException {
    return new MetaGroupMember(new Factory(), node) {

      @Override
      public DataClusterServer getDataClusterServer() {
        return mockDataClusterServer ? MetaGroupMemberTest.this.dataClusterServer
            : super.getDataClusterServer();
      }

      @Override
      public DataClient getDataClient(Node node) throws IOException {
        return new TestDataClient(node, dataGroupMemberMap);
      }

      @Override
      protected DataGroupMember getLocalDataMember(Node header, AsyncMethodCallback resultHandler,
          Object request) {
        return getDataGroupMember(header);
      }

      @Override
      protected DataGroupMember getLocalDataMember(Node header) {
        return getDataGroupMember(header);
      }

      @Override
      public void updateHardState(long currentTerm, Node leader) {
      }

      @Override
      public AsyncClient connectNode(Node node) {
        if (node.equals(thisNode)) {
          return null;
        }
        try {
          return new TestMetaClient(null, null, node, null) {
            @Override
            public void startElection(ElectionRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                long resp = dummyResponse.get();
                // MIN_VALUE means let the request time out
                if (resp != Long.MIN_VALUE) {
                  resultHandler.onComplete(resp);
                }
              }).start();
            }

            @Override
            public void sendHeartbeat(HeartBeatRequest request,
                AsyncMethodCallback<HeartBeatResponse> resultHandler) {
              new Thread(() -> {
                HeartBeatResponse response = new HeartBeatResponse();
                response.setTerm(Response.RESPONSE_AGREE);
                resultHandler.onComplete(response);
              }).start();
            }

            @Override
            public void appendEntry(AppendEntryRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                long resp = dummyResponse.get();
                // MIN_VALUE means let the request time out
                if (resp != Long.MIN_VALUE) {
                  resultHandler.onComplete(dummyResponse.get());
                }
              }).start();
            }

            @Override
            public void addNode(Node node, StartUpStatus startUpStatus,
                AsyncMethodCallback<AddNodeResponse> resultHandler) {
              new Thread(() -> {
                if (node.getNodeIdentifier() == 10) {
                  resultHandler.onComplete(new AddNodeResponse(
                      (int) Response.RESPONSE_IDENTIFIER_CONFLICT));
                } else {
                  partitionTable.addNode(node);
                  AddNodeResponse resp = new AddNodeResponse((int) dummyResponse.get());
                  resp.setPartitionTableBytes(partitionTable.serialize());
                  resultHandler.onComplete(resp);
                }
              }).start();
            }

            @Override
            public void executeNonQueryPlan(ExecutNonQueryReq request,
                AsyncMethodCallback<TSStatus> resultHandler) {
              new Thread(() -> {
                try {
                  PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
                  planExecutor.processNonQuery(plan);
                  resultHandler.onComplete(StatusUtils.OK);
                } catch (IOException | QueryProcessException e) {
                  resultHandler.onError(e);
                }
              }).start();
            }

            @Override
            public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
              new Thread(() -> resultHandler.onComplete(new TNodeStatus())).start();
            }

            @Override
            public void exile(AsyncMethodCallback<Void> resultHandler) {
              System.out.printf("%s was exiled%n", node);
              exiledNode = node;
            }

            @Override
            public void removeNode(Node node, AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                testMetaMember.applyRemoveNode(node);
                resultHandler.onComplete(Response.RESPONSE_AGREE);
              }).start();
            }
          };
        } catch (IOException e) {
          return null;
        }
      }
    };
  }

  private void buildDataGroups(DataClusterServer dataClusterServer) throws TTransportException {
    List<PartitionGroup> partitionGroups = partitionTable.getLocalGroups();

    dataClusterServer.setPartitionTable(partitionTable);
    for (PartitionGroup partitionGroup : partitionGroups) {
      DataGroupMember dataGroupMember = getDataGroupMember(partitionGroup, TestUtils.getNode(0));
      dataGroupMember.start();
      dataClusterServer.addDataGroupMember(dataGroupMember);
    }
  }

  @Test
  public void testClosePartition() throws QueryProcessException, StorageEngineException {
    // the operation is accepted
    dummyResponse.set(Response.RESPONSE_AGREE);
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setSchemas(new MeasurementSchema[]{TestUtils.getTestSchema(0, 0)});
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new String[]{String.valueOf(i)});
      PlanExecutor planExecutor = new PlanExecutor();
      planExecutor.processNonQuery(insertPlan);
    }
    testMetaMember.closePartition(TestUtils.getTestSg(0), 0, true);

    StorageGroupProcessor processor =
        StorageEngine.getInstance().getProcessor(TestUtils.getTestSg(0));
    assertTrue(processor.getWorkSequenceTsFileProcessors().isEmpty());

    int prevTimeout = RaftServer.connectionTimeoutInMS;
    RaftServer.connectionTimeoutInMS = 1;
    try {
      for (int i = 20; i < 30; i++) {
        insertPlan.setTime(i);
        insertPlan.setValues(new String[]{String.valueOf(i)});
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      // the net work is down
      dummyResponse.set(Long.MIN_VALUE);
      // network resume in 100ms
      new Thread(() -> {
        try {
          Thread.sleep(100);
          dummyResponse.set(Response.RESPONSE_AGREE);
        } catch (InterruptedException e) {
          // ignore
        }
      }).start();
      testMetaMember.closePartition(TestUtils.getTestSg(0), 0, true);
      assertTrue(processor.getWorkSequenceTsFileProcessors().isEmpty());

      for (int i = 30; i < 40; i++) {
        insertPlan.setTime(i);
        insertPlan.setValues(new String[]{String.valueOf(i)});
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      // indicating the leader is stale
      dummyResponse.set(100);
      testMetaMember.closePartition(TestUtils.getTestSg(0), 0, true);
      assertFalse(processor.getWorkSequenceTsFileProcessors().isEmpty());
    } finally {
      RaftServer.connectionTimeoutInMS = prevTimeout;
    }
  }

  @Test
  public void testAddNode() {
    Node newNode = TestUtils.getNode(10);
    testMetaMember.onElectionWins();
    testMetaMember.applyAddNode(newNode);
    assertTrue(partitionTable.getAllNodes().contains(newNode));
  }

  @Test
  public void testBuildCluster() throws TTransportException {
    testMetaMember.start();
    try {
      testMetaMember.buildCluster();
      long startTime = System.currentTimeMillis();
      long timeConsumption = 0;
      while (timeConsumption < 5000 && testMetaMember.getCharacter() != LEADER) {
        timeConsumption = System.currentTimeMillis() - startTime;
      }
      if (timeConsumption >= 5000) {
        fail("The member takes too long to be the leader");
      }
      assertEquals(LEADER, testMetaMember.getCharacter());
    } finally {
      testMetaMember.stop();
    }
  }

  @Test
  public void testJoinCluster() throws TTransportException, QueryProcessException {
    MetaGroupMember newMember = getMetaGroupMember(TestUtils.getNode(10));
    newMember.start();
    try {
      assertTrue(newMember.joinCluster());
      newMember.setCharacter(ELECTOR);
      while (!LEADER.equals(newMember.getCharacter())) {

      }
    } finally {
      newMember.stop();
    }
  }

  @Test
  public void testJoinClusterFailed() throws QueryProcessException {
    long prevInterval = RaftServer.heartBeatIntervalMs;
    RaftServer.heartBeatIntervalMs = 10;
    try {
      dummyResponse.set(Response.RESPONSE_NO_CONNECTION);
      MetaGroupMember newMember = getMetaGroupMember(TestUtils.getNode(10));
      assertFalse(newMember.joinCluster());
      newMember.closeLogManager();
    } finally {
      RaftServer.heartBeatIntervalMs = prevInterval;
    }
  }

  @Test
  public void testSendSnapshot() {
    SendSnapshotRequest request = new SendSnapshotRequest();
    List<String> newSgs = new ArrayList<>();
    for (int i = 0; i <= 10; i++) {
      newSgs.add(TestUtils.getTestSg(i));
    }
    List<Log> logs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      PhysicalPlanLog log = new PhysicalPlanLog();
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan();
      MeasurementSchema schema = TestUtils.getTestSchema(10, i);
      createTimeSeriesPlan.setPath(new Path(schema.getMeasurementId()));
      createTimeSeriesPlan.setDataType(schema.getType());
      createTimeSeriesPlan.setEncoding(schema.getEncodingType());
      createTimeSeriesPlan.setCompressor(schema.getCompressor());
      createTimeSeriesPlan.setProps(schema.getProps());
      log.setPlan(createTimeSeriesPlan);
      logs.add(log);
    }

    Map<String, Long> storageGroupTTL = new HashMap<>();
    storageGroupTTL.put("root.test0", 3600L);
    storageGroupTTL.put("root.test1", 72000L);

    Map<String, Boolean> userWaterMarkStatus = new HashMap<>();
    userWaterMarkStatus.put("user_1", true);
    userWaterMarkStatus.put("user_2", true);
    userWaterMarkStatus.put("user_3", false);
    userWaterMarkStatus.put("user_4", false);

    try {
      LocalFileAuthorizer authorizer = LocalFileAuthorizer.getInstance();
      authorizer.createUser("user_1", "password_1");
      authorizer.createUser("user_2", "password_2");
      authorizer.createUser("user_3", "password_3");
      authorizer.createUser("user_4", "password_4");
    } catch (AuthException e) {
      assertEquals("why failed?", e.getMessage());
    }

    MetaSimpleSnapshot snapshot = new MetaSimpleSnapshot(logs, newSgs, storageGroupTTL,
        userWaterMarkStatus);
    request.setSnapshotBytes(snapshot.serialize());
    AtomicReference<Void> reference = new AtomicReference<>();
    testMetaMember.sendSnapshot(request, new GenericHandler(TestUtils.getNode(0), reference));

    for (int i = 0; i < 10; i++) {
      assertTrue(MManager.getInstance().isPathExist(TestUtils.getTestSeries(10, i)));
    }

    // check whether the snapshot applied or not
    assertEquals(newSgs, MManager.getInstance().getAllStorageGroupNames());

    Map<String, Long> localStorageGroupTTL = MManager.getInstance().getStorageGroupsTTL();
    assertNotNull(localStorageGroupTTL);
    assertTrue(localStorageGroupTTL.containsKey("root.test0"));
    assertTrue(localStorageGroupTTL.containsKey("root.test1"));
    assertEquals(3600L, localStorageGroupTTL.get("root.test0").longValue());
    assertEquals(72000L, localStorageGroupTTL.get("root.test1").longValue());

    try {
      LocalFileAuthorizer authorizer = LocalFileAuthorizer.getInstance();
      assertTrue(authorizer.isUserUseWaterMark("user_1"));
      assertTrue(authorizer.isUserUseWaterMark("user_2"));
      assertFalse(authorizer.isUserUseWaterMark("user_3"));
      assertFalse(authorizer.isUserUseWaterMark("user_4"));
    } catch (AuthException e) {
      assertEquals("why failed?", e.getMessage());
    }
  }

  @Test
  public void testProcessNonQuery() {
    mockDataClusterServer = true;
    // as a leader
    testMetaMember.setCharacter(LEADER);
    for (int i = 10; i < 20; i++) {
      // process a non partitioned plan
      SetStorageGroupPlan setStorageGroupPlan =
          new SetStorageGroupPlan(new Path(TestUtils.getTestSg(i)));
      TSStatus status = testMetaMember.executeNonQuery(setStorageGroupPlan);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
      assertTrue(MManager.getInstance().isPathExist(TestUtils.getTestSg(i)));

      // process a partitioned plan
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan(
          new Path(schema.getMeasurementId()), schema.getType(),
          schema.getEncodingType(), schema.getCompressor(), schema.getProps(),
          Collections.emptyMap(), Collections.emptyMap(), null);
      status = testMetaMember.executeNonQuery(createTimeSeriesPlan);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
      assertTrue(MManager.getInstance().isPathExist(TestUtils.getTestSeries(i, 0)));
    }
  }

  @Test
  public void testPullTimeseriesSchema() throws MetadataException {

    for (int i = 0; i < 10; i++) {
      List<MeasurementSchema> schemas =
          testMetaMember.pullTimeSeriesSchemas(Collections.singletonList(TestUtils.getTestSg(i)));
      assertEquals(20, schemas.size());
      for (int j = 0; j < 10; j++) {
        assertEquals(TestUtils.getTestSchema(i, j), schemas.get(j));
      }
    }
  }

  @Test
  public void testGetSeriesType() throws MetadataException {
    // a local series
    assertEquals(Collections.singletonList(TSDataType.DOUBLE),
        testMetaMember
            .getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(0, 0)),
                null));
    // a remote series that can be fetched
    assertEquals(Collections.singletonList(TSDataType.DOUBLE),
        testMetaMember
            .getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(9, 0)),
                null));
    // a non-existent series
    MManager.getInstance().setStorageGroup(TestUtils.getTestSg(10));
    try {
      testMetaMember.getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(10
          , 100)), null);
    } catch (PathNotExistException e) {
      assertEquals("Path [root.test10.s100] does not exist", e.getMessage());
    }
    // a non-existent group
    try {
      testMetaMember.getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(11
          , 100)), null);
    } catch (StorageGroupNotSetException e) {
      assertEquals("Storage group is not set for current seriesPath: [root.test11.s100]",
          e.getMessage());
    }
  }


  @Test
  public void testGetReaderByTimestamp()
      throws QueryProcessException, StorageEngineException, IOException {
    mockDataClusterServer = true;
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setSchemas(new MeasurementSchema[]{TestUtils.getTestSchema(0, 0)});
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setDeviceId(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      try {
        MManager.getInstance().createTimeseries(schema.getMeasurementId(), schema.getType()
            , schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (MetadataException e) {
        // ignore
      }
      for (int j = 0; j < 10; j++) {
        insertPlan.setTime(j);
        insertPlan.setValues(new String[]{String.valueOf(j)});
        planExecutor.processNonQuery(insertPlan);
      }
    }

    QueryContext context = new RemoteQueryContext(
        QueryResourceManager.getInstance().assignQueryId(true));

    try {
      for (int i = 0; i < 10; i++) {
        IReaderByTimestamp readerByTimestamp = testMetaMember
            .getReaderByTimestamp(new Path(TestUtils.getTestSeries(i, 0)),
                Collections.singleton(TestUtils.getTestMeasurement(0)), TSDataType.DOUBLE,
                context);
        for (int j = 0; j < 10; j++) {
          assertEquals(j * 1.0, (double) readerByTimestamp.getValueInTimestamp(j), 0.00001);
        }
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testGetReader() throws QueryProcessException, StorageEngineException, IOException {
    mockDataClusterServer = true;
    InsertPlan insertPlan = new InsertPlan();
    insertPlan.setSchemas(new MeasurementSchema[]{TestUtils.getTestSchema(0, 0)});
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    for (int i = 0; i < 10; i++) {
      insertPlan.setDeviceId(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestSchema(i, 0);
      try {
        MManager.getInstance().createTimeseries(schema.getMeasurementId(), schema.getType()
            , schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (MetadataException e) {
        // ignore
      }
      for (int j = 0; j < 10; j++) {
        insertPlan.setTime(j);
        insertPlan.setValues(new String[]{String.valueOf(j)});
        planExecutor.processNonQuery(insertPlan);
      }
    }

    QueryContext context = new RemoteQueryContext(
        QueryResourceManager.getInstance().assignQueryId(true));

    try {
      for (int i = 0; i < 10; i++) {
        ManagedSeriesReader reader = testMetaMember
            .getSeriesReader(new Path(TestUtils.getTestSeries(i, 0)),
                Collections.singleton(TestUtils.getTestMeasurement(0)), TSDataType.DOUBLE,
                TimeFilter.gtEq(5),
                ValueFilter.ltEq(8.0), context);
        assertTrue(reader.hasNextBatch());
        BatchData batchData = reader.nextBatch();
        for (int j = 5; j < 9; j++) {
          assertTrue(batchData.hasCurrent());
          assertEquals(j, batchData.currentTime());
          assertEquals(j * 1.0, batchData.getDouble(), 0.00001);
          batchData.next();
        }
        assertFalse(batchData.hasCurrent());
        assertFalse(reader.hasNextBatch());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testGetMatchedPaths() throws MetadataException {
    List<String> matchedPaths = testMetaMember
        .getMatchedPaths(TestUtils.getTestSg(0) + ".*");
    assertEquals(20, matchedPaths.size());
    for (int j = 0; j < 10; j++) {
      assertEquals(TestUtils.getTestSeries(0, j), matchedPaths.get(j));
    }
    matchedPaths = testMetaMember
        .getMatchedPaths(TestUtils.getTestSg(10) + ".*");
    assertTrue(matchedPaths.isEmpty());
  }

  @Test
  public void testProcessValidHeartbeatReq() throws QueryProcessException {
    MetaGroupMember testMetaMember = getMetaGroupMember(TestUtils.getNode(10));
    try {
      HeartBeatRequest request = new HeartBeatRequest();
      request.setRequireIdentifier(true);
      HeartBeatResponse response = new HeartBeatResponse();
      testMetaMember.processValidHeartbeatReq(request, response);
      assertEquals(10, response.getFollowerIdentifier());

      request.setRegenerateIdentifier(true);
      testMetaMember.processValidHeartbeatReq(request, response);
      assertNotEquals(10, response.getFollowerIdentifier());
      assertTrue(response.isRequirePartitionTable());

      request.setPartitionTableBytes(partitionTable.serialize());
      testMetaMember.processValidHeartbeatReq(request, response);
      assertEquals(partitionTable, testMetaMember.getPartitionTable());
    } finally {
      testMetaMember.stop();
    }
  }

  @Test
  public void testProcessValidHeartbeatResp()
      throws TTransportException, QueryProcessException {
    MetaGroupMember metaGroupMember = getMetaGroupMember(TestUtils.getNode(10));
    metaGroupMember.start();
    metaGroupMember.onElectionWins();
    try {
      for (int i = 0; i < 10; i++) {
        HeartBeatResponse response = new HeartBeatResponse();
        response.setFollowerIdentifier(i);
        response.setRequirePartitionTable(true);
        metaGroupMember.processValidHeartbeatResp(response, TestUtils.getNode(i));
        metaGroupMember.removeBlindNode(TestUtils.getNode(i));
      }
      assertNotNull(metaGroupMember.getPartitionTable());
    } finally {
      metaGroupMember.stop();
    }
  }

  @Test
  public void testAppendEntry() {
    System.out.println("Start testAppendEntry()");
    System.out.println("Term before append: " + testMetaMember.getTerm().get());

    testMetaMember.setPartitionTable(null);
    CloseFileLog log = new CloseFileLog(TestUtils.getTestSg(0), 0, true);
    log.setCurrLogIndex(0);
    log.setCurrLogTerm(0);
    log.setPreviousLogIndex(-1);
    log.setPreviousLogTerm(-1);
    AppendEntryRequest request = new AppendEntryRequest();
    request.setEntry(log.serialize());
    request.setTerm(0);
    request.setLeaderCommit(0);
    request.setPrevLogIndex(-1);
    request.setPrevLogTerm(-1);
    request.setLeader(new Node("127.0.0.1", 30000, 0, 40000));
    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    testMetaMember.appendEntry(request, handler);
    assertEquals(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE, (long) result.get());
    System.out.println("Term after first append: " + testMetaMember.getTerm().get());

    testMetaMember.setPartitionTable(partitionTable);
    testMetaMember.appendEntry(request, handler);
    System.out.println("Term after second append: " + testMetaMember.getTerm().get());
    assertEquals(Response.RESPONSE_AGREE, (long) result.get());
  }

  @Test
  public void testRemoteAddNode() {
    int prevTimeout = RaftServer.connectionTimeoutInMS;
    RaftServer.connectionTimeoutInMS = 100;
    try {
      // cannot add node when partition table is not built
      testMetaMember.setPartitionTable(null);
      AtomicReference<AddNodeResponse> result = new AtomicReference<>();
      GenericHandler<AddNodeResponse> handler = new GenericHandler<>(TestUtils.getNode(0), result);
      testMetaMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
      AddNodeResponse response = result.get();
      assertEquals(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE, response.getRespNum());

      // cannot add itself
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(0), TestUtils.getStartUpStatus(), handler);
      assertNull(result.get());

      // process the request as a leader
      testMetaMember.setCharacter(LEADER);
      testMetaMember.onElectionWins();
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // adding an existing node is ok
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // process the request as a follower
      testMetaMember.setCharacter(FOLLOWER);
      testMetaMember.setLeader(TestUtils.getNode(1));
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(11), TestUtils.getStartUpStatus(), handler);
      while (result.get() == null) {

      }
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // cannot add a node with conflict id
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      Node node = TestUtils.getNode(12).setNodeIdentifier(10);
      testMetaMember.addNode(node, TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertEquals(Response.RESPONSE_IDENTIFIER_CONFLICT, response.getRespNum());

      //  cannot add a node due to configuration conflict, partition interval
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      node = TestUtils.getNode(13);
      StartUpStatus startUpStatus = TestUtils.getStartUpStatus();
      startUpStatus.setPartitionInterval(-1);
      testMetaMember.addNode(node, startUpStatus, handler);
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertFalse(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertTrue(response.getCheckStatusResponse().isHashSaltEquals());
      assertTrue(response.getCheckStatusResponse().isReplicationNumEquals());

      // cannot add a node due to configuration conflict, hash salt
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      node = TestUtils.getNode(12);
      startUpStatus = TestUtils.getStartUpStatus();
      startUpStatus.setHashSalt(0);
      testMetaMember.addNode(node, startUpStatus, handler);
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertTrue(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertFalse(response.getCheckStatusResponse().isHashSaltEquals());
      assertTrue(response.getCheckStatusResponse().isReplicationNumEquals());

      // cannot add a node due to configuration conflict, replication number
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      node = TestUtils.getNode(12);
      startUpStatus = TestUtils.getStartUpStatus();
      startUpStatus.setReplicationNumber(0);
      testMetaMember.addNode(node, startUpStatus, handler);
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertTrue(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertTrue(response.getCheckStatusResponse().isHashSaltEquals());
      assertFalse(response.getCheckStatusResponse().isReplicationNumEquals());

      // cannot add a node due to network failure
      dummyResponse.set(Response.RESPONSE_NO_CONNECTION);
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      new Thread(() -> {
        try {
          Thread.sleep(200);
          // the network restores now
          dummyResponse.set(Response.RESPONSE_AGREE);
        } catch (InterruptedException e) {
          //ignore
        }
      }).start();
      testMetaMember.addNode(TestUtils.getNode(12), TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());

      // cannot add a node due to leadership lost
      dummyResponse.set(100);
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(13), TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertNull(response);

    } finally {
      testMetaMember.stop();
      RaftServer.connectionTimeoutInMS = prevTimeout;
    }
  }

  @Test
  public void testLoadIdentifier() throws IOException, QueryProcessException {
    try (RandomAccessFile raf = new RandomAccessFile(MetaGroupMember.NODE_IDENTIFIER_FILE_NAME,
        "rw")) {
      raf.writeBytes("100");
    }
    MetaGroupMember metaGroupMember = getMetaGroupMember(new Node());
    assertEquals(100, metaGroupMember.getThisNode().getNodeIdentifier());
    metaGroupMember.closeLogManager();
  }

  @Test
  public void testRemoveNodeWithoutPartitionTable() {
    testMetaMember.setPartitionTable(null);
    AtomicBoolean passed = new AtomicBoolean(false);
    testMetaMember.removeNode(TestUtils.getNode(0), new AsyncMethodCallback<Long>() {
      @Override
      public void onComplete(Long aLong) {

      }

      @Override
      public void onError(Exception e) {
        passed.set(e instanceof PartitionTableUnavailableException);
      }
    });

    assertTrue(passed.get());
  }

  @Test
  public void testRemoveThisNode() {
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setLeader(testMetaMember.getThisNode());
    testMetaMember.setCharacter(LEADER);
    doRemoveNode(resultRef, testMetaMember.getThisNode());
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(testMetaMember.getAllNodes().contains(testMetaMember.getThisNode()));
  }

  @Test
  public void testRemoveLeader() {
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setLeader(TestUtils.getNode(40));
    testMetaMember.setCharacter(FOLLOWER);
    doRemoveNode(resultRef, TestUtils.getNode(40));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(testMetaMember.getAllNodes().contains(TestUtils.getNode(40)));
    assertEquals(ELECTOR, testMetaMember.getCharacter());
    assertEquals(Long.MIN_VALUE, testMetaMember.getLastHeartbeatReceivedTime());
  }

  @Test
  public void testRemoveNonLeader() {
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setLeader(TestUtils.getNode(40));
    testMetaMember.setCharacter(FOLLOWER);
    doRemoveNode(resultRef, TestUtils.getNode(20));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(testMetaMember.getAllNodes().contains(TestUtils.getNode(20)));
    assertEquals(0, testMetaMember.getLastHeartbeatReceivedTime());
  }

  @Test
  public void testRemoveNodeAsLeader() {
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setLeader(testMetaMember.getThisNode());
    testMetaMember.setCharacter(LEADER);
    doRemoveNode(resultRef, TestUtils.getNode(20));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(testMetaMember.getAllNodes().contains(TestUtils.getNode(20)));
    System.out.println("Checking exiled node in testRemoveNodeAsLeader()");
    assertEquals(TestUtils.getNode(20), exiledNode);
  }

  @Test
  public void testRemoveNonExistNode() {
    AtomicBoolean passed = new AtomicBoolean(false);
    testMetaMember.setCharacter(LEADER);
    testMetaMember.setLeader(testMetaMember.getThisNode());
    testMetaMember.removeNode(TestUtils.getNode(120), new AsyncMethodCallback<Long>() {
      @Override
      public void onComplete(Long aLong) {
        passed.set(aLong.equals(Response.RESPONSE_REJECT));
      }

      @Override
      public void onError(Exception e) {
        e.printStackTrace();
      }
    });

    assertTrue(passed.get());
  }

  @Test
  public void testRemoveTooManyNodes() {
    for (int i = 0; i < 8; i++) {
      AtomicReference<Long> resultRef = new AtomicReference<>();
      testMetaMember.setCharacter(LEADER);
      doRemoveNode(resultRef, TestUtils.getNode(90 - i * 10));
      assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
      assertFalse(testMetaMember.getAllNodes().contains(TestUtils.getNode(90 - i * 10)));
    }
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setCharacter(LEADER);
    doRemoveNode(resultRef, TestUtils.getNode(10));
    assertEquals(Response.RESPONSE_CLUSTER_TOO_SMALL, (long) resultRef.get());
    assertTrue(testMetaMember.getAllNodes().contains(TestUtils.getNode(10)));
  }

  private void doRemoveNode(AtomicReference<Long> resultRef, Node nodeToRemove) {
    testMetaMember.removeNode(nodeToRemove, new AsyncMethodCallback<Long>() {
      @Override
      public void onComplete(Long o) {
        resultRef.set(o);
      }

      @Override
      public void onError(Exception e) {
        e.printStackTrace();
      }
    });
    while (resultRef.get() == null) {

    }
  }
}