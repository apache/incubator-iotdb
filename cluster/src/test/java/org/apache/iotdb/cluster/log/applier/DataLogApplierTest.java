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

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.client.DataClientProvider;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestAsyncMetaClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.metadata.MetaPuller;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.GetAllPathsResult;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.cluster.server.service.DataAsyncService;
import org.apache.iotdb.cluster.server.service.MetaAsyncService;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import junit.framework.TestCase;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DataLogApplierTest extends IoTDBTest {

  private static final Logger logger = LoggerFactory.getLogger(DataLogApplierTest.class);
  private boolean partialWriteEnabled;

  private TestMetaGroupMember testMetaGroupMember =
      new TestMetaGroupMember() {
        @Override
        public boolean syncLeader(RaftMember.CheckConsistency checkConsistency) {
          try {
            // for testApplyCreateMultiTimeseiresWithPulling()
            IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg2"));
          } catch (MetadataException e) {
            logger.error("Cannot set sg for test", e);
          }
          return true;
        }

        @Override
        public DataGroupMember getLocalDataMember(RaftNode header, Object request) {
          return testDataGroupMember;
        }

        @Override
        public AsyncClient getAsyncClient(Node node, boolean activatedOnly) {
          return getAsyncClient(node);
        }

        @Override
        public AsyncClient getAsyncClient(Node node) {
          try {
            return new TestAsyncMetaClient(null, null, node, null) {
              @Override
              public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
                new Thread(
                        () ->
                            new MetaAsyncService(testMetaGroupMember)
                                .queryNodeStatus(resultHandler))
                    .start();
              }
            };
          } catch (IOException e) {
            return null;
          }
        }
      };

  private TestDataGroupMember testDataGroupMember =
      new TestDataGroupMember(
          TestUtils.getNode(10), Collections.singletonList(TestUtils.getNode(10)));

  private DataLogApplier applier;

  @Override
  @Before
  public void setUp()
      throws org.apache.iotdb.db.exception.StartupException, QueryProcessException,
          IllegalPathException {
    IoTDB.setMetaManager(CMManager.getInstance());
    testMetaGroupMember.setCoordinator(new Coordinator());
    MetaPuller.getInstance().init(testMetaGroupMember);
    super.setUp();
    MetaPuller.getInstance().init(testMetaGroupMember);
    PartitionGroup allNodes = new PartitionGroup();
    for (int i = 0; i < 100; i += 10) {
      allNodes.add(TestUtils.getNode(i));
    }

    testMetaGroupMember.setAllNodes(allNodes);
    testMetaGroupMember.setPartitionTable(new SlotPartitionTable(allNodes, TestUtils.getNode(0)));
    testMetaGroupMember.setThisNode(TestUtils.getNode(0));

    testMetaGroupMember.setLeader(testMetaGroupMember.getThisNode());
    testDataGroupMember.setLeader(TestUtils.getNode(10));
    testDataGroupMember.setCharacter(NodeCharacter.LEADER);
    testMetaGroupMember.setCharacter(NodeCharacter.LEADER);
    NodeStatusManager.getINSTANCE().setMetaGroupMember(testMetaGroupMember);
    partialWriteEnabled = IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert();
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(false);
    testMetaGroupMember.setClientProvider(
        new DataClientProvider(new Factory()) {
          @Override
          public AsyncDataClient getAsyncDataClient(Node node, int timeout) throws IOException {
            return new AsyncDataClient(null, null, node, null) {
              @Override
              public void getAllPaths(
                  RaftNode header,
                  List<String> path,
                  boolean withAlias,
                  AsyncMethodCallback<GetAllPathsResult> resultHandler) {
                new Thread(
                        () ->
                            new DataAsyncService(testDataGroupMember)
                                .getAllPaths(header, path, withAlias, resultHandler))
                    .start();
              }

              @Override
              public void pullTimeSeriesSchema(
                  PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
                new Thread(
                        () -> {
                          List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
                          for (String path : request.prefixPaths) {
                            if (path.startsWith(TestUtils.getTestSg(4))) {
                              for (int i = 0; i < 10; i++) {
                                timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(4, i));
                              }
                            } else if (!path.startsWith(TestUtils.getTestSg(5))) {
                              resultHandler.onError(new StorageGroupNotSetException(path));
                              return;
                            }
                          }
                          PullSchemaResp resp = new PullSchemaResp();
                          // serialize the schemas
                          ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                          DataOutputStream dataOutputStream =
                              new DataOutputStream(byteArrayOutputStream);
                          try {
                            dataOutputStream.writeInt(timeseriesSchemas.size());
                            for (TimeseriesSchema timeseriesSchema : timeseriesSchemas) {
                              timeseriesSchema.serializeTo(dataOutputStream);
                            }
                          } catch (IOException ignored) {
                            // unreachable for we are using a ByteArrayOutputStream
                          }
                          resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
                          resultHandler.onComplete(resp);
                        })
                    .start();
              }

              @Override
              public void pullMeasurementSchema(
                  PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
                new Thread(
                        () ->
                            new DataAsyncService(testDataGroupMember)
                                .pullMeasurementSchema(request, resultHandler))
                    .start();
              }
            };
          }
        });
    ((CMManager) IoTDB.metaManager).setMetaGroupMember(testMetaGroupMember);
    testDataGroupMember.setMetaGroupMember(testMetaGroupMember);
    applier = new DataLogApplier(testMetaGroupMember, testDataGroupMember);
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    testDataGroupMember.stop();
    testDataGroupMember.closeLogManager();
    testMetaGroupMember.stop();
    testMetaGroupMember.closeLogManager();
    super.tearDown();
    NodeStatusManager.getINSTANCE().setMetaGroupMember(null);
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(partialWriteEnabled);
  }

  @Test
  public void testApplyInsert()
      throws QueryProcessException, IOException, QueryFilterOptimizationException,
          StorageEngineException, MetadataException, InterruptedException {
    InsertRowPlan insertPlan = new InsertRowPlan();
    PhysicalPlanLog log = new PhysicalPlanLog();
    log.setPlan(insertPlan);

    // this series is already created
    insertPlan.setPrefixPath(new PartialPath(TestUtils.getTestSg(1)));
    insertPlan.setTime(1);
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    insertPlan.setValues(new Object[] {"1.0"});
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurementMNodes(new MeasurementMNode[] {TestUtils.getTestMeasurementMNode(0)});

    applier.apply(log);
    QueryDataSet dataSet = query(Collections.singletonList(TestUtils.getTestSeries(1, 0)), null);
    assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    assertEquals(1, record.getTimestamp());
    assertEquals(1, record.getFields().size());
    assertEquals(1.0, record.getFields().get(0).getDoubleV(), 0.00001);
    assertFalse(dataSet.hasNext());

    // this series is not created but can be fetched
    insertPlan.setPrefixPath(new PartialPath(TestUtils.getTestSg(4)));
    applier.apply(log);
    dataSet = query(Collections.singletonList(TestUtils.getTestSeries(4, 0)), null);
    assertTrue(dataSet.hasNext());
    record = dataSet.next();
    assertEquals(1, record.getTimestamp());
    assertEquals(1, record.getFields().size());
    assertEquals(1.0, record.getFields().get(0).getDoubleV(), 0.00001);
    assertFalse(dataSet.hasNext());

    // this series does not exists any where
    insertPlan.setPrefixPath(new PartialPath(TestUtils.getTestSg(5)));
    applier.apply(log);
    assertEquals(
        "org.apache.iotdb.db.exception.metadata.PathNotExistException: Path [root.test5.s0] does not exist",
        log.getException().getMessage());

    // this storage group is not even set
    insertPlan.setPrefixPath(new PartialPath(TestUtils.getTestSg(16)));
    applier.apply(log);
    assertEquals(
        "org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException: Storage group is not set for current seriesPath: [root.test16]",
        log.getException().getMessage());
  }

  @Test
  public void testApplyDeletion()
      throws QueryProcessException, MetadataException, QueryFilterOptimizationException,
          StorageEngineException, IOException, InterruptedException {
    DeletePlan deletePlan = new DeletePlan();
    deletePlan.setPaths(Collections.singletonList(new PartialPath(TestUtils.getTestSeries(0, 0))));
    deletePlan.setDeleteEndTime(50);
    applier.apply(new PhysicalPlanLog(deletePlan));
    QueryDataSet dataSet = query(Collections.singletonList(TestUtils.getTestSeries(0, 0)), null);
    int cnt = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(cnt + 51L, record.getTimestamp());
      assertEquals((cnt + 51) * 1.0, record.getFields().get(0).getDoubleV(), 0.00001);
      cnt++;
    }
    assertEquals(49, cnt);
  }

  @Test
  public void testApplyCloseFile() throws org.apache.iotdb.db.exception.IoTDBException {
    StorageGroupProcessor storageGroupProcessor =
        StorageEngine.getInstance().getProcessor(new PartialPath(TestUtils.getTestSg(0)));
    TestCase.assertFalse(storageGroupProcessor.getWorkSequenceTsFileProcessors().isEmpty());

    CloseFileLog closeFileLog = new CloseFileLog(TestUtils.getTestSg(0), 0, true);
    applier.apply(closeFileLog);
    TestCase.assertTrue(storageGroupProcessor.getWorkSequenceTsFileProcessors().isEmpty());
  }

  @Test
  public void testApplyFlush() throws IllegalPathException {
    // existing sg
    FlushPlan flushPlan =
        new FlushPlan(null, Collections.singletonList(new PartialPath(TestUtils.getTestSg(0))));
    PhysicalPlanLog log = new PhysicalPlanLog(flushPlan);

    applier.apply(log);
    assertNull(log.getException());

    // non-existing sg
    flushPlan =
        new FlushPlan(null, Collections.singletonList(new PartialPath(TestUtils.getTestSg(20))));
    log = new PhysicalPlanLog(flushPlan);

    applier.apply(log);
    assertEquals(
        "Storage group is not set for current seriesPath: [root.test20]",
        log.getException().getMessage());
  }

  @Test
  public void testApplyCreateMultiTimeseiresWithPulling() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg1"));
    CreateMultiTimeSeriesPlan multiTimeSeriesPlan = new CreateMultiTimeSeriesPlan();
    multiTimeSeriesPlan.setIndexes(Collections.emptyList());
    multiTimeSeriesPlan.setPaths(
        Arrays.asList(
            new PartialPath("root.sg1.s1"),
            // root.sg2 should be pulled
            new PartialPath("root.sg2.s1")));
    multiTimeSeriesPlan.setCompressors(
        Arrays.asList(CompressionType.UNCOMPRESSED, CompressionType.UNCOMPRESSED));
    multiTimeSeriesPlan.setDataTypes(Arrays.asList(TSDataType.DOUBLE, TSDataType.DOUBLE));
    multiTimeSeriesPlan.setEncodings(Arrays.asList(TSEncoding.GORILLA, TSEncoding.GORILLA));

    PhysicalPlanLog log = new PhysicalPlanLog(multiTimeSeriesPlan);
    // the applier should sync meta leader to get root.sg2 and report no error
    applier.apply(log);
    assertTrue(IoTDB.metaManager.getAllStorageGroupPaths().contains(new PartialPath("root.sg2")));
    assertNull(log.getException());
  }

  @Test
  public void testApplyDeletePartitionFilter() throws QueryProcessException {
    applier.setQueryExecutor(
        new PlanExecutor() {
          @Override
          public boolean processNonQuery(PhysicalPlan plan) {
            assertTrue(plan instanceof DeletePlan);
            DeletePlan deletePlan = (DeletePlan) plan;
            TimePartitionFilter planFilter = deletePlan.getPartitionFilter();
            TimePartitionFilter memberFilter = testDataGroupMember.getTimePartitionFilter();
            assertEquals(planFilter, memberFilter);
            return true;
          }
        });

    DeletePlan deletePlan = new DeletePlan();
    PhysicalPlanLog log = new PhysicalPlanLog(deletePlan);
    applier.apply(log);
    assertNull(log.getException());
  }
}
