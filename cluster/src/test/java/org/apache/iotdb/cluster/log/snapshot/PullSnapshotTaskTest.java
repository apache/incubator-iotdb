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

package org.apache.iotdb.cluster.log.snapshot;

import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.common.TestAsyncDataClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotManager.SlotStatus;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PullSnapshotTaskTest extends DataSnapshotTest {

  private static final Logger logger = LoggerFactory.getLogger(PullSnapshotTaskTest.class);
  private DataGroupMember sourceMember;
  private DataGroupMember targetMember;
  private List<TimeseriesSchema> timeseriesSchemas;
  private List<TsFileResource> tsFileResources;
  private boolean hintRegistered;
  private int requiredRetries;
  private int defaultCompactionThread =
      IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();

  @Override
  @Before
  public void setUp() throws MetadataException, StartupException {
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(0);
    super.setUp();
    hintRegistered = false;
    sourceMember =
        new TestDataGroupMember() {
          @Override
          public AsyncClient getAsyncClient(Node node, boolean activatedOnly) {
            return getAsyncClient(node);
          }

          @Override
          public AsyncClient getAsyncClient(Node node) {
            try {
              return new TestAsyncDataClient(node, null) {
                @Override
                public void pullSnapshot(
                    PullSnapshotRequest request,
                    AsyncMethodCallback<PullSnapshotResp> resultHandler) {
                  new Thread(
                          () -> {
                            try {
                              if (request.requireReadOnly) {
                                targetMember.setReadOnly();
                              }
                              resultHandler.onComplete(targetMember.getSnapshot(request));
                            } catch (IOException e) {
                              resultHandler.onError(e);
                            }
                          })
                      .start();
                }
              };
            } catch (IOException e) {
              return null;
            }
          }

          @Override
          public Client getSyncClient(Node node) {
            return new SyncDataClient(null) {
              @Override
              public PullSnapshotResp pullSnapshot(PullSnapshotRequest request) throws TException {
                try {
                  if (request.requireReadOnly) {
                    targetMember.setReadOnly();
                  }
                  return targetMember.getSnapshot(request);
                } catch (IOException e) {
                  throw new TException(e);
                }
              }

              @Override
              public ByteBuffer readFile(String filePath, long offset, int length)
                  throws TException {
                try {
                  return IOUtils.readFile(filePath, offset, length);
                } catch (IOException e) {
                  throw new TException(e);
                }
              }

              @Override
              public TProtocol getInputProtocol() {
                return new TBinaryProtocol(
                    new TTransport() {
                      @Override
                      public boolean isOpen() {
                        return false;
                      }

                      @Override
                      public void open() {}

                      @Override
                      public void close() {}

                      @Override
                      public int read(byte[] buf, int off, int len) {
                        return 0;
                      }

                      @Override
                      public void write(byte[] buf, int off, int len) {}

                      @Override
                      public TConfiguration getConfiguration() {
                        return null;
                      }

                      @Override
                      public void updateKnownMessageSize(long size) {}

                      @Override
                      public void checkReadBytesAvailable(long numBytes)
                          throws TTransportException {}
                    });
              }
            };
          }

          @Override
          public void registerPullSnapshotHint(PullSnapshotTaskDescriptor descriptor) {
            hintRegistered = true;
          }
        };
    sourceMember.setMetaGroupMember(metaGroupMember);
    sourceMember.setLogManager(new TestLogManager(0));
    sourceMember.setThisNode(TestUtils.getNode(0));
    targetMember =
        new TestDataGroupMember() {
          @Override
          public PullSnapshotResp getSnapshot(PullSnapshotRequest request) throws IOException {
            if (requiredRetries > 0) {
              requiredRetries--;
              throw new IOException("Faked pull snapshot exception");
            }

            try {
              tsFileResources = TestUtils.prepareTsFileResources(0, 10, 10, 10, true);
            } catch (WriteProcessException e) {
              return null;
            }
            Map<Integer, ByteBuffer> snapshotBytes = new HashMap<>();
            for (int i = 0; i < 10; i++) {
              FileSnapshot fileSnapshot = new FileSnapshot();
              fileSnapshot.addFile(tsFileResources.get(i), TestUtils.getNode(i));
              fileSnapshot.setTimeseriesSchemas(
                  Collections.singletonList(TestUtils.getTestTimeSeriesSchema(0, i)));
              timeseriesSchemas.add(TestUtils.getTestTimeSeriesSchema(0, i));
              snapshotBytes.put(i, fileSnapshot.serialize());
            }
            PullSnapshotResp pullSnapshotResp = new PullSnapshotResp();
            pullSnapshotResp.setSnapshotBytes(snapshotBytes);
            return pullSnapshotResp;
          }
        };
    targetMember.setThisNode(TestUtils.getNode(1));
    targetMember.setLogManager(new TestLogManager(1));

    timeseriesSchemas = new ArrayList<>();
    requiredRetries = 0;
  }

  @Test
  public void testAsync() throws IllegalPathException, StorageEngineException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    try {
      testNormal(false);
    } finally {
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    }
  }

  @Test
  public void testReadOnly() throws StorageEngineException, IllegalPathException {
    testNormal(true);
    assertTrue(targetMember.isReadOnly());
  }

  @Test
  public void testSync() throws IllegalPathException, StorageEngineException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);
    try {
      testNormal(false);
    } finally {
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    }
  }

  @Test
  public void testWithRetry() throws StorageEngineException, IllegalPathException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    int pullSnapshotRetryIntervalMs =
        ClusterDescriptor.getInstance().getConfig().getPullSnapshotRetryIntervalMs();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);
    ClusterDescriptor.getInstance().getConfig().setPullSnapshotRetryIntervalMs(100);
    try {
      requiredRetries = 3;
      testNormal(false);
    } finally {
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
      ClusterDescriptor.getInstance()
          .getConfig()
          .setPullSnapshotRetryIntervalMs(pullSnapshotRetryIntervalMs);
    }
  }

  private void testNormal(boolean requiresReadOnly)
      throws IllegalPathException, StorageEngineException {
    PartitionGroup partitionGroup = new PartitionGroup();
    partitionGroup.add(TestUtils.getNode(1));
    List<Integer> slots = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      slots.add(i);
      sourceMember.getSlotManager().setToPulling(i, TestUtils.getNode(1));
    }
    PullSnapshotTaskDescriptor descriptor =
        new PullSnapshotTaskDescriptor(partitionGroup, slots, requiresReadOnly);

    PullSnapshotTask task =
        new PullSnapshotTask(descriptor, sourceMember, FileSnapshot.Factory.INSTANCE, null);
    task.call();

    for (TimeseriesSchema timeseriesSchema : timeseriesSchemas) {
      assertTrue(IoTDB.metaManager.isPathExist(new PartialPath(timeseriesSchema.getFullPath())));
    }
    StorageGroupProcessor processor =
        StorageEngine.getInstance().getProcessor(new PartialPath(TestUtils.getTestSg(0)));
    assertEquals(9, processor.getPartitionMaxFileVersions(0));
    List<TsFileResource> loadedFiles = processor.getSequenceFileTreeSet();
    assertEquals(tsFileResources.size(), loadedFiles.size());
    for (int i = 0; i < 9; i++) {
      if (i != loadedFiles.get(i).getMaxPlanIndex()) {
        logger.error(
            "error occurred, i={}, minPlanIndex={}, maxPlanIndex={}, tsFileName={}",
            i,
            loadedFiles.get(i).getMinPlanIndex(),
            loadedFiles.get(i).getMaxPlanIndex(),
            loadedFiles.get(i).getTsFile().getAbsolutePath());
      }
      assertEquals(-1, loadedFiles.get(i).getMaxPlanIndex());
    }
    assertEquals(0, processor.getUnSequenceFileList().size());

    for (TsFileResource tsFileResource : tsFileResources) {
      // source files should be deleted after being pulled
      assertFalse(tsFileResource.getTsFile().exists());
    }
    assertTrue(hintRegistered);
    for (int i = 0; i < 20; i++) {
      assertEquals(SlotStatus.NULL, sourceMember.getSlotManager().getStatus(i));
    }

    assertFalse(task.getSnapshotSave().exists());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    sourceMember.closeLogManager();
    targetMember.closeLogManager();
    sourceMember.stop();
    targetMember.stop();
    super.tearDown();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(defaultCompactionThread);
  }
}
