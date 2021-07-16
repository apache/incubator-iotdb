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

package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.ClusterNode;

import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotNull;

public class DataClientProviderTest {
  private ClusterConfig clusterConfig = ClusterDescriptor.getInstance().getConfig();
  private int maxClientPerNodePerMember;
  private long waitClientTimeoutMS;

  @Before
  public void setUp() {
    maxClientPerNodePerMember = clusterConfig.getMaxClientPerNodePerMember();
    waitClientTimeoutMS = clusterConfig.getWaitClientTimeoutMS();
    clusterConfig.setMaxClientPerNodePerMember(2);
    clusterConfig.setWaitClientTimeoutMS(10L);
  }

  @After
  public void tearDown() {
    clusterConfig.setMaxClientPerNodePerMember(maxClientPerNodePerMember);
    clusterConfig.setWaitClientTimeoutMS(waitClientTimeoutMS);
  }

  @Test
  public void testAsync() throws IOException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    DataClientProvider provider = new DataClientProvider(new Factory());

    assertNotNull(provider.getAsyncDataClient(TestUtils.getNode(0), 100));
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
  }

  @Test
  public void testSync() throws IOException, InterruptedException {
    Node node = new Node();
    node.setDataPort(9003).setInternalIp("localhost").setClientIp("localhost");
    ServerSocket serverSocket = new ServerSocket(node.getDataPort());
    Thread listenThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  serverSocket.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    listenThread.start();

    try {
      boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);
      DataClientProvider provider = new DataClientProvider(new Factory());
      SyncDataClient client = null;
      try {
        client = provider.getSyncDataClient(node, 100);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
      assertNotNull(client);
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }

  @Test
  public void testSyncConcurrency() throws IOException, InterruptedException {
    Node node = new ClusterNode();
    node.setDataPort(9003).setInternalIp("localhost").setClientIp("localhost");
    ServerSocket serverSocket = new ServerSocket(node.getDataPort());
    Thread listenThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  serverSocket.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    listenThread.start();

    try {
      boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);
      DataClientProvider provider = new DataClientProvider(new Factory());
      SyncDataClient client = null;
      try {
        client = provider.getSyncDataClient(node, 100);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
      assertNotNull(client);

      // now try to test multi thread
      ExecutorService service = Executors.newFixedThreadPool(10);
      for (int i = 0; i < 5; i++) {
        service.submit(() -> provider.getSyncDataClient(node, 100));
      }

      // wait time should be great then 10 * 5ms
      Thread.currentThread().sleep(1000);
      int totalNumber = provider.getDataSyncClientPool().getNodeClientNumMap().get(node);
      Assert.assertEquals(6, totalNumber);

      for (int i = 0; i < 4; i++) {
        service.submit(() -> provider.getSyncDataClient(node, 100));
      }

      Thread.currentThread().sleep(100);
      // return one client to pool
      provider.getDataSyncClientPool().putClient(node, client);
      // wait all finish
      Thread.currentThread().sleep(1000);
      totalNumber = provider.getDataSyncClientPool().getNodeClientNumMap().get(node);

      // 6 + 4
      Assert.assertEquals(10, totalNumber);

      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }

  @Test
  public void testAsyncConcurrency() throws IOException, InterruptedException {
    Node node = new ClusterNode();
    node.setDataPort(9003).setInternalIp("localhost").setClientIp("localhost");
    ServerSocket serverSocket = new ServerSocket(node.getDataPort());
    Thread listenThread =
        new Thread(
            () -> {
              while (!Thread.interrupted()) {
                try {
                  serverSocket.accept();
                } catch (IOException e) {
                  return;
                }
              }
            });
    listenThread.start();

    try {
      boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
      DataClientProvider provider = new DataClientProvider(new Factory());
      AsyncDataClient client = null;
      try {
        client = provider.getAsyncDataClient(node, 100);
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
      assertNotNull(client);

      // now try to test multi thread
      ExecutorService service = Executors.newFixedThreadPool(10);
      for (int i = 0; i < 5; i++) {
        service.submit(() -> provider.getAsyncDataClient(node, 100));
      }

      // wait time should be great then 10ms
      Thread.currentThread().sleep(1000);
      int totalNumber = provider.getDataAsyncClientPool().getNodeClientNumMap().get(node);
      Assert.assertEquals(6, totalNumber);

      for (int i = 0; i < 4; i++) {
        service.submit(() -> provider.getAsyncDataClient(node, 100));
      }

      Thread.currentThread().sleep(100);
      // return one client to pool, but number do not -1
      provider.getDataAsyncClientPool().putClient(node, client);
      // wait all finish
      Thread.currentThread().sleep(1000);
      totalNumber = provider.getDataAsyncClientPool().getNodeClientNumMap().get(node);

      // 6 + 4
      Assert.assertEquals(10, totalNumber);

      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      serverSocket.close();
      listenThread.interrupt();
      listenThread.join();
    }
  }
}
