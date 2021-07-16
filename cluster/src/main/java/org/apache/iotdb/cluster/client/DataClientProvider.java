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

import org.apache.iotdb.cluster.client.async.AsyncClientPool;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncDataClient.FactoryAsync;
import org.apache.iotdb.cluster.client.sync.SyncClientPool;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;

import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;

public class DataClientProvider {

  /**
   * dataClientPool provides reusable thrift clients to connect to the DataGroupMembers of other
   * nodes
   */
  private AsyncClientPool dataAsyncClientPool;

  private SyncClientPool dataSyncClientPool;

  private static final String GET_CLIENT_FAILED_MSG = "can not get client for node=";

  public DataClientProvider(TProtocolFactory factory) {
    if (!ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      dataSyncClientPool = new SyncClientPool(new SyncDataClient.FactorySync(factory));
    } else {
      dataAsyncClientPool = new AsyncClientPool(new FactoryAsync(factory));
    }
  }

  AsyncClientPool getDataAsyncClientPool() {
    return dataAsyncClientPool;
  }

  SyncClientPool getDataSyncClientPool() {
    return dataSyncClientPool;
  }

  /**
   * Get a thrift client from the head of deque that will connect to "node" using the data port.
   *
   * @param node the node to be connected
   * @param timeout timeout threshold of connection
   */
  public AsyncDataClient getAsyncDataClient(Node node, int timeout) throws IOException {
    AsyncDataClient client = (AsyncDataClient) getDataAsyncClientPool().getClient(node);
    if (client == null) {
      throw new IOException(GET_CLIENT_FAILED_MSG + node);
    }
    client.setTimeout(timeout);
    return client;
  }

  /**
   * Get a thrift client from the tail of deque that will connect to "node" using the data port for
   * refresh.
   *
   * @param node the node to be connected
   * @param timeout timeout threshold of connection
   */
  public AsyncDataClient getAsyncDataClientForRefresh(Node node, int timeout) throws IOException {
    AsyncDataClient client = (AsyncDataClient) getDataAsyncClientPool().getClientForRefresh(node);
    if (client == null) {
      throw new IOException(GET_CLIENT_FAILED_MSG + node);
    }
    client.setTimeout(timeout);
    return client;
  }

  /**
   * IMPORTANT!!! After calling this function, the caller should make sure to call {@link
   * org.apache.iotdb.cluster.utils.ClientUtils#putBackSyncClient(Client)} to put the client back
   * into the client pool, otherwise there is a risk of client leakage.
   *
   * <p>Get a thrift client from the head of deque that will connect to "node" using the data port.
   *
   * @param node the node to be connected
   * @param timeout timeout threshold of connection
   */
  public SyncDataClient getSyncDataClient(Node node, int timeout) throws IOException {
    SyncDataClient client = (SyncDataClient) getDataSyncClientPool().getClient(node);
    if (client == null) {
      throw new IOException(GET_CLIENT_FAILED_MSG + node);
    }
    client.setTimeout(timeout);
    return client;
  }

  /**
   * IMPORTANT!!! After calling this function, the caller should make sure to call {@link
   * org.apache.iotdb.cluster.utils.ClientUtils#putBackSyncClient(Client)} to put the client back
   * into the client pool, otherwise there is a risk of client leakage.
   *
   * <p>Get a thrift client from the tail of deque that will connect to "node" using the data port.
   *
   * @param node the node to be connected
   * @param timeout timeout threshold of connection
   */
  public SyncDataClient getSyncDataClientForRefresh(Node node, int timeout) throws IOException {
    SyncDataClient client = (SyncDataClient) getDataSyncClientPool().getClientForRefresh(node);
    if (client == null) {
      throw new IOException(GET_CLIENT_FAILED_MSG + node);
    }
    client.setTimeout(timeout);
    return client;
  }
}
