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

package org.apache.iotdb.cluster.client.sync;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.utils.ClusterNode;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncClientPool {

  private static final Logger logger = LoggerFactory.getLogger(
      SyncClientPool.class);
  private static final long WAIT_CLIENT_TIMEOUT_MS = 5 * 1000L;
  private int maxConnectionForEachNode;
  private Map<ClusterNode, Deque<Client>> clientCaches = new ConcurrentHashMap<>();
  private Map<ClusterNode, Integer> nodeClientNumMap = new ConcurrentHashMap<>();
  private SyncClientFactory syncClientFactory;

  public SyncClientPool(SyncClientFactory syncClientFactory) {
    this.syncClientFactory = syncClientFactory;
    this.maxConnectionForEachNode =
        ClusterDescriptor.getInstance().getConfig().getMaxClientPerNodePerMember();
  }

  /**
   * Get a client of the given node from the cache if one is available, or create a new one.
   * @param node
   * @return
   * @throws IOException
   */
  public Client getClient(Node node) {
    ClusterNode clusterNode = new ClusterNode(node);
    //As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<Client> clientStack = clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (this) {
      if (clientStack.isEmpty()) {
        int nodeClientNum = nodeClientNumMap.getOrDefault(clusterNode, 0);
        if (nodeClientNum >= maxConnectionForEachNode) {
          return waitForClient(clientStack, clusterNode, nodeClientNum);
        } else {
          nodeClientNumMap.put(clusterNode, nodeClientNum + 1);
          return createClient(clusterNode, nodeClientNum);
        }
      } else {
        return clientStack.pop();
      }
    }
  }

  @SuppressWarnings("squid:S2273") // synchronized outside
  private Client waitForClient(Deque<Client> clientStack, ClusterNode node, int nodeClientNum) {
    // wait for an available client
    long waitStart = System.currentTimeMillis();
    while (clientStack.isEmpty()) {
      try {
        this.wait(WAIT_CLIENT_TIMEOUT_MS);
        if (clientStack.isEmpty() && System.currentTimeMillis() - waitStart >= WAIT_CLIENT_TIMEOUT_MS) {
          logger.warn("Cannot get an available client after {}ms, create a new one",
              WAIT_CLIENT_TIMEOUT_MS);
          nodeClientNumMap.put(node, nodeClientNum + 1);
          return createClient(node, nodeClientNum);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted when waiting for an available client of {}", node);
        return null;
      }
    }
    return clientStack.pop();
  }

  /**
   * Return a client of a node to the pool. Closed client should not be returned.
   * @param node
   * @param client
   */
  void putClient(Node node, Client client) {
    ClusterNode clusterNode = new ClusterNode(node);
    //As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<Client> clientStack = clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (this) {
      if (client.getInputProtocol() != null && client.getInputProtocol().getTransport().isOpen()) {
        clientStack.push(client);
      } else {
        try {
          clientStack.push(syncClientFactory.getSyncClient(node, this));
        } catch (TTransportException e) {
          logger.error("Cannot open transport for client", e);
          nodeClientNumMap.computeIfPresent(clusterNode, (n, oldValue) -> oldValue - 1);
        }
      }
      this.notifyAll();
    }
  }

  private Client createClient(ClusterNode node, int nodeClientNum) {
    try {
      return syncClientFactory.getSyncClient(node, this);
    } catch (TTransportException e) {
      if (e.getCause() instanceof ConnectException || e.getCause() instanceof SocketTimeoutException) {
        logger.debug("Cannot open transport for client {} : {}", node, e.getMessage());
      } else {
        logger.error("Cannot open transport for client {}", node, e);
      }
      nodeClientNumMap.put(node, nodeClientNum);
      return null;
    }
  }
}
