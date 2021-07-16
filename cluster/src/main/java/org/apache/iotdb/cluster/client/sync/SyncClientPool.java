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

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.cluster.utils.ClusterNode;
import org.apache.iotdb.db.utils.TestOnly;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SyncClientPool {

  private static final Logger logger = LoggerFactory.getLogger(SyncClientPool.class);
  private long waitClientTimeoutMS;
  private int maxConnectionForEachNode;
  private Map<ClusterNode, Deque<Client>> clientCaches = new ConcurrentHashMap<>();
  private Map<ClusterNode, Integer> nodeClientNumMap = new ConcurrentHashMap<>();
  private SyncClientFactory syncClientFactory;

  public SyncClientPool(SyncClientFactory syncClientFactory) {
    this.syncClientFactory = syncClientFactory;
    this.waitClientTimeoutMS = ClusterDescriptor.getInstance().getConfig().getWaitClientTimeoutMS();
    this.maxConnectionForEachNode =
        ClusterDescriptor.getInstance().getConfig().getMaxClientPerNodePerMember();
  }

  /**
   * Get a client of the given node from the cache if one is available, or null.
   *
   * <p>IMPORTANT!!! The caller should check whether the return value is null or not!
   *
   * @param node the node want to connect
   * @return if the node can connect, return the client, otherwise null
   */
  public Client getClientForRefresh(Node node) {
    ClusterNode clusterNode = new ClusterNode(node);
    // As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<Client> clientStack = clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (clientStack) {
      if (clientStack.isEmpty()) {
        return null;
      } else {
        return clientStack.poll();
      }
    }
  }

  /**
   * See getClient(Node node, boolean activatedOnly)
   *
   * @param node the node want to connect
   * @return if the node can connect, return the client, otherwise null
   */
  public Client getClient(Node node) {
    return getClient(node, true);
  }

  /**
   * Get a client of the given node from the cache if one is available, or create a new one.
   *
   * <p>IMPORTANT!!! The caller should check whether the return value is null or not!
   *
   * @param node the node want to connect
   * @param activatedOnly if true, only return a client if the node's NodeStatus.isActivated ==
   *     true, which avoid unnecessary wait for already down nodes, but heartbeat attempts should
   *     always try to connect so the node can be reactivated ASAP
   * @return if the node can connect, return the client, otherwise null
   */
  public Client getClient(Node node, boolean activatedOnly) {
    ClusterNode clusterNode = new ClusterNode(node);
    if (activatedOnly && !NodeStatusManager.getINSTANCE().isActivated(node)) {
      return null;
    }

    // As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<Client> clientStack = clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (clientStack) {
      if (clientStack.isEmpty()) {
        int nodeClientNum = nodeClientNumMap.getOrDefault(clusterNode, 0);
        if (nodeClientNum >= maxConnectionForEachNode) {
          return waitForClient(clientStack, clusterNode);
        } else {
          Client client = null;
          try {
            client = syncClientFactory.getSyncClient(clusterNode, this);
          } catch (TTransportException e) {
            logger.error("Cannot open transport for client {}", node, e);
            return null;
          }
          nodeClientNumMap.compute(
              clusterNode,
              (n, oldValue) -> {
                if (oldValue == null) return 1;
                return oldValue + 1;
              });
          return client;
        }
      } else {
        return clientStack.pop();
      }
    }
  }

  @SuppressWarnings("squid:S2273") // synchronized outside
  private Client waitForClient(Deque<Client> clientStack, ClusterNode clusterNode) {
    // wait for an available client
    long waitStart = System.currentTimeMillis();
    while (clientStack.isEmpty()) {
      try {
        clientStack.wait(waitClientTimeoutMS);
        if (clientStack.isEmpty()
            && System.currentTimeMillis() - waitStart >= waitClientTimeoutMS) {
          logger.warn(
              "Cannot get an available client after {}ms, create a new one", waitClientTimeoutMS);
          Client client = syncClientFactory.getSyncClient(clusterNode, this);
          nodeClientNumMap.computeIfPresent(clusterNode, (n, oldValue) -> oldValue + 1);
          return client;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted when waiting for an available client of {}", clusterNode);
        return null;
      } catch (TTransportException e) {
        logger.error("Cannot open transport for client {}", clusterNode, e);
        return null;
      }
    }
    return clientStack.pop();
  }

  /**
   * Return a client of a node to the pool. Closed client should not be returned.
   *
   * @param node connection node
   * @param client push client to pool
   */
  public void putClient(Node node, Client client) {
    ClusterNode clusterNode = new ClusterNode(node);
    // As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<Client> clientStack = clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (clientStack) {
      if (client.getInputProtocol() != null && client.getInputProtocol().getTransport().isOpen()) {
        clientStack.push(client);
        NodeStatusManager.getINSTANCE().activate(node);
      } else {
        try {
          clientStack.push(syncClientFactory.getSyncClient(node, this));
          NodeStatusManager.getINSTANCE().activate(node);
        } catch (TTransportException e) {
          logger.error("Cannot open transport for client {}", node, e);
          nodeClientNumMap.computeIfPresent(clusterNode, (n, oldValue) -> oldValue - 1);
          NodeStatusManager.getINSTANCE().deactivate(node);
        }
      }
      clientStack.notifyAll();
    }
  }

  @TestOnly
  public Map<ClusterNode, Integer> getNodeClientNumMap() {
    return nodeClientNumMap;
  }
}
