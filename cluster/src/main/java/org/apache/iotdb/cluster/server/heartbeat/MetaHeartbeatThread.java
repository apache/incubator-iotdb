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

package org.apache.iotdb.cluster.server.heartbeat;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaHeartbeatThread extends HeartbeatThread {

  private static final Logger logger = LoggerFactory.getLogger(MetaHeartbeatThread.class);
  private MetaGroupMember localMetaMember;

  public MetaHeartbeatThread(MetaGroupMember metaMember) {
    super(metaMember);
    this.localMetaMember = metaMember;
  }

  @Override
  void sendHeartbeat(Node node, AsyncClient client) {
    // if the node's identifier is not clear, require it
    request.setRequireIdentifier(!node.isSetNodeIdentifier());
    synchronized (localMetaMember.getIdConflictNodes()) {
      request.unsetRegenerateIdentifier();
      if (localMetaMember.getIdConflictNodes().contains(node)) {
        request.setRegenerateIdentifier(true);
      }
    }

    // if the node requires the partition table and it is ready, send it
    if (localMetaMember.isNodeBlind(node)) {
      if (localMetaMember.getPartitionTable() != null) {
        logger.debug("Send partition table to {}", node);
        request.setPartitionTableBytes(localMetaMember.getPartitionTable().serialize());
        // if the node does not receive the list, it will require it in the next heartbeat, so
        // we can remove it now
        localMetaMember.removeBlindNode(node);
      }
    }

    super.sendHeartbeat(node, client);
    request.unsetPartitionTableBytes();
  }
}
