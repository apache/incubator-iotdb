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

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.ElectionHandler;
import org.apache.iotdb.cluster.server.handlers.caller.HeartbeatHandler;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.ClientUtils;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HeartbeatThread takes the responsibility to send heartbeats (when this node is a leader), check
 * if the leader is still online (when this node is a follower) or start elections (when this node
 * is a elector).
 */
public class HeartbeatThread implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(HeartbeatThread.class);

  private RaftMember localMember;
  private String memberName;
  HeartBeatRequest request = new HeartBeatRequest();
  ElectionRequest electionRequest = new ElectionRequest();

  private Random random = new Random();
  boolean hasHadLeader = false;

  HeartbeatThread(RaftMember localMember) {
    this.localMember = localMember;
    memberName = localMember.getName();
  }

  @Override
  public void run() {
    logger.info("{}: Heartbeat thread starts...", memberName);
    // sleep random time to reduce first election conflicts
    long electionWait =
        ClusterConstant.getElectionLeastTimeOutMs()
            + Math.abs(random.nextLong() % ClusterConstant.getElectionRandomTimeOutMs());
    try {
      logger.info("{}: Sleep {}ms before first election", memberName, electionWait);
      Thread.sleep(electionWait);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    while (!Thread.interrupted()) {
      try {
        switch (localMember.getCharacter()) {
          case LEADER:
            // send heartbeats to the followers
            sendHeartbeats();
            synchronized (localMember.getHeartBeatWaitObject()) {
              localMember.getHeartBeatWaitObject().wait(RaftServer.getHeartBeatIntervalMs());
            }
            hasHadLeader = true;
            break;
          case FOLLOWER:
            // check if heartbeat times out
            long heartBeatInterval =
                System.currentTimeMillis() - localMember.getLastHeartbeatReceivedTime();
            if (heartBeatInterval >= RaftServer.getConnectionTimeoutInMS()) {
              // the leader is considered dead, an election will be started in the next loop
              logger.info("{}: The leader {} timed out", memberName, localMember.getLeader());
              localMember.setCharacter(NodeCharacter.ELECTOR);
              localMember.setLeader(ClusterConstant.EMPTY_NODE);
            } else {
              logger.debug(
                  "{}: Heartbeat from leader {} is still valid",
                  memberName,
                  localMember.getLeader());
              synchronized (localMember.getHeartBeatWaitObject()) {
                localMember.getHeartBeatWaitObject().wait(RaftServer.getConnectionTimeoutInMS());
              }
            }
            hasHadLeader = true;
            break;
          case ELECTOR:
          default:
            onElectionsStart();
            startElections();
            onElectionsEnd();
            break;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.error("{}: Unexpected heartbeat exception:", memberName, e);
      }
    }

    logger.info("{}: Heartbeat thread exits", memberName);
  }

  protected void onElectionsStart() {
    logger.info("{}: Start elections", memberName);
  }

  protected void onElectionsEnd() {
    logger.info("{}: End elections", memberName);
  }

  /** Send each node (except the local node) in the group of the member a heartbeat. */
  protected void sendHeartbeats() {
    synchronized (localMember.getTerm()) {
      request.setTerm(localMember.getTerm().get());
      request.setLeader(localMember.getThisNode());
      request.setCommitLogIndex(localMember.getLogManager().getCommitLogIndex());
      request.setCommitLogTerm(localMember.getLogManager().getCommitLogTerm());

      sendHeartbeats(localMember.getAllNodes());
    }
  }

  /** Send each node (except the local node) in list a heartbeat. */
  @SuppressWarnings("java:S2445")
  private void sendHeartbeats(Collection<Node> nodes) {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Send heartbeat to {} followers, commit log index = {}",
          memberName,
          nodes.size() - 1,
          request.getCommitLogIndex());
    }
    synchronized (nodes) {
      // avoid concurrent modification
      for (Node node : nodes) {
        if (node.equals(localMember.getThisNode())) {
          continue;
        }
        if (Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        if (localMember.getCharacter() != NodeCharacter.LEADER) {
          // if the character changes, abort the remaining heartbeats
          logger.warn("The leadership of node {} is ended.", localMember.getThisNode());
          return;
        }

        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          sendHeartbeatAsync(node);
        } else {
          sendHeartbeatSync(node);
        }
      }
    }
  }

  /**
   * Send a heartbeat to "node" through "client".
   *
   * @param node
   */
  void sendHeartbeatAsync(Node node) {
    AsyncClient client = localMember.getAsyncHeartbeatClient(node);
    if (client != null) {
      // connecting to the local node results in a null
      try {
        logger.debug("{}: Sending heartbeat to {}", memberName, node);
        client.sendHeartbeat(request, new HeartbeatHandler(localMember, node));
      } catch (Exception e) {
        logger.warn("{}: Cannot send heart beat to node {}", memberName, node, e);
      }
    }
  }

  void sendHeartbeatSync(Node node) {
    Client client = localMember.getSyncHeartbeatClient(node);
    HeartbeatHandler heartbeatHandler = new HeartbeatHandler(localMember, node);
    HeartBeatRequest req = new HeartBeatRequest();
    req.setCommitLogTerm(request.commitLogTerm);
    req.setCommitLogIndex(request.commitLogIndex);
    req.setRegenerateIdentifier(request.regenerateIdentifier);
    req.setRequireIdentifier(request.requireIdentifier);
    req.setTerm(request.term);
    req.setLeader(localMember.getThisNode());
    if (request.isSetHeader()) {
      req.setHeader(request.header);
    }
    if (request.isSetPartitionTableBytes()) {
      req.partitionTableBytes = request.partitionTableBytes;
      req.setPartitionTableBytesIsSet(true);
    }
    if (client != null) {
      localMember
          .getSerialToParallelPool()
          .submit(
              () -> {
                try {
                  logger.debug("{}: Sending heartbeat to {}", memberName, node);
                  HeartBeatResponse heartBeatResponse = client.sendHeartbeat(req);
                  heartbeatHandler.onComplete(heartBeatResponse);
                } catch (TTransportException e) {
                  logger.warn(
                      "{}: Cannot send heart beat to node {} due to network", memberName, node, e);
                  client.getInputProtocol().getTransport().close();
                } catch (Exception e) {
                  logger.warn("{}: Cannot send heart beat to node {}", memberName, node, e);
                } finally {
                  ClientUtils.putBackSyncHeartbeatClient(client);
                }
              });
    }
  }

  /**
   * Start elections until this node becomes a leader or a follower.
   *
   * @throws InterruptedException
   */
  private void startElections() throws InterruptedException {
    if (localMember.getAllNodes().size() == 1) {
      // single node group, this node is always the leader
      localMember.setCharacter(NodeCharacter.LEADER);
      localMember.setLeader(localMember.getThisNode());
      logger.info("{}: Winning the election because the node is the only node.", memberName);
    }

    // the election goes on until this node becomes a follower or a leader
    while (localMember.getCharacter() == NodeCharacter.ELECTOR) {
      startElection();
      if (localMember.getCharacter() == NodeCharacter.ELECTOR) {
        // sleep random time to reduce election conflicts
        long electionWait =
            ClusterConstant.getElectionLeastTimeOutMs()
                + Math.abs(random.nextLong() % ClusterConstant.getElectionRandomTimeOutMs());
        logger.info("{}: Sleep {}ms until next election", memberName, electionWait);
        Thread.sleep(electionWait);
      }
    }
    // take the election request as the first heartbeat
    localMember.setLastHeartbeatReceivedTime(System.currentTimeMillis());
  }

  /**
   * Start one round of election. Increase the local term, ask for vote from each of the nodes in
   * the group and become the leader if at least half of them agree.
   */
  @SuppressWarnings({"java:S2274"})
  // enable timeout
  void startElection() {
    if (localMember.isSkipElection()) {
      logger.info("{}: Skip election because this node has stopped.", memberName);
      return;
    }
    synchronized (localMember.getTerm()) {
      long nextTerm = localMember.getTerm().incrementAndGet();
      localMember.setVoteFor(localMember.getThisNode());
      localMember.updateHardState(nextTerm, this.localMember.getVoteFor());

      // the number of votes needed to become a leader,
      // quorumNum should be equal to localMember.getAllNodes().size() / 2 + 1,
      // but since it doesn’t need to vote for itself here, it directly decreases 1
      int quorumNum = localMember.getAllNodes().size() / 2;
      logger.info("{}: Election {} starts, quorum: {}", memberName, nextTerm, quorumNum);
      // set to true when the election has a result (rejected or succeeded)
      AtomicBoolean electionTerminated = new AtomicBoolean(false);
      // set to true when the election is won
      AtomicBoolean electionValid = new AtomicBoolean(false);
      // a decreasing vote counter
      AtomicInteger quorum = new AtomicInteger(quorumNum);

      // NOTICE, failingVoteCounter should be equal to quorumNum + 1
      AtomicInteger failingVoteCounter = new AtomicInteger(quorumNum + 1);

      electionRequest.setTerm(nextTerm);
      electionRequest.setElector(localMember.getThisNode());
      electionRequest.setLastLogTerm(localMember.getLogManager().getLastLogTerm());
      electionRequest.setLastLogIndex(localMember.getLogManager().getLastLogIndex());

      requestVote(
          localMember.getAllNodes(),
          electionRequest,
          nextTerm,
          quorum,
          electionTerminated,
          electionValid,
          failingVoteCounter);
      // erase the log index so it can be updated in the next heartbeat
      electionRequest.unsetLastLogIndex();

      try {
        logger.info(
            "{}: Wait for {}ms until election time out",
            memberName,
            RaftServer.getConnectionTimeoutInMS());
        localMember.getTerm().wait(RaftServer.getConnectionTimeoutInMS());
      } catch (InterruptedException e) {
        logger.info(
            "{}: Unexpected interruption when waiting the result of election {}",
            memberName,
            nextTerm);
        Thread.currentThread().interrupt();
      }

      // if the election times out, the remaining votes do not matter
      electionTerminated.set(true);
      if (electionValid.get()) {
        logger.info("{}: Election {} accepted", memberName, nextTerm);
        localMember.setCharacter(NodeCharacter.LEADER);
        localMember.setLeader(localMember.getThisNode());
      }
    }
  }

  /**
   * Request a vote from each of the "nodes". Each for vote will decrease the counter "quorum" and
   * when it reaches 0, the flag "electionValid" and "electionTerminated" will be set to true. Any
   * against vote will set the flag "electionTerminated" to true and ends the election.
   *
   * @param nodes
   * @param request
   * @param nextTerm the term of the election
   * @param quorum
   * @param electionTerminated
   * @param electionValid
   */
  @SuppressWarnings("java:S2445")
  private void requestVote(
      Collection<Node> nodes,
      ElectionRequest request,
      long nextTerm,
      AtomicInteger quorum,
      AtomicBoolean electionTerminated,
      AtomicBoolean electionValid,
      AtomicInteger failingVoteCounter) {
    synchronized (nodes) {
      // avoid concurrent modification
      for (Node node : nodes) {
        if (node.equals(localMember.getThisNode())) {
          continue;
        }

        ElectionHandler handler =
            new ElectionHandler(
                localMember,
                node,
                nextTerm,
                quorum,
                electionTerminated,
                electionValid,
                failingVoteCounter);
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          requestVoteAsync(node, handler, request);
        } else {
          requestVoteSync(node, handler, request);
        }
      }
    }
  }

  private void requestVoteAsync(Node node, ElectionHandler handler, ElectionRequest request) {
    AsyncClient client = localMember.getAsyncHeartbeatClient(node);
    if (client != null) {
      logger.info("{}: Requesting a vote from {}", memberName, node);
      try {
        client.startElection(request, handler);
      } catch (Exception e) {
        logger.error("{}: Cannot request a vote from {}", memberName, node, e);
      }
    }
  }

  private void requestVoteSync(Node node, ElectionHandler handler, ElectionRequest request) {
    Client client = localMember.getSyncHeartbeatClient(node);
    if (client != null) {
      logger.info("{}: Requesting a vote from {}", memberName, node);
      localMember
          .getSerialToParallelPool()
          .submit(
              () -> {
                try {
                  long result = client.startElection(request);
                  handler.onComplete(result);
                } catch (TException e) {
                  client.getInputProtocol().getTransport().close();
                  logger.warn(
                      "{}: Cannot request a vote from {} due to network", memberName, node, e);
                  handler.onError(e);
                } catch (Exception e) {
                  handler.onError(e);
                } finally {
                  ClientUtils.putBackSyncHeartbeatClient(client);
                }
              });
    }
  }
}
