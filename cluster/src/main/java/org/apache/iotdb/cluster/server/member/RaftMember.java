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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.catchup.LogCatchUpTask;
import org.apache.iotdb.cluster.log.catchup.SnapshotCatchUpTask;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.ForwardPlanHandler;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RaftMember implements RaftService.AsyncIface {

  ClusterConfig config = ClusterDescriptor.getINSTANCE().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(RaftMember.class);
  private static final int SEND_LOG_RETRY = 3;

  String name;

  Random random = new Random();
  protected Node thisNode;
  protected volatile List<Node> allNodes;

  volatile NodeCharacter character = NodeCharacter.ELECTOR;
  AtomicLong term = new AtomicLong(0);
  volatile Node leader;
  volatile long lastHeartbeatReceivedTime;

  LogManager logManager;

  ExecutorService heartBeatService;
  private ExecutorService catchUpService;

  private Map<Node, Long> lastCatchUpResponseTime = new ConcurrentHashMap<>();

  //will be initialized as different implementations in the subclasses
  private ClientPool clientPool;
  // when the commit progress is updated by a heart beat, this object is notified so that we may
  // know if this node is synchronized with the leader
  private Object syncLock = new Object();

  // when the header of the group is removed from the cluster, the members of the group should no
  // longer accept writes, but they still can be read candidates for weak consistency reads and
  // provide snapshots for the new holders
  volatile boolean readOnly = false;

  public RaftMember() {
  }

  RaftMember(String name, ClientPool pool) {
    this.name = name;
    this.clientPool = pool;
  }

  public void start() throws TTransportException {
    if (heartBeatService != null) {
      return;
    }

    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,
            name + "-HeartbeatThread@" + System.currentTimeMillis()));
    catchUpService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  public LogManager getLogManager() {
    return logManager;
  }

  void initLogManager() {

  }

  public void stop() {
    if (heartBeatService == null) {
      return;
    }

    heartBeatService.shutdownNow();
    catchUpService.shutdownNow();
    catchUpService = null;
    heartBeatService = null;
    logger.info("{} stopped", name);
  }

  @Override
  public void sendHeartbeat(HeartbeatRequest request, AsyncMethodCallback resultHandler) {
    logger.trace("{} received a heartbeat", name);
    synchronized (term) {
      long thisTerm = term.get();
      long leaderTerm = request.getTerm();
      HeartbeatResponse response = new HeartbeatResponse();

      if (leaderTerm < thisTerm) {
        // the leader is invalid
        response.setTerm(thisTerm);
        if (logger.isTraceEnabled()) {
          logger.trace("{} received a heartbeat from a stale leader {}", name, request.getLeader());
        }
      } else {
        processValidHeartbeatReq(request, response);

        response.setTerm(Response.RESPONSE_AGREE);
        response.setFollower(thisNode);
        response.setLastLogIndex(logManager.getLastLogIndex());
        response.setLastLogTerm(logManager.getLastLogTerm());

        // The term of the last log needs to be same with leader's term in order to preserve safety.
        if (logManager.getLastLogTerm() == leaderTerm) {//TODO why?
          synchronized (syncLock) {
            logManager.commitLog(request.getCommitLogIndex());
            syncLock.notifyAll();
          }
        }
        //TODO else?

        term.set(leaderTerm);
        setLeader(request.getLeader());
        if (character != NodeCharacter.FOLLOWER) {
          setCharacter(NodeCharacter.FOLLOWER);
        }
        setLastHeartbeatReceivedTime(System.currentTimeMillis());
        if (logger.isTraceEnabled()) {
          logger.trace("{} received heartbeat from a valid leader {}", name, request.getLeader());
        }
      }
      resultHandler.onComplete(response);
    }
  }

  @Override
  public void startElection(ElectionRequest electionRequest, AsyncMethodCallback resultHandler) {
    synchronized (term) {
      if (electionRequest.getElector().equals(leader)) {
        resultHandler.onComplete(Response.RESPONSE_AGREE);
        return;
      }

      if (character != NodeCharacter.ELECTOR) {
        // only elector votes
        resultHandler.onComplete(Response.RESPONSE_LEADER_STILL_ONLINE);
        return;
      }
      long response = processElectionRequest(electionRequest);
      logger.info("{} sending response {} to the elector {}", name, response,
          electionRequest.getElector());
      resultHandler.onComplete(response);
    }
  }

  private boolean checkRequestTerm(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    long leaderTerm = request.getTerm();
    long localTerm;

    synchronized (term) {
      // if the request comes before the heartbeat arrives, the local term may be smaller than the
      // leader term
      localTerm = term.get();
      if (leaderTerm < localTerm) {
        logger.debug("{} rejected the AppendEntryRequest for term: {}/{}", name, leaderTerm,
            localTerm);
        resultHandler.onComplete(localTerm);
        return false;
      } else if (leaderTerm > localTerm) {
        term.set(leaderTerm);
        localTerm = leaderTerm;
        if (character != NodeCharacter.FOLLOWER) {
          setCharacter(NodeCharacter.FOLLOWER);
        }
      }
    }
    logger.debug("{} accepted the AppendEntryRequest for term: {}", name, localTerm);
    return true;
  }

  private long appendEntry(Log log) {
    long resp;
    synchronized (logManager) {
      Log lastLog = logManager.getLastLog();
      long previousLogIndex = log.getPreviousLogIndex();
      long previousLogTerm = log.getPreviousLogTerm();

      if (logManager.getLastLogIndex() == previousLogIndex
          && logManager.getLastLogTerm() == previousLogTerm) {
        // the incoming log points to the local last log, append it
        logManager.appendLog(log);
        if (logger.isDebugEnabled()) {
          logger.debug("{} append a new log {}", name, log);
        }
        resp = Response.RESPONSE_AGREE;
      } else if (lastLog != null && lastLog.getPreviousLogIndex() == previousLogIndex
          && lastLog.getCurrLogTerm() <= log.getCurrLogTerm()) {
        /* <pre>
                                       +------+
                                     .'|      | new coming log
                                   .'  +------+
                                 .'
        +--------+     +--------+      +------+
        |        |-----|        |------|      | so called latest log   (local)
        +--------+     +--------+      +------+
        </pre>
        */
        // the incoming log points to the previous log of the local last log, and its term is
        // bigger than or equals to the local last log's, replace the local last log with it
        // because the local latest log is invalid.
        logManager.replaceLastLog(log);
        logger.debug("{} replaced the last log with {}", name, log);
        resp = Response.RESPONSE_AGREE;
      } else {
        long lastPrevLogTerm = lastLog == null ? -1 : lastLog.getPreviousLogTerm();
        long lastPrevLogId = lastLog == null ? -1 : lastLog.getPreviousLogIndex();
        // the incoming log points to an illegal position, reject it
        logger.debug("{} cannot append the log because the last log does not match, "
                + "local:term[{}],index[{}],previousTerm[{}],previousIndex[{}], "
                + "request:term[{}],index[{}],previousTerm[{}],previousIndex[{}]",
            name,
            logManager.getLastLogTerm(), logManager.getLastLogIndex(),
            lastPrevLogTerm, lastPrevLogId,
            log.getCurrLogTerm(), log.getCurrLogIndex(),
            previousLogTerm, previousLogIndex);
        resp = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return resp;
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    logger.debug("{} received an AppendEntryRequest", name);
    if (!checkRequestTerm(request, resultHandler)) {
      return;
    }

    try {
      Log log = LogParser.getINSTANCE().parse(request.entry);
      resultHandler.onComplete(appendEntry(log));
      logger.debug("{} AppendEntryRequest completed", name);
    } catch (UnknownLogTypeException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {
    //TODO-Cluster#354: implement
  }

  /**
   * Send the given log to all the followers and decide the result according to the specified
   * quorum.
   *
   * @param log
   * @param requiredQuorum the number of votes needed to make the log valid, when requiredQuorum <=
   *                       0, half of the cluster size will be used.
   * @return an AppendLogResult
   */
  private AppendLogResult sendLogToFollowers(Log log, int requiredQuorum) {
    if (requiredQuorum <= 0) {
      return sendLogToFollowers(log, new AtomicInteger(allNodes.size() / 2));
    } else {
      return sendLogToFollowers(log, new AtomicInteger(requiredQuorum));
    }
  }

  // synchronized: logs are serialized
  private synchronized AppendLogResult sendLogToFollowers(Log log, AtomicInteger quorum) {
    if (allNodes.size() == 1) {
      // single node group, does not need the agreement of others
      return AppendLogResult.OK;
    }

    logger.debug("{} sending a log to followers: {}", name, log);

    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(term.get());

    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(term.get());
    request.setEntry(log.serialize());
    if (getHeader() != null) {
      request.setHeader(getHeader());
    }

    synchronized (quorum) {//this synchronized codes are just for calling quorum.wait.

      // synchronized: avoid concurrent modification
      synchronized (allNodes) {
        for (Node node : allNodes) {
          AsyncClient client = connectNode(node);
          if (client != null) {
            AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
            handler.setReceiver(node);
            handler.setQuorum(quorum);
            handler.setLeaderShipStale(leaderShipStale);
            handler.setLog(log);
            handler.setReceiverTerm(newLeaderTerm);
            try {
              client.appendEntry(request, handler);
              logger.debug("{} sending a log to {}: {}", name, node, log);
            } catch (Exception e) {
              logger.warn("{} cannot append log to node {}", name, node, e);
            }
          }
        }
      }

      try {
        quorum.wait(RaftServer.connectionTimeoutInMS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (leaderShipStale.get()) {
      retireFromLeader(newLeaderTerm.get());
      return AppendLogResult.LEADERSHIP_STALE;
    }

    if (quorum.get() > 0) {
      return AppendLogResult.TIME_OUT;
    }

    return AppendLogResult.OK;
  }

  enum AppendLogResult {
    OK, TIME_OUT, LEADERSHIP_STALE
  }

  public AsyncClient connectNode(Node node) {
    if (node == null || node.equals(thisNode)) {
      return null;
    }

    AsyncClient client = null;
    try {
      client = clientPool.getClient(node);
    } catch (IOException e) {
      logger.warn("{} cannot connect to node {}", name, node, e);
    }
    return client;
  }

  public void setThisNode(Node thisNode) {
    this.thisNode = thisNode;
    allNodes.add(thisNode);
  }

  public void setCharacter(NodeCharacter character) {
    logger.info("{} has become a {}", name, character);
    this.character = character;
  }

  public NodeCharacter getCharacter() {
    return character;
  }

  public AtomicLong getTerm() {
    return term;
  }

  public long getLastHeartbeatReceivedTime() {
    return lastHeartbeatReceivedTime;
  }

  public Node getLeader() {
    return leader;
  }

  public void setTerm(AtomicLong term) {
    this.term = term;
  }

  public void setLastHeartbeatReceivedTime(long lastHeartbeatReceivedTime) {
    this.lastHeartbeatReceivedTime = lastHeartbeatReceivedTime;
  }

  public void setLeader(Node leader) {
    if (!Objects.equals(leader, this.leader)) {
      logger.info("{} has become a follower of {}", getName(), leader);
      this.leader = leader;
    }
  }

  public Node getThisNode() {
    return thisNode;
  }

  public Collection<Node> getAllNodes() {
    return allNodes;
  }

  public Map<Node, Long> getLastCatchUpResponseTime() {
    return lastCatchUpResponseTime;
  }


  public void processValidHeartbeatResp(HeartbeatResponse response, Node receiver) {

  }

  /**
   * The actions performed when the node wins in an election (becoming a leader).
   */
  public void onElectionWins() {

  }

  void processValidHeartbeatReq(HeartbeatRequest request, HeartbeatResponse response) {

  }

  public void retireFromLeader(long newTerm) {
    synchronized (term) {
      long currTerm = term.get();
      // confirm that the heartbeat of the new leader hasn't come
      if (currTerm < newTerm) {
        term.set(newTerm);
        setCharacter(NodeCharacter.FOLLOWER);
        setLeader(null);
        setLastHeartbeatReceivedTime(System.currentTimeMillis());
      }
    }
  }

  long processElectionRequest(ElectionRequest electionRequest) {
    // reject the election if one of the four holds:
    // 1. the term of the candidate is no bigger than the voter's
    // 2. the lastLogIndex of the candidate is smaller than the voter's
    // 3. the lastLogIndex of the candidate equals to the voter's but its lastLogTerm is
    // smaller than the voter's
    long thatTerm = electionRequest.getTerm();
    long thatLastLogId = electionRequest.getLastLogIndex();
    long thatLastLogTerm = electionRequest.getLastLogTerm();
    logger.info("{} received an election request, term:{}, metaLastLogId:{}, metaLastLogTerm:{}",
        name, thatTerm,
        thatLastLogId, thatLastLogTerm);

    long lastLogIndex = logManager.getLastLogIndex();
    long lastLogTerm = logManager.getLastLogTerm();

    synchronized (term) {
      long thisTerm = term.get();
      long resp = verifyElector(thisTerm, lastLogIndex, lastLogTerm, thatTerm, thatLastLogId,
          thatLastLogTerm);
      if (resp == Response.RESPONSE_AGREE) {
        term.set(thatTerm);
        setCharacter(NodeCharacter.FOLLOWER);
        lastHeartbeatReceivedTime = System.currentTimeMillis();
        leader = electionRequest.getElector();
        // interrupt election
        term.notifyAll();
      }
      return resp;
    }
  }

  long verifyElector(long thisTerm, long thisLastLogIndex, long thisLastLogTerm,
      long thatTerm, long thatLastLogId, long thatLastLogTerm) {
    long response;
    if (thatTerm <= thisTerm) {
      response = thisTerm;
      logger.debug("{} rejected an election request, term:{}/{}",
          name, thatTerm, thisTerm);
    } else if (thatLastLogTerm < thisLastLogTerm
        || (thatLastLogTerm == thisLastLogTerm && thatLastLogId < thisLastLogIndex)) {
      logger.debug("{} rejected an election request, logIndex:{}/{}, logTerm:{}/{}",
          name, thatLastLogId, thisLastLogIndex, thatLastLogTerm, thisLastLogTerm);
      response = Response.RESPONSE_LOG_MISMATCH;
    } else {
      logger.debug("{} accepted an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          name, thatTerm, thisTerm, thatLastLogId, thisLastLogIndex, thatLastLogTerm,
          thisLastLogTerm);
      response = Response.RESPONSE_AGREE;
    }
    return response;
  }

  /**
   * Update the followers' log by sending logs whose index >= followerLastLogIndex to the follower.
   * If some of the logs are not in memory, also send the snapshot.
   * <br>notice that if a part of data is in the snapshot, then it is not in the logs</>
   *
   * @param follower
   * @param followerLastLogIndex
   */
  public void catchUp(Node follower, long followerLastLogIndex) {
    // for one follower, there is at most one ongoing catch-up
    synchronized (follower) {
      // check if the last catch-up is still ongoing
      Long lastCatchupResp = lastCatchUpResponseTime.get(follower);
      if (lastCatchupResp != null
          && System.currentTimeMillis() - lastCatchupResp < RaftServer.connectionTimeoutInMS) {
        logger.debug("{}: last catch up of {} is ongoing", name, follower);
        return;
      } else {
        // record the start of the catch-up
        lastCatchUpResponseTime.put(follower, System.currentTimeMillis());
      }
    }
    if (followerLastLogIndex == -1) {
      // if the follower does not have any logs, send from the first one
      followerLastLogIndex = 0;
    }

    AsyncClient client = connectNode(follower);
    if (client != null) {
      List<Log> logs;
      boolean allLogsValid;
      Snapshot snapshot = null;
      synchronized (logManager) {
        allLogsValid = logManager.logValid(followerLastLogIndex);
        logs = logManager.getLogs(followerLastLogIndex, Long.MAX_VALUE);
        if (!allLogsValid) {
          snapshot = logManager.getSnapshot();
        }
      }

      if (allLogsValid) {
        if (logger.isDebugEnabled()) {
          logger.debug("{} makes {} catch up with {} cached logs", name, follower, logs.size());
        }
        catchUpService.submit(new LogCatchUpTask(logs, follower, this));
      } else {
        logger.debug("{}: Logs in {} are too old, catch up with snapshot", name, follower);
        catchUpService.submit(new SnapshotCatchUpTask(logs, snapshot, follower, this));
      }
    } else {
      lastCatchUpResponseTime.remove(follower);
      logger.warn("{}: Catch-up failed: node {} is currently unavailable", name, follower);
    }
  }

  public String getName() {
    return name;
  }

  /**
   * @return the header of the data raft group or null if this is in a meta group.
   */
  public Node getHeader() {
    return null;
  }

  /**
   * Forward a plan to a node using the default client.
   *
   * @param plan
   * @param node
   * @param header must be set for data group communication, set to null for meta group
   *               communication
   * @return
   */
  TSStatus forwardPlan(PhysicalPlan plan, Node node, Node header) {
    if (node == thisNode || node == null) {
      logger.debug("{}: plan {} has no where to be forwarded", name, plan);
      return StatusUtils.NO_LEADER;
    }

    logger.info("{}: Forward {} to node {}", name, plan, node);

    AsyncClient client = connectNode(node);
    if (client != null) {
      return forwardPlan(plan, client, node, header);
    }
    return StatusUtils.TIME_OUT;
  }

  TSStatus forwardPlan(PhysicalPlan plan, AsyncClient client, Node receiver, Node header) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      plan.serializeTo(dataOutputStream);
      AtomicReference<TSStatus> status = new AtomicReference<>();
      ExecutNonQueryReq req = new ExecutNonQueryReq();
      req.setPlanBytes(byteArrayOutputStream.toByteArray());
      if (header != null) {
        req.setHeader(header);
      }
      synchronized (status) {
        client.executeNonQueryPlan(req, new ForwardPlanHandler(status, plan, receiver));
        status.wait(RaftServer.connectionTimeoutInMS);
      }
      return status.get() == null ? StatusUtils.TIME_OUT : status.get();
    } catch (IOException | TException e) {
      TSStatus status = StatusUtils.INTERNAL_ERROR.deepCopy();
      status.setMessage(e.getMessage());
      return status;
    } catch (InterruptedException e) {
      return StatusUtils.TIME_OUT;
    }
  }

  /**
   * Only the group leader can call this method. Will commit the log locally and send it to
   * followers
   *
   * @param plan
   * @return
   */
  TSStatus processPlanLocally(PhysicalPlan plan) {
    logger.debug("{}: Processing plan {}", name, plan);
    synchronized (logManager) {
      if (readOnly) {
        return StatusUtils.NODE_READ_ONLY;
      }

      PhysicalPlanLog log = new PhysicalPlanLog();
      log.setCurrLogTerm(getTerm().get());
      log.setPreviousLogIndex(logManager.getLastLogIndex());
      log.setPreviousLogTerm(logManager.getLastLogTerm());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      log.setPlan(plan);
      logManager.appendLog(log);

      retry:
      for (int i = SEND_LOG_RETRY; i >= 0; i--) {
        logger.debug("{}: Send plan {} to other nodes, retry remaining {}", name, plan, i);
        AppendLogResult result = sendLogToFollowers(log, allNodes.size() / 2);
        switch (result) {
          case OK:
            logger.debug("{}: Plan {} is accepted", name, plan);
            try {
              logManager.commitLog(log);
            } catch (QueryProcessException e) {
              if (e.getCause() instanceof PathAlreadyExistException) {
                // ignore duplicated creation
                return StatusUtils.OK;
              }
              logger.info("{}: The log {} is not successfully applied, reverting", name, log, e);
              logManager.removeLastLog();
              TSStatus status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
              status.setMessage(e.getMessage());
              return status;
            }
            return StatusUtils.OK;
          case TIME_OUT:
            logger.debug("{}: Plan {} timed out", name, plan);
            if (i == 1) {
              return StatusUtils.TIME_OUT;
            }
            break;
          case LEADERSHIP_STALE:
          default:
            logManager.removeLastLog();
            break retry;
        }
      }
    }
    return null;
  }

  /**
   * if the node is not a leader, will send it to the leader. Otherwise do it locally (whether to
   * send it to followers depends on the implementation of executeNonQuery()).
   *
   * @param request
   * @param resultHandler
   */
  public void executeNonQueryPlan(ExecutNonQueryReq request,
      AsyncMethodCallback<TSStatus> resultHandler) {
    if (character != NodeCharacter.LEADER) {
      AsyncClient client = connectNode(leader);
      if (client != null) {
        try {
          client.executeNonQueryPlan(request, resultHandler);
        } catch (TException e) {
          resultHandler.onError(e);
        }
      } else {
        resultHandler.onComplete(StatusUtils.NO_LEADER);
      }
      return;
    }
    try {
      PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
      logger.debug("{}: Received a plan {}", name, plan);
      resultHandler.onComplete(executeNonQuery(plan));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  /**
   * Request and check the leader's commitId to see whether this node has caught up. If not, wait
   * until this node catches up.
   *
   * @return true if the node has caught up, false otherwise
   */
  public boolean syncLeader() {
    if (character == NodeCharacter.LEADER) {
      return true;
    }
    if (leader == null) {
      return false;
    }
    logger.debug("{}: try synchronizing with leader {}", name, leader);
    long startTime = System.currentTimeMillis();
    long waitedTime = 0;
    AtomicReference<Long> commitIdResult = new AtomicReference<>(Long.MAX_VALUE);
    while (waitedTime < RaftServer.syncLeaderMaxWaitMs) {
      AsyncClient client = connectNode(leader);
      if (client == null) {
        return false;
      }
      try {
        synchronized (commitIdResult) {
          client.requestCommitIndex(getHeader(), new GenericHandler<>(leader, commitIdResult));
          commitIdResult.wait(RaftServer.syncLeaderMaxWaitMs);
        }
        long leaderCommitId = commitIdResult.get();
        long localCommitId = logManager.getCommitLogIndex();
        logger.debug("{}: synchronizing commitIndex {}/{}", name, localCommitId, leaderCommitId);
        if (leaderCommitId <= localCommitId) {
          // this node has caught up
          return true;
        }
        // wait for next heartbeat to catch up
        waitedTime = System.currentTimeMillis() - startTime;
        synchronized (syncLock) {
          syncLock.wait(RaftServer.heartBeatIntervalMs);
        }
      } catch (TException | InterruptedException e) {
        logger.error("{}: Cannot request commit index from {}", name, leader, e);
      }
    }
    return false;
  }

  abstract TSStatus executeNonQuery(PhysicalPlan plan);

  @Override
  public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
    if (character == NodeCharacter.LEADER) {
      resultHandler.onComplete(logManager.getCommitLogIndex());
      return;
    }
    AsyncClient client = connectNode(leader);
    if (client == null) {
      resultHandler.onError(new LeaderUnknownException(getAllNodes()));
      return;
    }
    try {
      client.requestCommitIndex(header, resultHandler);
    } catch (TException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void readFile(String filePath, long offset, int length, Node header,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    try (BufferedInputStream bufferedInputStream =
        new BufferedInputStream(new FileInputStream(filePath))) {
      bufferedInputStream.skip(offset);
      byte[] bytes = new byte[length];
      ByteBuffer result = ByteBuffer.wrap(bytes);
      int len = bufferedInputStream.read(bytes);
      result.limit(Math.max(len, 0));

      resultHandler.onComplete(result);
    } catch (IOException e) {
      resultHandler.onError(e);
    }
  }

  public void setReadOnly() {
    synchronized (logManager) {
      readOnly = true;
    }
  }

  public void setAllNodes(List<Node> allNodes) {
    this.allNodes = allNodes;
  }
}
