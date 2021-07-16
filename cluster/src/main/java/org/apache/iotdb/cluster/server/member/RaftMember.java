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

import org.apache.iotdb.cluster.client.async.AsyncClientPool;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncClientPool;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.CommitLogCallback;
import org.apache.iotdb.cluster.log.CommitLogTask;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogDispatcher;
import org.apache.iotdb.cluster.log.LogDispatcher.SendLogRequest;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.catchup.CatchUpTask;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.rpc.thrift.RequestCommitIndexResponse;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendNodeEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.monitor.Peer;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.cluster.utils.PlanSerializer;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.metadata.DuplicatedTemplateException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RaftMember process the common raft logic like leader election, log appending, catch-up and so on.
 */
@SuppressWarnings("java:S3077") // reference volatile is enough
public abstract class RaftMember {
  private static final Logger logger = LoggerFactory.getLogger(RaftMember.class);
  public static final boolean USE_LOG_DISPATCHER = false;

  private static final String MSG_FORWARD_TIMEOUT = "{}: Forward {} to {} time out";
  private static final String MSG_FORWARD_ERROR =
      "{}: encountered an error when forwarding {} to" + " {}";
  private static final String MSG_NO_LEADER_COMMIT_INDEX =
      "{}: Cannot request commit index from {}";
  private static final String MSG_NO_LEADER_IN_SYNC = "{}: No leader is found when synchronizing";
  public static final String MSG_LOG_IS_ACCEPTED = "{}: log {} is accepted";
  /**
   * when there is no leader, wait for waitLeaderTimeMs before return a NoLeader response to the
   * client.
   */
  private static long waitLeaderTimeMs = 60 * 1000L;

  /**
   * when the leader of this node changes, the condition will be notified so other threads that wait
   * on this may be woken.
   */
  private final Object waitLeaderCondition = new Object();
  /** the lock is to make sure that only one thread can apply snapshot at the same time */
  private final Object snapshotApplyLock = new Object();

  protected Node thisNode = ClusterConstant.EMPTY_NODE;
  /** the nodes that belong to the same raft group as thisNode. */
  protected List<Node> allNodes;

  ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  /** the name of the member, to distinguish several members in the logs. */
  String name;
  /** to choose nodes to send request of joining cluster randomly. */
  Random random = new Random();
  /** when the node is a leader, this map is used to track log progress of each follower. */
  Map<Node, Peer> peerMap;
  /**
   * the current term of the node, this object also works as lock of some transactions of the member
   * like elections.
   */
  AtomicLong term = new AtomicLong(0);

  volatile NodeCharacter character = NodeCharacter.ELECTOR;
  AtomicReference<Node> leader = new AtomicReference<>(ClusterConstant.EMPTY_NODE);
  /**
   * the node that thisNode has voted for in this round of election, which prevents a node voting
   * twice in a single election.
   */
  volatile Node voteFor;
  /**
   * when this node is a follower, this records the unix-epoch timestamp when the last heartbeat
   * arrived, and is reported in the timed member report to show how long the leader has been
   * offline.
   */
  volatile long lastHeartbeatReceivedTime;
  /** the raft logs are all stored and maintained in the log manager */
  RaftLogManager logManager;
  /**
   * the single thread pool that runs the heartbeat thread, which send heartbeats to the follower
   * when this node is a leader, or start elections when this node is an elector.
   */
  ExecutorService heartBeatService;
  /**
   * if set to true, the node will reject all log appends when the header of a group is removed from
   * the cluster, the members of the group should no longer accept writes, but they still can be
   * candidates for weak consistency reads and provide snapshots for the new data holders
   */
  volatile boolean readOnly = false;
  /**
   * lastLogIndex when generating the previous member report, to show the log ingestion rate of the
   * member by comparing it with the current last log index.
   */
  long lastReportedLogIndex;
  /** the thread pool that runs catch-up tasks */
  private ExecutorService catchUpService;
  /**
   * lastCatchUpResponseTime records when is the latest response of each node's catch-up. There
   * should be only one catch-up task for each node to avoid duplication, but the task may time out
   * or the task may corrupt unexpectedly, and in that case, the next catch up should be enabled. So
   * if we find a catch-up task that does not respond for long, we will start a new one instead of
   * waiting for the previous one to finish.
   */
  private Map<Node, Long> lastCatchUpResponseTime = new ConcurrentHashMap<>();
  /**
   * the pool that provides reusable Thrift clients to connect to other RaftMembers and execute RPC
   * requests. It will be initialized according to the implementation of the subclasses
   */
  private AsyncClientPool asyncClientPool;

  private AsyncClientPool asyncSendLogClientPool;
  private SyncClientPool syncClientPool;
  private AsyncClientPool asyncHeartbeatClientPool;
  private SyncClientPool syncHeartbeatClientPool;
  /**
   * when the commit progress is updated by a heartbeat, this object is notified so that we may know
   * if this node is up-to-date with the leader, and whether the given consistency is reached
   */
  private Object syncLock = new Object();
  /**
   * when this node sends logs to the followers, the send is performed in parallel in this pool, so
   * that a slow or unavailable node will not block other nodes.
   */
  private ExecutorService appendLogThreadPool;
  /**
   * when using sync server, this thread pool is used to convert serial operations (like sending
   * heartbeats and asking for votes) into paralleled ones, so the process will not be blocked by
   * one slow node.
   */
  private ExecutorService serialToParallelPool;
  /** a thread pool that is used to do commit log tasks asynchronous in heartbeat thread */
  private ExecutorService commitLogPool;
  /**
   * logDispatcher buff the logs orderly according to their log indexes and send them sequentially,
   * which avoids the followers receiving out-of-order logs, forcing them to wait for previous logs.
   */
  private LogDispatcher logDispatcher;

  /**
   * localExecutor is used to directly execute plans like load configuration in the underlying IoTDB
   */
  protected PlanExecutor localExecutor;

  protected RaftMember() {}

  protected RaftMember(
      String name,
      AsyncClientPool asyncPool,
      SyncClientPool syncPool,
      AsyncClientPool asyncHeartbeatPool,
      SyncClientPool syncHeartbeatPool) {
    this.name = name;
    this.asyncClientPool = asyncPool;
    this.syncClientPool = syncPool;
    this.asyncHeartbeatClientPool = asyncHeartbeatPool;
    this.syncHeartbeatClientPool = syncHeartbeatPool;
    this.asyncSendLogClientPool = asyncClientPool;
  }

  protected RaftMember(
      String name,
      AsyncClientPool asyncPool,
      SyncClientPool syncPool,
      AsyncClientPool asyncHeartbeatPool,
      SyncClientPool syncHeartbeatPool,
      AsyncClientPool asyncSendLogClientPool) {
    this.name = name;
    this.asyncClientPool = asyncPool;
    this.syncClientPool = syncPool;
    this.asyncHeartbeatClientPool = asyncHeartbeatPool;
    this.syncHeartbeatClientPool = syncHeartbeatPool;
    this.asyncSendLogClientPool = asyncSendLogClientPool;
  }

  /**
   * Start the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   */
  public void start() {
    if (heartBeatService != null) {
      return;
    }

    startBackGroundThreads();
    logger.info("{} started", name);
  }

  void startBackGroundThreads() {
    heartBeatService =
        Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, name + "-HeartbeatThread@" + System.currentTimeMillis()));
    catchUpService =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat(getName() + "-CatchUpThread%d").build());
    appendLogThreadPool =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 10,
            new ThreadFactoryBuilder().setNameFormat(getName() + "-AppendLog%d").build());
    if (!config.isUseAsyncServer()) {
      serialToParallelPool =
          new ThreadPoolExecutor(
              allNodes.size(),
              Math.max(allNodes.size(), Runtime.getRuntime().availableProcessors()),
              1000L,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<>(),
              new ThreadFactoryBuilder().setNameFormat(getName() + "-SerialToParallel%d").build());
    }
    commitLogPool =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat(getName() + "-CommitLog%d").build());
  }

  public String getName() {
    return name;
  }

  public RaftLogManager getLogManager() {
    return logManager;
  }

  @TestOnly
  public void setLogManager(RaftLogManager logManager) {
    if (this.logManager != null) {
      this.logManager.close();
    }
    this.logManager = logManager;
  }

  /**
   * Stop the heartbeat thread and the catch-up thread pool. Calling the method twice does not
   * induce side effects.
   */
  public void stop() {
    closeLogManager();
    if (heartBeatService == null) {
      return;
    }

    heartBeatService.shutdownNow();
    catchUpService.shutdownNow();
    appendLogThreadPool.shutdownNow();
    try {
      heartBeatService.awaitTermination(10, TimeUnit.SECONDS);
      catchUpService.awaitTermination(10, TimeUnit.SECONDS);
      appendLogThreadPool.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error(
          "Unexpected interruption when waiting for heartBeatService and catchUpService "
              + "to end",
          e);
    }
    if (serialToParallelPool != null) {
      serialToParallelPool.shutdownNow();
      try {
        serialToParallelPool.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for asyncThreadPool to end", e);
      }
    }

    if (commitLogPool != null) {
      commitLogPool.shutdownNow();
      try {
        commitLogPool.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for commitLogPool to end", e);
      }
    }
    catchUpService = null;
    heartBeatService = null;
    appendLogThreadPool = null;
    logger.info("Member {} stopped", name);
  }

  public void closeLogManager() {
    if (logManager != null) {
      logManager.close();
    }
  }

  /**
   * Process the HeartBeatRequest from the leader. If the term of the leader is smaller than the
   * local term, reject the request by telling it the newest term. Else if the local logs are
   * consistent with the leader's, commit them. Else help the leader find the last matched log. Also
   * update the leadership, heartbeat timer and term of the local node.
   */
  public HeartBeatResponse processHeartbeatRequest(HeartBeatRequest request) {
    logger.trace("{} received a heartbeat", name);
    synchronized (term) {
      long thisTerm = term.get();
      long leaderTerm = request.getTerm();
      HeartBeatResponse response = new HeartBeatResponse();

      if (leaderTerm < thisTerm) {
        // a leader with a term lower than this node is invalid, send it the local term to inform
        // it to resign
        response.setTerm(thisTerm);
        if (logger.isTraceEnabled()) {
          logger.trace("{} received a heartbeat from a stale leader {}", name, request.getLeader());
        }
      } else {
        // try updating local term
        stepDown(leaderTerm, true);
        setLeader(request.getLeader());
        if (character != NodeCharacter.FOLLOWER) {
          // interrupt current election
          term.notifyAll();
        }

        // the heartbeat comes from a valid leader, process it with the sub-class logic
        processValidHeartbeatReq(request, response);

        response.setTerm(Response.RESPONSE_AGREE);
        // tell the leader who I am in case of catch-up
        response.setFollower(thisNode);

        synchronized (logManager) {
          // tell the leader the local log progress so it may decide whether to perform a catch up
          response.setLastLogIndex(logManager.getLastLogIndex());
          response.setLastLogTerm(logManager.getLastLogTerm());
        }

        tryUpdateCommitIndex(leaderTerm, request.getCommitLogIndex(), request.getCommitLogTerm());

        if (logger.isTraceEnabled()) {
          logger.trace("{} received heartbeat from a valid leader {}", name, request.getLeader());
        }
      }
      return response;
    }
  }

  private void tryUpdateCommitIndex(long leaderTerm, long commitIndex, long commitTerm) {
    if (leaderTerm >= term.get() && logManager.getCommitLogIndex() < commitIndex) {
      // there are more local logs that can be committed, commit them in a ThreadPool so the
      // heartbeat response will not be blocked
      CommitLogTask commitLogTask = new CommitLogTask(logManager, commitIndex, commitTerm);
      commitLogTask.registerCallback(new CommitLogCallback(this));
      // if the log is not consistent, the commitment will be blocked until the leader makes the
      // node catch up
      if (commitLogPool != null && !commitLogPool.isShutdown()) {
        commitLogPool.submit(commitLogTask);
      }

      logger.debug(
          "{}: Inconsistent log found, leaderCommit: {}-{}, localCommit: {}-{}, "
              + "localLast: {}-{}",
          name,
          commitIndex,
          commitTerm,
          logManager.getCommitLogIndex(),
          logManager.getCommitLogTerm(),
          logManager.getLastLogIndex(),
          logManager.getLastLogTerm());
    }
  }

  /**
   * Process an ElectionRequest. If the request comes from the last leader, accept it. Else decide
   * whether to accept by examining the log status of the elector.
   */
  public long processElectionRequest(ElectionRequest electionRequest) {
    synchronized (term) {
      long currentTerm = term.get();
      long response =
          checkElectorTerm(currentTerm, electionRequest.getTerm(), electionRequest.getElector());
      if (response != Response.RESPONSE_AGREE) {
        return response;
      }

      // compare the log progress of the elector with this node
      response = checkElectorLogProgress(electionRequest);
      logger.info(
          "{} sending response {} to the elector {}", name, response, electionRequest.getElector());
      return response;
    }
  }

  private long checkElectorTerm(long currentTerm, long electorTerm, Node elector) {
    if (electorTerm < currentTerm) {
      // the elector has a smaller term thus the request is invalid
      logger.info(
          "{} sending localTerm {} to the elector {} because it's term {} is smaller.",
          name,
          currentTerm,
          elector,
          electorTerm);
      return currentTerm;
    }
    if (currentTerm == electorTerm && voteFor != null && !Objects.equals(voteFor, elector)) {
      // this node has voted in this round, but not for the elector, as one node cannot vote
      // twice, reject the request
      logger.info(
          "{} sending rejection to the elector {} because member already has voted {} in this term {}.",
          name,
          elector,
          voteFor,
          currentTerm);
      return Response.RESPONSE_REJECT;
    }
    if (electorTerm > currentTerm) {
      // the elector has a larger term, this node should update its term first
      logger.info(
          "{} received an election from elector {} which has bigger term {} than localTerm {}, raftMember should step down first and then continue to decide whether to grant it's vote by log status.",
          name,
          elector,
          electorTerm,
          currentTerm);
      stepDown(electorTerm, false);
    }
    return Response.RESPONSE_AGREE;
  }

  /**
   * Process an AppendEntryRequest. First check the term of the leader, then parse the log and
   * finally see if we can find a position to append the log.
   */
  public long appendEntry(AppendEntryRequest request) throws UnknownLogTypeException {
    logger.debug("{} received an AppendEntryRequest: {}", name, request);
    // the term checked here is that of the leader, not that of the log
    long checkResult = checkRequestTerm(request.term, request.leader);
    if (checkResult != Response.RESPONSE_AGREE) {
      return checkResult;
    }

    long startTime = Timer.Statistic.RAFT_RECEIVER_LOG_PARSE.getOperationStartTime();
    int logByteSize = request.getEntry().length;
    Log log = LogParser.getINSTANCE().parse(request.entry);
    log.setByteSize(logByteSize);
    Timer.Statistic.RAFT_RECEIVER_LOG_PARSE.calOperationCostTimeFromStart(startTime);

    long result = appendEntry(request.prevLogIndex, request.prevLogTerm, request.leaderCommit, log);
    logger.debug("{} AppendEntryRequest of {} completed with result {}", name, log, result);

    return result;
  }

  /** Similar to appendEntry, while the incoming load is batch of logs instead of a single log. */
  public long appendEntries(AppendEntriesRequest request) throws UnknownLogTypeException {
    logger.debug("{} received an AppendEntriesRequest", name);

    // the term checked here is that of the leader, not that of the log
    long checkResult = checkRequestTerm(request.term, request.leader);
    if (checkResult != Response.RESPONSE_AGREE) {
      return checkResult;
    }

    long response;
    List<Log> logs = new ArrayList<>();
    int logByteSize = 0;
    long startTime = Timer.Statistic.RAFT_RECEIVER_LOG_PARSE.getOperationStartTime();
    for (ByteBuffer buffer : request.getEntries()) {
      buffer.mark();
      Log log;
      logByteSize = buffer.limit() - buffer.position();
      try {
        log = LogParser.getINSTANCE().parse(buffer);
        log.setByteSize(logByteSize);
      } catch (BufferUnderflowException e) {
        buffer.reset();
        throw e;
      }
      logs.add(log);
    }

    Timer.Statistic.RAFT_RECEIVER_LOG_PARSE.calOperationCostTimeFromStart(startTime);

    response = appendEntries(request.prevLogIndex, request.prevLogTerm, request.leaderCommit, logs);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} AppendEntriesRequest of log size {} completed with result {}",
          name,
          request.getEntries().size(),
          response);
    }
    return response;
  }

  public PlanExecutor getLocalExecutor() throws QueryProcessException {
    if (localExecutor == null) {
      localExecutor = new PlanExecutor();
    }
    return localExecutor;
  }

  /**
   * Get an asynchronous heartbeat thrift client to the given node.
   *
   * @return an asynchronous thrift client or null if the caller tries to connect the local node.
   */
  public AsyncClient getAsyncHeartbeatClient(Node node) {
    return getAsyncClient(node, asyncHeartbeatClientPool, false);
  }

  /**
   * NOTICE: client.putBack() must be called after use.
   *
   * @return the heartbeat client for the node
   */
  public Client getSyncHeartbeatClient(Node node) {
    return getSyncClient(syncHeartbeatClientPool, node, false);
  }

  public void sendLogAsync(
      Log log,
      AtomicInteger voteCounter,
      Node node,
      AtomicBoolean leaderShipStale,
      AtomicLong newLeaderTerm,
      AppendEntryRequest request,
      Peer peer) {
    AsyncClient client = getSendLogAsyncClient(node);
    if (client != null) {
      AppendNodeEntryHandler handler =
          getAppendNodeEntryHandler(log, voteCounter, node, leaderShipStale, newLeaderTerm, peer);
      try {
        client.appendEntry(request, handler);
        logger.debug("{} sending a log to {}: {}", name, node, log);
      } catch (Exception e) {
        logger.warn("{} cannot append log to node {}", name, node, e);
      }
    }
  }

  private Client getSyncClient(SyncClientPool pool, Node node, boolean activatedOnly) {
    if (ClusterConstant.EMPTY_NODE.equals(node) || node == null) {
      return null;
    }

    return pool.getClient(node, activatedOnly);
  }

  public NodeCharacter getCharacter() {
    return character;
  }

  public void setCharacter(NodeCharacter character) {
    if (!Objects.equals(character, this.character)) {
      logger.info("{} has become a {}", name, character);
      this.character = character;
    }
  }

  public long getLastHeartbeatReceivedTime() {
    return lastHeartbeatReceivedTime;
  }

  public void setLastHeartbeatReceivedTime(long lastHeartbeatReceivedTime) {
    this.lastHeartbeatReceivedTime = lastHeartbeatReceivedTime;
  }

  public Node getLeader() {
    return leader.get();
  }

  public void setLeader(Node leader) {
    if (!Objects.equals(leader, this.leader.get())) {
      if (ClusterConstant.EMPTY_NODE.equals(leader) || leader == null) {
        logger.info("{} has been set to null in term {}", getName(), term.get());
      } else if (!Objects.equals(leader, this.thisNode)) {
        logger.info("{} has become a follower of {} in term {}", getName(), leader, term.get());
      }
      synchronized (waitLeaderCondition) {
        if (leader == null) {
          this.leader.set(ClusterConstant.EMPTY_NODE);
        } else {
          this.leader.set(leader);
        }
        if (!ClusterConstant.EMPTY_NODE.equals(this.leader.get())) {
          waitLeaderCondition.notifyAll();
        }
      }
    }
  }

  public Collection<Node> getAllNodes() {
    return allNodes;
  }

  public void setAllNodes(List<Node> allNodes) {
    this.allNodes = allNodes;
  }

  public Map<Node, Long> getLastCatchUpResponseTime() {
    return lastCatchUpResponseTime;
  }

  /** Sub-classes will add their own process of HeartBeatResponse in this method. */
  public void processValidHeartbeatResp(HeartBeatResponse response, Node receiver) {}

  /** The actions performed when the node wins in an election (becoming a leader). */
  public void onElectionWins() {}

  /**
   * Update the followers' log by sending logs whose index >= followerLastMatchedLogIndex to the
   * follower. If some of the required logs are removed, also send the snapshot. <br>
   * notice that if a part of data is in the snapshot, then it is not in the logs.
   */
  public void catchUp(Node follower, long lastLogIdx) {
    // for one follower, there is at most one ongoing catch-up, so the same data will not be sent
    // twice to the node
    synchronized (catchUpService) {
      // check if the last catch-up is still ongoing and does not time out yet
      Long lastCatchupResp = lastCatchUpResponseTime.get(follower);
      if (lastCatchupResp != null
          && System.currentTimeMillis() - lastCatchupResp < config.getCatchUpTimeoutMS()) {
        logger.debug("{}: last catch up of {} is ongoing", name, follower);
        return;
      } else {
        // record the start of the catch-up
        lastCatchUpResponseTime.put(follower, System.currentTimeMillis());
      }
    }
    logger.info("{}: Start to make {} catch up", name, follower);
    if (!catchUpService.isShutdown()) {
      Future<?> future =
          catchUpService.submit(new CatchUpTask(follower, peerMap.get(follower), this, lastLogIdx));
      catchUpService.submit(
          () -> {
            try {
              future.get();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
              logger.error("{}: Catch up task exits with unexpected exception", name, e);
            }
          });
    }
  }

  /**
   * If the node is not a leader, the request will be sent to the leader or reports an error if
   * there is no leader. Otherwise execute the plan locally (whether to send it to followers depends
   * on the type of the plan).
   */
  public TSStatus executeNonQueryPlan(ExecutNonQueryReq request)
      throws IOException, IllegalPathException {
    // process the plan locally
    PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);

    TSStatus answer = executeNonQueryPlan(plan);
    logger.debug("{}: Received a plan {}, executed answer: {}", name, plan, answer);
    return answer;
  }

  /**
   * Execute a non-query plan. Subclass may have their individual implements.
   *
   * @param plan a non-query plan.
   * @return A TSStatus indicating the execution result.
   */
  abstract TSStatus executeNonQueryPlan(PhysicalPlan plan);

  /**
   * according to the consistency configuration, decide whether to execute syncLeader or not and
   * throws exception when failed. Note that the write request will always try to sync leader
   */
  public void syncLeaderWithConsistencyCheck(boolean isWriteRequest)
      throws CheckConsistencyException {
    if (isWriteRequest) {
      syncLeader(new StrongCheckConsistency());
    } else {
      switch (config.getConsistencyLevel()) {
        case STRONG_CONSISTENCY:
          syncLeader(new StrongCheckConsistency());
          return;
        case MID_CONSISTENCY:
          // if leaderCommitId bigger than localAppliedId a value,
          // will throw CHECK_MID_CONSISTENCY_EXCEPTION
          syncLeader(new MidCheckConsistency());
          return;
        case WEAK_CONSISTENCY:
          // do nothing
          return;
        default:
          // this should not happen in theory
          throw new CheckConsistencyException(
              "unknown consistency=" + config.getConsistencyLevel().name());
      }
    }
  }

  /** call back after syncLeader */
  public interface CheckConsistency {

    /**
     * deal leaderCommitId and localAppliedId after syncLeader
     *
     * @param leaderCommitId leader commit id
     * @param localAppliedId local applied id
     * @throws CheckConsistencyException maybe throw CheckConsistencyException, which is defined in
     *     implements.
     */
    void postCheckConsistency(long leaderCommitId, long localAppliedId)
        throws CheckConsistencyException;
  }

  public static class MidCheckConsistency implements CheckConsistency {

    /**
     * if leaderCommitId - localAppliedId > MaxReadLogLag, will throw
     * CHECK_MID_CONSISTENCY_EXCEPTION
     *
     * @param leaderCommitId leader commit id
     * @param localAppliedId local applied id
     * @throws CheckConsistencyException
     */
    @Override
    public void postCheckConsistency(long leaderCommitId, long localAppliedId)
        throws CheckConsistencyException {
      if (leaderCommitId == Long.MAX_VALUE
          || leaderCommitId == Long.MIN_VALUE
          || leaderCommitId - localAppliedId
              > ClusterDescriptor.getInstance().getConfig().getMaxReadLogLag()) {
        throw CheckConsistencyException.CHECK_MID_CONSISTENCY_EXCEPTION;
      }
    }
  }

  public static class StrongCheckConsistency implements CheckConsistency {

    /**
     * if leaderCommitId > localAppliedId, will throw CHECK_STRONG_CONSISTENCY_EXCEPTION
     *
     * @param leaderCommitId leader commit id
     * @param localAppliedId local applied id
     * @throws CheckConsistencyException
     */
    @Override
    public void postCheckConsistency(long leaderCommitId, long localAppliedId)
        throws CheckConsistencyException {
      if (leaderCommitId > localAppliedId
          || leaderCommitId == Long.MAX_VALUE
          || leaderCommitId == Long.MIN_VALUE) {
        throw CheckConsistencyException.CHECK_STRONG_CONSISTENCY_EXCEPTION;
      }
    }
  }

  /**
   * Request and check the leader's commitId to see whether this node has caught up. If not, wait
   * until this node catches up.
   *
   * @param checkConsistency check after syncleader
   * @return true if the node has caught up, false otherwise
   * @throws CheckConsistencyException if leaderCommitId bigger than localAppliedId a threshold
   *     value after timeout
   */
  public boolean syncLeader(CheckConsistency checkConsistency) throws CheckConsistencyException {
    if (character == NodeCharacter.LEADER) {
      return true;
    }
    waitLeader();
    if (leader.get() == null || ClusterConstant.EMPTY_NODE.equals(leader.get())) {
      // the leader has not been elected, we must assume the node falls behind
      logger.warn(MSG_NO_LEADER_IN_SYNC, name);
      return false;
    }
    if (character == NodeCharacter.LEADER) {
      return true;
    }
    logger.debug("{}: try synchronizing with the leader {}", name, leader.get());
    return waitUntilCatchUp(checkConsistency);
  }

  /** Wait until the leader of this node becomes known or time out. */
  public void waitLeader() {
    long startTime = System.currentTimeMillis();
    while (leader.get() == null || ClusterConstant.EMPTY_NODE.equals(leader.get())) {
      synchronized (waitLeaderCondition) {
        try {
          waitLeaderCondition.wait(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Unexpected interruption when waiting for a leader", e);
        }
      }
      long consumedTime = System.currentTimeMillis() - startTime;
      if (consumedTime >= getWaitLeaderTimeMs()) {
        logger.warn("{}: leader is still offline after {}ms", name, consumedTime);
        break;
      }
    }
    logger.debug("{}: current leader is {}", name, leader.get());
  }

  /**
   * Request the leader's commit index and wait until the local commit index becomes not less than
   * it.
   *
   * @return true if this node has caught up before timeout, false otherwise
   * @throws CheckConsistencyException if leaderCommitId bigger than localAppliedId a threshold
   *     value after timeout
   */
  protected boolean waitUntilCatchUp(CheckConsistency checkConsistency)
      throws CheckConsistencyException {
    long leaderCommitId = Long.MIN_VALUE;
    RequestCommitIndexResponse response;
    try {
      response = config.isUseAsyncServer() ? requestCommitIdAsync() : requestCommitIdSync();
      leaderCommitId = response.getCommitLogIndex();

      tryUpdateCommitIndex(
          response.getTerm(), response.getCommitLogIndex(), response.getCommitLogTerm());

      return syncLocalApply(leaderCommitId);
    } catch (TException e) {
      logger.error(MSG_NO_LEADER_COMMIT_INDEX, name, leader.get(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error(MSG_NO_LEADER_COMMIT_INDEX, name, leader.get(), e);
    } finally {
      if (checkConsistency != null) {
        checkConsistency.postCheckConsistency(
            leaderCommitId, logManager.getMaxHaveAppliedCommitIndex());
      }
    }
    return false;
  }

  /**
   * sync local applyId to leader commitId
   *
   * @param leaderCommitId leader commit id
   * @return true if leaderCommitId <= localAppliedId
   */
  private boolean syncLocalApply(long leaderCommitId) {
    long startTime = System.currentTimeMillis();
    long waitedTime = 0;
    long localAppliedId = 0;
    while (waitedTime < RaftServer.getSyncLeaderMaxWaitMs()) {
      try {
        localAppliedId = logManager.getMaxHaveAppliedCommitIndex();
        logger.debug("{}: synchronizing commitIndex {}/{}", name, localAppliedId, leaderCommitId);
        if (leaderCommitId <= localAppliedId) {
          // this node has caught up
          if (logger.isDebugEnabled()) {
            waitedTime = System.currentTimeMillis() - startTime;
            logger.debug("{}: synchronized with the leader after {}ms", name, waitedTime);
          }
          return true;
        }
        // wait for next heartbeat to catch up
        // the local node will not perform a commit here according to the leaderCommitId because
        // the node may have some inconsistent logs with the leader
        waitedTime = System.currentTimeMillis() - startTime;
        synchronized (syncLock) {
          syncLock.wait(RaftServer.getHeartBeatIntervalMs());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error(MSG_NO_LEADER_COMMIT_INDEX, name, leader.get(), e);
      }
    }
    logger.warn("{}: Failed to synchronize with the leader after {}ms", name, waitedTime);

    return false;
  }

  /**
   * Create a log for "plan" and append it locally and to all followers. Only the group leader can
   * call this method. Will commit the log locally and send it to followers
   *
   * @return OK if over half of the followers accept the log or null if the leadership is lost
   *     during the appending
   */
  TSStatus processPlanLocally(PhysicalPlan plan) {
    if (USE_LOG_DISPATCHER) {
      return processPlanLocallyV2(plan);
    }

    logger.debug("{}: Processing plan {}", name, plan);
    if (readOnly) {
      return StatusUtils.NODE_READ_ONLY;
    }
    long startTime = Timer.Statistic.RAFT_SENDER_APPEND_LOG.getOperationStartTime();
    PhysicalPlanLog log = new PhysicalPlanLog();
    // assign term and index to the new log and append it
    synchronized (logManager) {
      log.setCurrLogTerm(getTerm().get());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      log.setPlan(plan);
      plan.setIndex(log.getCurrLogIndex());
      // appendLogInGroup will serialize log, and set log size, and we will use the size after it
      logManager.append(log);
    }
    Timer.Statistic.RAFT_SENDER_APPEND_LOG.calOperationCostTimeFromStart(startTime);

    try {
      if (appendLogInGroup(log)) {
        return StatusUtils.OK;
      }
    } catch (LogExecutionException e) {
      return handleLogExecutionException(log.getPlan(), IOUtils.getRootCause(e));
    }
    return StatusUtils.TIME_OUT;
  }

  private TSStatus processPlanLocallyV2(PhysicalPlan plan) {
    logger.debug("{}: Processing plan {}", name, plan);
    if (readOnly) {
      return StatusUtils.NODE_READ_ONLY;
    }
    PhysicalPlanLog log = new PhysicalPlanLog();
    // assign term and index to the new log and append it
    SendLogRequest sendLogRequest;

    long startTime =
        Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2.getOperationStartTime();
    synchronized (logManager) {
      Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_APPEND_V2.calOperationCostTimeFromStart(
          startTime);

      log.setCurrLogTerm(getTerm().get());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);
      log.setPlan(plan);
      plan.setIndex(log.getCurrLogIndex());

      startTime = Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.getOperationStartTime();
      // logDispatcher will serialize log, and set log size, and we will use the size after it
      logManager.append(log);
      Timer.Statistic.RAFT_SENDER_APPEND_LOG_V2.calOperationCostTimeFromStart(startTime);

      startTime = Statistic.RAFT_SENDER_BUILD_LOG_REQUEST.getOperationStartTime();
      sendLogRequest = buildSendLogRequest(log);
      Statistic.RAFT_SENDER_BUILD_LOG_REQUEST.calOperationCostTimeFromStart(startTime);

      startTime = Statistic.RAFT_SENDER_OFFER_LOG.getOperationStartTime();
      log.setCreateTime(System.nanoTime());
      getLogDispatcher().offer(sendLogRequest);
      Statistic.RAFT_SENDER_OFFER_LOG.calOperationCostTimeFromStart(startTime);
    }

    try {
      AppendLogResult appendLogResult =
          waitAppendResult(
              sendLogRequest.getVoteCounter(),
              sendLogRequest.getLeaderShipStale(),
              sendLogRequest.getNewLeaderTerm());
      Timer.Statistic.RAFT_SENDER_LOG_FROM_CREATE_TO_ACCEPT.calOperationCostTimeFromStart(
          sendLogRequest.getLog().getCreateTime());
      switch (appendLogResult) {
        case OK:
          logger.debug(MSG_LOG_IS_ACCEPTED, name, log);
          startTime = Timer.Statistic.RAFT_SENDER_COMMIT_LOG.getOperationStartTime();
          commitLog(log);
          Timer.Statistic.RAFT_SENDER_COMMIT_LOG.calOperationCostTimeFromStart(startTime);
          return StatusUtils.OK;
        case TIME_OUT:
          logger.debug("{}: log {} timed out...", name, log);
          break;
        case LEADERSHIP_STALE:
          // abort the appending, the new leader will fix the local logs by catch-up
        default:
          break;
      }
    } catch (LogExecutionException e) {
      return handleLogExecutionException(log.getPlan(), IOUtils.getRootCause(e));
    }
    return StatusUtils.TIME_OUT;
  }

  public SendLogRequest buildSendLogRequest(Log log) {
    AtomicInteger voteCounter = new AtomicInteger(allNodes.size() / 2);
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(term.get());

    long startTime = Statistic.RAFT_SENDER_BUILD_APPEND_REQUEST.getOperationStartTime();
    AppendEntryRequest appendEntryRequest = buildAppendEntryRequest(log, false);
    Statistic.RAFT_SENDER_BUILD_APPEND_REQUEST.calOperationCostTimeFromStart(startTime);

    return new SendLogRequest(log, voteCounter, leaderShipStale, newLeaderTerm, appendEntryRequest);
  }

  /**
   * The maximum time to wait if there is no leader in the group, after which a
   * LeadNotFoundException will be thrown.
   */
  static long getWaitLeaderTimeMs() {
    return waitLeaderTimeMs;
  }

  static void setWaitLeaderTimeMs(long waitLeaderTimeMs) {
    RaftMember.waitLeaderTimeMs = waitLeaderTimeMs;
  }

  @SuppressWarnings("java:S2274") // enable timeout
  protected RequestCommitIndexResponse requestCommitIdAsync()
      throws TException, InterruptedException {
    // use Long.MAX_VALUE to indicate a timeout
    RequestCommitIndexResponse response =
        new RequestCommitIndexResponse(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
    AtomicReference<RequestCommitIndexResponse> commitIdResult = new AtomicReference<>(response);
    AsyncClient client = getAsyncClient(leader.get());
    if (client == null) {
      // cannot connect to the leader
      logger.warn(MSG_NO_LEADER_IN_SYNC, name);
      return commitIdResult.get();
    }
    synchronized (commitIdResult) {
      client.requestCommitIndex(getHeader(), new GenericHandler<>(leader.get(), commitIdResult));
      commitIdResult.wait(RaftServer.getSyncLeaderMaxWaitMs());
    }
    return commitIdResult.get();
  }

  private RequestCommitIndexResponse requestCommitIdSync() throws TException {
    Client client = getSyncClient(leader.get());
    RequestCommitIndexResponse response;
    if (client == null) {
      // cannot connect to the leader
      logger.warn(MSG_NO_LEADER_IN_SYNC, name);
      // use Long.MAX_VALUE to indicate a timeouts
      response = new RequestCommitIndexResponse(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
      return response;
    }
    try {
      response = client.requestCommitIndex(getHeader());
    } catch (TException e) {
      client.getInputProtocol().getTransport().close();
      throw e;
    } finally {
      ClientUtils.putBackSyncClient(client);
    }
    return response;
  }

  /**
   * Tell the requester the current commit index if the local node is the leader of the group headed
   * by header. Or forward it to the leader. Otherwise report an error.
   *
   * @return Long.MIN_VALUE if the node is not a leader, or the commitIndex
   */
  public long getCommitIndex() {
    if (character == NodeCharacter.LEADER) {
      return logManager.getCommitLogIndex();
    } else {
      return Long.MIN_VALUE;
    }
  }

  public void setReadOnly() {
    synchronized (logManager) {
      readOnly = true;
    }
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void initPeerMap() {
    peerMap = new ConcurrentHashMap<>();
    for (Node entry : allNodes) {
      peerMap.computeIfAbsent(entry, k -> new Peer(logManager.getLastLogIndex()));
    }
  }

  public Map<Node, Peer> getPeerMap() {
    return peerMap;
  }

  /** @return true if there is a log whose index is "index" and term is "term", false otherwise */
  public boolean matchLog(long index, long term) {
    boolean matched = logManager.matchTerm(term, index);
    logger.debug("Log {}-{} matched: {}", index, term, matched);
    return matched;
  }

  public ExecutorService getSerialToParallelPool() {
    return serialToParallelPool;
  }

  public Object getSyncLock() {
    return syncLock;
  }

  /** Sub-classes will add their own process of HeartBeatRequest in this method. */
  void processValidHeartbeatReq(HeartBeatRequest request, HeartBeatResponse response) {}

  /**
   * Verify the validity of an ElectionRequest, and make itself a follower of the elector if the
   * request is valid.
   *
   * @return Response.RESPONSE_AGREE if the elector is valid or the local term if the elector has a
   *     smaller term or Response.RESPONSE_LOG_MISMATCH if the elector has older logs.
   */
  long checkElectorLogProgress(ElectionRequest electionRequest) {

    long thatTerm = electionRequest.getTerm();
    long thatLastLogIndex = electionRequest.getLastLogIndex();
    long thatLastLogTerm = electionRequest.getLastLogTerm();

    // check the log progress of the elector
    long resp = checkLogProgress(thatLastLogIndex, thatLastLogTerm);
    if (resp == Response.RESPONSE_AGREE) {
      logger.info(
          "{} accepted an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          name,
          thatTerm,
          term.get(),
          thatLastLogIndex,
          logManager.getLastLogIndex(),
          thatLastLogTerm,
          logManager.getLastLogTerm());
      setCharacter(NodeCharacter.FOLLOWER);
      lastHeartbeatReceivedTime = System.currentTimeMillis();
      setVoteFor(electionRequest.getElector());
      updateHardState(thatTerm, getVoteFor());
    } else {
      logger.info(
          "{} rejected an election request, term:{}/{}, logIndex:{}/{}, logTerm:{}/{}",
          name,
          thatTerm,
          term.get(),
          thatLastLogIndex,
          logManager.getLastLogIndex(),
          thatLastLogTerm,
          logManager.getLastLogTerm());
    }
    return resp;
  }

  /**
   * Reject the election if the lastLogTerm of the candidate equals to the voter's but its
   * lastLogIndex is smaller than the voter's Otherwise accept the election.
   *
   * @return Response.RESPONSE_AGREE if the elector is valid or the local term if the elector has a
   *     smaller term or Response.RESPONSE_LOG_MISMATCH if the elector has older logs.
   */
  long checkLogProgress(long lastLogIndex, long lastLogTerm) {
    long response;
    synchronized (logManager) {
      if (logManager.isLogUpToDate(lastLogTerm, lastLogIndex)) {
        response = Response.RESPONSE_AGREE;
      } else {
        response = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return response;
  }

  /**
   * Forward a non-query plan to a node using the default client.
   *
   * @param plan a non-query plan
   * @param node cannot be the local node
   * @param header must be set for data group communication, set to null for meta group
   *     communication
   * @return a TSStatus indicating if the forwarding is successful.
   */
  public TSStatus forwardPlan(PhysicalPlan plan, Node node, Node header) {
    if (node == null || node.equals(thisNode)) {
      logger.debug("{}: plan {} has no where to be forwarded", name, plan);
      return StatusUtils.NO_LEADER;
    }
    logger.debug("{}: Forward {} to node {}", name, plan, node);

    TSStatus status;
    if (config.isUseAsyncServer()) {
      status = forwardPlanAsync(plan, node, header);
    } else {
      status = forwardPlanSync(plan, node, header);
    }
    if (status.getCode() == TSStatusCode.NO_CONNECTION.getStatusCode()
        && (header == null || header.equals(getHeader()))
        && (leader.get() != null)
        && leader.get().equals(node)) {
      // leader is down, trigger a new election by resetting heartbeat
      lastHeartbeatReceivedTime = -1;
      leader.set(null);
      waitLeader();
    }
    return status;
  }

  /**
   * Forward a non-query plan to "receiver" using "client".
   *
   * @param plan a non-query plan
   * @param header to determine which DataGroupMember of "receiver" will process the request.
   * @return a TSStatus indicating if the forwarding is successful.
   */
  private TSStatus forwardPlanAsync(PhysicalPlan plan, Node receiver, Node header) {
    AsyncClient client = getAsyncClient(receiver);
    if (client == null) {
      logger.debug("{}: can not get client for node={}", name, receiver);
      return StatusUtils.NO_CONNECTION
          .deepCopy()
          .setMessage(String.format("%s cannot be reached", receiver));
    }
    return forwardPlanAsync(plan, receiver, header, client);
  }

  public TSStatus forwardPlanAsync(
      PhysicalPlan plan, Node receiver, Node header, AsyncClient client) {
    try {
      TSStatus tsStatus = SyncClientAdaptor.executeNonQuery(client, plan, header, receiver);
      if (tsStatus == null) {
        tsStatus = StatusUtils.TIME_OUT;
        logger.warn(MSG_FORWARD_TIMEOUT, name, plan, receiver);
      }
      return tsStatus;
    } catch (IOException | TException e) {
      logger.error(MSG_FORWARD_ERROR, name, plan, receiver, e);
      return StatusUtils.getStatus(StatusUtils.INTERNAL_ERROR, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("{}: forward {} to {} interrupted", name, plan, receiver);
      return StatusUtils.TIME_OUT;
    }
  }

  private TSStatus forwardPlanSync(PhysicalPlan plan, Node receiver, Node header) {
    Client client = getSyncClient(receiver);
    if (client == null) {
      logger.warn(MSG_FORWARD_TIMEOUT, name, plan, receiver);
      return StatusUtils.TIME_OUT;
    }
    return forwardPlanSync(plan, receiver, header, client);
  }

  public TSStatus forwardPlanSync(PhysicalPlan plan, Node receiver, Node header, Client client) {
    long startTime = System.currentTimeMillis();
    try {
      ExecutNonQueryReq req = new ExecutNonQueryReq();
      req.setPlanBytes(PlanSerializer.getInstance().serialize(plan));
      if (header != null) {
        req.setHeader(header);
      }
      TSStatus tsStatus = client.executeNonQueryPlan(req);
      if (tsStatus == null) {
        tsStatus = StatusUtils.TIME_OUT;
        logger.warn(MSG_FORWARD_TIMEOUT, name, plan, receiver);
      }
      return tsStatus;
    } catch (IOException e) {
      logger.error(MSG_FORWARD_ERROR, name, plan, receiver, e);
      return StatusUtils.getStatus(StatusUtils.INTERNAL_ERROR, e.getMessage());
    } catch (TException e) {
      TSStatus status;
      if (e.getCause() instanceof SocketTimeoutException) {
        status = StatusUtils.TIME_OUT;
        logger.warn(
            MSG_FORWARD_TIMEOUT + ": {}ms",
            name,
            plan,
            receiver,
            System.currentTimeMillis() - startTime);
      } else {
        logger.error(MSG_FORWARD_ERROR, name, plan, receiver, e);
        status = StatusUtils.getStatus(StatusUtils.INTERNAL_ERROR, e.getMessage());
      }
      // the connection may be broken, close it to avoid it being reused
      client.getInputProtocol().getTransport().close();
      return status;
    } finally {
      ClientUtils.putBackSyncClient(client);
    }
  }

  /**
   * Get an asynchronous thrift client of the given node.
   *
   * @return an asynchronous thrift client or null if the caller tries to connect the local node or
   *     the node cannot be reached.
   */
  public AsyncClient getAsyncClient(Node node) {
    return getAsyncClient(node, asyncClientPool, true);
  }

  public AsyncClient getAsyncClient(Node node, boolean activatedOnly) {
    return getAsyncClient(node, asyncClientPool, activatedOnly);
  }

  public AsyncClient getSendLogAsyncClient(Node node) {
    return getAsyncClient(node, asyncSendLogClientPool, true);
  }

  private AsyncClient getAsyncClient(Node node, AsyncClientPool pool, boolean activatedOnly) {
    if (ClusterConstant.EMPTY_NODE.equals(node) || node == null) {
      return null;
    }
    try {
      return pool.getClient(node, activatedOnly);
    } catch (IOException e) {
      logger.warn("{} cannot connect to node {}", name, node, e);
      return null;
    }
  }

  /**
   * NOTICE: client.putBack() must be called after use. the caller needs to check to see if the
   * return value is null
   *
   * @param node the node to connect
   * @return the client if node is available, otherwise null
   */
  public Client getSyncClient(Node node) {
    return getSyncClient(syncClientPool, node, true);
  }

  public Client getSyncClient(Node node, boolean activatedOnly) {
    return getSyncClient(syncClientPool, node, activatedOnly);
  }

  public AtomicLong getTerm() {
    return term;
  }

  private synchronized LogDispatcher getLogDispatcher() {
    if (logDispatcher == null) {
      logDispatcher = new LogDispatcher(this);
    }
    return logDispatcher;
  }

  /**
   * wait until "voteCounter" counts down to zero, which means the quorum has received the log, or
   * one follower tells the node that it is no longer a valid leader, or a timeout is triggered.
   */
  @SuppressWarnings({"java:S2445"}) // safe synchronized
  private AppendLogResult waitAppendResult(
      AtomicInteger voteCounter, AtomicBoolean leaderShipStale, AtomicLong newLeaderTerm) {
    // wait for the followers to vote
    long startTime = Timer.Statistic.RAFT_SENDER_VOTE_COUNTER.getOperationStartTime();
    synchronized (voteCounter) {
      long waitStart = System.currentTimeMillis();
      long alreadyWait = 0;
      while (voteCounter.get() > 0
          && alreadyWait < RaftServer.getWriteOperationTimeoutMS()
          && voteCounter.get() != Integer.MAX_VALUE) {
        try {
          voteCounter.wait(RaftServer.getWriteOperationTimeoutMS());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Unexpected interruption when sending a log", e);
        }
        alreadyWait = System.currentTimeMillis() - waitStart;
      }
    }
    Timer.Statistic.RAFT_SENDER_VOTE_COUNTER.calOperationCostTimeFromStart(startTime);

    // a node has a larger term than the local node, so this node is no longer a valid leader
    if (leaderShipStale.get()) {
      stepDown(newLeaderTerm.get(), false);
      return AppendLogResult.LEADERSHIP_STALE;
    }
    // the node knows it is no long the leader from other requests
    if (character != NodeCharacter.LEADER) {
      return AppendLogResult.LEADERSHIP_STALE;
    }

    // cannot get enough agreements within a certain amount of time
    if (voteCounter.get() > 0) {
      return AppendLogResult.TIME_OUT;
    }

    // voteCounter has counted down to zero
    return AppendLogResult.OK;
  }

  @SuppressWarnings("java:S2445")
  void commitLog(Log log) throws LogExecutionException {
    long startTime =
        Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_COMMIT.getOperationStartTime();
    synchronized (logManager) {
      Statistic.RAFT_SENDER_COMPETE_LOG_MANAGER_BEFORE_COMMIT.calOperationCostTimeFromStart(
          startTime);

      startTime = Statistic.RAFT_SENDER_COMMIT_LOG_IN_MANAGER.getOperationStartTime();
      logManager.commitTo(log.getCurrLogIndex());
    }
    Statistic.RAFT_SENDER_COMMIT_LOG_IN_MANAGER.calOperationCostTimeFromStart(startTime);
    // when using async applier, the log here may not be applied. To return the execution
    // result, we must wait until the log is applied.
    startTime = Statistic.RAFT_SENDER_COMMIT_WAIT_LOG_APPLY.getOperationStartTime();
    synchronized (log) {
      while (!log.isApplied()) {
        // wait until the log is applied
        try {
          log.wait(5);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new LogExecutionException(e);
        }
      }
    }
    Statistic.RAFT_SENDER_COMMIT_WAIT_LOG_APPLY.calOperationCostTimeFromStart(startTime);
    if (log.getException() != null) {
      throw new LogExecutionException(log.getException());
    }
  }

  protected TSStatus handleLogExecutionException(PhysicalPlan log, Throwable cause) {
    if (cause instanceof BatchProcessException) {
      return RpcUtils.getStatus(Arrays.asList(((BatchProcessException) cause).getFailingStatus()));
    }
    if (cause instanceof DuplicatedTemplateException) {
      return StatusUtils.DUPLICATED_TEMPLATE.deepCopy().setMessage(cause.getMessage());
    }
    if (cause instanceof StorageGroupNotSetException) {
      TSStatus status = StatusUtils.getStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST);
      status.setMessage(cause.getMessage());
      return status;
    }
    TSStatus tsStatus =
        StatusUtils.getStatus(StatusUtils.EXECUTE_STATEMENT_ERROR, cause.getMessage());
    if (cause instanceof RuntimeException) {
      logger.error("RuntimeException during executing {}", log, cause);
    }
    if (cause instanceof IoTDBException) {
      tsStatus.setCode(((IoTDBException) cause).getErrorCode());
    }
    if (!(cause instanceof PathNotExistException)
        && !(cause instanceof PathAlreadyExistException)
        && !(cause instanceof StorageGroupAlreadySetException)) {
      logger.debug("{} cannot be executed because ", log, cause);
    }
    return tsStatus;
  }

  AppendEntryRequest buildAppendEntryRequest(Log log, boolean serializeNow) {
    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(term.get());
    if (serializeNow) {
      ByteBuffer byteBuffer = log.serialize();
      log.setByteSize(byteBuffer.array().length);
      request.setEntry(byteBuffer);
    }
    request.setLeader(getThisNode());
    // don't need lock because even if it's larger than the commitIndex when appending this log to
    // logManager, the follower can handle the larger commitIndex with no effect
    request.setLeaderCommit(logManager.getCommitLogIndex());
    request.setPrevLogIndex(log.getCurrLogIndex() - 1);
    try {
      request.setPrevLogTerm(logManager.getTerm(log.getCurrLogIndex() - 1));
    } catch (Exception e) {
      logger.error("getTerm failed for newly append entries", e);
    }
    if (getHeader() != null) {
      // data groups use header to find a particular DataGroupMember
      request.setHeader(getHeader());
    }
    return request;
  }

  /**
   * If "newTerm" is larger than the local term, give up the leadership, become a follower and reset
   * heartbeat timer.
   *
   * @param fromLeader true if the request is from a leader, false if the request is from an
   *     elector.
   */
  public void stepDown(long newTerm, boolean fromLeader) {
    synchronized (term) {
      long currTerm = term.get();
      // confirm that the heartbeat of the new leader hasn't come
      if (currTerm < newTerm) {
        logger.info("{} has update it's term to {}", getName(), newTerm);
        term.set(newTerm);
        setVoteFor(null);
        setLeader(ClusterConstant.EMPTY_NODE);
        updateHardState(newTerm, getVoteFor());
      }

      if (fromLeader) {
        // only when the request is from a leader should we update lastHeartbeatReceivedTime,
        // otherwise the node may be stuck in FOLLOWER state by a stale node.
        setCharacter(NodeCharacter.FOLLOWER);
        lastHeartbeatReceivedTime = System.currentTimeMillis();
      }
    }
  }

  public Node getThisNode() {
    return thisNode;
  }

  public void setThisNode(Node thisNode) {
    this.thisNode = thisNode;
  }

  /** @return the header of the data raft group or null if this is in a meta group. */
  public Node getHeader() {
    return null;
  }

  public void updateHardState(long currentTerm, Node voteFor) {
    HardState state = logManager.getHardState();
    state.setCurrentTerm(currentTerm);
    state.setVoteFor(voteFor);
    logManager.updateHardState(state);
  }

  public Node getVoteFor() {
    return voteFor;
  }

  public void setVoteFor(Node voteFor) {
    if (!Objects.equals(voteFor, this.voteFor)) {
      logger.info("{} has update it's voteFor to {}", getName(), voteFor);
      this.voteFor = voteFor;
    }
  }

  /**
   * Append a log to all followers in the group until half of them accept the log or the leadership
   * is lost.
   *
   * @return true if the log is accepted by the quorum of the group, false otherwise
   */
  boolean appendLogInGroup(Log log) throws LogExecutionException {
    if (allNodes.size() == 1) {
      // single node group, no followers
      long startTime = Timer.Statistic.RAFT_SENDER_COMMIT_LOG.getOperationStartTime();
      logger.debug(MSG_LOG_IS_ACCEPTED, name, log);
      commitLog(log);
      Timer.Statistic.RAFT_SENDER_COMMIT_LOG.calOperationCostTimeFromStart(startTime);
      return true;
    }

    int retryTime = 0;
    while (true) {
      long startTime = Timer.Statistic.RAFT_SENDER_SEND_LOG_TO_FOLLOWERS.getOperationStartTime();
      logger.debug("{}: Send log {} to other nodes, retry times: {}", name, log, retryTime);
      if (character != NodeCharacter.LEADER) {
        logger.debug("Has lose leadership, so need not to send log");
        return false;
      }
      AppendLogResult result = sendLogToFollowers(log, allNodes.size() / 2);
      Timer.Statistic.RAFT_SENDER_SEND_LOG_TO_FOLLOWERS.calOperationCostTimeFromStart(startTime);
      switch (result) {
        case OK:
          startTime = Timer.Statistic.RAFT_SENDER_COMMIT_LOG.getOperationStartTime();
          logger.debug(MSG_LOG_IS_ACCEPTED, name, log);
          commitLog(log);
          Timer.Statistic.RAFT_SENDER_COMMIT_LOG.calOperationCostTimeFromStart(startTime);
          return true;
        case TIME_OUT:
          logger.debug("{}: log {} timed out, retrying...", name, log);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          retryTime++;
          if (retryTime > 5) {
            return false;
          }
          break;
        case LEADERSHIP_STALE:
          // abort the appending, the new leader will fix the local logs by catch-up
        default:
          return false;
      }
    }
  }

  /**
   * Send the given log to all the followers and decide the result by how many followers return a
   * success.
   *
   * @param requiredQuorum the number of votes needed to make the log valid, when requiredQuorum <=
   *     0, half of the cluster size will be used.
   * @return an AppendLogResult
   */
  private AppendLogResult sendLogToFollowers(Log log, int requiredQuorum) {
    if (requiredQuorum <= 0) {
      // use half of the members' size as the quorum
      return sendLogToFollowers(log, new AtomicInteger(allNodes.size() / 2));
    } else {
      // make sure quorum does not exceed the number of members - 1
      return sendLogToFollowers(
          log, new AtomicInteger(Math.min(requiredQuorum, allNodes.size() - 1)));
    }
  }

  /**
   * Send the log to each follower. Every time a follower returns a success, "voteCounter" is
   * decreased by 1 and when it counts to 0, return an OK. If any follower returns a higher term
   * than the local term, retire from leader and return a LEADERSHIP_STALE. If "voteCounter" is
   * still positive after a certain time, return TIME_OUT.
   *
   * @param voteCounter a decreasing vote counter
   * @return an AppendLogResult indicating a success or a failure and why
   */
  private AppendLogResult sendLogToFollowers(Log log, AtomicInteger voteCounter) {
    if (allNodes.size() == 1) {
      // single node group, does not need the agreement of others
      return AppendLogResult.OK;
    }
    logger.debug("{} sending a log to followers: {}", name, log);

    // if a follower has larger term than this node, leaderShipStale will be set to true and
    // newLeaderTerm will store the follower's term
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AtomicLong newLeaderTerm = new AtomicLong(term.get());

    AppendEntryRequest request = buildAppendEntryRequest(log, true);

    try {
      if (allNodes.size() > 2) {
        // if there are more than one followers, send the requests in parallel so that one slow
        // follower will not be blocked
        for (Node node : allNodes) {
          appendLogThreadPool.submit(
              () ->
                  sendLogToFollower(
                      log, voteCounter, node, leaderShipStale, newLeaderTerm, request));
          if (character != NodeCharacter.LEADER) {
            return AppendLogResult.LEADERSHIP_STALE;
          }
        }
      } else {
        // there is only one member, send to it within this thread to reduce thread switching
        // overhead
        for (Node node : allNodes) {
          sendLogToFollower(log, voteCounter, node, leaderShipStale, newLeaderTerm, request);
          if (character != NodeCharacter.LEADER) {
            return AppendLogResult.LEADERSHIP_STALE;
          }
        }
      }

    } catch (ConcurrentModificationException e) {
      // retry if allNodes has changed
      return AppendLogResult.TIME_OUT;
    }

    return waitAppendResult(voteCounter, leaderShipStale, newLeaderTerm);
  }

  /** Send "log" to "node". */
  public void sendLogToFollower(
      Log log,
      AtomicInteger voteCounter,
      Node node,
      AtomicBoolean leaderShipStale,
      AtomicLong newLeaderTerm,
      AppendEntryRequest request) {
    if (node.equals(thisNode)) {
      return;
    }
    /**
     * if the peer's log progress is too stale, wait until it catches up, otherwise, there may be
     * too many waiting requests on the peer's side.
     */
    long startTime = Timer.Statistic.RAFT_SENDER_WAIT_FOR_PREV_LOG.getOperationStartTime();
    Peer peer = peerMap.computeIfAbsent(node, k -> new Peer(logManager.getLastLogIndex()));
    if (!waitForPrevLog(peer, log)) {
      logger.warn("{}: node {} timed out when appending {}", name, node, log);
      return;
    }
    Timer.Statistic.RAFT_SENDER_WAIT_FOR_PREV_LOG.calOperationCostTimeFromStart(startTime);

    if (character != NodeCharacter.LEADER) {
      return;
    }

    if (config.isUseAsyncServer()) {
      sendLogAsync(log, voteCounter, node, leaderShipStale, newLeaderTerm, request, peer);
    } else {
      sendLogSync(log, voteCounter, node, leaderShipStale, newLeaderTerm, request, peer);
    }
  }

  /**
   * wait until the difference of log index between the matched log of peer and the given log become
   * no bigger than maxLogDiff.
   */
  @SuppressWarnings("java:S2445") // safe synchronized
  public boolean waitForPrevLog(Peer peer, Log log) {
    final int maxLogDiff = config.getMaxNumOfLogsInMem();
    long waitStart = System.currentTimeMillis();
    long alreadyWait = 0;
    // if the peer falls behind too much, wait until it catches up, otherwise there may be too
    // many client threads in the peer
    while (peer.getMatchIndex() < log.getCurrLogIndex() - maxLogDiff
        && character == NodeCharacter.LEADER
        && alreadyWait <= RaftServer.getWriteOperationTimeoutMS()) {
      synchronized (peer) {
        try {
          peer.wait(RaftServer.getWriteOperationTimeoutMS());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Waiting for peer to catch up interrupted");
          return false;
        }
      }
      alreadyWait = System.currentTimeMillis() - waitStart;
    }
    return alreadyWait <= RaftServer.getWriteOperationTimeoutMS();
  }

  private void sendLogSync(
      Log log,
      AtomicInteger voteCounter,
      Node node,
      AtomicBoolean leaderShipStale,
      AtomicLong newLeaderTerm,
      AppendEntryRequest request,
      Peer peer) {
    Client client = getSyncClient(node);
    if (client != null) {
      AppendNodeEntryHandler handler =
          getAppendNodeEntryHandler(log, voteCounter, node, leaderShipStale, newLeaderTerm, peer);
      try {
        logger.debug("{} sending a log to {}: {}", name, node, log);
        long result = client.appendEntry(request);
        handler.onComplete(result);
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        handler.onError(e);
      } catch (Exception e) {
        handler.onError(e);
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
    }
  }

  public AppendNodeEntryHandler getAppendNodeEntryHandler(
      Log log,
      AtomicInteger voteCounter,
      Node node,
      AtomicBoolean leaderShipStale,
      AtomicLong newLeaderTerm,
      Peer peer) {
    AppendNodeEntryHandler handler = new AppendNodeEntryHandler();
    handler.setReceiver(node);
    handler.setVoteCounter(voteCounter);
    handler.setLeaderShipStale(leaderShipStale);
    handler.setLog(log);
    handler.setMember(this);
    handler.setPeer(peer);
    handler.setReceiverTerm(newLeaderTerm);
    return handler;
  }

  @TestOnly
  public void setAppendLogThreadPool(ExecutorService appendLogThreadPool) {
    this.appendLogThreadPool = appendLogThreadPool;
  }

  public Object getSnapshotApplyLock() {
    return snapshotApplyLock;
  }

  /**
   * Find the local previous log of "log". If such log is found, discard all local logs behind it
   * and append "log" to it. Otherwise report a log mismatch.
   *
   * @return Response.RESPONSE_AGREE when the log is successfully appended or Response
   *     .RESPONSE_LOG_MISMATCH if the previous log of "log" is not found.
   */
  private long appendEntry(long prevLogIndex, long prevLogTerm, long leaderCommit, Log log) {
    long resp = checkPrevLogIndex(prevLogIndex);
    if (resp != Response.RESPONSE_AGREE) {
      return resp;
    }

    long startTime = Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.getOperationStartTime();
    long success;
    synchronized (logManager) {
      success = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, log);
    }
    Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);
    if (success != -1) {
      logger.debug("{} append a new log {}", name, log);
      resp = Response.RESPONSE_AGREE;
    } else {
      // the incoming log points to an illegal position, reject it
      resp = Response.RESPONSE_LOG_MISMATCH;
    }
    return resp;
  }

  /** Wait until all logs before "prevLogIndex" arrive or a timeout is reached. */
  private boolean waitForPrevLog(long prevLogIndex) {
    long waitStart = System.currentTimeMillis();
    long alreadyWait = 0;
    Object logUpdateCondition = logManager.getLogUpdateCondition(prevLogIndex);
    long lastLogIndex = logManager.getLastLogIndex();
    while (lastLogIndex < prevLogIndex && alreadyWait <= RaftServer.getWriteOperationTimeoutMS()) {
      try {
        // each time new logs are appended, this will be notified
        synchronized (logUpdateCondition) {
          logUpdateCondition.wait(1);
        }
        lastLogIndex = logManager.getLastLogIndex();
        if (lastLogIndex >= prevLogIndex) {
          return true;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      alreadyWait = System.currentTimeMillis() - waitStart;
    }

    return alreadyWait <= RaftServer.getWriteOperationTimeoutMS();
  }

  private long checkPrevLogIndex(long prevLogIndex) {
    long lastLogIndex = logManager.getLastLogIndex();
    long startTime = Timer.Statistic.RAFT_RECEIVER_WAIT_FOR_PREV_LOG.getOperationStartTime();
    if (lastLogIndex < prevLogIndex && !waitForPrevLog(prevLogIndex)) {
      // there are logs missing between the incoming log and the local last log, and such logs
      // did not come within a timeout, report a mismatch to the sender and it shall fix this
      // through catch-up
      Timer.Statistic.RAFT_RECEIVER_INDEX_DIFF.add(prevLogIndex - lastLogIndex);
      return Response.RESPONSE_LOG_MISMATCH;
    }
    Timer.Statistic.RAFT_RECEIVER_WAIT_FOR_PREV_LOG.calOperationCostTimeFromStart(startTime);
    return Response.RESPONSE_AGREE;
  }

  /**
   * Find the local previous log of "log". If such log is found, discard all local logs behind it
   * and append "log" to it. Otherwise report a log mismatch.
   *
   * @param logs append logs
   * @return Response.RESPONSE_AGREE when the log is successfully appended or Response
   *     .RESPONSE_LOG_MISMATCH if the previous log of "log" is not found.
   */
  private long appendEntries(
      long prevLogIndex, long prevLogTerm, long leaderCommit, List<Log> logs) {
    logger.debug(
        "{}, prevLogIndex={}, prevLogTerm={}, leaderCommit={}",
        name,
        prevLogIndex,
        prevLogTerm,
        leaderCommit);
    if (logs.isEmpty()) {
      return Response.RESPONSE_AGREE;
    }

    long resp = checkPrevLogIndex(prevLogIndex);
    if (resp != Response.RESPONSE_AGREE) {
      return resp;
    }

    synchronized (logManager) {
      long startTime = Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.getOperationStartTime();
      resp = logManager.maybeAppend(prevLogIndex, prevLogTerm, leaderCommit, logs);
      Timer.Statistic.RAFT_RECEIVER_APPEND_ENTRY.calOperationCostTimeFromStart(startTime);
      if (resp != -1) {
        if (logger.isDebugEnabled()) {
          logger.debug("{} append a new log list {}, commit to {}", name, logs, leaderCommit);
        }
        resp = Response.RESPONSE_AGREE;
      } else {
        // the incoming log points to an illegal position, reject it
        resp = Response.RESPONSE_LOG_MISMATCH;
      }
    }
    return resp;
  }

  /**
   * Check the term of the AppendEntryRequest. The term checked is the term of the leader, not the
   * term of the log. A new leader can still send logs of old leaders.
   *
   * @return -1 if the check is passed, >0 otherwise
   */
  private long checkRequestTerm(long leaderTerm, Node leader) {
    long localTerm;

    synchronized (term) {
      // if the request comes before the heartbeat arrives, the local term may be smaller than the
      // leader term
      localTerm = term.get();
      if (leaderTerm < localTerm) {
        logger.debug(
            "{} rejected the AppendEntriesRequest for term: {}/{}", name, leaderTerm, localTerm);
        return localTerm;
      } else {
        if (leaderTerm > localTerm) {
          stepDown(leaderTerm, true);
        } else {
          lastHeartbeatReceivedTime = System.currentTimeMillis();
        }
        setLeader(leader);
        if (character != NodeCharacter.FOLLOWER) {
          term.notifyAll();
        }
      }
    }
    logger.debug("{} accepted the AppendEntryRequest for term: {}", name, localTerm);
    return Response.RESPONSE_AGREE;
  }

  enum AppendLogResult {
    OK,
    TIME_OUT,
    LEADERSHIP_STALE
  }
}
