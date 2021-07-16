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

import org.apache.iotdb.cluster.client.DataClientProvider;
import org.apache.iotdb.cluster.client.async.AsyncClientPool;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaHeartbeatClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncClientPool;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaHeartbeatClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.ConfigInconsistentException;
import org.apache.iotdb.cluster.exception.EmptyIntervalException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.exception.StartUpCheckFailureException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.applier.MetaLogApplier;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.log.manage.MetaSingleSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.partition.NodeAdditionResult;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.query.ClusterPlanRouter;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RefreshReuqest;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.server.ClientServer;
import org.apache.iotdb.cluster.server.DataClusterServer;
import org.apache.iotdb.cluster.server.HardLinkCleaner;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendGroupEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.caller.NodeStatusHandler;
import org.apache.iotdb.cluster.server.heartbeat.DataHeartbeatServer;
import org.apache.iotdb.cluster.server.heartbeat.MetaHeartbeatThread;
import org.apache.iotdb.cluster.server.member.DataGroupMember.Factory;
import org.apache.iotdb.cluster.server.monitor.NodeReport;
import org.apache.iotdb.cluster.server.monitor.NodeReport.MetaMemberReport;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.cluster.utils.PartitionUtils.Intervals;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.cluster.utils.ClusterUtils.WAIT_START_UP_CHECK_TIME_SEC;
import static org.apache.iotdb.cluster.utils.ClusterUtils.analyseStartUpCheckResult;

@SuppressWarnings("java:S1135")
public class MetaGroupMember extends RaftMember {

  /** the file that contains the identifier of this node */
  static final String NODE_IDENTIFIER_FILE_NAME =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "node_identifier";
  /** the file that contains the serialized partition table */
  static final String PARTITION_FILE_NAME =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir() + File.separator + "partitions";
  /** in case of data loss, some file changes would be made to a temporary file first */
  private static final String TEMP_SUFFIX = ".tmp";

  private static final Logger logger = LoggerFactory.getLogger(MetaGroupMember.class);
  /**
   * when joining a cluster this node will retry at most "DEFAULT_JOIN_RETRY" times before returning
   * a failure to the client
   */
  private static final int DEFAULT_JOIN_RETRY = 10;

  /**
   * every "REPORT_INTERVAL_SEC" seconds, a reporter thread will print the status of all raft
   * members in this node
   */
  private static final int REPORT_INTERVAL_SEC = 10;

  /**
   * every "REFRESH_CLIENT_SEC" seconds, a dataClientRefresher thread will try to refresh one thrift
   * connection for each nodes other than itself.
   */
  private static final int REFRESH_CLIENT_SEC = 1;

  /** how many times is a data record replicated, also the number of nodes in a data group */
  private static final int REPLICATION_NUM =
      ClusterDescriptor.getInstance().getConfig().getReplicationNum();

  /**
   * during snapshot, hardlinks of data files are created to for downloading. hardlinks will be
   * checked every hour by default to see if they have expired, and will be cleaned if so.
   */
  private static final long CLEAN_HARDLINK_INTERVAL_SEC = 3600;

  /**
   * blind nodes are nodes that do not have the partition table, and if this node is the leader, the
   * partition table should be sent to them at the next heartbeat
   */
  private Set<Node> blindNodes = new HashSet<>();
  /**
   * as a leader, when a follower sent this node its identifier, the identifier may conflict with
   * other nodes', such conflicting nodes will be recorded and at the next heartbeat, and they will
   * be required to regenerate an identifier.
   */
  private Set<Node> idConflictNodes = new HashSet<>();
  /**
   * the identifier and its belonging node, for conflict detection, may be used in more places in
   * the future
   */
  private Map<Integer, Node> idNodeMap = null;

  /** nodes in the cluster and data partitioning */
  private PartitionTable partitionTable;
  /** router calculates the partition groups that a partitioned plan should be sent to */
  private ClusterPlanRouter router;
  /**
   * each node contains multiple DataGroupMembers and they are managed by a DataClusterServer acting
   * as a broker
   */
  private DataClusterServer dataClusterServer;

  /** each node starts a data heartbeat server to transfer heartbeat requests */
  private DataHeartbeatServer dataHeartbeatServer;

  /**
   * an override of TSServiceImpl, which redirect JDBC and Session requests to the MetaGroupMember
   * so they can be processed cluster-wide
   */
  private ClientServer clientServer;

  private DataClientProvider dataClientProvider;

  private ScheduledExecutorService dataClientRefresher;

  /**
   * a single thread pool, every "REPORT_INTERVAL_SEC" seconds, "reportThread" will print the status
   * of all raft members in this node
   */
  private ScheduledExecutorService reportThread;

  /**
   * containing configurations that should be kept the same cluster-wide, and must be checked before
   * establishing a cluster or joining a cluster.
   */
  private StartUpStatus startUpStatus;

  /**
   * localExecutor is used to directly execute plans like load configuration in the underlying IoTDB
   */
  private PlanExecutor localExecutor;

  /** hardLinkCleaner will periodically clean expired hardlinks created during snapshots */
  private ScheduledExecutorService hardLinkCleanerThread;

  private Coordinator coordinator;

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  public Coordinator getCoordinator() {
    return this.coordinator;
  }

  public ClusterPlanRouter getRouter() {
    return router;
  }

  @TestOnly
  public MetaGroupMember() {}

  public MetaGroupMember(TProtocolFactory factory, Node thisNode, Coordinator coordinator)
      throws QueryProcessException {
    super(
        "Meta",
        new AsyncClientPool(new AsyncMetaClient.FactoryAsync(factory)),
        new SyncClientPool(new SyncMetaClient.FactorySync(factory)),
        new AsyncClientPool(new AsyncMetaHeartbeatClient.FactoryAsync(factory)),
        new SyncClientPool(new SyncMetaHeartbeatClient.FactorySync(factory)));
    allNodes = new ArrayList<>();
    initPeerMap();

    dataClientProvider = new DataClientProvider(factory);

    // committed logs are applied to the state machine (the IoTDB instance) through the applier
    LogApplier metaLogApplier = new MetaLogApplier(this);
    logManager = new MetaSingleSnapshotLogManager(metaLogApplier, this);
    term.set(logManager.getHardState().getCurrentTerm());
    voteFor = logManager.getHardState().getVoteFor();

    setThisNode(thisNode);
    // load the identifier from the disk or generate a new one
    loadIdentifier();
    allNodes.add(thisNode);

    Factory dataMemberFactory = new Factory(factory, this);
    dataClusterServer = new DataClusterServer(thisNode, dataMemberFactory, this);
    dataHeartbeatServer = new DataHeartbeatServer(thisNode, dataClusterServer);
    clientServer = new ClientServer(this);
    startUpStatus = getNewStartUpStatus();

    // try loading the partition table if there was a previous cluster
    this.coordinator = coordinator;
    loadPartitionTable();
  }

  /**
   * Find the DataGroupMember that manages the partition of "storageGroupName"@"partitionId", and
   * close the partition through that member. Notice: only partitions owned by this node can be
   * closed by the method.
   *
   * @return true if the member is a leader and the partition is closed, false otherwise
   */
  public void closePartition(String storageGroupName, long partitionId, boolean isSeq) {
    Node header =
        partitionTable.routeToHeaderByTime(
            storageGroupName, partitionId * StorageEngine.getTimePartitionInterval());
    DataGroupMember localDataMember = getLocalDataMember(header);
    if (localDataMember == null || localDataMember.getCharacter() != NodeCharacter.LEADER) {
      return;
    }
    localDataMember.closePartition(storageGroupName, partitionId, isSeq);
  }

  public DataClusterServer getDataClusterServer() {
    return dataClusterServer;
  }

  public DataHeartbeatServer getDataHeartbeatServer() {
    return dataHeartbeatServer;
  }

  /**
   * Add seed nodes from the config, start the heartbeat and catch-up thread pool, initialize
   * QueryCoordinator and FileFlushPolicy, then start the reportThread. Calling the method twice
   * does not induce side effect.
   */
  @Override
  public void start() {
    if (heartBeatService != null) {
      return;
    }
    addSeedNodes();
    NodeStatusManager.getINSTANCE().setMetaGroupMember(this);
    super.start();
  }

  @Override
  void startBackGroundThreads() {
    super.startBackGroundThreads();
    reportThread =
        Executors.newSingleThreadScheduledExecutor(n -> new Thread(n, "NodeReportThread"));
    hardLinkCleanerThread =
        Executors.newSingleThreadScheduledExecutor(n -> new Thread(n, "HardLinkCleaner"));
    dataClientRefresher =
        Executors.newSingleThreadScheduledExecutor(n -> new Thread(n, "DataClientRefresher"));
  }

  /**
   * Stop the heartbeat and catch-up thread pool, DataClusterServer, ClientServer and reportThread.
   * Calling the method twice does not induce side effects.
   */
  @Override
  public void stop() {
    super.stop();
    if (getDataClusterServer() != null) {
      getDataClusterServer().stop();
    }
    if (getDataHeartbeatServer() != null) {
      getDataHeartbeatServer().stop();
    }
    if (clientServer != null) {
      clientServer.stop();
    }
    if (dataClientRefresher != null) {
      dataClientRefresher.shutdownNow();
      try {
        dataClientRefresher.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for reportThread to end", e);
      }
    }
    if (reportThread != null) {
      reportThread.shutdownNow();
      try {
        reportThread.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for reportThread to end", e);
      }
    }
    if (hardLinkCleanerThread != null) {
      hardLinkCleanerThread.shutdownNow();
      try {
        hardLinkCleanerThread.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for hardlinkCleaner to end", e);
      }
    }

    logger.info("{}: stopped", name);
  }

  /**
   * Start DataClusterServer and ClientServer so this node will be able to respond to other nodes
   * and clients.
   */
  protected void initSubServers() throws TTransportException, StartupException {
    getDataClusterServer().start();
    getDataHeartbeatServer().start();
    clientServer.setCoordinator(this.coordinator);
    clientServer.start();
  }

  /**
   * Parse the seed nodes from the cluster configuration and add them into the node list. Each
   * seedUrl should be like "{hostName}:{metaPort}:{dataPort}:{clientPort}" Ignore bad-formatted
   * seedUrls.
   */
  protected void addSeedNodes() {
    if (allNodes.size() > 1) {
      // a local partition table was loaded and allNodes were updated, there is no need to add
      // nodes from seedUrls
      return;
    }
    List<String> seedUrls = config.getSeedNodeUrls();
    // initialize allNodes
    for (String seedUrl : seedUrls) {
      Node node = ClusterUtils.parseNode(seedUrl);
      if (node != null
          && (!node.getInternalIp().equals(thisNode.internalIp)
              || node.getMetaPort() != thisNode.getMetaPort())
          && !allNodes.contains(node)) {
        // do not add the local node since it is added in the constructor
        allNodes.add(node);
      }
    }
  }

  /**
   * Apply the addition of a new node. Register its identifier, add it to the node list and
   * partition table, serialize the partition table and update the DataGroupMembers.
   */
  public void applyAddNode(Node newNode) {
    synchronized (allNodes) {
      if (!allNodes.contains(newNode)) {
        logger.debug("Adding a new node {} into {}", newNode, allNodes);
        registerNodeIdentifier(newNode, newNode.getNodeIdentifier());
        allNodes.add(newNode);

        // update the partition table
        NodeAdditionResult result = partitionTable.addNode(newNode);
        ((SlotPartitionTable) partitionTable).setLastLogIndex(logManager.getLastLogIndex());
        savePartitionTable();

        // update local data members
        getDataClusterServer().addNode(newNode, result);
      }
    }
  }

  /**
   * This node itself is a seed node, and it is going to build the initial cluster with other seed
   * nodes. This method is to skip one-by-one additions to establish a large cluster quickly.
   */
  public void buildCluster() throws ConfigInconsistentException, StartUpCheckFailureException {
    // see if the seed nodes have consistent configurations
    checkSeedNodesStatus();
    // just establish the heartbeat thread and it will do the remaining
    threadTaskInit();
    if (allNodes.size() == 1) {
      // if there is only one node in the cluster, no heartbeat will be received, and
      // consequently data group will not be built, so we directly build data members here
      if (partitionTable == null) {
        partitionTable = new SlotPartitionTable(allNodes, thisNode);
        logger.info("Partition table is set up");
      }
      router = new ClusterPlanRouter(partitionTable);
      this.coordinator.setRouter(router);
      startSubServers();
    }
  }

  private void threadTaskInit() {
    heartBeatService.submit(new MetaHeartbeatThread(this));
    reportThread.scheduleAtFixedRate(
        this::generateNodeReport, REPORT_INTERVAL_SEC, REPORT_INTERVAL_SEC, TimeUnit.SECONDS);
    hardLinkCleanerThread.scheduleAtFixedRate(
        new HardLinkCleaner(),
        CLEAN_HARDLINK_INTERVAL_SEC,
        CLEAN_HARDLINK_INTERVAL_SEC,
        TimeUnit.SECONDS);
    dataClientRefresher.scheduleAtFixedRate(
        this::refreshClientOnce, REFRESH_CLIENT_SEC, REFRESH_CLIENT_SEC, TimeUnit.SECONDS);
  }

  private void refreshClientOnce() {
    for (Node receiver : allNodes) {
      if (!receiver.equals(thisNode)) {
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          refreshClientOnceAsync(receiver);
        } else {
          refreshClientOnceSync(receiver);
        }
      }
    }
  }

  private void refreshClientOnceSync(Node receiver) {
    RaftService.Client client = null;
    try {
      client =
          getClientProvider()
              .getSyncDataClientForRefresh(receiver, RaftServer.getWriteOperationTimeoutMS());
      RefreshReuqest req = new RefreshReuqest();
      client.refreshConnection(req);
    } catch (IOException ignored) {
    } catch (TException e) {
      logger.info("encounter refreshing client timeout, throw broken connection", e);
      // the connection may be broken, close it to avoid it being reused
      client.getInputProtocol().getTransport().close();
    } finally {
      if (client != null) {
        ClientUtils.putBackSyncClient(client);
      }
    }
  }

  private void refreshClientOnceAsync(Node receiver) {
    RaftService.AsyncClient client;
    try {
      client =
          getClientProvider()
              .getAsyncDataClientForRefresh(receiver, RaftServer.getWriteOperationTimeoutMS());
    } catch (IOException e) {
      return;
    }
    try {
      client.refreshConnection(new RefreshReuqest(), new GenericHandler<>(receiver, null));
    } catch (TException e) {
      logger.info("encounter refreshing client timeout, throw broken connection", e);
    }
  }

  private void generateNodeReport() {
    try {
      if (logger.isInfoEnabled()) {
        NodeReport report = genNodeReport();
        logger.info(report.toString());
      }
    } catch (Exception e) {
      logger.error("{} exception occurred when generating node report", name, e);
    }
  }

  /**
   * This node is not a seed node and wants to join an established cluster. Pick up a node randomly
   * from the seed nodes and send a join request to it.
   *
   * @return true if the node has successfully joined the cluster, false otherwise.
   */
  public void joinCluster() throws ConfigInconsistentException, StartUpCheckFailureException {
    if (allNodes.size() == 1) {
      logger.error("Seed nodes not provided, cannot join cluster");
      throw new ConfigInconsistentException();
    }

    int retry = DEFAULT_JOIN_RETRY;
    while (retry > 0) {
      // randomly pick up a node to try
      Node node = allNodes.get(random.nextInt(allNodes.size()));
      if (node.equals(thisNode)) {
        continue;
      }
      logger.info("start joining the cluster with the help of {}", node);
      try {
        if (joinCluster(node, startUpStatus)) {
          logger.info("Joined a cluster, starting the heartbeat thread");
          setCharacter(NodeCharacter.FOLLOWER);
          setLastHeartbeatReceivedTime(System.currentTimeMillis());
          threadTaskInit();
          return;
        }
        // wait 5s to start the next try
        Thread.sleep(ClusterDescriptor.getInstance().getConfig().getJoinClusterTimeOutMs());
      } catch (TException e) {
        logger.warn("Cannot join the cluster from {}, because:", node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to join a cluster", e);
      }
      // start the next try
      retry--;
    }
    // all tries failed
    logger.error("Cannot join the cluster after {} retries", DEFAULT_JOIN_RETRY);
    throw new StartUpCheckFailureException();
  }

  public StartUpStatus getNewStartUpStatus() {
    StartUpStatus newStartUpStatus = new StartUpStatus();
    newStartUpStatus.setPartitionInterval(
        IoTDBDescriptor.getInstance().getConfig().getPartitionInterval());
    newStartUpStatus.setHashSalt(ClusterConstant.HASH_SALT);
    newStartUpStatus.setReplicationNumber(
        ClusterDescriptor.getInstance().getConfig().getReplicationNum());
    newStartUpStatus.setClusterName(ClusterDescriptor.getInstance().getConfig().getClusterName());
    List<String> seedUrls = ClusterDescriptor.getInstance().getConfig().getSeedNodeUrls();
    List<Node> seedNodeList = new ArrayList<>();
    for (String seedUrl : seedUrls) {
      seedNodeList.add(ClusterUtils.parseNode(seedUrl));
    }
    newStartUpStatus.setSeedNodeList(seedNodeList);
    return newStartUpStatus;
  }

  /**
   * Send a join cluster request to "node". If the joining is accepted, set the partition table,
   * start DataClusterServer and ClientServer and initialize DataGroupMembers.
   *
   * @return rue if the node has successfully joined the cluster, false otherwise.
   */
  private boolean joinCluster(Node node, StartUpStatus startUpStatus)
      throws TException, InterruptedException, ConfigInconsistentException {

    AddNodeResponse resp;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncMetaClient client = (AsyncMetaClient) getAsyncClient(node);
      if (client == null) {
        return false;
      }
      resp = SyncClientAdaptor.addNode(client, thisNode, startUpStatus);
    } else {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(node);
      if (client == null) {
        return false;
      }
      try {
        resp = client.addNode(thisNode, startUpStatus);
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        throw e;
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
    }

    if (resp == null) {
      logger.warn("Join cluster request timed out");
    } else if (resp.getRespNum() == Response.RESPONSE_AGREE) {
      logger.info("Node {} admitted this node into the cluster", node);
      ByteBuffer partitionTableBuffer = resp.partitionTableBytes;
      acceptPartitionTable(partitionTableBuffer, true);
      getDataClusterServer().pullSnapshots();
      return true;
    } else if (resp.getRespNum() == Response.RESPONSE_IDENTIFIER_CONFLICT) {
      logger.info(
          "The identifier {} conflicts the existing ones, regenerate a new one",
          thisNode.getNodeIdentifier());
      setNodeIdentifier(genNodeIdentifier());
    } else if (resp.getRespNum() == Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT) {
      handleConfigInconsistency(resp);
    } else {
      logger.warn("Joining the cluster is rejected by {} for response {}", node, resp.getRespNum());
    }
    return false;
  }

  private void handleConfigInconsistency(AddNodeResponse resp) throws ConfigInconsistentException {
    if (logger.isErrorEnabled()) {
      CheckStatusResponse checkStatusResponse = resp.getCheckStatusResponse();
      String parameters =
          (checkStatusResponse.isPartitionalIntervalEquals() ? "" : ", partition interval")
              + (checkStatusResponse.isHashSaltEquals() ? "" : ", hash salt")
              + (checkStatusResponse.isReplicationNumEquals() ? "" : ", replication number")
              + (checkStatusResponse.isSeedNodeEquals() ? "" : ", seedNodes")
              + (checkStatusResponse.isClusterNameEquals() ? "" : ", clusterName");
      logger.error(
          "The start up configuration{} conflicts the cluster. Please reset the configurations. ",
          parameters.substring(1));
    }
    throw new ConfigInconsistentException();
  }

  /**
   * Process the heartbeat request from a valid leader. Generate and tell the leader the identifier
   * of the node if necessary. If the partition table is missing, use the one from the request or
   * require it in the response.
   */
  @Override
  void processValidHeartbeatReq(HeartBeatRequest request, HeartBeatResponse response) {
    if (request.isRequireIdentifier()) {
      // the leader wants to know who the node is
      if (request.isRegenerateIdentifier()) {
        // the previously sent id conflicted, generate a new one
        setNodeIdentifier(genNodeIdentifier());
      }
      logger.debug("Send identifier {} to the leader", thisNode.getNodeIdentifier());
      response.setFollowerIdentifier(thisNode.getNodeIdentifier());
    }

    if (partitionTable == null) {
      // this node does not have a partition table yet
      if (request.isSetPartitionTableBytes()) {
        synchronized (this) {
          // if the leader has sent the partition table then accept it
          if (partitionTable == null) {
            ByteBuffer byteBuffer = request.partitionTableBytes;
            acceptPartitionTable(byteBuffer, true);
          }
        }
      } else {
        // require the partition table
        logger.debug("Request cluster nodes from the leader");
        response.setRequirePartitionTable(true);
      }
    }
  }

  /**
   * Deserialize a partition table from the buffer, save it locally, add nodes from the partition
   * table and start DataClusterServer and ClientServer.
   */
  public synchronized void acceptPartitionTable(
      ByteBuffer partitionTableBuffer, boolean needSerialization) {
    SlotPartitionTable newTable = new SlotPartitionTable(thisNode);
    newTable.deserialize(partitionTableBuffer);
    // avoid overwriting current partition table with a previous one
    if (partitionTable != null) {
      long currIndex = ((SlotPartitionTable) partitionTable).getLastLogIndex();
      long incomingIndex = newTable.getLastLogIndex();
      logger.info(
          "Current partition table index {}, new partition table index {}",
          currIndex,
          incomingIndex);
      if (currIndex >= incomingIndex) {
        return;
      }
    }
    partitionTable = newTable;

    if (needSerialization) {
      // if the partition table is read locally, there is no need to serialize it again
      savePartitionTable();
    }

    router = new ClusterPlanRouter(newTable);
    this.coordinator.setRouter(router);

    updateNodeList(newTable.getAllNodes());

    startSubServers();
  }

  private void updateNodeList(Collection<Node> nodes) {
    allNodes = new ArrayList<>(nodes);
    initPeerMap();
    logger.info("All nodes in the partition table: {}", allNodes);
    initIdNodeMap();
    for (Node n : allNodes) {
      idNodeMap.put(n.getNodeIdentifier(), n);
    }
  }

  /**
   * Process a HeartBeatResponse from a follower. If the follower has provided its identifier, try
   * registering for it and if all nodes have registered and there is no available partition table,
   * initialize a new one and start the ClientServer and DataClusterServer. If the follower requires
   * a partition table, add it to the blind node list so that at the next heartbeat this node will
   * send it a partition table
   */
  @Override
  public void processValidHeartbeatResp(HeartBeatResponse response, Node receiver) {
    // register the id of the node
    if (response.isSetFollowerIdentifier()) {
      // register the follower, the response.getFollower() contains the node information of the
      // receiver.
      registerNodeIdentifier(response.getFollower(), response.getFollowerIdentifier());
      // if all nodes' ids are known, we can build the partition table
      if (allNodesIdKnown()) {
        // When the meta raft group is established, the follower reports its node information to the
        // leader through the first heartbeat. After the leader knows the node information of all
        // nodes, it can replace the incomplete node information previously saved locally, and build
        // partitionTable to send it to other followers.
        allNodes = new ArrayList<>(idNodeMap.values());
        if (partitionTable == null) {
          partitionTable = new SlotPartitionTable(allNodes, thisNode);
          logger.info("Partition table is set up");
        }
        router = new ClusterPlanRouter(partitionTable);
        this.coordinator.setRouter(router);
        startSubServers();
      }
    }
    // record the requirement of partition table of the follower
    if (response.isRequirePartitionTable()) {
      addBlindNode(receiver);
    }
  }

  /**
   * When a node requires a partition table in its heartbeat response, add it into blindNodes so in
   * the next heartbeat the partition table will be sent to the node.
   */
  private void addBlindNode(Node node) {
    logger.debug("Node {} requires the node list", node);
    blindNodes.add(node);
  }

  /** @return whether a node wants the partition table. */
  public boolean isNodeBlind(Node node) {
    return blindNodes.contains(node);
  }

  /**
   * Remove the node from the blindNodes when the partition table is sent, so partition table will
   * not be sent in each heartbeat.
   */
  public void removeBlindNode(Node node) {
    blindNodes.remove(node);
  }

  /** Register the identifier for the node if it does not conflict with other nodes. */
  private void registerNodeIdentifier(Node node, int identifier) {
    synchronized (idNodeMap) {
      Node conflictNode = idNodeMap.get(identifier);
      if (conflictNode != null && !conflictNode.equals(node)) {
        idConflictNodes.add(node);
        return;
      }
      node.setNodeIdentifier(identifier);
      logger.info("Node {} registered with id {}", node, identifier);
      idNodeMap.put(identifier, node);
      idConflictNodes.remove(node);
    }
  }

  /**
   * idNodeMap is initialized when the first leader wins or the follower receives the partition
   * table from the leader or a node recovers
   */
  private void initIdNodeMap() {
    idNodeMap = new HashMap<>();
    idNodeMap.put(thisNode.getNodeIdentifier(), thisNode);
  }

  /** @return Whether all nodes' identifier is known. */
  private boolean allNodesIdKnown() {
    return idNodeMap != null && idNodeMap.size() == allNodes.size();
  }

  /**
   * Start the DataClusterServer and ClientServer so this node can serve other nodes and clients.
   * Also build DataGroupMembers using the partition table.
   */
  protected synchronized void startSubServers() {
    logger.info("Starting sub-servers...");
    synchronized (partitionTable) {
      try {
        getDataClusterServer().buildDataGroupMembers(partitionTable);
        initSubServers();
        sendHandshake();
      } catch (TTransportException | StartupException e) {
        logger.error("Build partition table failed: ", e);
        stop();
        return;
      }
    }
    logger.info("Sub-servers started.");
  }

  /** When the node restarts, it sends handshakes to all other nodes so they may know it is back. */
  private void sendHandshake() {
    for (Node node : allNodes) {
      try {
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          AsyncMetaClient asyncClient = (AsyncMetaClient) getAsyncClient(node);
          if (asyncClient != null) {
            asyncClient.handshake(thisNode, new GenericHandler<>(node, null));
          }
        } else {
          SyncMetaClient syncClient = (SyncMetaClient) getSyncClient(node);
          if (syncClient != null) {
            syncClient.handshake(thisNode);
          }
        }
      } catch (TException e) {
        // ignore handshake exceptions
      }
    }
  }

  /**
   * Process the join cluster request of "node". Only proceed when the partition table is ready.
   *
   * @param node cannot be the local node
   */
  public AddNodeResponse addNode(Node node, StartUpStatus startUpStatus)
      throws AddSelfException, LogExecutionException {
    AddNodeResponse response = new AddNodeResponse();
    if (partitionTable == null) {
      logger.info("Cannot add node now because the partition table is not set");
      response.setRespNum((int) Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE);
      return response;
    }

    logger.info("A node {} wants to join this cluster", node);
    if (node.equals(thisNode)) {
      throw new AddSelfException();
    }

    waitLeader();
    // try to process the request locally
    if (processAddNodeLocally(node, startUpStatus, response)) {
      return response;
    }
    // if it cannot be processed locally, forward it
    return null;
  }

  /**
   * Process the join cluster request of "node" as a MetaLeader. A node already joined is accepted
   * immediately. If the identifier of "node" conflicts with an existing node, the request will be
   * turned down.
   *
   * @param node cannot be the local node
   * @param startUpStatus the start up status of the new node
   * @param response the response that will be sent to "node"
   * @return true if the process is over, false if the request should be forwarded
   */
  private boolean processAddNodeLocally(
      Node node, StartUpStatus startUpStatus, AddNodeResponse response)
      throws LogExecutionException {
    if (character != NodeCharacter.LEADER) {
      return false;
    }
    if (allNodes.contains(node)) {
      logger.debug("Node {} is already in the cluster", node);
      response.setRespNum((int) Response.RESPONSE_AGREE);
      synchronized (partitionTable) {
        response.setPartitionTableBytes(partitionTable.serialize());
      }
      return true;
    }

    Node idConflictNode = idNodeMap.get(node.getNodeIdentifier());
    if (idConflictNode != null) {
      logger.debug("{}'s id conflicts with {}", node, idConflictNode);
      response.setRespNum((int) Response.RESPONSE_IDENTIFIER_CONFLICT);
      return true;
    }

    // check status of the new node
    if (!checkNodeConfig(startUpStatus, response)) {
      return true;
    }

    // node adding is serialized to reduce potential concurrency problem
    synchronized (logManager) {
      AddNodeLog addNodeLog = new AddNodeLog();
      addNodeLog.setCurrLogTerm(getTerm().get());
      addNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      addNodeLog.setNewNode(node);

      logManager.append(addNodeLog);

      int retryTime = 1;
      while (true) {
        logger.info("Send the join request of {} to other nodes, retry time: {}", node, retryTime);
        AppendLogResult result = sendLogToAllGroups(addNodeLog);
        switch (result) {
          case OK:
            logger.info("Join request of {} is accepted", node);
            commitLog(addNodeLog);

            synchronized (partitionTable) {
              response.setPartitionTableBytes(partitionTable.serialize());
            }
            response.setRespNum((int) Response.RESPONSE_AGREE);
            logger.info("Sending join response of {}", node);
            return true;
          case TIME_OUT:
            logger.info("Join request of {} timed out", node);
            retryTime++;
            continue;
          case LEADERSHIP_STALE:
          default:
            return false;
        }
      }
    }
  }

  private boolean checkNodeConfig(StartUpStatus remoteStartUpStatus, AddNodeResponse response) {
    long remotePartitionInterval = remoteStartUpStatus.getPartitionInterval();
    int remoteHashSalt = remoteStartUpStatus.getHashSalt();
    int remoteReplicationNum = remoteStartUpStatus.getReplicationNumber();
    String remoteClusterName = remoteStartUpStatus.getClusterName();
    List<Node> remoteSeedNodeList = remoteStartUpStatus.getSeedNodeList();
    long localPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    int localHashSalt = ClusterConstant.HASH_SALT;
    int localReplicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    String localClusterName = ClusterDescriptor.getInstance().getConfig().getClusterName();
    boolean partitionIntervalEquals = true;
    boolean hashSaltEquals = true;
    boolean replicationNumEquals = true;
    boolean seedNodeEquals = true;
    boolean clusterNameEquals = true;

    if (localPartitionInterval != remotePartitionInterval) {
      partitionIntervalEquals = false;
      logger.info(
          "Remote partition interval conflicts with the leader's. Leader: {}, remote: {}",
          localPartitionInterval,
          remotePartitionInterval);
    }
    if (localHashSalt != remoteHashSalt) {
      hashSaltEquals = false;
      logger.info(
          "Remote hash salt conflicts with the leader's. Leader: {}, remote: {}",
          localHashSalt,
          remoteHashSalt);
    }
    if (localReplicationNum != remoteReplicationNum) {
      replicationNumEquals = false;
      logger.info(
          "Remote replication number conflicts with the leader's. Leader: {}, remote: {}",
          localReplicationNum,
          remoteReplicationNum);
    }
    if (!Objects.equals(localClusterName, remoteClusterName)) {
      clusterNameEquals = false;
      logger.info(
          "Remote cluster name conflicts with the leader's. Leader: {}, remote: {}",
          localClusterName,
          remoteClusterName);
    }
    if (!ClusterUtils.checkSeedNodes(true, allNodes, remoteSeedNodeList)) {
      seedNodeEquals = false;
      if (logger.isInfoEnabled()) {
        logger.info(
            "Remote seed node list conflicts with the leader's. Leader: {}, remote: {}",
            Arrays.toString(allNodes.toArray(new Node[0])),
            remoteSeedNodeList);
      }
    }
    if (!(partitionIntervalEquals
        && hashSaltEquals
        && replicationNumEquals
        && seedNodeEquals
        && clusterNameEquals)) {
      response.setRespNum((int) Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT);
      response.setCheckStatusResponse(
          new CheckStatusResponse(
              partitionIntervalEquals,
              hashSaltEquals,
              replicationNumEquals,
              seedNodeEquals,
              clusterNameEquals));
      return false;
    }
    return true;
  }

  /**
   * Check if the seed nodes are consistent with other nodes. Only used when establishing the
   * initial cluster.
   */
  private void checkSeedNodesStatus()
      throws ConfigInconsistentException, StartUpCheckFailureException {
    if (getAllNodes().size() == 1) {
      // one-node cluster, skip the check
      return;
    }

    boolean canEstablishCluster = false;
    long startTime = System.currentTimeMillis();
    // the initial 1 represents this node
    AtomicInteger consistentNum = new AtomicInteger(1);
    AtomicInteger inconsistentNum = new AtomicInteger(0);
    while (!canEstablishCluster) {
      consistentNum.set(1);
      inconsistentNum.set(0);
      checkSeedNodesStatusOnce(consistentNum, inconsistentNum);
      logger.debug(
          "Status check result: {}-{}/{}",
          consistentNum.get(),
          inconsistentNum.get(),
          getAllNodes().size());
      canEstablishCluster =
          analyseStartUpCheckResult(
              consistentNum.get(), inconsistentNum.get(), getAllNodes().size());
      // If reach the start up time threshold, shut down.
      // Otherwise, wait for a while, start the loop again.
      if (System.currentTimeMillis() - startTime > ClusterUtils.START_UP_TIME_THRESHOLD_MS) {
        throw new StartUpCheckFailureException();
      } else if (!canEstablishCluster) {
        try {
          Thread.sleep(ClusterUtils.START_UP_CHECK_TIME_INTERVAL_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Unexpected interruption when waiting for next start up check", e);
        }
      }
    }
  }

  private void checkSeedNodesStatusOnce(
      AtomicInteger consistentNum, AtomicInteger inconsistentNum) {
    // use a thread pool to avoid being blocked by an unavailable node
    ExecutorService pool = new ScheduledThreadPoolExecutor(getAllNodes().size() - 1);
    for (Node seedNode : getAllNodes()) {
      Node thisNode = getThisNode();
      if (seedNode.equals(thisNode)) {
        continue;
      }
      pool.submit(
          () -> {
            logger.debug("Checking status with {}", seedNode);
            CheckStatusResponse response = null;
            try {
              response = checkStatus(seedNode);
            } catch (Exception e) {
              logger.warn("Exception during status check", e);
            }
            logger.debug("CheckStatusResponse from {}: {}", seedNode, response);
            if (response != null) {
              // check the response
              ClusterUtils.examineCheckStatusResponse(
                  response, consistentNum, inconsistentNum, seedNode);
            } else {
              logger.warn(
                  "Start up exception. Cannot connect to node {}. Try again in next turn.",
                  seedNode);
            }
          });
    }
    pool.shutdown();
    try {
      if (!pool.awaitTermination(WAIT_START_UP_CHECK_TIME_SEC, TimeUnit.SECONDS)) {
        pool.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for start up checks", e);
    }
  }

  private CheckStatusResponse checkStatus(Node seedNode) {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncMetaClient client = (AsyncMetaClient) getAsyncClient(seedNode, false);
      if (client == null) {
        return null;
      }
      try {
        return SyncClientAdaptor.checkStatus(client, getStartUpStatus());
      } catch (TException e) {
        logger.warn("Error occurs when check status on node : {}", seedNode);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Current thread is interrupted.");
      }
    } else {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(seedNode, false);
      if (client == null) {
        return null;
      }
      try {
        return client.checkStatus(getStartUpStatus());
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        logger.warn("Error occurs when check status on node : {}", seedNode);
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
    }
    return null;
  }

  /**
   * Send the log the all data groups and return a success only when each group's quorum has
   * accepted this log.
   */
  private AppendLogResult sendLogToAllGroups(Log log) {
    List<Node> nodeRing = partitionTable.getAllNodes();

    AtomicLong newLeaderTerm = new AtomicLong(term.get());
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AppendEntryRequest request = buildAppendEntryRequest(log, true);

    // ask for votes from each node
    int[] groupRemainings = askGroupVotes(nodeRing, request, leaderShipStale, log, newLeaderTerm);

    if (!leaderShipStale.get()) {
      // if all quorums of all groups have received this log, it is considered succeeded.
      for (int remaining : groupRemainings) {
        if (remaining > 0) {
          return AppendLogResult.TIME_OUT;
        }
      }
    } else {
      return AppendLogResult.LEADERSHIP_STALE;
    }

    return AppendLogResult.OK;
  }

  /**
   * Send "request" to each node in "nodeRing" and when a node returns a success, decrease all
   * counters of the groups it is in of "groupRemainings"
   *
   * @return a int array indicating how many votes are left in each group to make an agreement
   */
  @SuppressWarnings({"java:S2445", "java:S2274"})
  // groupRemaining is shared with the handlers,
  // and we do not wait infinitely to enable timeouts
  private int[] askGroupVotes(
      List<Node> nodeRing,
      AppendEntryRequest request,
      AtomicBoolean leaderShipStale,
      Log log,
      AtomicLong newLeaderTerm) {
    // each node will be the header of a group, we use the node to represent the group
    int nodeSize = nodeRing.size();
    // the decreasing counters of how many nodes in a group has received the log, each time a
    // node receive the log, the counters of all groups it is in will decrease by 1
    int[] groupRemainings = new int[nodeSize];
    // a group is considered successfully received the log if such members receive the log
    int groupQuorum = REPLICATION_NUM / 2 + 1;
    Arrays.fill(groupRemainings, groupQuorum);

    synchronized (groupRemainings) {
      // ask a vote from every node
      for (int i = 0; i < nodeSize; i++) {
        Node node = nodeRing.get(i);
        if (node.equals(thisNode)) {
          // this node automatically gives an agreement, decrease counters of all groups the local
          // node is in
          for (int j = 0; j < REPLICATION_NUM; j++) {
            int groupIndex = i - j;
            if (groupIndex < 0) {
              groupIndex += groupRemainings.length;
            }
            groupRemainings[groupIndex]--;
          }
        } else {
          askRemoteGroupVote(
              node, groupRemainings, i, leaderShipStale, log, newLeaderTerm, request);
        }
      }

      try {
        groupRemainings.wait(RaftServer.getWriteOperationTimeoutMS());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for the group votes", e);
      }
    }
    return groupRemainings;
  }

  private void askRemoteGroupVote(
      Node node,
      int[] groupRemainings,
      int nodeIndex,
      AtomicBoolean leaderShipStale,
      Log log,
      AtomicLong newLeaderTerm,
      AppendEntryRequest request) {
    AppendGroupEntryHandler handler =
        new AppendGroupEntryHandler(
            groupRemainings, nodeIndex, node, leaderShipStale, log, newLeaderTerm, this);
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncMetaClient client = (AsyncMetaClient) getAsyncClient(node);
      try {
        if (client != null) {
          client.appendEntry(request, handler);
        }
      } catch (TException e) {
        logger.error("Cannot send log to node {}", node, e);
      }
    } else {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(node);
      if (client == null) {
        logger.error("No available client for {}", node);
        return;
      }
      getSerialToParallelPool()
          .submit(
              () -> {
                try {
                  handler.onComplete(client.appendEntry(request));
                } catch (TException e) {
                  client.getInputProtocol().getTransport().close();
                  handler.onError(e);
                } finally {
                  ClientUtils.putBackSyncClient(client);
                }
              });
    }
  }

  public Set<Node> getIdConflictNodes() {
    return idConflictNodes;
  }

  /**
   * When this node becomes the MetaLeader (for the first time), it should init the idNodeMap, so
   * that if can require identifiers from all nodes and check if there are conflicts.
   */
  @Override
  public void onElectionWins() {
    if (idNodeMap == null) {
      initIdNodeMap();
    }
  }

  /** Load the partition table from a local file if it can be found. */
  private void loadPartitionTable() {
    File partitionFile = new File(PARTITION_FILE_NAME);
    if (!partitionFile.exists() && !recoverPartitionTableFile()) {
      logger.info("No partition table file found");
      return;
    }
    initIdNodeMap();
    try (DataInputStream inputStream =
        new DataInputStream(new BufferedInputStream(new FileInputStream(partitionFile)))) {
      int size = inputStream.readInt();
      byte[] tableBuffer = new byte[size];
      int readCnt = inputStream.read(tableBuffer);
      if (readCnt < size) {
        throw new IOException(
            String.format("Expected partition table size: %s, actual read: %s", size, readCnt));
      }

      ByteBuffer wrap = ByteBuffer.wrap(tableBuffer);
      acceptPartitionTable(wrap, false);

      logger.info("Load {} nodes: {}", allNodes.size(), allNodes);
    } catch (IOException e) {
      logger.error("Cannot load the partition table", e);
    }
  }

  private boolean recoverPartitionTableFile() {
    File tempFile = new File(PARTITION_FILE_NAME + TEMP_SUFFIX);
    if (!tempFile.exists()) {
      return false;
    }
    File partitionFile = new File(PARTITION_FILE_NAME);
    return tempFile.renameTo(partitionFile);
  }

  /**
   * Serialize the partition table to a fixed position on the disk. Will first serialize to a
   * temporary file and than replace the old file.
   */
  private synchronized void savePartitionTable() {
    File tempFile = new File(PARTITION_FILE_NAME + TEMP_SUFFIX);
    tempFile.getParentFile().mkdirs();
    File oldFile = new File(PARTITION_FILE_NAME);
    try (DataOutputStream outputStream =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))) {
      synchronized (partitionTable) {
        byte[] tableBuffer = partitionTable.serialize().array();
        outputStream.writeInt(tableBuffer.length);
        outputStream.write(tableBuffer);
        outputStream.flush();
      }
    } catch (IOException e) {
      logger.error("Cannot save the partition table", e);
    }
    if (oldFile.exists()) {
      try {
        Files.delete(Paths.get(oldFile.getAbsolutePath()));
      } catch (IOException e) {
        logger.warn("Old partition table file is not successfully deleted", e);
      }
    }

    if (!tempFile.renameTo(oldFile)) {
      logger.warn("New partition table file is not successfully renamed");
    }
    logger.info("Partition table is saved");
  }

  /**
   * Load the identifier from the disk, if the identifier file does not exist, a new identifier will
   * be generated. Do nothing if the identifier is already set.
   */
  private void loadIdentifier() {
    if (thisNode.isSetNodeIdentifier()) {
      return;
    }
    File file = new File(NODE_IDENTIFIER_FILE_NAME);
    Integer nodeId = null;
    if (file.exists()) {
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        nodeId = Integer.parseInt(reader.readLine());
        logger.info("Recovered node identifier {}", nodeId);
      } catch (Exception e) {
        logger.warn("Cannot read the identifier from file, generating a new one", e);
      }
    }
    if (nodeId != null) {
      setNodeIdentifier(nodeId);
      return;
    }

    setNodeIdentifier(genNodeIdentifier());
  }

  /**
   * Generate a new identifier using the hash of IP, metaPort and sysTime.
   *
   * @return a new identifier
   */
  private int genNodeIdentifier() {
    return Objects.hash(
        thisNode.getInternalIp(), thisNode.getMetaPort(), System.currentTimeMillis());
  }

  /** Set the node's identifier to "identifier", also save it to a local file in text format. */
  private void setNodeIdentifier(int identifier) {
    logger.info("The identifier of this node has been set to {}", identifier);
    thisNode.setNodeIdentifier(identifier);
    File idFile = new File(NODE_IDENTIFIER_FILE_NAME);
    idFile.getParentFile().mkdirs();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(idFile))) {
      writer.write(String.valueOf(identifier));
    } catch (IOException e) {
      logger.error("Cannot save the node identifier", e);
    }
  }

  public PartitionTable getPartitionTable() {
    return partitionTable;
  }

  /**
   * Process a snapshot sent by the MetaLeader. Deserialize the snapshot and apply it. The type of
   * the snapshot should be MetaSimpleSnapshot.
   */
  public void receiveSnapshot(SendSnapshotRequest request) throws SnapshotInstallationException {
    MetaSimpleSnapshot snapshot = new MetaSimpleSnapshot();
    snapshot.deserialize(request.snapshotBytes);
    snapshot.getDefaultInstaller(this).install(snapshot, -1);
  }

  /**
   * Execute a non-query plan. According to the type of the plan, the plan will be executed on all
   * nodes (like timeseries deletion) or the nodes that belong to certain groups (like data
   * ingestion).
   *
   * @param plan a non-query plan.
   */
  @Override
  public TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    TSStatus result;
    long startTime = Timer.Statistic.META_GROUP_MEMBER_EXECUTE_NON_QUERY.getOperationStartTime();
    if (PartitionUtils.isGlobalMetaPlan(plan)) {
      // do it in local, only the follower forward the plan to local
      logger.debug("receive a global meta plan {}", plan);
      result = processNonPartitionedMetaPlan(plan);
    } else {
      // do nothing
      logger.warn("receive a plan {} could not be processed in local", plan);
      result = StatusUtils.UNSUPPORTED_OPERATION;
    }
    Timer.Statistic.META_GROUP_MEMBER_EXECUTE_NON_QUERY.calOperationCostTimeFromStart(startTime);
    return result;
  }

  /**
   * A non-partitioned plan (like storage group creation) should be executed on all metagroup nodes,
   * so the MetaLeader should take the responsible to make sure that every node receives the plan.
   * Thus the plan will be processed locally only by the MetaLeader and forwarded by non-leader
   * nodes.
   */
  public TSStatus processNonPartitionedMetaPlan(PhysicalPlan plan) {
    if (character == NodeCharacter.LEADER) {
      TSStatus status = processPlanLocally(plan);
      if (status != null) {
        return status;
      }
    } else if (!ClusterConstant.EMPTY_NODE.equals(leader.get())) {
      TSStatus result = forwardPlan(plan, leader.get(), null);
      if (!StatusUtils.NO_LEADER.equals(result)) {
        result.setRedirectNode(
            new EndPoint(leader.get().getClientIp(), leader.get().getClientPort()));
        return result;
      }
    }

    waitLeader();
    // the leader can be itself after waiting
    if (character == NodeCharacter.LEADER) {
      TSStatus status = processPlanLocally(plan);
      if (status != null) {
        return status;
      }
    }
    TSStatus result = forwardPlan(plan, leader.get(), null);
    if (!StatusUtils.NO_LEADER.equals(result)) {
      result.setRedirectNode(
          new EndPoint(leader.get().getClientIp(), leader.get().getClientPort()));
    }
    return result;
  }

  /**
   * Get the data groups that should be queried when querying "path" with "filter". First, the time
   * interval qualified by the filter will be extracted. If any side of the interval is open, query
   * all groups. Otherwise compute all involved groups w.r.t. the time partitioning.
   */
  public List<PartitionGroup> routeFilter(Filter filter, PartialPath path)
      throws StorageEngineException, EmptyIntervalException {
    Intervals intervals = PartitionUtils.extractTimeInterval(filter);
    if (intervals.isEmpty()) {
      throw new EmptyIntervalException(filter);
    }
    return routeIntervals(intervals, path);
  }

  /**
   * obtaining partition group based on path and intervals
   *
   * @param intervals time intervals, include minimum and maximum value
   * @param path partial path
   * @return data partition on which the current interval of the current path is stored
   * @throws StorageEngineException if Failed to get storage group path
   */
  public List<PartitionGroup> routeIntervals(Intervals intervals, PartialPath path)
      throws StorageEngineException {
    List<PartitionGroup> partitionGroups = new ArrayList<>();
    PartialPath storageGroupName = null;
    try {
      storageGroupName = IoTDB.metaManager.getStorageGroupPath(path);
    } catch (MetadataException e) {
      throw new StorageEngineException(e);
    }

    // if cluster is not enable-partition, a partial data storage in one PartitionGroup
    if (!StorageEngine.isEnablePartition()) {
      PartitionGroup partitionGroup = partitionTable.route(storageGroupName.getFullPath(), 0L);
      partitionGroups.add(partitionGroup);
      return partitionGroups;
    }

    long firstLB = intervals.getLowerBound(0);
    long lastUB = intervals.getUpperBound(intervals.getIntervalSize() - 1);

    if (firstLB == Long.MIN_VALUE || lastUB == Long.MAX_VALUE) {
      // as there is no TimeLowerBound or TimeUpperBound, the query should be broadcast to every
      // group
      partitionGroups.addAll(partitionTable.getGlobalGroups());
    } else {
      // compute the related data groups of all intervals
      // TODO-Cluster#690: change to a broadcast when the computation is too expensive
      Set<Node> groupHeaders = new HashSet<>();
      for (int i = 0; i < intervals.getIntervalSize(); i++) {
        // compute the headers of groups involved in every interval
        PartitionUtils.getIntervalHeaders(
            storageGroupName.getFullPath(),
            intervals.getLowerBound(i),
            intervals.getUpperBound(i),
            partitionTable,
            groupHeaders);
      }
      // translate the headers to groups
      for (Node groupHeader : groupHeaders) {
        partitionGroups.add(partitionTable.getHeaderGroup(groupHeader));
      }
    }
    return partitionGroups;
  }

  @SuppressWarnings("java:S2274")
  public Map<Node, Boolean> getAllNodeStatus() {
    if (getPartitionTable() == null) {
      // the cluster is being built.
      return null;
    }
    Map<Node, Boolean> nodeStatus = new HashMap<>();
    for (Node node : allNodes) {
      nodeStatus.put(node, thisNode.equals(node));
    }

    try {
      if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
        getNodeStatusAsync(nodeStatus);
      } else {
        getNodeStatusSync(nodeStatus);
      }
    } catch (TException e) {
      logger.warn("Cannot get the status of all nodes", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Cannot get the status of all nodes", e);
    }
    return nodeStatus;
  }

  @SuppressWarnings({"java:S2445", "java:S2274"})
  private void getNodeStatusAsync(Map<Node, Boolean> nodeStatus)
      throws TException, InterruptedException {
    NodeStatusHandler nodeStatusHandler = new NodeStatusHandler(nodeStatus);
    synchronized (nodeStatus) {
      for (Node node : allNodes) {
        TSMetaService.AsyncClient client = (AsyncClient) getAsyncClient(node);
        if (!node.equals(thisNode) && client != null) {
          client.checkAlive(nodeStatusHandler);
        }
      }
      nodeStatus.wait(ClusterConstant.CHECK_ALIVE_TIME_OUT_MS);
    }
  }

  private void getNodeStatusSync(Map<Node, Boolean> nodeStatus) {
    NodeStatusHandler nodeStatusHandler = new NodeStatusHandler(nodeStatus);
    for (Node node : allNodes) {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(node);
      if (!node.equals(thisNode) && client != null) {
        Node response = null;
        try {
          response = client.checkAlive();
        } catch (TException e) {
          client.getInputProtocol().getTransport().close();
        } finally {
          ClientUtils.putBackSyncClient(client);
        }
        nodeStatusHandler.onComplete(response);
      }
    }
  }

  @TestOnly
  public void setPartitionTable(PartitionTable partitionTable) {
    this.partitionTable = partitionTable;
    router = new ClusterPlanRouter(partitionTable);
    this.coordinator.setRouter(router);
    DataClusterServer dClusterServer = getDataClusterServer();
    if (dClusterServer != null) {
      dClusterServer.setPartitionTable(partitionTable);
    }
  }

  /**
   * Process the request of removing a node from the cluster. Reject the request if partition table
   * is unavailable or the node is not the MetaLeader and it does not know who the leader is.
   * Otherwise (being the MetaLeader), the request will be processed locally and broadcast to every
   * node.
   *
   * @param node the node to be removed.
   */
  public long removeNode(Node node)
      throws PartitionTableUnavailableException, LogExecutionException {
    if (partitionTable == null) {
      logger.info("Cannot add node now because the partition table is not set");
      throw new PartitionTableUnavailableException(thisNode);
    }

    waitLeader();
    // try to process the request locally, if it cannot be processed locally, forward it
    return processRemoveNodeLocally(node);
  }

  /**
   * Process a node removal request locally and broadcast it to the whole cluster. The removal will
   * be rejected if number of nodes will fall below half of the replication number after this
   * operation.
   *
   * @param node the node to be removed.
   * @return Long.MIN_VALUE if further forwarding is required, or the execution result
   */
  private long processRemoveNodeLocally(Node node) throws LogExecutionException {
    if (character != NodeCharacter.LEADER) {
      return Response.RESPONSE_NULL;
    }

    // if we cannot have enough replica after the removal, reject it
    if (allNodes.size() <= ClusterDescriptor.getInstance().getConfig().getReplicationNum()) {
      return Response.RESPONSE_CLUSTER_TOO_SMALL;
    }

    // find the node to be removed in the node list
    Node target = null;
    synchronized (allNodes) {
      for (Node n : allNodes) {
        if (n.internalIp.equals(node.internalIp) && n.metaPort == node.metaPort) {
          target = n;
          break;
        }
      }
    }

    if (target == null) {
      logger.debug("Node {} is not in the cluster", node);
      return Response.RESPONSE_REJECT;
    }

    // node removal must be serialized to reduce potential concurrency problem
    synchronized (logManager) {
      RemoveNodeLog removeNodeLog = new RemoveNodeLog();
      removeNodeLog.setCurrLogTerm(getTerm().get());
      removeNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      removeNodeLog.setRemovedNode(target);

      logManager.append(removeNodeLog);

      int retryTime = 1;
      while (true) {
        logger.info(
            "Send the node removal request of {} to other nodes, retry time: {}",
            target,
            retryTime);
        AppendLogResult result = sendLogToAllGroups(removeNodeLog);

        switch (result) {
          case OK:
            logger.info("Removal request of {} is accepted", target);
            commitLog(removeNodeLog);
            return Response.RESPONSE_AGREE;
          case TIME_OUT:
            logger.info("Removal request of {} timed out", target);
            break;
            // retry
          case LEADERSHIP_STALE:
          default:
            return Response.RESPONSE_NULL;
        }
      }
    }
  }

  /**
   * Remove a node from the node list, partition table and update DataGroupMembers. If the removed
   * node is the local node, also stop heartbeat and catch-up service of metadata, but the heartbeat
   * and catch-up service of data are kept alive for other nodes to pull data. If the removed node
   * is a leader, send an exile to the removed node so that it can know it is removed.
   *
   * @param oldNode the node to be removed
   */
  public void applyRemoveNode(Node oldNode) {
    synchronized (allNodes) {
      if (allNodes.contains(oldNode)) {
        logger.debug("Removing a node {} from {}", oldNode, allNodes);
        allNodes.remove(oldNode);
        idNodeMap.remove(oldNode.nodeIdentifier);

        // update the partition table
        NodeRemovalResult result = partitionTable.removeNode(oldNode);
        ((SlotPartitionTable) partitionTable).setLastLogIndex(logManager.getLastLogIndex());

        // update DataGroupMembers, as the node is removed, the members of some groups are
        // changed and there will also be one less group
        getDataClusterServer().removeNode(oldNode, result);
        // the leader is removed, start the next election ASAP
        if (oldNode.equals(leader.get())) {
          setCharacter(NodeCharacter.ELECTOR);
          lastHeartbeatReceivedTime = Long.MIN_VALUE;
        }

        if (oldNode.equals(thisNode)) {
          // use super.stop() so that the data server will not be closed because other nodes may
          // want to pull data from this node
          super.stop();
          if (clientServer != null) {
            clientServer.stop();
          }
        } else if (thisNode.equals(leader.get())) {
          // as the old node is removed, it cannot know this by heartbeat or log, so it should be
          // directly kicked out of the cluster
          exileNode(oldNode);
        }

        // save the updated partition table
        savePartitionTable();
      }
    }
  }

  private void exileNode(Node node) {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncMetaClient asyncMetaClient = (AsyncMetaClient) getAsyncClient(node);
      try {
        if (asyncMetaClient != null) {
          asyncMetaClient.exile(new GenericHandler<>(node, null));
        }
      } catch (TException e) {
        logger.warn("Cannot inform {} its removal", node, e);
      }
    } else {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(node);
      if (client == null) {
        return;
      }
      try {
        client.exile();
      } catch (TException e) {
        client.getInputProtocol().getTransport().close();
        logger.warn("Cannot inform {} its removal", node, e);
      } finally {
        ClientUtils.putBackSyncClient(client);
      }
    }
  }

  /**
   * Generate a report containing the character, leader, term, last log and read-only-status. This
   * will help to see if the node is in a consistent and right state during debugging.
   */
  private MetaMemberReport genMemberReport() {
    long prevLastLogIndex = lastReportedLogIndex;
    lastReportedLogIndex = logManager.getLastLogIndex();
    return new MetaMemberReport(
        character,
        leader.get(),
        term.get(),
        logManager.getLastLogTerm(),
        lastReportedLogIndex,
        logManager.getCommitLogIndex(),
        logManager.getCommitLogTerm(),
        readOnly,
        lastHeartbeatReceivedTime,
        prevLastLogIndex,
        logManager.getMaxHaveAppliedCommitIndex());
  }

  /**
   * Generate a report containing the status of both MetaGroupMember and DataGroupMembers of this
   * node. This will help to see if the node is in a consistent and right state during debugging.
   */
  private NodeReport genNodeReport() {
    NodeReport report = new NodeReport(thisNode);
    report.setMetaMemberReport(genMemberReport());
    report.setDataMemberReportList(dataClusterServer.genMemberReports());
    return report;
  }

  @Override
  public void setAllNodes(List<Node> allNodes) {
    super.setAllNodes(allNodes);
    initPeerMap();
    idNodeMap = new HashMap<>();
    for (Node node : allNodes) {
      idNodeMap.put(node.getNodeIdentifier(), node);
    }
  }

  /**
   * Get a local DataGroupMember that is in the group of "header" and should process "request".
   *
   * @param header the header of the group which the local node is in
   * @param request the toString() of this parameter should explain what the request is and it is
   *     only used in logs for tracing
   */
  public DataGroupMember getLocalDataMember(Node header, Object request) {
    return dataClusterServer.getDataMember(header, null, request);
  }

  /**
   * Get a local DataGroupMember that is in the group of "header" for an internal request.
   *
   * @param header the header of the group which the local node is in
   */
  public DataGroupMember getLocalDataMember(Node header) {
    return dataClusterServer.getDataMember(header, null, "Internal call");
  }

  public DataClientProvider getClientProvider() {
    return dataClientProvider;
  }

  @Override
  public void closeLogManager() {
    super.closeLogManager();
    if (dataClusterServer != null) {
      dataClusterServer.closeLogManagers();
    }
  }

  public PlanExecutor getLocalExecutor() throws QueryProcessException {
    if (localExecutor == null) {
      localExecutor = new PlanExecutor();
    }
    return localExecutor;
  }

  public StartUpStatus getStartUpStatus() {
    return startUpStatus;
  }

  @TestOnly
  public void setClientProvider(DataClientProvider dataClientProvider) {
    this.dataClientProvider = dataClientProvider;
  }

  public void handleHandshake(Node sender) {
    NodeStatusManager.getINSTANCE().activate(sender);
  }
}
