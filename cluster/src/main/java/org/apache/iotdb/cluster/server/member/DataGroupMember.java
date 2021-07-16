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
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncDataClient.SingleManagerFactory;
import org.apache.iotdb.cluster.client.async.AsyncDataHeartbeatClient;
import org.apache.iotdb.cluster.client.sync.SyncClientPool;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncDataHeartbeatClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.LogExecutionException;
import org.apache.iotdb.cluster.exception.SnapshotInstallationException;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.applier.AsyncDataLogApplier;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.manage.FilePartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.FileSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.PullSnapshotTask;
import org.apache.iotdb.cluster.log.snapshot.PullSnapshotTaskDescriptor;
import org.apache.iotdb.cluster.partition.NodeAdditionResult;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotManager;
import org.apache.iotdb.cluster.partition.slot.SlotManager.SlotStatus;
import org.apache.iotdb.cluster.partition.slot.SlotNodeAdditionResult;
import org.apache.iotdb.cluster.partition.slot.SlotNodeRemovalResult;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.query.LocalQueryExecutor;
import org.apache.iotdb.cluster.query.manage.ClusterQueryManager;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.PullSnapshotHintService;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.heartbeat.DataHeartbeatThread;
import org.apache.iotdb.cluster.server.monitor.NodeReport.DataMemberReport;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.cluster.server.monitor.Peer;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataGroupMember extends RaftMember {

  private static final Logger logger = LoggerFactory.getLogger(DataGroupMember.class);

  /**
   * The MetaGroupMember that in charge of the DataGroupMember. Mainly for providing partition table
   * and MetaLogManager.
   */
  private MetaGroupMember metaGroupMember;

  /** The thread pool that runs the pull snapshot tasks. Pool size is the # of CPU cores. */
  private ExecutorService pullSnapshotService;

  /**
   * When the member applies a pulled snapshot, it register hints in this service which will
   * periodically inform the data source that one member has pulled snapshot.
   */
  private PullSnapshotHintService pullSnapshotHintService;

  /**
   * "queryManger" records the remote nodes which have queried this node, and the readers or
   * executors this member has created for those queries. When the queries end, an EndQueryRequest
   * will be sent to this member and related resources will be released.
   */
  private ClusterQueryManager queryManager;

  /**
   * "slotManager" tracks the status of slots during data transfers so that we can know whether the
   * slot has non-pulled data.
   */
  protected SlotManager slotManager;

  private LocalQueryExecutor localQueryExecutor;

  /**
   * When a new partition table is installed, all data members will be checked if unchanged. If not,
   * such members will be removed.
   */
  private boolean unchanged;

  @TestOnly
  public DataGroupMember() {
    // constructor for test
    setQueryManager(new ClusterQueryManager());
    localQueryExecutor = new LocalQueryExecutor(this);
  }

  DataGroupMember(
      TProtocolFactory factory,
      PartitionGroup nodes,
      Node thisNode,
      MetaGroupMember metaGroupMember) {
    super(
        "Data(" + nodes.getHeader().getInternalIp() + ":" + nodes.getHeader().getMetaPort() + ")",
        new AsyncClientPool(new AsyncDataClient.FactoryAsync(factory)),
        new SyncClientPool(new SyncDataClient.FactorySync(factory)),
        new AsyncClientPool(new AsyncDataHeartbeatClient.FactoryAsync(factory)),
        new SyncClientPool(new SyncDataHeartbeatClient.FactorySync(factory)),
        new AsyncClientPool(new SingleManagerFactory(factory)));
    this.thisNode = thisNode;
    this.metaGroupMember = metaGroupMember;
    allNodes = nodes;
    setQueryManager(new ClusterQueryManager());
    slotManager = new SlotManager(ClusterConstant.SLOT_NUM, getMemberDir());
    LogApplier applier = new DataLogApplier(metaGroupMember, this);
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncApplier()) {
      applier = new AsyncDataLogApplier(applier, name);
    }
    logManager =
        new FilePartitionedSnapshotLogManager(
            applier, metaGroupMember.getPartitionTable(), allNodes.get(0), thisNode, this);
    initPeerMap();
    term.set(logManager.getHardState().getCurrentTerm());
    voteFor = logManager.getHardState().getVoteFor();
    localQueryExecutor = new LocalQueryExecutor(this);
  }

  /**
   * Start heartbeat, catch-up, pull snapshot services and start all unfinished pull-snapshot-tasks.
   * Calling the method twice does not induce side effects.
   *
   * @throws TTransportException
   */
  @Override
  public void start() {
    if (heartBeatService != null) {
      return;
    }
    super.start();
    heartBeatService.submit(new DataHeartbeatThread(this));
    pullSnapshotService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    pullSnapshotHintService = new PullSnapshotHintService(this);
    resumePullSnapshotTasks();
  }

  /**
   * Stop heartbeat, catch-up and pull snapshot services and release all query resources. Calling
   * the method twice does not induce side effects.
   */
  @Override
  public void stop() {
    logger.info("{}: stopping...", name);
    super.stop();
    if (pullSnapshotService != null) {
      pullSnapshotService.shutdownNow();
      try {
        pullSnapshotService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for pullSnapshotService to end", e);
      }
      pullSnapshotService = null;
      pullSnapshotHintService.stop();
    }

    try {
      getQueryManager().endAllQueries();
    } catch (StorageEngineException e) {
      logger.error("Cannot release queries of {}", name, e);
    }
    logger.info("{}: stopped", name);
  }

  /**
   * The first node (on the hash ring) in this data group is the header. It determines the duty
   * (what range on the ring do the group take responsibility for) of the group and although other
   * nodes in this may change, this node is unchangeable unless the data group is dismissed. It is
   * also the identifier of this data group.
   */
  @Override
  public Node getHeader() {
    return allNodes.get(0);
  }

  public ClusterQueryManager getQueryManager() {
    return queryManager;
  }

  protected void setQueryManager(ClusterQueryManager queryManager) {
    this.queryManager = queryManager;
  }

  public static class Factory {

    private TProtocolFactory protocolFactory;
    private MetaGroupMember metaGroupMember;

    Factory(TProtocolFactory protocolFactory, MetaGroupMember metaGroupMember) {
      this.protocolFactory = protocolFactory;
      this.metaGroupMember = metaGroupMember;
    }

    public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode) {
      return new DataGroupMember(protocolFactory, partitionGroup, thisNode, metaGroupMember);
    }
  }

  /**
   * Try to add a Node into the group to which the member belongs.
   *
   * @param node
   * @return true if this node should leave the group because of the addition of the node, false
   *     otherwise
   */
  public synchronized boolean addNode(Node node, NodeAdditionResult result) {
    // when a new node is added, start an election instantly to avoid the stale leader still
    // taking the leadership, which guarantees the valid leader will not have the stale
    // partition table
    synchronized (term) {
      term.incrementAndGet();
      setLeader(ClusterConstant.EMPTY_NODE);
      setVoteFor(thisNode);
      updateHardState(term.get(), getVoteFor());
      setLastHeartbeatReceivedTime(System.currentTimeMillis());
      setCharacter(NodeCharacter.ELECTOR);
    }

    // mark slots that do not belong to this group any more
    Set<Integer> lostSlots =
        ((SlotNodeAdditionResult) result)
            .getLostSlots()
            .getOrDefault(getHeader(), Collections.emptySet());
    for (Integer lostSlot : lostSlots) {
      slotManager.setToSending(lostSlot);
    }

    synchronized (allNodes) {
      int insertIndex = -1;
      // find the position to insert the new node, the nodes are ordered by their identifiers
      for (int i = 0; i < allNodes.size() - 1; i++) {
        Node prev = allNodes.get(i);
        Node next = allNodes.get(i + 1);
        if (prev.nodeIdentifier < node.nodeIdentifier && node.nodeIdentifier < next.nodeIdentifier
            || prev.nodeIdentifier < node.nodeIdentifier
                && next.nodeIdentifier < prev.nodeIdentifier
            || node.nodeIdentifier < next.nodeIdentifier
                && next.nodeIdentifier < prev.nodeIdentifier) {
          insertIndex = i + 1;
          break;
        }
      }
      if (insertIndex > 0) {
        allNodes.add(insertIndex, node);
        peerMap.putIfAbsent(node, new Peer(logManager.getLastLogIndex()));
        // remove the last node because the group size is fixed to replication number
        Node removedNode = allNodes.remove(allNodes.size() - 1);
        peerMap.remove(removedNode);
        // if the local node is the last node and the insertion succeeds, this node should leave
        // the group
        logger.debug("{}: Node {} is inserted into the data group {}", name, node, allNodes);
        return removedNode.equals(thisNode);
      }
      return false;
    }
  }

  /**
   * Process the election request from another node in the group. To win the vote from the local
   * member, a node must have both meta and data logs no older than then local member, or it will be
   * turned down.
   *
   * @param electionRequest
   * @return Response.RESPONSE_META_LOG_STALE if the meta logs of the elector fall behind
   *     Response.RESPONSE_LOG_MISMATCH if the data logs of the elector fall behind Response.SUCCESS
   *     if the vote is given to the elector the term of local member if the elector's term is no
   *     bigger than the local member
   */
  @Override
  long checkElectorLogProgress(ElectionRequest electionRequest) {
    // to be a data group leader, a node should also be qualified to be the meta group leader
    // which guarantees the data group leader has the newest partition table.
    long thatTerm = electionRequest.getTerm();
    long thatMetaLastLogIndex = electionRequest.getLastLogIndex();
    long thatMetaLastLogTerm = electionRequest.getLastLogTerm();
    long thatDataLastLogIndex = electionRequest.getDataLogLastIndex();
    long thatDataLastLogTerm = electionRequest.getDataLogLastTerm();
    logger.info(
        "{} received an dataGroup election request, term:{}, metaLastLogIndex:{}, metaLastLogTerm:{}, dataLastLogIndex:{}, dataLastLogTerm:{}",
        name,
        thatTerm,
        thatMetaLastLogIndex,
        thatMetaLastLogTerm,
        thatDataLastLogIndex,
        thatDataLastLogTerm);

    // check meta logs
    // term of the electors' MetaGroupMember is not verified, so 0 and 1 are used to make sure
    // the verification does not fail
    long metaResponse = metaGroupMember.checkLogProgress(thatMetaLastLogIndex, thatMetaLastLogTerm);
    if (metaResponse == Response.RESPONSE_LOG_MISMATCH) {
      return Response.RESPONSE_META_LOG_STALE;
    }

    long resp = checkLogProgress(thatDataLastLogIndex, thatDataLastLogTerm);
    if (resp == Response.RESPONSE_AGREE) {
      logger.info(
          "{} accepted an dataGroup election request, term:{}/{}, dataLogIndex:{}/{}, dataLogTerm:{}/{}, metaLogIndex:{}/{},metaLogTerm:{}/{}",
          name,
          thatTerm,
          term.get(),
          thatDataLastLogIndex,
          logManager.getLastLogIndex(),
          thatDataLastLogTerm,
          logManager.getLastLogTerm(),
          thatMetaLastLogIndex,
          metaGroupMember.getLogManager().getLastLogIndex(),
          thatMetaLastLogTerm,
          metaGroupMember.getLogManager().getLastLogTerm());
      setCharacter(NodeCharacter.FOLLOWER);
      lastHeartbeatReceivedTime = System.currentTimeMillis();
      setVoteFor(electionRequest.getElector());
      updateHardState(thatTerm, getVoteFor());
    } else {
      logger.info(
          "{} rejected an dataGroup election request, term:{}/{}, dataLogIndex:{}/{}, dataLogTerm:{}/{}, metaLogIndex:{}/{},metaLogTerm:{}/{}",
          name,
          thatTerm,
          term.get(),
          thatDataLastLogIndex,
          logManager.getLastLogIndex(),
          thatDataLastLogTerm,
          logManager.getLastLogTerm(),
          thatMetaLastLogIndex,
          metaGroupMember.getLogManager().getLastLogIndex(),
          thatMetaLastLogTerm,
          metaGroupMember.getLogManager().getLastLogTerm());
    }
    return resp;
  }

  /**
   * Deserialize and install a snapshot sent by the leader. The type of the snapshot must be
   * currently PartitionedSnapshot with FileSnapshot inside.
   *
   * @param request
   */
  public void receiveSnapshot(SendSnapshotRequest request) throws SnapshotInstallationException {
    logger.info(
        "{}: received a snapshot from {} with size {}",
        name,
        request.getHeader(),
        request.getSnapshotBytes().length);
    PartitionedSnapshot<FileSnapshot> snapshot =
        new PartitionedSnapshot<>(FileSnapshot.Factory.INSTANCE);

    snapshot.deserialize(ByteBuffer.wrap(request.getSnapshotBytes()));
    if (logger.isDebugEnabled()) {
      logger.debug("{} received a snapshot {}", name, snapshot);
    }
    snapshot.getDefaultInstaller(this).install(snapshot, -1);
  }

  /**
   * Send the requested snapshots to the applier node.
   *
   * @param request
   */
  public PullSnapshotResp getSnapshot(PullSnapshotRequest request) throws IOException {
    waitLeader();
    if (character != NodeCharacter.LEADER && !readOnly) {
      return null;
    }
    // if the requester pulls the snapshots because the header of the group is removed, then the
    // member should no longer receive new data
    if (request.isRequireReadOnly()) {
      setReadOnly();
    }

    List<Integer> requiredSlots = request.getRequiredSlots();
    for (Integer requiredSlot : requiredSlots) {
      // wait if the data of the slot is in another node
      slotManager.waitSlot(requiredSlot);
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} slots are requested, first:{}, last: {}",
          name,
          requiredSlots.size(),
          requiredSlots.get(0),
          requiredSlots.get(requiredSlots.size() - 1));
    }

    // If the logs between [currCommitLogIndex, currLastLogIndex] are committed after the
    // snapshot is generated, they will be invisible to the new slot owner and thus lost forever
    long currLastLogIndex = logManager.getLastLogIndex();
    logger.info(
        "{}: Waiting for logs to commit before snapshot, {}/{}",
        name,
        logManager.getCommitLogIndex(),
        currLastLogIndex);
    while (logManager.getCommitLogIndex() < currLastLogIndex) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("{}: Unexpected interruption when waiting for logs to commit", name, e);
      }
    }

    // this synchronized should work with the one in AppendEntry when a log is going to commit,
    // which may prevent the newly arrived data from being invisible to the new header.
    synchronized (logManager) {
      PullSnapshotResp resp = new PullSnapshotResp();
      Map<Integer, ByteBuffer> resultMap = new HashMap<>();
      logManager.takeSnapshot();

      PartitionedSnapshot<Snapshot> allSnapshot = (PartitionedSnapshot) logManager.getSnapshot();
      for (int requiredSlot : requiredSlots) {
        Snapshot snapshot = allSnapshot.getSnapshot(requiredSlot);
        if (snapshot != null) {
          resultMap.put(requiredSlot, snapshot.serialize());
        }
      }
      resp.setSnapshotBytes(resultMap);
      logger.debug("{}: Sending {} snapshots to the requester", name, resultMap.size());
      return resp;
    }
  }

  /**
   * Pull snapshots from the previous holders after newNode joins the cluster.
   *
   * @param slots
   * @param newNode
   */
  public void pullNodeAdditionSnapshots(List<Integer> slots, Node newNode) {
    synchronized (logManager) {
      logger.info("{} pulling {} slots from remote", name, slots.size());
      PartitionedSnapshot<Snapshot> snapshot = (PartitionedSnapshot) logManager.getSnapshot();
      Map<Integer, Node> prevHolders =
          ((SlotPartitionTable) metaGroupMember.getPartitionTable()).getPreviousNodeMap(newNode);

      // group the slots by their owners
      Map<Node, List<Integer>> holderSlotsMap = new HashMap<>();
      for (int slot : slots) {
        // skip the slot if the corresponding data is already replicated locally
        if (snapshot.getSnapshot(slot) == null) {
          Node node = prevHolders.get(slot);
          if (node != null) {
            holderSlotsMap.computeIfAbsent(node, n -> new ArrayList<>()).add(slot);
          }
        }
      }

      // pull snapshots from each owner's data group
      for (Entry<Node, List<Integer>> entry : holderSlotsMap.entrySet()) {
        Node node = entry.getKey();
        List<Integer> nodeSlots = entry.getValue();
        PullSnapshotTaskDescriptor taskDescriptor =
            new PullSnapshotTaskDescriptor(
                metaGroupMember.getPartitionTable().getHeaderGroup(node), nodeSlots, false);
        pullFileSnapshot(taskDescriptor, null);
      }
    }
  }

  /**
   * Pull FileSnapshots (timeseries schemas and lists of TsFiles) of "nodeSlots" from one of the
   * "prevHolders". The actual pulling will be performed in a separate thread.
   *
   * @param descriptor
   * @param snapshotSave set to the corresponding disk file if the task is resumed from disk, or set
   *     ot null otherwise
   */
  private void pullFileSnapshot(PullSnapshotTaskDescriptor descriptor, File snapshotSave) {
    Iterator<Integer> iterator = descriptor.getSlots().iterator();
    while (iterator.hasNext()) {
      Integer nodeSlot = iterator.next();
      SlotStatus status = slotManager.getStatus(nodeSlot);
      if (status != SlotStatus.NULL) {
        // the pulling may already be issued during restart, skip it in that case
        iterator.remove();
      } else {
        // mark the slot as pulling to control reads and writes of the pulling slot
        slotManager.setToPulling(nodeSlot, descriptor.getPreviousHolders().getHeader());
      }
    }

    if (descriptor.getSlots().isEmpty()) {
      return;
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "{}: {} and other {} slots are set to pulling",
          name,
          descriptor.getSlots().get(0),
          descriptor.getSlots().size() - 1);
    }

    pullSnapshotService.submit(
        new PullSnapshotTask<>(descriptor, this, FileSnapshot.Factory.INSTANCE, snapshotSave));
  }

  /** Restart all unfinished pull-snapshot-tasks of the member. */
  private void resumePullSnapshotTasks() {
    File snapshotTaskDir = new File(getPullSnapshotTaskDir());
    if (!snapshotTaskDir.exists()) {
      return;
    }

    File[] files = snapshotTaskDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (!file.getName().endsWith(PullSnapshotTask.TASK_SUFFIX)) {
          continue;
        }
        try (DataInputStream dataInputStream =
            new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
          PullSnapshotTaskDescriptor descriptor = new PullSnapshotTaskDescriptor();
          descriptor.deserialize(dataInputStream);
          pullFileSnapshot(descriptor, file);
        } catch (IOException e) {
          logger.error("Cannot resume pull-snapshot-task in file {}", file, e);
          try {
            Files.delete(file.toPath());
          } catch (IOException ex) {
            logger.debug("Cannot remove pull snapshot task file {}", file, e);
          }
        }
      }
    }
  }

  /** @return a directory that stores the information of ongoing pulling snapshot tasks. */
  public String getPullSnapshotTaskDir() {
    return getMemberDir() + "snapshot_task" + File.separator;
  }

  /** @return the path of the directory that is provided exclusively for the member. */
  private String getMemberDir() {
    return IoTDBDescriptor.getInstance().getConfig().getSystemDir()
        + File.separator
        + "raft"
        + File.separator
        + getHeader().nodeIdentifier
        + File.separator;
  }

  public MetaGroupMember getMetaGroupMember() {
    return metaGroupMember;
  }

  /**
   * If the member is the leader, let all members in the group close the specified partition of a
   * storage group, else just return false.
   *
   * @param storageGroupName
   * @param partitionId
   * @param isSeq
   */
  void closePartition(String storageGroupName, long partitionId, boolean isSeq) {
    if (character != NodeCharacter.LEADER) {
      return;
    }
    CloseFileLog log = new CloseFileLog(storageGroupName, partitionId, isSeq);
    synchronized (logManager) {
      log.setCurrLogTerm(getTerm().get());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      logManager.append(log);

      logger.info("Send the close file request of {} to other nodes", log);
    }
    try {
      appendLogInGroup(log);
    } catch (LogExecutionException e) {
      logger.error("Cannot close partition {}#{} seq:{}", storageGroupName, partitionId, isSeq, e);
    }
  }

  public boolean flushFileWhenDoSnapshot(
      Map<String, List<Pair<Long, Boolean>>> storageGroupPartitions) {
    if (character != NodeCharacter.LEADER) {
      return false;
    }

    Map<PartialPath, List<Pair<Long, Boolean>>> localDataMemberStorageGroupPartitions =
        new HashMap<>();
    for (Entry<String, List<Pair<Long, Boolean>>> entry : storageGroupPartitions.entrySet()) {
      List<Pair<Long, Boolean>> localListPair = new ArrayList<>();

      String storageGroupName = entry.getKey();
      List<Pair<Long, Boolean>> tmpPairList = entry.getValue();
      for (Pair<Long, Boolean> pair : tmpPairList) {
        long partitionId = pair.left;
        Node header =
            metaGroupMember
                .getPartitionTable()
                .routeToHeaderByTime(
                    storageGroupName, partitionId * StorageEngine.getTimePartitionInterval());
        DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
        if (localDataMember.getHeader().equals(this.getHeader())) {
          localListPair.add(new Pair<>(partitionId, pair.right));
        }
      }
      try {
        localDataMemberStorageGroupPartitions.put(new PartialPath(storageGroupName), localListPair);
      } catch (IllegalPathException e) {
        // ignore
      }
    }

    if (localDataMemberStorageGroupPartitions.size() <= 0) {
      logger.info("{}: have no data to flush", name);
      return true;
    }
    FlushPlan flushPlan = new FlushPlan(null, true, localDataMemberStorageGroupPartitions);
    try {
      PlanExecutor.flushSpecifiedStorageGroups(flushPlan);
      return true;
    } catch (StorageGroupNotSetException e) {
      logger.error("Some SGs are missing while flushing", e);
    }
    return false;
  }

  /**
   * Execute a non-query plan. If the member is a leader, a log for the plan will be created and
   * process through the raft procedure, otherwise the plan will be forwarded to the leader.
   *
   * @param plan a non-query plan.
   * @return
   */
  @Override
  public TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    long startTime = System.currentTimeMillis();
    if (ClusterDescriptor.getInstance().getConfig().getReplicationNum() == 1) {
      try {
        getLocalExecutor().processNonQuery(plan);
        return StatusUtils.OK;
      } catch (Exception e) {
        Throwable cause = IOUtils.getRootCause(e);
        if (cause instanceof StorageGroupNotSetException
            || cause instanceof UndefinedTemplateException) {
          try {
            metaGroupMember.syncLeaderWithConsistencyCheck(true);
            if (plan instanceof InsertPlan && ((InsertPlan) plan).getFailedMeasurements() != null) {
              ((InsertPlan) plan).recoverFromFailure();
            }
            getLocalExecutor().processNonQuery(plan);
            return StatusUtils.OK;
          } catch (CheckConsistencyException ce) {
            return StatusUtils.getStatus(StatusUtils.CONSISTENCY_FAILURE, ce.getMessage());
          } catch (Exception ne) {
            return handleLogExecutionException(plan, IOUtils.getRootCause(ne));
          }
        }
        return handleLogExecutionException(plan, cause);
      } finally {
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed > 5000) {
          logger.error("PlanExecutor execute slowly : time cost : {}ms", elapsed);
        }
      }
    } else {
      TSStatus status = executeNonQueryPlanWithKnownLeader(plan);
      if (!StatusUtils.NO_LEADER.equals(status)) {
        return status;
      }

      startTime = Timer.Statistic.DATA_GROUP_MEMBER_WAIT_LEADER.getOperationStartTime();
      waitLeader();
      Timer.Statistic.DATA_GROUP_MEMBER_WAIT_LEADER.calOperationCostTimeFromStart(startTime);

      return executeNonQueryPlanWithKnownLeader(plan);
    }
  }

  private TSStatus executeNonQueryPlanWithKnownLeader(PhysicalPlan plan) {
    if (character == NodeCharacter.LEADER) {
      long startTime = Statistic.DATA_GROUP_MEMBER_LOCAL_EXECUTION.getOperationStartTime();
      TSStatus status = processPlanLocally(plan);
      Statistic.DATA_GROUP_MEMBER_LOCAL_EXECUTION.calOperationCostTimeFromStart(startTime);
      if (status != null) {
        return status;
      }
    } else if (leader.get() != null && !ClusterConstant.EMPTY_NODE.equals(leader.get())) {
      long startTime = Timer.Statistic.DATA_GROUP_MEMBER_FORWARD_PLAN.getOperationStartTime();
      TSStatus result = forwardPlan(plan, leader.get(), getHeader());
      Timer.Statistic.DATA_GROUP_MEMBER_FORWARD_PLAN.calOperationCostTimeFromStart(startTime);
      if (!StatusUtils.NO_LEADER.equals(result)) {
        result.setRedirectNode(
            new EndPoint(leader.get().getClientIp(), leader.get().getClientPort()));
        return result;
      }
    }
    return StatusUtils.NO_LEADER;
  }

  /**
   * When the node does not play a member in a group any more, the corresponding local data should
   * be removed.
   */
  public void removeLocalData(List<Integer> slots) {
    if (slots.isEmpty()) {
      return;
    }

    Set<Integer> slotSet = new HashSet<>(slots);
    List<PartialPath> allStorageGroupNames = IoTDB.metaManager.getAllStorageGroupPaths();
    TimePartitionFilter filter =
        (storageGroupName, timePartitionId) -> {
          int slot =
              SlotPartitionTable.getSlotStrategy()
                  .calculateSlotByPartitionNum(
                      storageGroupName, timePartitionId, ClusterConstant.SLOT_NUM);
          return slotSet.contains(slot);
        };
    for (PartialPath sg : allStorageGroupNames) {
      StorageEngine.getInstance().removePartitions(sg, filter);
    }
    for (Integer slot : slots) {
      slotManager.setToNull(slot);
    }

    if (logger.isInfoEnabled()) {
      logger.info(
          "{}: data of {} and other {} slots are removed", name, slots.get(0), slots.size() - 1);
    }
  }

  /**
   * When a node is removed and IT IS NOT THE HEADER of the group, the member should take over some
   * slots from the removed group, and add a new node to the group the removed node was in the
   * group.
   */
  @SuppressWarnings("java:S2445") // the reference of allNodes is unchanged
  public void removeNode(Node removedNode, NodeRemovalResult removalResult) {
    synchronized (allNodes) {
      if (allNodes.contains(removedNode)) {
        // update the group if the deleted node was in it
        allNodes = metaGroupMember.getPartitionTable().getHeaderGroup(getHeader());
        initPeerMap();
        if (removedNode.equals(leader.get())) {
          // if the leader is removed, also start an election immediately
          synchronized (term) {
            setCharacter(NodeCharacter.ELECTOR);
            setLastHeartbeatReceivedTime(Long.MIN_VALUE);
          }
        }
      }
      List<Integer> slotsToPull =
          ((SlotNodeRemovalResult) removalResult).getNewSlotOwners().get(getHeader());
      if (slotsToPull != null) {
        // pull the slots that should be taken over
        PullSnapshotTaskDescriptor taskDescriptor =
            new PullSnapshotTaskDescriptor(removalResult.getRemovedGroup(), slotsToPull, true);
        pullFileSnapshot(taskDescriptor, null);
      }
    }
  }

  /**
   * Generate a report containing the character, leader, term, last log term, last log index, header
   * and readOnly or not of this member.
   *
   * @return
   */
  public DataMemberReport genReport() {
    long prevLastLogIndex = lastReportedLogIndex;
    lastReportedLogIndex = logManager.getLastLogIndex();
    return new DataMemberReport(
        character,
        leader.get(),
        term.get(),
        logManager.getLastLogTerm(),
        lastReportedLogIndex,
        logManager.getCommitLogIndex(),
        logManager.getCommitLogTerm(),
        getHeader(),
        readOnly,
        NodeStatusManager.getINSTANCE().getLastResponseLatency(getHeader()),
        lastHeartbeatReceivedTime,
        prevLastLogIndex,
        logManager.getMaxHaveAppliedCommitIndex());
  }

  @TestOnly
  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
    this.localQueryExecutor = new LocalQueryExecutor(this);
  }

  @TestOnly
  void setLogManager(PartitionedSnapshotLogManager<Snapshot> logManager) {
    if (this.logManager != null) {
      this.logManager.close();
    }
    this.logManager = logManager;
    super.setLogManager(logManager);
    initPeerMap();
  }

  public SlotManager getSlotManager() {
    return slotManager;
  }

  public boolean onSnapshotInstalled(List<Integer> slots) {
    List<Integer> removableSlots = new ArrayList<>();
    for (Integer slot : slots) {
      int sentReplicaNum = slotManager.sentOneReplication(slot);
      if (sentReplicaNum >= ClusterDescriptor.getInstance().getConfig().getReplicationNum()) {
        removableSlots.add(slot);
      }
    }
    removeLocalData(removableSlots);
    return true;
  }

  public void registerPullSnapshotHint(PullSnapshotTaskDescriptor descriptor) {
    pullSnapshotHintService.registerHint(descriptor);
  }

  public LocalQueryExecutor getLocalQueryExecutor() {
    return localQueryExecutor;
  }

  @TestOnly
  public void setLocalQueryExecutor(LocalQueryExecutor localQueryExecutor) {
    this.localQueryExecutor = localQueryExecutor;
  }

  public boolean isUnchanged() {
    return unchanged;
  }

  public void setUnchanged(boolean unchanged) {
    this.unchanged = unchanged;
  }
}
