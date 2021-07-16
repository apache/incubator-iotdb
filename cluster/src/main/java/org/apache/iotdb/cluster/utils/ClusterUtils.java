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

package org.apache.iotdb.cluster.utils;

import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.ConfigInconsistentException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.rpc.RpcTransportFactory;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterUtils {

  private static final Logger logger = LoggerFactory.getLogger(ClusterUtils.class);

  public static final int WAIT_START_UP_CHECK_TIME_SEC = 5;

  public static final long START_UP_TIME_THRESHOLD_MS = 5 * 60 * 1000L;

  public static final long START_UP_CHECK_TIME_INTERVAL_MS = 3 * 1000L;

  /**
   * the data group member's heartbeat offset relative to the {@link
   * ClusterConfig#getInternalDataPort()}, which means the dataHeartbeatPort = getInternalDataPort()
   * + DATA_HEARTBEAT_OFFSET.
   */
  public static final int DATA_HEARTBEAT_PORT_OFFSET = 1;

  /**
   * the meta group member's heartbeat offset relative to the {@link
   * ClusterConfig#getInternalMetaPort()}, which means the metaHeartbeatPort = getInternalMetaPort()
   * + META_HEARTBEAT_OFFSET.
   */
  public static final int META_HEARTBEAT_PORT_OFFSET = 1;

  public static final String UNKNOWN_CLIENT_IP = "UNKNOWN_IP";

  private ClusterUtils() {
    // util class
  }

  public static CheckStatusResponse checkStatus(
      StartUpStatus remoteStartUpStatus, StartUpStatus localStartUpStatus) {
    boolean partitionIntervalEquals = true;
    boolean hashSaltEquals = true;
    boolean replicationNumEquals = true;
    boolean seedNodeListEquals = true;
    boolean clusterNameEqual = true;
    boolean multiRaftFactorEqual = true;

    if (localStartUpStatus.getPartitionInterval() != remoteStartUpStatus.getPartitionInterval()) {
      partitionIntervalEquals = false;
      logger.error(
          "Remote partition interval conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getPartitionInterval(),
          remoteStartUpStatus.getPartitionInterval());
    }
    if (localStartUpStatus.getMultiRaftFactor() != remoteStartUpStatus.getMultiRaftFactor()) {
      multiRaftFactorEqual = false;
      logger.error(
          "Remote multi-raft factor conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getMultiRaftFactor(),
          remoteStartUpStatus.getMultiRaftFactor());
    }
    if (localStartUpStatus.getHashSalt() != remoteStartUpStatus.getHashSalt()) {
      hashSaltEquals = false;
      logger.error(
          "Remote hash salt conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getHashSalt(),
          remoteStartUpStatus.getHashSalt());
    }
    if (localStartUpStatus.getReplicationNumber() != remoteStartUpStatus.getReplicationNumber()) {
      replicationNumEquals = false;
      logger.error(
          "Remote replication number conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getReplicationNumber(),
          remoteStartUpStatus.getReplicationNumber());
    }
    if (!Objects.equals(
        localStartUpStatus.getClusterName(), remoteStartUpStatus.getClusterName())) {
      clusterNameEqual = false;
      logger.error(
          "Remote cluster name conflicts with local. local: {}, remote: {}",
          localStartUpStatus.getClusterName(),
          remoteStartUpStatus.getClusterName());
    }
    if (!ClusterUtils.checkSeedNodes(
        false, localStartUpStatus.getSeedNodeList(), remoteStartUpStatus.getSeedNodeList())) {
      seedNodeListEquals = false;
      if (logger.isErrorEnabled()) {
        logger.error(
            "Remote seed node list conflicts with local. local: {}, remote: {}",
            localStartUpStatus.getSeedNodeList(),
            remoteStartUpStatus.getSeedNodeList());
      }
    }

    return new CheckStatusResponse(
        partitionIntervalEquals,
        hashSaltEquals,
        replicationNumEquals,
        seedNodeListEquals,
        clusterNameEqual,
        multiRaftFactorEqual);
  }

  public static boolean checkSeedNodes(
      boolean isClusterEstablished, List<Node> localSeedNodes, List<Node> remoteSeedNodes) {
    return isClusterEstablished
        ? seedNodesContains(localSeedNodes, remoteSeedNodes)
        : seedNodesEquals(localSeedNodes, remoteSeedNodes);
  }

  private static boolean seedNodesEquals(List<Node> thisNodeList, List<Node> thatNodeList) {
    Node[] thisNodeArray = thisNodeList.toArray(new Node[0]);
    Node[] thatNodeArray = thatNodeList.toArray(new Node[0]);
    Arrays.sort(thisNodeArray, ClusterUtils::compareSeedNode);
    Arrays.sort(thatNodeArray, ClusterUtils::compareSeedNode);
    if (thisNodeArray.length != thatNodeArray.length) {
      return false;
    } else {
      for (int i = 0; i < thisNodeArray.length; i++) {
        if (compareSeedNode(thisNodeArray[i], thatNodeArray[i]) != 0) {
          return false;
        }
      }
      return true;
    }
  }

  private static int compareSeedNode(Node thisSeedNode, Node thatSeedNode) {
    int ipCompare = thisSeedNode.getInternalIp().compareTo(thatSeedNode.getInternalIp());
    if (ipCompare != 0) {
      return ipCompare;
    } else {
      return thisSeedNode.getMetaPort() - thatSeedNode.getMetaPort();
    }
  }

  private static boolean seedNodesContains(List<Node> seedNodeList, List<Node> subSeedNodeList) {
    // Because identifier is not compared here, List.contains() is not suitable
    if (subSeedNodeList == null) {
      return false;
    }
    seedNodeList.sort(ClusterUtils::compareSeedNode);
    subSeedNodeList.sort(ClusterUtils::compareSeedNode);
    int i = 0;
    int j = 0;
    while (i < seedNodeList.size() && j < subSeedNodeList.size()) {
      int compareResult = compareSeedNode(seedNodeList.get(i), subSeedNodeList.get(j));
      if (compareResult > 0) {
        if (logger.isErrorEnabled()) {
          logger.error("Node {} not found in cluster", subSeedNodeList.get(j));
        }
        return false;
      } else if (compareResult < 0) {
        i++;
      } else {
        j++;
      }
    }
    return j == subSeedNodeList.size();
  }

  public static void examineCheckStatusResponse(
      CheckStatusResponse response,
      AtomicInteger consistentNum,
      AtomicInteger inconsistentNum,
      Node seedNode) {
    boolean partitionIntervalEquals = response.partitionalIntervalEquals;
    boolean hashSaltEquals = response.hashSaltEquals;
    boolean replicationNumEquals = response.replicationNumEquals;
    boolean seedNodeListEquals = response.seedNodeEquals;
    boolean clusterNameEqual = response.clusterNameEquals;
    if (!partitionIntervalEquals) {
      logger.error("Local partition interval conflicts with seed node[{}].", seedNode);
    }
    if (!hashSaltEquals) {
      logger.error("Local hash salt conflicts with seed node[{}]", seedNode);
    }
    if (!replicationNumEquals) {
      logger.error("Local replication number conflicts with seed node[{}]", seedNode);
    }
    if (!seedNodeListEquals) {
      logger.error("Local seed node list conflicts with seed node[{}]", seedNode);
    }
    if (!clusterNameEqual) {
      logger.error("Local cluster name conflicts with seed node[{}]", seedNode);
    }
    if (partitionIntervalEquals
        && hashSaltEquals
        && replicationNumEquals
        && seedNodeListEquals
        && clusterNameEqual) {
      consistentNum.incrementAndGet();
    } else {
      inconsistentNum.incrementAndGet();
    }
  }

  public static boolean analyseStartUpCheckResult(
      int consistentNum, int inconsistentNum, int totalSeedNum) throws ConfigInconsistentException {
    if (consistentNum == totalSeedNum) {
      // break the loop and establish the cluster
      return true;
    } else if (inconsistentNum > 0) {
      // find config InConsistence, stop building cluster
      throw new ConfigInconsistentException();
    } else {
      // The status of some nodes was not obtained, possibly because those node did not start
      // successfully,
      // this node can't connect to those nodes, try in next turn
      return false;
    }
  }

  public static TServer createTThreadPoolServer(
      TServerTransport socket,
      String clientThreadPrefix,
      TProcessor processor,
      TProtocolFactory protocolFactory) {
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    int maxConcurrentClientNum =
        Math.max(CommonUtils.getCpuCores(), config.getMaxConcurrentClientNum());
    TThreadPoolServer.Args poolArgs =
        new TThreadPoolServer.Args(socket)
            .maxWorkerThreads(maxConcurrentClientNum)
            .minWorkerThreads(CommonUtils.getCpuCores());

    poolArgs.executorService(
        new ThreadPoolExecutor(
            poolArgs.minWorkerThreads,
            poolArgs.maxWorkerThreads,
            poolArgs.stopTimeoutVal,
            poolArgs.stopTimeoutUnit,
            new SynchronousQueue<>(),
            new ThreadFactory() {
              private AtomicLong threadIndex = new AtomicLong(0);

              @Override
              public Thread newThread(Runnable r) {
                return new Thread(r, clientThreadPrefix + threadIndex.incrementAndGet());
              }
            }));
    poolArgs.processor(processor);
    poolArgs.protocolFactory(protocolFactory);
    // async service requires FramedTransport
    poolArgs.transportFactory(RpcTransportFactory.INSTANCE);

    // run the thrift server in a separate thread so that the main thread is not blocked
    return new TThreadPoolServer(poolArgs);
  }

  /**
   * Convert a string representation of a Node to an object.
   *
   * @param str A string that is generated by Node.toString()
   * @return a Node object
   */
  public static Node stringToNode(String str) {

    int ipFirstPos = str.indexOf("internalIp:") + "internalIp:".length();
    int ipLastPos = str.indexOf(',', ipFirstPos);
    int metaPortFirstPos = str.indexOf("metaPort:", ipLastPos) + "metaPort:".length();
    int metaPortLastPos = str.indexOf(',', metaPortFirstPos);
    int idFirstPos = str.indexOf("nodeIdentifier:", metaPortLastPos) + "nodeIdentifier:".length();
    int idLastPos = str.indexOf(',', idFirstPos);
    int dataPortFirstPos = str.indexOf("dataPort:", idLastPos) + "dataPort:".length();
    int dataPortLastPos = str.indexOf(',', dataPortFirstPos);
    int clientPortFirstPos = str.indexOf("clientPort:", dataPortLastPos) + "clientPort:".length();
    int clientPortLastPos = str.indexOf(',', clientPortFirstPos);
    int clientIpFirstPos = str.indexOf("clientIp:", clientPortLastPos) + "clientIp:".length();
    int clientIpLastPos = str.indexOf(')', clientIpFirstPos);

    String ip = str.substring(ipFirstPos, ipLastPos);
    int metaPort = Integer.parseInt(str.substring(metaPortFirstPos, metaPortLastPos));
    int id = Integer.parseInt(str.substring(idFirstPos, idLastPos));
    int dataPort = Integer.parseInt(str.substring(dataPortFirstPos, dataPortLastPos));
    int clientPort = Integer.parseInt(str.substring(clientPortFirstPos, clientPortLastPos));
    String clientIp = str.substring(clientIpFirstPos, clientIpLastPos);
    return new Node(ip, metaPort, id, dataPort, clientPort, clientIp);
  }

  public static Node parseNode(String nodeUrl) {
    Node result = new Node();
    String[] split = nodeUrl.split(":");
    if (split.length != 2) {
      logger.warn("Bad seed url: {}", nodeUrl);
      return null;
    }
    String ip = split[0];
    try {
      int metaPort = Integer.parseInt(split[1]);
      result.setInternalIp(ip).setMetaPort(metaPort).setClientIp(UNKNOWN_CLIENT_IP);
    } catch (NumberFormatException e) {
      logger.warn("Bad seed url: {}", nodeUrl);
    }
    return result;
  }

  public static PartitionGroup partitionByPathTimeWithSync(
      PartialPath prefixPath, MetaGroupMember metaGroupMember) throws MetadataException {
    PartitionGroup partitionGroup;
    try {
      partitionGroup = metaGroupMember.getPartitionTable().partitionByPathTime(prefixPath, 0);
    } catch (StorageGroupNotSetException e) {
      // the storage group is not found locally, but may be found in the leader, retry after
      // synchronizing with the leader
      try {
        metaGroupMember.syncLeaderWithConsistencyCheck(true);
      } catch (CheckConsistencyException checkConsistencyException) {
        throw new MetadataException(checkConsistencyException.getMessage());
      }
      partitionGroup = metaGroupMember.getPartitionTable().partitionByPathTime(prefixPath, 0);
    }
    return partitionGroup;
  }

  public static void setVersionForSpecialPlan(PhysicalPlan plan, long currLogIndex)
      throws MetadataException {
    long majorVersion = 0;
    long minorVersion = 0;
    if (plan.getOperatorType() == null) {
      logger.warn("the plan={} have no operator, ignore it", plan);
      return;
    }
    switch (plan.getOperatorType()) {
      case SET_STORAGE_GROUP:
        majorVersion = currLogIndex;
        minorVersion = 0;
        plan.setMajorVersion(majorVersion);
        plan.setMinorVersion(minorVersion);
        break;
      case DELETE_STORAGE_GROUP:
        List<PartialPath> deletePathList = new ArrayList<>();
        for (PartialPath storageGroupPath : plan.getPaths()) {
          List<PartialPath> allRelatedStorageGroupPath =
              IoTDB.metaManager.getStorageGroupPaths(storageGroupPath);
          if (allRelatedStorageGroupPath.isEmpty()) {
            throw new PathNotExistException(storageGroupPath.getFullPath(), true);
          }
          for (PartialPath path : allRelatedStorageGroupPath) {
            StorageGroupMNode storageGroupMNode = IoTDB.metaManager.getStorageGroupNodeByPath(path);
            majorVersion = storageGroupMNode.getMajorVersion();
            minorVersion = storageGroupMNode.getMinorVersion();
            path.setMajorVersion(majorVersion);
            path.setMinorVersion(minorVersion);
            deletePathList.add(path);
          }
        }
        // replace the to be deleted paths
        ((DeleteStorageGroupPlan) plan).setPaths(deletePathList);
        break;
      case CREATE_TIMESERIES:
        PartialPath partialPath = ((CreateTimeSeriesPlan) plan).getPath();
        StorageGroupMNode storageGroupMNode =
            IoTDB.metaManager.getStorageGroupNodeByPath(partialPath);
        majorVersion = storageGroupMNode.getMajorVersion();
        minorVersion = currLogIndex;
        ((CreateTimeSeriesPlan) plan).getPath().setMajorVersion(majorVersion);
        ((CreateTimeSeriesPlan) plan).getPath().setMinorVersion(minorVersion);
        break;
      case ALTER_TIMESERIES:
        partialPath = ((AlterTimeSeriesPlan) plan).getPath();
        storageGroupMNode = IoTDB.metaManager.getStorageGroupNodeByPath(partialPath);
        majorVersion = storageGroupMNode.getMajorVersion();
        minorVersion = currLogIndex;
        ((AlterTimeSeriesPlan) plan).getPath().setMajorVersion(majorVersion);
        ((AlterTimeSeriesPlan) plan).getPath().setMinorVersion(minorVersion);
        break;
      case CREATE_ALIGNED_TIMESERIES:
        partialPath = ((CreateAlignedTimeSeriesPlan) plan).getPrefixPath();
        storageGroupMNode = IoTDB.metaManager.getStorageGroupNodeByPath(partialPath);
        majorVersion = storageGroupMNode.getMajorVersion();
        minorVersion = currLogIndex;
        ((CreateAlignedTimeSeriesPlan) plan).getPrefixPath().setMajorVersion(majorVersion);
        ((CreateAlignedTimeSeriesPlan) plan).getPrefixPath().setMinorVersion(minorVersion);
        break;
      case CREATE_MULTI_TIMESERIES:
      case DELETE_TIMESERIES:
        List<PartialPath> partialPaths = plan.getPaths();
        for (PartialPath path : partialPaths) {
          storageGroupMNode = IoTDB.metaManager.getStorageGroupNodeByPath(path);
          majorVersion = storageGroupMNode.getMajorVersion();
          path.setMajorVersion(storageGroupMNode.getMajorVersion());
          path.setMinorVersion(currLogIndex);
          logger.debug(
              "set majorVersion={}, minorVersion={} for path={}, plan={}",
              majorVersion,
              currLogIndex,
              path,
              plan);
        }
        break;
      default:
        // do nothing
        break;
    }
    logger.debug(
        "set majorVersion={}, minorVersion={} for plan={}", majorVersion, minorVersion, plan);
  }

  public static int getSlotByPathTimeWithSync(
      PartialPath prefixPath, MetaGroupMember metaGroupMember) throws MetadataException {
    int slot;
    try {
      PartialPath storageGroup = IoTDB.metaManager.getStorageGroupPath(prefixPath);
      slot =
          SlotPartitionTable.getSlotStrategy()
              .calculateSlotByPartitionNum(storageGroup.getFullPath(), 0, ClusterConstant.SLOT_NUM);
    } catch (StorageGroupNotSetException e) {
      // the storage group is not found locally, but may be found in the leader, retry after
      // synchronizing with the leader
      try {
        metaGroupMember.syncLeaderWithConsistencyCheck(true);
      } catch (CheckConsistencyException checkConsistencyException) {
        throw new MetadataException(checkConsistencyException.getMessage());
      }
      PartialPath storageGroup = IoTDB.metaManager.getStorageGroupPath(prefixPath);
      slot =
          SlotPartitionTable.getSlotStrategy()
              .calculateSlotByPartitionNum(storageGroup.getFullPath(), 0, ClusterConstant.SLOT_NUM);
    }
    return slot;
  }

  public static ByteBuffer serializeMigrationStatus(Map<PartitionGroup, Integer> migrationStatus) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeInt(migrationStatus.size());
      for (Entry<PartitionGroup, Integer> entry : migrationStatus.entrySet()) {
        entry.getKey().serialize(dataOutputStream);
        dataOutputStream.writeInt(entry.getValue());
      }
    } catch (IOException e) {
      // ignored
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public static Map<PartitionGroup, Integer> deserializeMigrationStatus(ByteBuffer buffer) {
    Map<PartitionGroup, Integer> migrationStatus = new HashMap<>();
    int size = buffer.getInt();
    while (size-- > 0) {
      PartitionGroup partitionGroup = new PartitionGroup();
      partitionGroup.deserialize(buffer);
      migrationStatus.put(partitionGroup, buffer.getInt());
    }
    return migrationStatus;
  }

  public static boolean nodeEqual(Node node1, Node node2) {
    ClusterNode clusterNode1 = new ClusterNode(node1);
    ClusterNode clusterNode2 = new ClusterNode(node2);
    return clusterNode1.equals(clusterNode2);
  }
}
