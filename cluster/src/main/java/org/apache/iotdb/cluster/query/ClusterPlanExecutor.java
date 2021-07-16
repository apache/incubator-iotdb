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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.query.dataset.ClusterAlignByDeviceDataSet;
import org.apache.iotdb.cluster.query.filter.SlotSgFilter;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.executor.IQueryRouter;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterPlanExecutor extends PlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterPlanExecutor.class);
  private MetaGroupMember metaGroupMember;

  public static final int THREAD_POOL_SIZE = 6;
  public static final String LOG_FAIL_CONNECT = "Failed to connect to node: {}";

  public ClusterPlanExecutor(MetaGroupMember metaGroupMember) throws QueryProcessException {
    super();
    this.metaGroupMember = metaGroupMember;
    this.queryRouter = new ClusterQueryRouter(metaGroupMember);
  }

  @Override
  public QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException, QueryFilterOptimizationException,
          QueryProcessException, MetadataException, InterruptedException {
    if (queryPlan instanceof QueryPlan) {
      logger.debug("Executing a query: {}", queryPlan);
      return processDataQuery((QueryPlan) queryPlan, context);
    } else if (queryPlan instanceof ShowPlan) {
      try {
        metaGroupMember.syncLeaderWithConsistencyCheck(false);
      } catch (CheckConsistencyException e) {
        throw new QueryProcessException(e.getMessage());
      }
      return processShowQuery((ShowPlan) queryPlan, context);
    } else if (queryPlan instanceof AuthorPlan) {
      try {
        metaGroupMember.syncLeaderWithConsistencyCheck(false);
      } catch (CheckConsistencyException e) {
        throw new QueryProcessException(e.getMessage());
      }
      return processAuthorQuery((AuthorPlan) queryPlan);
    } else {
      throw new QueryProcessException(String.format("Unrecognized query plan %s", queryPlan));
    }
  }

  @Override
  @TestOnly
  protected List<PartialPath> getPathsName(PartialPath path) throws MetadataException {
    return ((CMManager) IoTDB.metaManager).getMatchedPaths(path);
  }

  @Override
  protected int getPathsNum(PartialPath path) throws MetadataException {
    return getNodesNumInGivenLevel(path, -1);
  }

  @Override
  protected int getNodesNumInGivenLevel(PartialPath path, int level) throws MetadataException {
    // make sure this node knows all storage groups
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e);
    }
    // get all storage groups this path may belong to
    // the key is the storage group name and the value is the path to be queried with storage group
    // added, e.g:
    // "root.*" will be translated into:
    // "root.group1" -> "root.group1.*", "root.group2" -> "root.group2.*" ...
    Map<String, String> sgPathMap = IoTDB.metaManager.determineStorageGroup(path);
    if (sgPathMap.isEmpty()) {
      throw new PathNotExistException(path.getFullPath());
    }
    logger.debug("The storage groups of path {} are {}", path, sgPathMap.keySet());
    int ret;
    try {
      ret = getPathCount(sgPathMap, level);
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e);
    }
    logger.debug("The number of paths satisfying {}@{} is {}", path, level, ret);
    return ret;
  }

  /**
   * Split the paths by the data group they belong to and query them from the groups separately.
   *
   * @param sgPathMap the key is the storage group name and the value is the path to be queried with
   *     storage group added
   * @param level the max depth to match the pattern, -1 means matching the whole pattern
   * @return the number of paths that match the pattern at given level
   * @throws MetadataException
   */
  private int getPathCount(Map<String, String> sgPathMap, int level)
      throws MetadataException, CheckConsistencyException {
    AtomicInteger result = new AtomicInteger();
    // split the paths by the data group they belong to
    Map<PartitionGroup, List<String>> groupPathMap = new HashMap<>();
    for (Entry<String, String> sgPathEntry : sgPathMap.entrySet()) {
      String storageGroupName = sgPathEntry.getKey();
      PartialPath pathUnderSG = new PartialPath(sgPathEntry.getValue());
      // find the data group that should hold the timeseries schemas of the storage group
      PartitionGroup partitionGroup =
          metaGroupMember.getPartitionTable().route(storageGroupName, 0);
      if (partitionGroup.contains(metaGroupMember.getThisNode())) {
        // this node is a member of the group, perform a local query after synchronizing with the
        // leader
        metaGroupMember
            .getLocalDataMember(partitionGroup.getHeader())
            .syncLeaderWithConsistencyCheck(false);
        int localResult = getLocalPathCount(pathUnderSG, level);
        logger.debug(
            "{}: get path count of {} locally, result {}",
            metaGroupMember.getName(),
            partitionGroup,
            localResult);
        result.addAndGet(localResult);
      } else {
        // batch the queries of the same group to reduce communication
        groupPathMap
            .computeIfAbsent(partitionGroup, p -> new ArrayList<>())
            .add(pathUnderSG.getFullPath());
      }
    }
    if (groupPathMap.isEmpty()) {
      return result.get();
    }

    ExecutorService remoteQueryThreadPool = Executors.newFixedThreadPool(groupPathMap.size());
    List<Future<Void>> remoteFutures = new ArrayList<>();
    // query each data group separately
    for (Entry<PartitionGroup, List<String>> partitionGroupPathEntry : groupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupPathEntry.getKey();
      List<String> pathsToQuery = partitionGroupPathEntry.getValue();
      remoteFutures.add(
          remoteQueryThreadPool.submit(
              () -> {
                try {
                  result.addAndGet(getRemotePathCount(partitionGroup, pathsToQuery, level));
                } catch (MetadataException e) {
                  logger.warn(
                      "Cannot get remote path count of {} from {}",
                      pathsToQuery,
                      partitionGroup,
                      e);
                }
                return null;
              }));
    }
    waitForThreadPool(remoteFutures, remoteQueryThreadPool, "getPathCount()");

    return result.get();
  }

  private int getLocalPathCount(PartialPath path, int level) throws MetadataException {
    int localResult;
    if (level == -1) {
      localResult = IoTDB.metaManager.getAllTimeseriesCount(path);
    } else {
      localResult = IoTDB.metaManager.getNodesCountInGivenLevel(path, level);
    }
    return localResult;
  }

  private int getRemotePathCount(
      PartitionGroup partitionGroup, List<String> pathsToQuery, int level)
      throws MetadataException {
    // choose the node with lowest latency or highest throughput
    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      try {
        Integer count;
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          AsyncDataClient client =
              metaGroupMember
                  .getClientProvider()
                  .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
          client.setTimeout(RaftServer.getReadOperationTimeoutMS());
          count =
              SyncClientAdaptor.getPathCount(
                  client, partitionGroup.getHeader(), pathsToQuery, level);
        } else {
          try (SyncDataClient syncDataClient =
              metaGroupMember
                  .getClientProvider()
                  .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {
            try {
              count = syncDataClient.getPathCount(partitionGroup.getHeader(), pathsToQuery, level);
            } catch (TException e) {
              // the connection may be broken, close it to avoid it being reused
              syncDataClient.getInputProtocol().getTransport().close();
              throw e;
            }
          }
        }
        logger.debug(
            "{}: get path count of {} from {}, result {}",
            metaGroupMember.getName(),
            partitionGroup,
            node,
            count);
        if (count != null) {
          return count;
        }
      } catch (IOException | TException e) {
        throw new MetadataException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetadataException(e);
      }
    }
    logger.warn("Cannot get paths of {} from {}", pathsToQuery, partitionGroup);
    return 0;
  }

  @Override
  protected Set<PartialPath> getDevices(PartialPath path) throws MetadataException {
    // make sure this node knows all storage groups
    ((CMManager) IoTDB.metaManager).syncMetaLeader();
    return ((CMManager) IoTDB.metaManager).getMatchedDevices(path);
  }

  @Override
  protected List<PartialPath> getNodesList(PartialPath schemaPattern, int level)
      throws MetadataException {

    ConcurrentSkipListSet<PartialPath> nodeSet = new ConcurrentSkipListSet<>();
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    List<Future<Void>> futureList = new ArrayList<>();
    for (PartitionGroup group : metaGroupMember.getPartitionTable().getGlobalGroups()) {
      futureList.add(
          pool.submit(
              () -> {
                List<PartialPath> paths;
                paths = getNodesList(group, schemaPattern, level);
                if (paths != null) {
                  nodeSet.addAll(paths);
                } else {
                  logger.error(
                      "Fail to get node list of {}@{} from {}", schemaPattern, level, group);
                }
                return null;
              }));
    }
    waitForThreadPool(futureList, pool, "getNodesList()");
    return new ArrayList<>(nodeSet);
  }

  private List<PartialPath> getNodesList(PartitionGroup group, PartialPath schemaPattern, int level)
      throws CheckConsistencyException, MetadataException {
    if (group.contains(metaGroupMember.getThisNode())) {
      return getLocalNodesList(group, schemaPattern, level);
    } else {
      return getRemoteNodesList(group, schemaPattern, level);
    }
  }

  private List<PartialPath> getLocalNodesList(
      PartitionGroup group, PartialPath schemaPattern, int level)
      throws CheckConsistencyException, MetadataException {
    Node header = group.getHeader();
    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    localDataMember.syncLeaderWithConsistencyCheck(false);
    try {
      return IoTDB.metaManager.getNodesList(
          schemaPattern,
          level,
          new SlotSgFilter(
              ((SlotPartitionTable) metaGroupMember.getPartitionTable()).getNodeSlots(header)));
    } catch (MetadataException e) {
      logger.error(
          "Cannot not get node list of {}@{} from {} locally", schemaPattern, level, group);
      throw e;
    }
  }

  private List<PartialPath> getRemoteNodesList(
      PartitionGroup group, PartialPath schemaPattern, int level) {
    List<String> paths = null;
    for (Node node : group) {
      try {
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          AsyncDataClient client =
              metaGroupMember
                  .getClientProvider()
                  .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
          paths =
              SyncClientAdaptor.getNodeList(
                  client, group.getHeader(), schemaPattern.getFullPath(), level);
        } else {
          try (SyncDataClient syncDataClient =
              metaGroupMember
                  .getClientProvider()
                  .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {
            try {
              paths =
                  syncDataClient.getNodeList(group.getHeader(), schemaPattern.getFullPath(), level);
            } catch (TException e) {
              // the connection may be broken, close it to avoid it being reused
              syncDataClient.getInputProtocol().getTransport().close();
              throw e;
            }
          }
        }
        if (paths != null) {
          break;
        }
      } catch (IOException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (TException e) {
        logger.error("Error occurs when getting node lists in node {}.", node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting node lists in node {}.", node, e);
        Thread.currentThread().interrupt();
      }
    }
    return PartialPath.fromStringList(paths);
  }

  @Override
  protected Set<String> getNodeNextChildren(PartialPath path) throws MetadataException {
    ConcurrentSkipListSet<String> resultSet = new ConcurrentSkipListSet<>();
    List<PartitionGroup> globalGroups = metaGroupMember.getPartitionTable().getGlobalGroups();
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    List<Future<Void>> futureList = new ArrayList<>();
    for (PartitionGroup group : globalGroups) {
      futureList.add(
          pool.submit(
              () -> {
                Set<String> nextChildrenNodes = null;
                try {
                  nextChildrenNodes = getChildNodeInNextLevel(group, path);
                } catch (CheckConsistencyException e) {
                  logger.error("Fail to get next children nodes of {} from {}", path, group, e);
                }
                if (nextChildrenNodes != null) {
                  resultSet.addAll(nextChildrenNodes);
                } else {
                  logger.error("Fail to get next children nodes of {} from {}", path, group);
                }
                return null;
              }));
    }
    waitForThreadPool(futureList, pool, "getChildNodeInNextLevel()");
    return resultSet;
  }

  private Set<String> getChildNodeInNextLevel(PartitionGroup group, PartialPath path)
      throws CheckConsistencyException {
    if (group.contains(metaGroupMember.getThisNode())) {
      return getLocalChildNodeInNextLevel(group, path);
    } else {
      return getRemoteChildNodeInNextLevel(group, path);
    }
  }

  private Set<String> getLocalChildNodeInNextLevel(PartitionGroup group, PartialPath path)
      throws CheckConsistencyException {
    Node header = group.getHeader();
    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    localDataMember.syncLeaderWithConsistencyCheck(false);
    try {
      return IoTDB.metaManager.getChildNodeInNextLevel(path);
    } catch (MetadataException e) {
      logger.error("Cannot not get next children nodes of {} from {} locally", path, group);
      return Collections.emptySet();
    }
  }

  private Set<String> getRemoteChildNodeInNextLevel(PartitionGroup group, PartialPath path) {
    Set<String> nextChildrenNodes = null;
    for (Node node : group) {
      try {
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          AsyncDataClient client =
              metaGroupMember
                  .getClientProvider()
                  .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
          nextChildrenNodes =
              SyncClientAdaptor.getChildNodeInNextLevel(
                  client, group.getHeader(), path.getFullPath());
        } else {
          try (SyncDataClient syncDataClient =
              metaGroupMember
                  .getClientProvider()
                  .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {
            try {
              nextChildrenNodes =
                  syncDataClient.getChildNodeInNextLevel(group.getHeader(), path.getFullPath());
            } catch (TException e) {
              // the connection may be broken, close it to avoid it being reused
              syncDataClient.getInputProtocol().getTransport().close();
              throw e;
            }
          }
        }
        if (nextChildrenNodes != null) {
          break;
        }
      } catch (IOException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (TException e) {
        logger.error("Error occurs when getting node lists in node {}.", node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting node lists in node {}.", node, e);
        Thread.currentThread().interrupt();
      }
    }
    return nextChildrenNodes;
  }

  @Override
  protected Set<String> getPathNextChildren(PartialPath path) throws MetadataException {
    ConcurrentSkipListSet<String> resultSet = new ConcurrentSkipListSet<>();
    ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    List<Future<Void>> futureList = new ArrayList<>();

    for (PartitionGroup group : metaGroupMember.getPartitionTable().getGlobalGroups()) {
      futureList.add(
          pool.submit(
              () -> {
                Set<String> nextChildren = null;
                try {
                  nextChildren = getNextChildren(group, path);
                } catch (CheckConsistencyException e) {
                  logger.error("Fail to get next children of {} from {}", path, group, e);
                }
                if (nextChildren != null) {
                  resultSet.addAll(nextChildren);
                } else {
                  logger.error("Fail to get next children of {} from {}", path, group);
                }
                return null;
              }));
    }
    waitForThreadPool(futureList, pool, "getPathNextChildren()");
    return resultSet;
  }

  public static void waitForThreadPool(
      List<Future<Void>> futures, ExecutorService pool, String methodName)
      throws MetadataException {
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        logger.error("Unexpected interruption when waiting for {}", methodName, e);
        Thread.currentThread().interrupt();
      } catch (RuntimeException | ExecutionException e) {
        throw new MetadataException(e);
      }
    }

    pool.shutdown();
    try {
      pool.awaitTermination(RaftServer.getReadOperationTimeoutMS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for {}", methodName, e);
    }
  }

  private Set<String> getNextChildren(PartitionGroup group, PartialPath path)
      throws CheckConsistencyException {
    if (group.contains(metaGroupMember.getThisNode())) {
      return getLocalNextChildren(group, path);
    } else {
      return getRemoteNextChildren(group, path);
    }
  }

  private Set<String> getLocalNextChildren(PartitionGroup group, PartialPath path)
      throws CheckConsistencyException {
    Node header = group.getHeader();
    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    localDataMember.syncLeaderWithConsistencyCheck(false);
    try {
      return IoTDB.metaManager.getChildNodePathInNextLevel(path);
    } catch (MetadataException e) {
      logger.error("Cannot not get next children of {} from {} locally", path, group);
      return Collections.emptySet();
    }
  }

  private Set<String> getRemoteNextChildren(PartitionGroup group, PartialPath path) {
    Set<String> nextChildren = null;
    for (Node node : group) {
      try {
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          AsyncDataClient client =
              metaGroupMember
                  .getClientProvider()
                  .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
          nextChildren =
              SyncClientAdaptor.getNextChildren(client, group.getHeader(), path.getFullPath());
        } else {
          try (SyncDataClient syncDataClient =
              metaGroupMember
                  .getClientProvider()
                  .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {
            try {
              nextChildren =
                  syncDataClient.getChildNodePathInNextLevel(group.getHeader(), path.getFullPath());
            } catch (TException e) {
              // the connection may be broken, close it to avoid it being reused
              syncDataClient.getInputProtocol().getTransport().close();
              throw e;
            }
          }
        }
        if (nextChildren != null) {
          break;
        }
      } catch (IOException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (TException e) {
        logger.error("Error occurs when getting node lists in node {}.", node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting node lists in node {}.", node, e);
        Thread.currentThread().interrupt();
      }
    }
    return nextChildren;
  }

  @Override
  protected List<StorageGroupMNode> getAllStorageGroupNodes() {
    try {
      metaGroupMember.syncLeader(null);
    } catch (CheckConsistencyException e) {
      logger.warn("Failed to check consistency.", e);
    }
    return IoTDB.metaManager.getAllStorageGroupNodes();
  }

  @Override
  protected AlignByDeviceDataSet getAlignByDeviceDataSet(
      AlignByDevicePlan plan, QueryContext context, IQueryRouter router) {
    return new ClusterAlignByDeviceDataSet(plan, context, router);
  }

  @Override
  protected void loadConfiguration(LoadConfigurationPlan plan) throws QueryProcessException {
    switch (plan.getLoadConfigurationPlanType()) {
      case GLOBAL:
        IoTDBDescriptor.getInstance().loadHotModifiedProps(plan.getIoTDBProperties());
        ClusterDescriptor.getInstance().loadHotModifiedProps(plan.getClusterProperties());
        break;
      case LOCAL:
        IoTDBDescriptor.getInstance().loadHotModifiedProps();
        ClusterDescriptor.getInstance().loadHotModifiedProps();
        break;
      default:
        throw new QueryProcessException(
            String.format(
                "Unrecognized load configuration plan type: %s",
                plan.getLoadConfigurationPlanType()));
    }
  }

  @Override
  public void delete(DeletePlan deletePlan) throws QueryProcessException {
    if (deletePlan.getPaths().isEmpty()) {
      logger.info("TimeSeries list to be deleted is empty.");
      return;
    }
    for (PartialPath path : deletePlan.getPaths()) {
      delete(
          path,
          deletePlan.getDeleteStartTime(),
          deletePlan.getDeleteEndTime(),
          deletePlan.getIndex());
    }
  }

  @Override
  public void delete(PartialPath path, long startTime, long endTime, long planIndex)
      throws QueryProcessException {
    try {
      StorageEngine.getInstance().delete(path, startTime, endTime, planIndex);
    } catch (StorageEngineException e) {
      throw new QueryProcessException(e);
    }
  }
}
