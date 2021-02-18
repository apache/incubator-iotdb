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

package org.apache.iotdb.cluster.metadata;

import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.LOG_FAIL_CONNECT;
import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.THREAD_POOL_SIZE;
import static org.apache.iotdb.cluster.query.ClusterPlanExecutor.waitForThreadPool;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.GetAllPathsResult;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("java:S1135") // ignore todos
public class CMManager extends MManager {

  private static final Logger logger = LoggerFactory.getLogger(CMManager.class);

  private ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
  // only cache the series who is writing, we need not to cache series who is reading
  // because the read is slow, so pull from remote is little cost comparing to the disk io
  private RemoteMetaCache mRemoteMetaCache;
  private MetaPuller metaPuller;
  private MetaGroupMember metaGroupMember;
  private Coordinator coordinator;

  private CMManager() {
    super();
    metaPuller = MetaPuller.getInstance();
    int remoteCacheSize = config.getmRemoteSchemaCacheSize();
    mRemoteMetaCache = new RemoteMetaCache(remoteCacheSize);
  }

  private static class MManagerHolder {

    private MManagerHolder() {
      // allowed to do nothing
    }

    private static final CMManager INSTANCE = new CMManager();
  }

  /**
   * we should not use this function in other place, but only in IoTDB class
   *
   * @return
   */
  public static CMManager getInstance() {
    return CMManager.MManagerHolder.INSTANCE;
  }

  @Override
  public String deleteTimeseries(PartialPath prefixPath) throws MetadataException {
    cacheLock.writeLock().lock();
    mRemoteMetaCache.removeItem(prefixPath);
    cacheLock.writeLock().unlock();
    return super.deleteTimeseries(prefixPath);
  }

  @Override
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    cacheLock.writeLock().lock();
    for (PartialPath storageGroup : storageGroups) {
      mRemoteMetaCache.removeItem(storageGroup);
    }
    cacheLock.writeLock().unlock();
    super.deleteStorageGroups(storageGroups);
  }

  @Override
  public TSDataType getSeriesType(PartialPath path) throws MetadataException {
    // try remote cache first
    try {
      cacheLock.readLock().lock();
      MeasurementMNode measurementMNode = mRemoteMetaCache.get(path);
      if (measurementMNode != null) {
        return measurementMNode.getSchema().getType();
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    // try local MTree
    TSDataType seriesType;
    try {
      seriesType = super.getSeriesType(path);
    } catch (PathNotExistException e) {
      // pull from remote node
      List<MeasurementSchema> schemas = metaPuller
          .pullMeasurementSchemas(Collections.singletonList(path));
      if (!schemas.isEmpty()) {
        MeasurementSchema measurementSchema = schemas.get(0);
        MeasurementMNode measurementMNode = new MeasurementMNode(null,
            measurementSchema.getMeasurementId(),
            measurementSchema, null);
        cacheMeta(path, measurementMNode);
        return schemas.get(0).getType();
      } else {
        throw e;
      }
    }
    return seriesType;
  }

  /**
   * the org.apache.iotdb.db.writelog.recover.logReplayer will call this to get schema after restart
   * we should retry to get schema util we get the schema
   *
   * @param deviceId
   * @param measurements
   * @return
   * @throws MetadataException
   */
  @Override
  public MeasurementMNode[] getMNodes(PartialPath deviceId, String[] measurements)
      throws MetadataException {
    try {
      return super.getMNodes(deviceId, measurements);
    } catch (MetadataException e) {
      // some measurements not exist in local
      // try cache
      MeasurementMNode[] measurementMNodes = new MeasurementMNode[measurements.length];
      int failedMeasurementIndex = getMNodesLocally(deviceId, measurements, measurementMNodes);
      if (failedMeasurementIndex == -1) {
        return measurementMNodes;
      }

      // will retry util get schema
      pullSeriesSchemas(deviceId, measurements);

      // try again
      failedMeasurementIndex = getMNodesLocally(deviceId, measurements, measurementMNodes);
      if (failedMeasurementIndex != -1) {
        throw new MetadataException(deviceId.getFullPath() + IoTDBConstant.PATH_SEPARATOR
            + measurements[failedMeasurementIndex] + " is not found");
      }
      return measurementMNodes;
    }
  }

  /**
   * @return -1 if all schemas are found, or the first index of the non-exist schema
   */
  private int getMNodesLocally(PartialPath deviceId, String[] measurements,
      MeasurementMNode[] measurementMNodes) {
    int failedMeasurementIndex = -1;
    cacheLock.readLock().lock();
    try {
      for (int i = 0; i < measurements.length && failedMeasurementIndex == -1; i++) {
        MeasurementMNode measurementMNode =
            mRemoteMetaCache.get(deviceId.concatNode(measurements[i]));
        if (measurementMNode == null) {
          failedMeasurementIndex = i;
        } else {
          measurementMNodes[i] = measurementMNode;
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return failedMeasurementIndex;
  }

  private void pullSeriesSchemas(PartialPath deviceId, String[] measurementList)
      throws MetadataException {
    List<PartialPath> schemasToPull = new ArrayList<>();
    for (String s : measurementList) {
      schemasToPull.add(deviceId.concatNode(s));
    }
    List<MeasurementSchema> schemas = metaPuller.pullMeasurementSchemas(schemasToPull);
    for (MeasurementSchema schema : schemas) {
      // TODO-Cluster: also pull alias?
      MeasurementMNode measurementMNode = new MeasurementMNode(null, schema.getMeasurementId(),
          schema, null);
      cacheMeta(deviceId.concatNode(schema.getMeasurementId()), measurementMNode);
    }
    logger.debug("Pulled {}/{} schemas from remote", schemas.size(), measurementList.length);
  }

  @Override
  public void cacheMeta(PartialPath seriesPath, MeasurementMNode measurementMNode) {
    cacheLock.writeLock().lock();
    mRemoteMetaCache.put(seriesPath, measurementMNode);
    cacheLock.writeLock().unlock();
  }

  @Override
  public void updateLastCache(PartialPath seriesPath, TimeValuePair timeValuePair,
      boolean highPriorityUpdate, Long latestFlushedTime,
      MeasurementMNode node) {
    cacheLock.writeLock().lock();
    try {
      MeasurementMNode measurementMNode = mRemoteMetaCache.get(seriesPath);
      if (measurementMNode != null) {
        measurementMNode.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
    // maybe local also has the timeseries
    super.updateLastCache(seriesPath, timeValuePair, highPriorityUpdate, latestFlushedTime,
        node);
  }

  @Override
  public TimeValuePair getLastCache(PartialPath seriesPath) {
    MeasurementMNode measurementMNode = mRemoteMetaCache.get(seriesPath);
    if (measurementMNode != null) {
      return measurementMNode.getCachedLast();
    }

    return super.getLastCache(seriesPath);
  }

  @Override
  public MNode getSeriesSchemasAndReadLockDevice(InsertPlan plan) throws MetadataException {
    MeasurementMNode[] measurementMNodes = new MeasurementMNode[plan.getMeasurements().length];
    try {
      MNode deviceNode = getDeviceNode(plan.getDeviceId());

      int nonExistSchemaIndex = getMNodesLocally(plan.getDeviceId(), plan.getMeasurements(),
          measurementMNodes);
      if (nonExistSchemaIndex == -1) {
        plan.setMeasurementMNodes(measurementMNodes);
        return deviceNode;
      }
    } catch (PathNotExistException exception) {
      // ignore, so we could try local MTree
    }
    // auto-create schema in IoTDBConfig is always disabled in the cluster version, and we have
    // another config in ClusterConfig to do this
    return super.getSeriesSchemasAndReadLockDevice(plan);
  }

  @Override
  public MeasurementSchema getSeriesSchema(PartialPath device, String measurement)
      throws MetadataException {
    try {
      MeasurementSchema measurementSchema = super.getSeriesSchema(device, measurement);
      if (measurementSchema != null) {
        return measurementSchema;
      }
    } catch (PathNotExistException e) {
      // not found in local
    }

    // try cache
    cacheLock.readLock().lock();
    try {
      MeasurementMNode measurementMNode = mRemoteMetaCache.get(device.concatNode(measurement));
      if (measurementMNode != null) {
        return measurementMNode.getSchema();
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    // pull from remote
    pullSeriesSchemas(device, new String[]{measurement});

    // try again
    cacheLock.readLock().lock();
    try {
      MeasurementMNode measurementMeta =
          mRemoteMetaCache.get(device.concatNode(measurement));
      if (measurementMeta != null) {
        return measurementMeta.getSchema();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return super.getSeriesSchema(device, measurement);
  }

  private static class RemoteMetaCache extends LRUCache<PartialPath, MeasurementMNode> {

    RemoteMetaCache(int cacheSize) {
      super(cacheSize);
    }

    @Override
    protected MeasurementMNode loadObjectByKey(PartialPath key) {
      return null;
    }

    @Override
    public synchronized void removeItem(PartialPath key) {
      cache.keySet().removeIf(s -> s.getFullPath().startsWith(key.getFullPath()));
    }

    @Override
    public synchronized MeasurementMNode get(PartialPath key) {
      try {
        return super.get(key);
      } catch (IOException e) {
        // not happening
        return null;
      }
    }
  }

  /**
   * create storage groups for CreateTimeseriesPlan and InsertPlan, also create timeseries for
   * InsertPlan. Only the two kind of plans can use this method.
   */
  public void createSchema(PhysicalPlan plan) throws MetadataException, CheckConsistencyException {
    // try to set storage group
    List<PartialPath> deviceIds;
    // only handle InsertPlan, CreateTimeSeriesPlan and CreateMultiTimeSeriesPlan currently
    if (plan instanceof InsertPlan && !(plan instanceof InsertMultiTabletPlan)) {
      deviceIds = Collections.singletonList(((InsertPlan) plan).getDeviceId());
    } else if (plan instanceof CreateTimeSeriesPlan) {
      deviceIds = Collections.singletonList(((CreateTimeSeriesPlan) plan).getPath());
    } else {
      deviceIds = plan.getPaths().stream().distinct().collect(Collectors.toList());
    }

    for (PartialPath deviceId : deviceIds) {
      createStorageGroup(deviceId);
    }

    // need to verify the storage group is created
    verifyCreatedSgSuccess(deviceIds, plan);

    if (plan instanceof InsertPlan) {
      // try to create timeseries
      boolean isAutoCreateTimeseriesSuccess = createTimeseries((InsertPlan) plan);
      if (!isAutoCreateTimeseriesSuccess) {
        throw new MetadataException(
            "Failed to create timeseries from InsertPlan automatically."
        );
      }
    }
  }

  @SuppressWarnings("squid:S3776")
  private void verifyCreatedSgSuccess(List<PartialPath> deviceIds, PhysicalPlan physicalPlan)
      throws MetadataException {
    long startTime = System.currentTimeMillis();
    boolean[] ready = new boolean[deviceIds.size()];
    Arrays.fill(ready, false);
    while (true) {
      boolean allReady = true;
      for (int i = 0; i < deviceIds.size(); i++) {
        if (ready[i]) {
          continue;
        }
        PartialPath storageGroupName = MetaUtils
            .getStorageGroupPathByLevel(deviceIds.get(i), IoTDBDescriptor.getInstance()
                .getConfig().getDefaultStorageGroupLevel());
        if (IoTDB.metaManager.isStorageGroup(storageGroupName)) {
          ready[i] = true;
        } else {
          allReady = false;
        }
      }

      if (allReady || System.currentTimeMillis() - startTime > ClusterDescriptor.getInstance()
          .getConfig().getConnectionTimeoutInMS()) {
        break;
      } else {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
          logger.debug("Failed to wait for creating sgs for plan {}", physicalPlan, e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Create storage group automatically for deviceId.
   *
   * @param deviceId
   */
  private void createStorageGroup(PartialPath deviceId) throws MetadataException {
    PartialPath storageGroupName = MetaUtils
        .getStorageGroupPathByLevel(deviceId, IoTDBDescriptor.getInstance()
            .getConfig().getDefaultStorageGroupLevel());
    SetStorageGroupPlan setStorageGroupPlan = new SetStorageGroupPlan(
        storageGroupName);
    TSStatus setStorageGroupResult =
        metaGroupMember.processNonPartitionedMetaPlan(setStorageGroupPlan);
    if (setStorageGroupResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode() &&
        setStorageGroupResult.getCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR
            .getStatusCode()) {
      throw new MetadataException(
          String.format("Status Code: %d, failed to set storage group %s",
              setStorageGroupResult.getCode(), storageGroupName)
      );
    }
  }

  /**
   * @param insertMultiTabletPlan the InsertMultiTabletPlan
   * @return true if all InsertTabletPlan in InsertMultiTabletPlan create timeseries success,
   * otherwise false
   */
  public boolean createTimeseries(InsertMultiTabletPlan insertMultiTabletPlan)
      throws CheckConsistencyException, IllegalPathException {
    boolean allSuccess = true;
    for (InsertTabletPlan insertTabletPlan : insertMultiTabletPlan.getInsertTabletPlanList()) {
      boolean success = createTimeseries(insertTabletPlan);
      allSuccess = allSuccess && success;
      if (!success) {
        logger.error("create timeseries for device={} failed", insertTabletPlan.getDeviceId());
      }
    }
    return allSuccess;
  }


  /**
   * Create timeseries automatically for an InsertPlan.
   *
   * @param insertPlan some of the timeseries in it are not created yet
   * @return true of all uncreated timeseries are created
   */
  public boolean createTimeseries(InsertPlan insertPlan)
      throws IllegalPathException, CheckConsistencyException {
    if (insertPlan instanceof InsertMultiTabletPlan) {
      return createTimeseries((InsertMultiTabletPlan) insertPlan);
    }

    List<String> seriesList = new ArrayList<>();
    PartialPath deviceId = insertPlan.getDeviceId();
    PartialPath storageGroupName;
    try {
      storageGroupName = MetaUtils
          .getStorageGroupPathByLevel(deviceId, IoTDBDescriptor.getInstance()
              .getConfig().getDefaultStorageGroupLevel());
    } catch (MetadataException e) {
      logger.error("Failed to infer storage group from deviceId {}", deviceId);
      return false;
    }
    for (String measurementId : insertPlan.getMeasurements()) {
      seriesList.add(deviceId.getFullPath() + TsFileConstant.PATH_SEPARATOR + measurementId);
    }
    PartitionGroup partitionGroup =
        metaGroupMember.getPartitionTable().route(storageGroupName.getFullPath(), 0);
    List<String> unregisteredSeriesList = getUnregisteredSeriesList(seriesList, partitionGroup);
    logger.debug("Unregisterd series of {} are {}", seriesList, unregisteredSeriesList);

    return createTimeseries(unregisteredSeriesList, seriesList, insertPlan);
  }

  /**
   * create timeseries from paths in "unregisteredSeriesList". If data types are provided by the
   * InsertPlan, use them, otherwise infer the types from the values. Use default encodings and
   * compressions of the corresponding data type.
   */
  private boolean createTimeseries(List<String> unregisteredSeriesList,
      List<String> seriesList, InsertPlan insertPlan) throws IllegalPathException {
    for (String seriesPath : unregisteredSeriesList) {
      int index = seriesList.indexOf(seriesPath);
      TSDataType dataType;
      // use data types in insertPlan if provided, otherwise infer them from the values
      if (insertPlan.getDataTypes() != null && insertPlan.getDataTypes()[index] != null) {
        dataType = insertPlan.getDataTypes()[index];
      } else {
        dataType = TypeInferenceUtils.getPredictedDataType(insertPlan instanceof InsertTabletPlan
            ? Array.get(((InsertTabletPlan) insertPlan).getColumns()[index], 0)
            : ((InsertRowPlan) insertPlan).getValues()[index], true);
      }
      // use default encoding and compression from the config
      TSEncoding encoding = getDefaultEncoding(dataType);
      CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan(
          new PartialPath(seriesPath),
          dataType, encoding, compressionType, null, null, null, null);
      // TODO-Cluster: add executeNonQueryBatch() to create the series in batch
      TSStatus result;
      try {
        result = coordinator.processPartitionedPlan(createTimeSeriesPlan);
      } catch (UnsupportedPlanException e) {
        logger.error("Failed to create timeseries {} automatically. Unsupported plan exception {} ",
            seriesPath, e.getMessage());
        return false;
      }
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode() &&
          result.getCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode() &&
          result.getCode() != TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
        logger.error("{} failed to execute create timeseries {}: {}", metaGroupMember.getThisNode(),
            seriesPath, result);
        return false;
      }
    }
    return true;
  }

  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  /**
   * To check which timeseries in the input list is unregistered from one node in "partitionGroup".
   */
  private List<String> getUnregisteredSeriesList(List<String> seriesList,
      PartitionGroup partitionGroup) throws CheckConsistencyException {
    if (partitionGroup.contains(metaGroupMember.getThisNode())) {
      return getUnregisteredSeriesListLocally(seriesList, partitionGroup);
    } else {
      return getUnregisteredSeriesListRemotely(seriesList, partitionGroup);
    }
  }

  private List<String> getUnregisteredSeriesListLocally(List<String> seriesList,
      PartitionGroup partitionGroup) throws CheckConsistencyException {
    DataGroupMember dataMember = metaGroupMember.getDataClusterServer()
        .getDataMember(partitionGroup.getHeader(), null, null);
    return dataMember.getLocalQueryExecutor().getUnregisteredTimeseries(seriesList);
  }

  private List<String> getUnregisteredSeriesListRemotely(List<String> seriesList,
      PartitionGroup partitionGroup) {
    for (Node node : partitionGroup) {
      try {
        List<String> result;
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          AsyncDataClient client = metaGroupMember.getClientProvider().getAsyncDataClient(node,
              RaftServer.getReadOperationTimeoutMS());
          result = SyncClientAdaptor
              .getUnregisteredMeasurements(client, partitionGroup.getHeader(), seriesList);
        } else {
          SyncDataClient syncDataClient =
              metaGroupMember.getClientProvider().getSyncDataClient(node,
                  RaftServer.getReadOperationTimeoutMS());
          result = syncDataClient.getUnregisteredTimeseries(partitionGroup.getHeader(), seriesList);
          ClientUtils.putBackSyncClient(syncDataClient);
        }

        if (result != null) {
          return result;
        }
      } catch (TException | IOException e) {
        logger.error("{}: cannot getting unregistered {} and other {} paths from {}",
            metaGroupMember.getName(),
            seriesList.get(0), seriesList.get(seriesList.size() - 1), node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("{}: getting unregistered series list {} ... {} is interrupted from {}",
            metaGroupMember.getName(),
            seriesList.get(0), seriesList.get(seriesList.size() - 1), node, e);
      }
    }
    return Collections.emptyList();
  }

  /**
   * Pull the all timeseries schemas of given prefixPaths from remote nodes. All prefixPaths must
   * contain a storage group. The pulled schemas will be cache in CMManager.
   *
   * @param ignoredGroup do not pull schema from the group to avoid backward dependency. If a user
   *                     send an insert request before registering schemas, then this method may
   *                     pull schemas from the same groups. If this method is called by an applier,
   *                     it holds the lock of LogManager, while the pulling thread may want this
   *                     lock too, resulting in a deadlock.
   */
  public void pullTimeSeriesSchemas(List<PartialPath> prefixPaths,
      Node ignoredGroup)
      throws MetadataException {
    logger.debug("{}: Pulling timeseries schemas of {}, ignored group {}",
        metaGroupMember.getName(), prefixPaths, ignoredGroup);
    // split the paths by the data groups that should hold them
    Map<PartitionGroup, List<String>> partitionGroupPathMap = new HashMap<>();
    for (PartialPath prefixPath : prefixPaths) {
      if (SQLConstant.RESERVED_TIME.equalsIgnoreCase(prefixPath.getFullPath())) {
        continue;
      }
      PartitionGroup partitionGroup = ClusterUtils
          .partitionByPathTimeWithSync(prefixPath, metaGroupMember);
      if (!partitionGroup.getHeader().equals(ignoredGroup)) {
        partitionGroupPathMap.computeIfAbsent(partitionGroup, g -> new ArrayList<>())
            .add(prefixPath.getFullPath());
      }
    }

    // pull timeseries schema from every group involved
    if (logger.isDebugEnabled()) {
      logger.debug("{}: pulling schemas of {} and other {} paths from {} groups",
          metaGroupMember.getName(),
          prefixPaths.get(0), prefixPaths.size() - 1,
          partitionGroupPathMap.size());
    }
    for (Entry<PartitionGroup, List<String>> partitionGroupListEntry : partitionGroupPathMap
        .entrySet()) {
      PartitionGroup partitionGroup = partitionGroupListEntry.getKey();
      List<String> paths = partitionGroupListEntry.getValue();
      pullTimeSeriesSchemas(partitionGroup, paths);
    }
  }

  /**
   * Pull timeseries schemas of "prefixPaths" from "partitionGroup". If this node is a member of
   * "partitionGroup", synchronize with the group leader and collect local schemas. Otherwise pull
   * schemas from one node in the group. The pulled schemas will be cached in CMManager.
   */
  private void pullTimeSeriesSchemas(PartitionGroup partitionGroup,
      List<String> prefixPaths) {
    if (partitionGroup.contains(metaGroupMember.getThisNode())) {
      // the node is in the target group, synchronize with leader should be enough
      metaGroupMember.getLocalDataMember(partitionGroup.getHeader(),
          "Pull timeseries of " + prefixPaths).syncLeader();
      return;
    }

    // pull schemas from a remote node
    PullSchemaRequest pullSchemaRequest = new PullSchemaRequest();
    pullSchemaRequest.setHeader(partitionGroup.getHeader());
    pullSchemaRequest.setPrefixPaths(prefixPaths);

    // decide the node access order with the help of QueryCoordinator
    List<Node> nodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : nodes) {
      if (tryPullTimeSeriesSchemas(node, pullSchemaRequest)) {
        break;
      }
    }
  }

  /**
   * send the PullSchemaRequest to "node" and cache the results in CMManager if they are
   * successfully returned.
   *
   * @return true if the pull succeeded, false otherwise
   */
  private boolean tryPullTimeSeriesSchemas(Node node, PullSchemaRequest request) {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Pulling timeseries schemas of {} and other {} paths from {}",
          metaGroupMember.getName(),
          request.getPrefixPaths().get(0), request.getPrefixPaths().size() - 1, node);
    }

    List<TimeseriesSchema> schemas = null;
    try {
      schemas = pullTimeSeriesSchemas(node, request);
    } catch (IOException | TException e) {
      logger
          .error("{}: Cannot pull timeseries schemas of {} and other {} paths from {}",
              metaGroupMember.getName(),
              request.getPrefixPaths().get(0), request.getPrefixPaths().size() - 1, node, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger
          .error("{}: Cannot pull timeseries schemas of {} and other {} paths from {}",
              metaGroupMember.getName(),
              request.getPrefixPaths().get(0), request.getPrefixPaths().size() - 1, node, e);
    }

    if (schemas != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Pulled {} timeseries schemas of {} and other {} paths from {} of {}",
            metaGroupMember.getName(), schemas.size(), request.getPrefixPaths().get(0),
            request.getPrefixPaths().size() - 1, node, request.getHeader());
      }
      for (TimeseriesSchema schema : schemas) {
        SchemaUtils.cacheTimeseriesSchema(schema);
      }
      return true;
    }
    return false;
  }

  /**
   * send a PullSchemaRequest to a node to pull TimeseriesSchemas, and return the pulled schema or
   * null if there was a timeout.
   */
  private List<TimeseriesSchema> pullTimeSeriesSchemas(Node node,
      PullSchemaRequest request) throws TException, InterruptedException, IOException {
    List<TimeseriesSchema> schemas;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = metaGroupMember.getClientProvider().getAsyncDataClient(node,
          RaftServer.getReadOperationTimeoutMS());
      schemas = SyncClientAdaptor.pullTimeseriesSchema(client, request);
    } else {
      SyncDataClient syncDataClient = metaGroupMember.getClientProvider().getSyncDataClient(node,
          RaftServer.getReadOperationTimeoutMS());
      PullSchemaResp pullSchemaResp = syncDataClient.pullTimeSeriesSchema(request);
      ByteBuffer buffer = pullSchemaResp.schemaBytes;
      int size = buffer.getInt();
      schemas = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        schemas.add(TimeseriesSchema.deserializeFrom(buffer));
      }
    }

    return schemas;
  }

  /**
   * Get the data types of "paths". If "aggregation" is not null, every path will use the
   * aggregation. First get types locally and if some paths does not exists, pull them from other
   * nodes.
   *
   * @return the left one of the pair is the column types (considering aggregation), the right one
   * of the pair is the measurement types (not considering aggregation)
   */
  public Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByPaths(List<PartialPath> pathStrs,
      String aggregation) throws MetadataException {
    try {
      return getSeriesTypesByPathsLocally(pathStrs, aggregation);
    } catch (PathNotExistException e) {
      // pull schemas remotely and cache them
      pullTimeSeriesSchemas(pathStrs, null);
      return getSeriesTypesByPathsLocally(pathStrs, aggregation);
    }
  }

  private Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByPathsLocally(
      List<PartialPath> pathStrs,
      String aggregation) throws MetadataException {
    List<TSDataType> measurementDataTypes = SchemaUtils.getSeriesTypesByPaths(pathStrs,
        (String) null);
    // if the aggregation function is null, the type of column in result set
    // is equal to the real type of the measurement
    if (aggregation == null) {
      return new Pair<>(measurementDataTypes, measurementDataTypes);
    } else {
      // if the aggregation function is not null,
      // we should recalculate the type of column in result set
      List<TSDataType> columnDataTypes = SchemaUtils
          .getAggregatedDataTypes(measurementDataTypes, aggregation);
      return new Pair<>(columnDataTypes, measurementDataTypes);
    }
  }

  /**
   * Get the data types of "paths". If "aggregations" is not null, each one of it correspond to one
   * in "paths". First get types locally and if some paths does not exists, pull them from other
   * nodes.
   *
   * @param aggregations nullable, when not null, correspond to "paths" one-to-one.
   * @return the left one of the pair is the column types (considering aggregation), the right one
   * of the pair is the measurement types (not considering aggregation)
   */
  public Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByPath(List<PartialPath> paths,
      List<String> aggregations) throws
      MetadataException {
    try {
      return getSeriesTypesByPathLocally(paths, aggregations);
    } catch (PathNotExistException e) {
      return getSeriesTypesByPathRemotely(paths, aggregations);
    }
  }

  /**
   * get data types of the given paths considering the aggregations from CMManger.
   *
   * @param aggregations nullable, when not null, correspond to "paths" one-to-one.
   * @return the left one of the pair is the column types (considering aggregation), the right one
   * of the pair is the measurement types (not considering aggregation)
   */
  private Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByPathLocally(
      List<PartialPath> paths,
      List<String> aggregations) throws MetadataException {
    List<TSDataType> measurementDataTypes = SchemaUtils.getSeriesTypesByPaths(paths);
    // if the aggregation function is null, the type of column in result set
    // is equal to the real type of the measurement
    if (aggregations == null) {
      return new Pair<>(measurementDataTypes, measurementDataTypes);
    } else {
      // if the aggregation function is not null,
      // we should recalculate the type of column in result set
      List<TSDataType> columnDataTypes = SchemaUtils.getSeriesTypesByPaths(paths, aggregations);
      return new Pair<>(columnDataTypes, measurementDataTypes);
    }
  }

  /**
   * pull schemas from remote nodes and cache them, then get data types of the given paths
   * considering the aggregations from CMManger.
   *
   * @param aggregations nullable, when not null, correspond to "paths" one-to-one.
   * @return the left one of the pair is the column types (considering aggregation), the right one
   * of the pair is the measurement types (not considering aggregation)
   */
  private Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByPathRemotely(
      List<PartialPath> paths,
      List<String> aggregations) throws MetadataException {
    // pull schemas remotely and cache them
    pullTimeSeriesSchemas(paths, null);

    return getSeriesTypesByPathLocally(paths, aggregations);
  }

  /**
   * Get all devices after removing wildcards in the path
   *
   * @param originPath a path potentially with wildcard
   * @return all paths after removing wildcards in the path
   */
  public Set<PartialPath> getMatchedDevices(PartialPath originPath) throws MetadataException {
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
    Map<String, String> sgPathMap = determineStorageGroup(originPath);
    Set<PartialPath> ret = getMatchedDevices(sgPathMap);
    logger.debug("The devices of path {} are {}", originPath, ret);
    return ret;
  }

  /**
   * Split the paths by the data group they belong to and query them from the groups separately.
   *
   * @param sgPathMap the key is the storage group name and the value is the path to be queried with
   *                  storage group added
   * @return a collection of all queried paths
   */
  private List<PartialPath> getMatchedPaths(Map<String, String> sgPathMap, boolean withAlias)
      throws MetadataException {
    List<PartialPath> result = new ArrayList<>();
    // split the paths by the data group they belong to
    Map<PartitionGroup, List<String>> groupPathMap = new HashMap<>();
    for (Entry<String, String> sgPathEntry : sgPathMap.entrySet()) {
      String storageGroupName = sgPathEntry.getKey();
      PartialPath pathUnderSG = new PartialPath(sgPathEntry.getValue());
      // find the data group that should hold the timeseries schemas of the storage group
      PartitionGroup partitionGroup = metaGroupMember.getPartitionTable()
          .route(storageGroupName, 0);
      if (partitionGroup.contains(metaGroupMember.getThisNode())) {
        // this node is a member of the group, perform a local query after synchronizing with the
        // leader
        metaGroupMember.getLocalDataMember(partitionGroup.getHeader()).syncLeader();
        List<PartialPath> allTimeseriesName = getMatchedPathsLocally(pathUnderSG, withAlias);
        logger.debug("{}: get matched paths of {} locally, result {}", metaGroupMember.getName(),
            partitionGroup, allTimeseriesName);
        result.addAll(allTimeseriesName);
      } else {
        // batch the queries of the same group to reduce communication
        groupPathMap.computeIfAbsent(partitionGroup, p -> new ArrayList<>())
            .add(pathUnderSG.getFullPath());
      }
    }

    // query each data group separately
    for (Entry<PartitionGroup, List<String>> partitionGroupPathEntry : groupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupPathEntry.getKey();
      List<String> pathsToQuery = partitionGroupPathEntry.getValue();
      result.addAll(getMatchedPaths(partitionGroup, pathsToQuery, withAlias));
    }

    return result;
  }

  private List<PartialPath> getMatchedPathsLocally(PartialPath partialPath, boolean withAlias)
      throws MetadataException {
    if (!withAlias) {
      return getAllTimeseriesPath(partialPath);
    } else {
      return super.getAllTimeseriesPathWithAlias(partialPath, -1, -1).left;
    }
  }

  private List<PartialPath> getMatchedPaths(PartitionGroup partitionGroup,
      List<String> pathsToQuery, boolean withAlias)
      throws MetadataException {
    // choose the node with lowest latency or highest throughput
    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      try {
        List<PartialPath> paths = getMatchedPaths(node, partitionGroup.getHeader(), pathsToQuery,
            withAlias);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: get matched paths of {} and other {} paths from {} in {}, result {}",
              metaGroupMember.getName(), pathsToQuery.get(0), pathsToQuery.size() - 1, node,
              partitionGroup.getHeader(), paths);
        }
        if (paths != null) {
          // a non-null result contains correct result even if it is empty, so query next group
          return paths;
        }
      } catch (IOException | TException e) {
        throw new MetadataException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetadataException(e);
      }
    }
    logger.warn("Cannot get paths of {} from {}", pathsToQuery, partitionGroup);
    return Collections.emptyList();
  }

  @SuppressWarnings("java:S1168") // null and empty list are different
  private List<PartialPath> getMatchedPaths(Node node, Node header, List<String> pathsToQuery,
      boolean withAlias)
      throws IOException, TException, InterruptedException {
    GetAllPathsResult result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = metaGroupMember.getClientProvider().getAsyncDataClient(node,
          RaftServer.getReadOperationTimeoutMS());
      result = SyncClientAdaptor.getAllPaths(client, header,
          pathsToQuery, withAlias);
    } else {
      SyncDataClient syncDataClient = metaGroupMember.getClientProvider().getSyncDataClient(node,
          RaftServer.getReadOperationTimeoutMS());
      result = syncDataClient.getAllPaths(header, pathsToQuery, withAlias);
      ClientUtils.putBackSyncClient(syncDataClient);
    }

    if (result != null) {
      // paths may be empty, implying that the group does not contain matched paths, so we do not
      // need to query other nodes in the group
      List<PartialPath> partialPaths = new ArrayList<>();
      for (int i = 0; i < result.paths.size(); i++) {
        try {
          PartialPath partialPath = new PartialPath(result.paths.get(i));
          if (withAlias) {
            partialPath.setMeasurementAlias(result.aliasList.get(i));
          }
          partialPaths.add(partialPath);
        } catch (IllegalPathException e) {
          // ignore
        }
      }
      return partialPaths;
    } else {
      // a null implies a network failure, so we have to query other nodes in the group
      return null;
    }
  }

  /**
   * Split the paths by the data group they belong to and query them from the groups separately.
   *
   * @param sgPathMap the key is the storage group name and the value is the path to be queried with
   *                  storage group added
   * @return a collection of all queried devices
   */
  private Set<PartialPath> getMatchedDevices(Map<String, String> sgPathMap)
      throws MetadataException {
    Set<PartialPath> result = new HashSet<>();
    // split the paths by the data group they belong to
    Map<PartitionGroup, List<String>> groupPathMap = new HashMap<>();
    for (Entry<String, String> sgPathEntry : sgPathMap.entrySet()) {
      String storageGroupName = sgPathEntry.getKey();
      PartialPath pathUnderSG = new PartialPath(sgPathEntry.getValue());
      // find the data group that should hold the timeseries schemas of the storage group
      PartitionGroup partitionGroup = metaGroupMember.getPartitionTable()
          .route(storageGroupName, 0);
      if (partitionGroup.contains(metaGroupMember.getThisNode())) {
        // this node is a member of the group, perform a local query after synchronizing with the
        // leader
        metaGroupMember.getLocalDataMember(partitionGroup.getHeader()).syncLeader();
        Set<PartialPath> allDevices = getDevices(pathUnderSG);
        logger.debug("{}: get matched paths of {} locally, result {}", metaGroupMember.getName(),
            partitionGroup,
            allDevices);
        result.addAll(allDevices);
      } else {
        // batch the queries of the same group to reduce communication
        groupPathMap.computeIfAbsent(partitionGroup, p -> new ArrayList<>())
            .add(pathUnderSG.getFullPath());
      }
    }

    // query each data group separately
    for (Entry<PartitionGroup, List<String>> partitionGroupPathEntry : groupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupPathEntry.getKey();
      List<String> pathsToQuery = partitionGroupPathEntry.getValue();

      result.addAll(getMatchedDevices(partitionGroup, pathsToQuery));
    }

    return result;
  }

  private Set<PartialPath> getMatchedDevices(PartitionGroup partitionGroup,
      List<String> pathsToQuery)
      throws MetadataException {
    // choose the node with lowest latency or highest throughput
    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      try {
        Set<String> paths = getMatchedDevices(node, partitionGroup.getHeader(), pathsToQuery);
        logger.debug("{}: get matched paths of {} from {}, result {}", metaGroupMember.getName(),
            partitionGroup,
            node, paths);
        if (paths != null) {
          // query next group
          Set<PartialPath> partialPaths = new HashSet<>();
          for (String path : paths) {
            partialPaths.add(new PartialPath(path));
          }
          return partialPaths;
        }
      } catch (IOException | TException e) {
        throw new MetadataException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetadataException(e);
      }
    }
    logger.warn("Cannot get paths of {} from {}", pathsToQuery, partitionGroup);
    return Collections.emptySet();
  }

  private Set<String> getMatchedDevices(Node node, Node header, List<String> pathsToQuery)
      throws IOException, TException, InterruptedException {
    Set<String> paths;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = metaGroupMember.getClientProvider().getAsyncDataClient(node,
          RaftServer.getReadOperationTimeoutMS());
      paths = SyncClientAdaptor.getAllDevices(client, header,
          pathsToQuery);
    } else {
      SyncDataClient syncDataClient = metaGroupMember.getClientProvider().getSyncDataClient(node,
          RaftServer.getReadOperationTimeoutMS());
      paths = syncDataClient.getAllDevices(header, pathsToQuery);
      ClientUtils.putBackSyncClient(syncDataClient);
    }
    return paths;
  }

  /**
   * Similar to method getAllTimeseriesPath(), but return Path with alias alias.
   */
  @Override
  public Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(PartialPath prefixPath,
      int limit, int offset) throws MetadataException {
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
    Map<String, String> sgPathMap = determineStorageGroup(prefixPath);
    List<PartialPath> result = getMatchedPaths(sgPathMap, true);

    int skippedOffset = 0;
    // apply offset and limit
    if (offset > 0 && result.size() > offset) {
      skippedOffset = offset;
      result = result.subList(offset, result.size());
    } else if (offset > 0) {
      skippedOffset = result.size();
      result = Collections.emptyList();
    }
    if (limit > 0 && result.size() > limit) {
      result = result.subList(0, limit);
    }
    logger.debug("The paths of path {} are {}", prefixPath, result);

    return new Pair<>(result, skippedOffset);
  }

  /**
   * Get all paths after removing wildcards in the path
   *
   * @param originPath a path potentially with wildcard
   * @return all paths after removing wildcards in the path
   */
  public List<PartialPath> getMatchedPaths(PartialPath originPath) throws MetadataException {
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
    Map<String, String> sgPathMap = determineStorageGroup(originPath);
    List<PartialPath> ret = getMatchedPaths(sgPathMap, false);
    logger.debug("The paths of path {} are {}", originPath, ret);
    return ret;
  }

  /**
   * Get all paths after removing wildcards in the path
   *
   * @param originalPaths a list of paths, potentially with wildcard
   * @return a pair of path lists, the first are the existing full paths, the second are invalid
   * original paths
   */
  public Pair<List<PartialPath>, List<PartialPath>> getMatchedPaths(
      List<PartialPath> originalPaths) {
    ConcurrentSkipListSet<PartialPath> fullPaths = new ConcurrentSkipListSet<>();
    ConcurrentSkipListSet<PartialPath> nonExistPaths = new ConcurrentSkipListSet<>();
    ExecutorService getAllPathsService = Executors
        .newFixedThreadPool(metaGroupMember.getPartitionTable().getGlobalGroups().size());
    for (PartialPath pathStr : originalPaths) {
      getAllPathsService.submit(() -> {
        try {
          List<PartialPath> fullPathStrs = getMatchedPaths(pathStr);
          if (fullPathStrs.isEmpty()) {
            nonExistPaths.add(pathStr);
            logger.debug("Path {} is not found.", pathStr);
          } else {
            fullPaths.addAll(fullPathStrs);
          }
        } catch (MetadataException e) {
          logger.error("Failed to get full paths of the prefix path: {} because", pathStr, e);
        }
      });
    }
    getAllPathsService.shutdown();
    try {
      getAllPathsService
          .awaitTermination(RaftServer.getReadOperationTimeoutMS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for get all paths services to stop", e);
    }
    return new Pair<>(new ArrayList<>(fullPaths), new ArrayList<>(nonExistPaths));
  }

  /**
   * Get the local paths that match any path in "paths". The result is not deduplicated.
   *
   * @param paths paths potentially contain wildcards
   */
  public List<String> getAllPaths(List<String> paths) throws MetadataException {
    List<String> ret = new ArrayList<>();
    for (String path : paths) {
      getAllTimeseriesPath(
          new PartialPath(path)).stream().map(PartialPath::getFullPath).forEach(ret::add);
    }
    return ret;
  }

  /**
   * Get the local devices that match any path in "paths". The result is deduplicated.
   *
   * @param paths paths potentially contain wildcards
   */
  public Set<String> getAllDevices(List<String> paths) throws MetadataException {
    Set<String> results = new HashSet<>();
    for (String path : paths) {
      getAllTimeseriesPath(
          new PartialPath(path)).stream().map(PartialPath::getFullPath).forEach(results::add);
    }
    return results;
  }

  /**
   * Get the nodes of a prefix "path" at "nodeLevel". The method currently requires strong
   * consistency.
   *
   * @param path
   * @param nodeLevel
   */
  public List<String> getNodeList(String path, int nodeLevel)
      throws MetadataException {
    return getNodesList(new PartialPath(path), nodeLevel).stream().map(PartialPath::getFullPath)
        .collect(Collectors.toList());
  }

  public Set<String> getChildNodePathInNextLevel(String path)
      throws MetadataException {
    return getChildNodePathInNextLevel(new PartialPath(path));
  }

  /**
   * Replace partial paths (paths not containing measurements), and abstract paths (paths containing
   * wildcards) with full paths.
   */
  public void convertToFullPaths(PhysicalPlan plan)
      throws PathNotExistException {
    Pair<List<PartialPath>, List<PartialPath>> getMatchedPathsRet =
        getMatchedPaths(plan.getPaths());
    List<PartialPath> fullPaths = getMatchedPathsRet.left;
    List<PartialPath> nonExistPath = getMatchedPathsRet.right;
    plan.setPaths(fullPaths);
    if (!nonExistPath.isEmpty()) {
      throw new PathNotExistException(nonExistPath.stream().map(PartialPath::getFullPath).collect(
          Collectors.toList()));
    }
  }

  @Override
  protected MeasurementMNode getMeasurementMNode(MNode deviceMNode, String measurement, TSDataType dataType)
      throws MetadataException {
    MNode child = deviceMNode.getChild(measurement);
    if (child == null) {
      child = mRemoteMetaCache.get(deviceMNode.getPartialPath().concatNode(measurement));
    }

    if (child == null) {
      return null;
    }
    return changeMNodeToMeasurementMNode(child, dataType);
  }

  public List<ShowTimeSeriesResult> showLocalTimeseries(ShowTimeSeriesPlan plan,
      QueryContext context) throws MetadataException {
    return super.showTimeseries(plan, context);
  }

  @Override
  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException {
    ConcurrentSkipListSet<ShowTimeSeriesResult> resultSet = new ConcurrentSkipListSet<>();
    ExecutorService pool = new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 0,
        TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    List<PartitionGroup> globalGroups = metaGroupMember.getPartitionTable().getGlobalGroups();

    int limit = plan.getLimit() == 0 ? Integer.MAX_VALUE : plan.getLimit();
    int offset = plan.getOffset();
    // do not use limit and offset in sub-queries unless offset is 0, otherwise the results are
    // not combinable
    if (offset != 0) {
      plan.setLimit(0);
      plan.setOffset(0);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Fetch timeseries schemas of {} from {} groups", plan.getPath(),
          globalGroups.size());
    }

    List<Future<Void>> futureList = new ArrayList<>();
    for (PartitionGroup group : globalGroups) {
      futureList.add(pool.submit(() -> {
        try {
          showTimeseries(group, plan, resultSet, context);
        } catch (CheckConsistencyException e) {
          logger.error("Cannot get show timeseries result of {} from {}", plan, group);
        }
        return null;
      }));
    }

    waitForThreadPool(futureList, pool, "showTimeseries()");
    List<ShowTimeSeriesResult> showTimeSeriesResults = applyShowTimeseriesLimitOffset(resultSet,
        limit, offset);
    logger.debug("Show {} has {} results", plan.getPath(), showTimeSeriesResults.size());
    return showTimeSeriesResults;
  }

  private List<ShowTimeSeriesResult> applyShowTimeseriesLimitOffset(
      ConcurrentSkipListSet<ShowTimeSeriesResult> resultSet,
      int limit, int offset) {
    List<ShowTimeSeriesResult> showTimeSeriesResults = new ArrayList<>();
    Iterator<ShowTimeSeriesResult> iterator = resultSet.iterator();
    while (iterator.hasNext() && limit > 0) {
      if (offset > 0) {
        offset--;
        iterator.next();
      } else {
        limit--;
        showTimeSeriesResults.add(iterator.next());
      }
    }

    return showTimeSeriesResults;
  }

  private void showTimeseries(PartitionGroup group, ShowTimeSeriesPlan plan,
      Set<ShowTimeSeriesResult> resultSet, QueryContext context)
      throws CheckConsistencyException, MetadataException {
    if (group.contains(metaGroupMember.getThisNode())) {
      showLocalTimeseries(group, plan, resultSet, context);
    } else {
      showRemoteTimeseries(group, plan, resultSet);
    }
  }

  private void showLocalTimeseries(PartitionGroup group, ShowTimeSeriesPlan plan,
      Set<ShowTimeSeriesResult> resultSet, QueryContext context)
      throws CheckConsistencyException, MetadataException {
    Node header = group.getHeader();
    DataGroupMember localDataMember = metaGroupMember.getLocalDataMember(header);
    localDataMember.syncLeaderWithConsistencyCheck(false);
    try {
      List<ShowTimeSeriesResult> localResult = super.showTimeseries(plan, context);
      resultSet.addAll(localResult);
      logger.debug("Fetched {} schemas of {} from {}", localResult.size(), plan.getPath(), group);
    } catch (MetadataException e) {
      logger
          .error("Cannot execute show timeseries plan  {} from {} locally.", plan, group);
      throw e;
    }
  }

  private void showRemoteTimeseries(PartitionGroup group, ShowTimeSeriesPlan plan,
      Set<ShowTimeSeriesResult> resultSet) {
    ByteBuffer resultBinary = null;
    for (Node node : group) {
      try {
        resultBinary = showRemoteTimeseries(node, group, plan);
        if (resultBinary != null) {
          break;
        }
      } catch (IOException e) {
        logger.error(LOG_FAIL_CONNECT, node, e);
      } catch (TException e) {
        logger.error("Error occurs when getting timeseries schemas in node {}.", node, e);
      } catch (InterruptedException e) {
        logger.error("Interrupted when getting timeseries schemas in node {}.", node, e);
        Thread.currentThread().interrupt();
      }
    }

    if (resultBinary != null) {
      int size = resultBinary.getInt();
      logger.debug("Fetched {} schemas of {} from {}", size, plan.getPath(), group);
      for (int i = 0; i < size; i++) {
        resultSet.add(ShowTimeSeriesResult.deserialize(resultBinary));
      }
    } else {
      logger.error("Failed to execute show timeseries {} in group: {}.", plan, group);
    }
  }

  private ByteBuffer showRemoteTimeseries(Node node, PartitionGroup group, ShowTimeSeriesPlan plan)
      throws IOException, TException, InterruptedException {
    ByteBuffer resultBinary;

    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = metaGroupMember
          .getClientProvider().getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
      resultBinary = SyncClientAdaptor.getAllMeasurementSchema(client, group.getHeader(),
          plan);
    } else {
      SyncDataClient syncDataClient = metaGroupMember
          .getClientProvider().getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
      plan.serialize(dataOutputStream);
      resultBinary = syncDataClient.getAllMeasurementSchema(group.getHeader(),
          ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      ClientUtils.putBackSyncClient(syncDataClient);
    }
    return resultBinary;
  }

  public GetAllPathsResult getAllPaths(List<String> paths, boolean withAlias)
      throws MetadataException {
    List<String> retPaths = new ArrayList<>();
    List<String> alias = null;
    if (withAlias) {
      alias = new ArrayList<>();
    }

    if (withAlias) {
      for (String path : paths) {
        List<PartialPath> allTimeseriesPathWithAlias = super
            .getAllTimeseriesPathWithAlias(new PartialPath(path), -1, -1).left;
        for (PartialPath timeseriesPathWithAlias : allTimeseriesPathWithAlias) {
          retPaths.add(timeseriesPathWithAlias.getFullPath());
          alias.add(timeseriesPathWithAlias.getMeasurementAlias());
        }
      }
    } else {
      retPaths = getAllPaths(paths);
    }

    GetAllPathsResult getAllPathsResult = new GetAllPathsResult();
    getAllPathsResult.setPaths(retPaths);
    getAllPathsResult.setAliasList(alias);
    return getAllPathsResult;
  }

  @Override
  public PartialPath getStorageGroupPath(PartialPath path) throws StorageGroupNotSetException {
    try {
      return super.getStorageGroupPath(path);
    } catch (StorageGroupNotSetException e) {
      metaGroupMember.syncLeader();
      return super.getStorageGroupPath(path);
    }
  }
}
