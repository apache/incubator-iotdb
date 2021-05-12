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
package org.apache.iotdb.db.engine;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.ServerConfigConsistent;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy.DirectFlushPolicy;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.VirtualStorageGroupManager;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.ShutdownException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryFileManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class StorageEngine implements IService {
  private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long TTL_CHECK_INTERVAL = 60 * 1000L;
  private static final long TIER_MIGRATION_CHECK_INTERVAL = 60 * 60 * 1000L;

  /**
   * Time range for dividing storage group, the time unit is the same with IoTDB's
   * TimestampPrecision
   */
  @ServerConfigConsistent private static long timePartitionInterval = -1;
  /** whether enable data partition if disabled, all data belongs to partition 0 */
  @ServerConfigConsistent private static boolean enablePartition = config.isEnablePartition();

  private final boolean enableMemControl = config.isEnableMemControl();

  /**
   * a folder (system/storage_groups/ by default) that persist system info. Each Storage Processor
   * will have a subfolder under the systemDir.
   */
  private final String systemDir;
  /** storage group name -> storage group processor */
  private final ConcurrentHashMap<PartialPath, VirtualStorageGroupManager> processorMap =
      new ConcurrentHashMap<>();

  private AtomicBoolean isAllSgReady = new AtomicBoolean(false);

  private ExecutorService recoverAllSgThreadPool;
  private ScheduledExecutorService ttlCheckThread;
  private ScheduledExecutorService tierMigrationCheckThread;
  private TsFileFlushPolicy fileFlushPolicy = new DirectFlushPolicy();
  private ExecutorService recoveryThreadPool;
  // add customized listeners here for flush and close events
  private List<CloseFileListener> customCloseFileListeners = new ArrayList<>();
  private List<FlushListener> customFlushListeners = new ArrayList<>();

  private StorageEngine() {
    systemDir = FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";

    // build time Interval to divide time partition
    if (!enablePartition) {
      timePartitionInterval = Long.MAX_VALUE;
    } else {
      initTimePartition();
    }

    // create systemDir
    try {
      FileUtils.forceMkdir(SystemFileFactory.INSTANCE.getFile(systemDir));
    } catch (IOException e) {
      throw new StorageEngineFailureException(e);
    }
    // recover upgrade process
    UpgradeUtils.recoverUpgrade();

    recover();
  }

  public static StorageEngine getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static void initTimePartition() {
    timePartitionInterval =
        convertMilliWithPrecision(
            IoTDBDescriptor.getInstance().getConfig().getPartitionInterval() * 1000L);
  }

  public static long convertMilliWithPrecision(long milliTime) {
    long result = milliTime;
    String timePrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    switch (timePrecision) {
      case "ns":
        result = milliTime * 1000_000L;
        break;
      case "us":
        result = milliTime * 1000L;
        break;
      default:
        break;
    }
    return result;
  }

  public static long getTimePartitionInterval() {
    if (timePartitionInterval == -1) {
      initTimePartition();
    }
    return timePartitionInterval;
  }

  @TestOnly
  public static void setTimePartitionInterval(long timePartitionInterval) {
    StorageEngine.timePartitionInterval = timePartitionInterval;
  }

  public static long getTimePartition(long time) {
    return enablePartition ? time / timePartitionInterval : 0;
  }

  public static boolean isEnablePartition() {
    return enablePartition;
  }

  @TestOnly
  public static void setEnablePartition(boolean enablePartition) {
    StorageEngine.enablePartition = enablePartition;
  }

  /** block insertion if the insertion is rejected by memory control */
  public static void blockInsertionIfReject(TsFileProcessor tsFileProcessor)
      throws WriteProcessRejectException {
    long startTime = System.currentTimeMillis();
    while (SystemInfo.getInstance().isRejected()) {
      if (tsFileProcessor != null && tsFileProcessor.shouldFlush()) {
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(config.getCheckPeriodWhenInsertBlocked());
        if (System.currentTimeMillis() - startTime > config.getMaxWaitingTimeWhenInsertBlocked()) {
          throw new WriteProcessRejectException(
              "System rejected over " + (System.currentTimeMillis() - startTime) + "ms");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public boolean isAllSgReady() {
    return isAllSgReady.get();
  }

  public void setAllSgReady(boolean allSgReady) {
    isAllSgReady.set(allSgReady);
  }

  public void recover() {
    setAllSgReady(false);
    recoveryThreadPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(), "Recovery-Thread-Pool");
    recoverAllSgThreadPool = IoTDBThreadPoolFactory.newSingleThreadExecutor("Begin-Recovery-Pool");
    recoverAllSgThreadPool.submit(this::recoverAllSgs);
  }

  private void recoverAllSgs() {
    /*
     * recover all storage group processors.
     */
    List<Future<Void>> futures = new ArrayList<>();
    recoverStorageGroupProcessor(futures);

    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StorageEngineFailureException("StorageEngine failed to recover.", e);
      }
    }
    recoveryThreadPool.shutdown();
    setAllSgReady(true);
  }

  /**
   * recover logic storage group processor
   *
   * @param futures recover future task
   */
  private void recoverStorageGroupProcessor(List<Future<Void>> futures) {
    List<StorageGroupMNode> sgNodes = IoTDB.metaManager.getAllStorageGroupNodes();
    for (StorageGroupMNode storageGroup : sgNodes) {
      futures.add(
          recoveryThreadPool.submit(
              () -> {
                try {
                  // for recovery in test
                  VirtualStorageGroupManager virtualStorageGroupManager =
                      processorMap.computeIfAbsent(
                          storageGroup.getPartialPath(), id -> new VirtualStorageGroupManager());

                  virtualStorageGroupManager.recover(storageGroup);

                  logger.info(
                      "Storage Group Processor {} is recovered successfully",
                      storageGroup.getFullPath());
                } catch (Exception e) {
                  logger.error(
                      "meet error when recovering storage group: {}",
                      storageGroup.getFullPath(),
                      e);
                }
                return null;
              }));
    }
  }

  @Override
  public void start() {
    ttlCheckThread = Executors.newSingleThreadScheduledExecutor();
    ttlCheckThread.scheduleAtFixedRate(
        this::checkTTL, TTL_CHECK_INTERVAL, TTL_CHECK_INTERVAL, TimeUnit.MILLISECONDS);

    tierMigrationCheckThread = Executors.newSingleThreadScheduledExecutor();
    tierMigrationCheckThread.scheduleAtFixedRate(
        this::checkTierMigration,
        TIER_MIGRATION_CHECK_INTERVAL,
        TIER_MIGRATION_CHECK_INTERVAL,
        TimeUnit.MILLISECONDS);
  }

  private void checkTTL() {
    try {
      for (VirtualStorageGroupManager processor : processorMap.values()) {
        processor.checkTTL();
      }
    } catch (ConcurrentModificationException e) {
      // ignore
    } catch (Exception e) {
      logger.error("An error occurred when checking TTL", e);
    }

    if (isAllSgReady.get() && !recoverAllSgThreadPool.isShutdown()) {
      recoverAllSgThreadPool.shutdownNow();
    }
  }

  private void checkTierMigration() {
    try {
      for (VirtualStorageGroupManager processor : processorMap.values()) {
        processor.checkTierMigration();
      }
    } catch (Exception e) {
      logger.error("An error occurred when checking Tier Migration", e);
    }
  }

  @Override
  public void stop() {
    syncCloseAllProcessor();
    if (ttlCheckThread != null) {
      ttlCheckThread.shutdownNow();
      try {
        ttlCheckThread.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("TTL check thread still doesn't exit after 60s");
        Thread.currentThread().interrupt();
        throw new StorageEngineFailureException(
            "StorageEngine failed to stop because of " + "ttlCheckThread.", e);
      }
    }
    if (tierMigrationCheckThread != null) {
      tierMigrationCheckThread.shutdownNow();
      try {
        tierMigrationCheckThread.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("Tier migration check thread still doesn't exit after 60s");
        Thread.currentThread().interrupt();
        throw new StorageEngineFailureException(
            "StorageEngine failed to stop because of " + "tierMigrationCheckThread.", e);
      }
    }
    recoveryThreadPool.shutdownNow();
    if (!recoverAllSgThreadPool.isShutdown()) {
      recoverAllSgThreadPool.shutdownNow();
      try {
        recoverAllSgThreadPool.awaitTermination(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("recoverAllSgThreadPool thread still doesn't exit after 60s");
        Thread.currentThread().interrupt();
        throw new StorageEngineFailureException(
            "StorageEngine failed to stop because of " + "recoverAllSgThreadPool.", e);
      }
    }
    for (PartialPath storageGroup : IoTDB.metaManager.getAllStorageGroupPaths()) {
      this.releaseWalDirectByteBufferPoolInOneStorageGroup(storageGroup);
    }
    this.reset();
  }

  @Override
  public void shutdown(long milliseconds) throws ShutdownException {
    try {
      forceCloseAllProcessor();
    } catch (TsFileProcessorException e) {
      throw new ShutdownException(e);
    }
    if (ttlCheckThread != null) {
      ttlCheckThread.shutdownNow();
      try {
        ttlCheckThread.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("TTL check thread still doesn't exit after 30s");
        Thread.currentThread().interrupt();
      }
    }
    if (tierMigrationCheckThread != null) {
      tierMigrationCheckThread.shutdownNow();
      try {
        tierMigrationCheckThread.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("Tier migration check thread still doesn't exit after 30s");
        Thread.currentThread().interrupt();
      }
    }
    recoveryThreadPool.shutdownNow();
    this.reset();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.STORAGE_ENGINE_SERVICE;
  }

  /**
   * This method is for sync, delete tsfile or sth like them, just get storage group directly by sg
   * name
   *
   * @param path storage group path
   * @return storage group processor
   */
  public StorageGroupProcessor getProcessorDirectly(PartialPath path)
      throws StorageEngineException {
    PartialPath storageGroupPath;
    try {
      StorageGroupMNode storageGroupMNode = IoTDB.metaManager.getStorageGroupNodeByPath(path);
      storageGroupPath = storageGroupMNode.getPartialPath();
      return getStorageGroupProcessorByPath(storageGroupPath, storageGroupMNode);
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * This method is for insert and query or sth like them, this may get a virtual storage group
   *
   * @param path device path
   * @return storage group processor
   */
  public StorageGroupProcessor getProcessor(PartialPath path) throws StorageEngineException {
    try {
      StorageGroupMNode storageGroupMNode = IoTDB.metaManager.getStorageGroupNodeByPath(path);
      return getStorageGroupProcessorByPath(path, storageGroupMNode);
    } catch (StorageGroupProcessorException | MetadataException e) {
      throw new StorageEngineException(e);
    }
  }

  /**
   * get storage group processor by device path
   *
   * @param devicePath path of the device
   * @param storageGroupMNode mnode of the storage group, we need synchronize this to avoid
   *     modification in mtree
   * @return found or new storage group processor
   */
  @SuppressWarnings("java:S2445")
  // actually storageGroupMNode is a unique object on the mtree, synchronize it is reasonable
  private StorageGroupProcessor getStorageGroupProcessorByPath(
      PartialPath devicePath, StorageGroupMNode storageGroupMNode)
      throws StorageGroupProcessorException, StorageEngineException {
    VirtualStorageGroupManager virtualStorageGroupManager =
        processorMap.get(storageGroupMNode.getPartialPath());
    if (virtualStorageGroupManager == null) {
      // if finish recover
      if (isAllSgReady.get()) {
        waitAllSgReady(devicePath);
        synchronized (storageGroupMNode) {
          virtualStorageGroupManager = processorMap.get(storageGroupMNode.getPartialPath());
          if (virtualStorageGroupManager == null) {
            virtualStorageGroupManager = new VirtualStorageGroupManager();
            processorMap.put(storageGroupMNode.getPartialPath(), virtualStorageGroupManager);
          }
        }
      } else {
        // not finished recover, refuse the request
        throw new StorageEngineException(
            "the sg "
                + storageGroupMNode.getPartialPath()
                + " may not ready now, please wait and retry later",
            TSStatusCode.STORAGE_GROUP_NOT_READY.getStatusCode());
      }
    }
    return virtualStorageGroupManager.getProcessor(devicePath, storageGroupMNode);
  }

  /**
   * build a new storage group processor
   *
   * @param virtualStorageGroupId virtual storage group id e.g. 1
   * @param logicalStorageGroupName logical storage group name e.g. root.sg1
   */
  public StorageGroupProcessor buildNewStorageGroupProcessor(
      PartialPath logicalStorageGroupName,
      StorageGroupMNode storageGroupMNode,
      String virtualStorageGroupId)
      throws StorageGroupProcessorException {
    StorageGroupProcessor processor;
    logger.info(
        "construct a processor instance, the storage group is {}, Thread is {}",
        logicalStorageGroupName,
        Thread.currentThread().getId());
    processor =
        new StorageGroupProcessor(
            systemDir + File.separator + logicalStorageGroupName,
            virtualStorageGroupId,
            fileFlushPolicy,
            storageGroupMNode.getFullPath());
    processor.setDataTTL(storageGroupMNode.getDataTTL());
    processor.setCustomFlushListeners(customFlushListeners);
    processor.setCustomCloseFileListeners(customCloseFileListeners);
    return processor;
  }

  private void waitAllSgReady(PartialPath storageGroupPath) throws StorageEngineException {
    if (isAllSgReady.get()) {
      return;
    }

    long waitStart = System.currentTimeMillis();
    long waited = 0L;
    final long MAX_WAIT = 5000L;
    while (!isAllSgReady.get() && waited < MAX_WAIT) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StorageEngineException(e);
      }
      waited = System.currentTimeMillis() - waitStart;
    }

    if (!isAllSgReady.get()) {
      // not finished recover, refuse the request
      throw new StorageEngineException(
          "the sg " + storageGroupPath + " may not ready now, please wait and retry later",
          TSStatusCode.STORAGE_GROUP_NOT_READY.getStatusCode());
    }
  }

  /** This function is just for unit test. */
  public synchronized void reset() {
    for (VirtualStorageGroupManager virtualStorageGroupManager : processorMap.values()) {
      virtualStorageGroupManager.reset();
    }
  }

  /**
   * insert an InsertRowPlan to a storage group.
   *
   * @param insertRowPlan physical plan of insertion
   */
  public void insert(InsertRowPlan insertRowPlan) throws StorageEngineException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessException e) {
        throw new StorageEngineException(e);
      }
    }
    StorageGroupProcessor storageGroupProcessor = getProcessor(insertRowPlan.getDeviceId());

    try {
      storageGroupProcessor.insert(insertRowPlan);
      if (config.isEnableStatMonitor()) {
        try {
          StorageGroupMNode storageGroupMNode =
              IoTDB.metaManager.getStorageGroupNodeByPath(insertRowPlan.getDeviceId());
          updateMonitorStatistics(
              processorMap.get(storageGroupMNode.getPartialPath()), insertRowPlan);
        } catch (MetadataException e) {
          logger.error("failed to record status", e);
        }
      }
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  public void insert(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan)
      throws StorageEngineException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessException e) {
        throw new StorageEngineException(e);
      }
    }
    StorageGroupProcessor storageGroupProcessor =
        getProcessor(insertRowsOfOneDevicePlan.getDeviceId());

    // TODO monitor: update statistics
    try {
      storageGroupProcessor.insert(insertRowsOfOneDevicePlan);
    } catch (WriteProcessException e) {
      throw new StorageEngineException(e);
    }
  }

  /** insert a InsertTabletPlan to a storage group */
  public void insertTablet(InsertTabletPlan insertTabletPlan)
      throws StorageEngineException, BatchProcessException {
    if (enableMemControl) {
      try {
        blockInsertionIfReject(null);
      } catch (WriteProcessRejectException e) {
        TSStatus[] results = new TSStatus[insertTabletPlan.getRowCount()];
        Arrays.fill(results, RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT));
        throw new BatchProcessException(results);
      }
    }
    StorageGroupProcessor storageGroupProcessor;
    try {
      storageGroupProcessor = getProcessor(insertTabletPlan.getDeviceId());
    } catch (StorageEngineException e) {
      throw new StorageEngineException(
          String.format(
              "Get StorageGroupProcessor of device %s " + "failed", insertTabletPlan.getDeviceId()),
          e);
    }

    storageGroupProcessor.insertTablet(insertTabletPlan);

    if (config.isEnableStatMonitor()) {
      try {
        StorageGroupMNode storageGroupMNode =
            IoTDB.metaManager.getStorageGroupNodeByPath(insertTabletPlan.getDeviceId());
        updateMonitorStatistics(
            processorMap.get(storageGroupMNode.getPartialPath()), insertTabletPlan);
      } catch (MetadataException e) {
        logger.error("failed to record status", e);
      }
    }
  }

  private void updateMonitorStatistics(
      VirtualStorageGroupManager virtualStorageGroupManager, InsertPlan insertPlan) {
    StatMonitor monitor = StatMonitor.getInstance();
    int successPointsNum =
        insertPlan.getMeasurements().length - insertPlan.getFailedMeasurementNumber();
    // update to storage group statistics
    virtualStorageGroupManager.updateMonitorSeriesValue(successPointsNum);
    // update to global statistics
    monitor.updateStatGlobalValue(successPointsNum);
  }

  /** flush command Sync asyncCloseOneProcessor all file node processors. */
  public void syncCloseAllProcessor() {
    logger.info("Start closing all storage group processor");
    for (VirtualStorageGroupManager processor : processorMap.values()) {
      processor.syncCloseAllWorkingTsFileProcessors();
    }
  }

  public void forceCloseAllProcessor() throws TsFileProcessorException {
    logger.info("Start force closing all storage group processor");
    for (VirtualStorageGroupManager processor : processorMap.values()) {
      processor.forceCloseAllWorkingTsFileProcessors();
    }
  }

  public void closeStorageGroupProcessor(
      PartialPath storageGroupPath, boolean isSeq, boolean isSync) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }

    VirtualStorageGroupManager virtualStorageGroupManager = processorMap.get(storageGroupPath);
    virtualStorageGroupManager.closeStorageGroupProcessor(isSeq, isSync);
  }

  /**
   * @param storageGroupPath the storage group name
   * @param partitionId the partition id
   * @param isSeq is sequence tsfile or unsequence tsfile
   * @param isSync close tsfile synchronously or asynchronously
   * @throws StorageGroupNotSetException
   */
  public void closeStorageGroupProcessor(
      PartialPath storageGroupPath, long partitionId, boolean isSeq, boolean isSync)
      throws StorageGroupNotSetException {
    if (!processorMap.containsKey(storageGroupPath)) {
      throw new StorageGroupNotSetException(storageGroupPath.getFullPath());
    }

    VirtualStorageGroupManager virtualStorageGroupManager = processorMap.get(storageGroupPath);
    virtualStorageGroupManager.closeStorageGroupProcessor(partitionId, isSeq, isSync);
  }

  public void delete(PartialPath path, long startTime, long endTime, long planIndex)
      throws StorageEngineException {
    try {
      List<PartialPath> sgPaths = IoTDB.metaManager.searchAllRelatedStorageGroups(path);
      for (PartialPath storageGroupPath : sgPaths) {
        // storage group has no data
        if (!processorMap.containsKey(storageGroupPath)) {
          continue;
        }

        PartialPath newPath = path.alterPrefixPath(storageGroupPath);
        processorMap.get(storageGroupPath).delete(newPath, startTime, endTime, planIndex);
      }
    } catch (IOException | MetadataException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /** delete data of timeseries "{deviceId}.{measurementId}" */
  public void deleteTimeseries(PartialPath path, long planIndex) throws StorageEngineException {
    try {
      List<PartialPath> sgPaths = IoTDB.metaManager.searchAllRelatedStorageGroups(path);
      for (PartialPath storageGroupPath : sgPaths) {
        // storage group has no data
        if (!processorMap.containsKey(storageGroupPath)) {
          continue;
        }

        PartialPath newPath = path.alterPrefixPath(storageGroupPath);
        processorMap
            .get(storageGroupPath)
            .delete(newPath, Long.MIN_VALUE, Long.MAX_VALUE, planIndex);
      }
    } catch (IOException | MetadataException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  /** query data. */
  public QueryDataSource query(
      SingleSeriesExpression seriesExpression,
      QueryContext context,
      QueryFileManager filePathsManager)
      throws StorageEngineException, QueryProcessException {
    PartialPath fullPath = (PartialPath) seriesExpression.getSeriesPath();
    PartialPath deviceId = fullPath.getDevicePath();
    StorageGroupProcessor storageGroupProcessor = getProcessor(deviceId);
    return storageGroupProcessor.query(
        fullPath, context, filePathsManager, seriesExpression.getFilter());
  }

  /**
   * count all Tsfiles which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded
   */
  public int countUpgradeFiles() {
    int totalUpgradeFileNum = 0;
    for (VirtualStorageGroupManager virtualStorageGroupManager : processorMap.values()) {
      totalUpgradeFileNum += virtualStorageGroupManager.countUpgradeFiles();
    }
    return totalUpgradeFileNum;
  }

  /**
   * upgrade all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void upgradeAll() throws StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException(
          "Current system mode is read only, does not support file upgrade");
    }
    for (VirtualStorageGroupManager virtualStorageGroupManager : processorMap.values()) {
      virtualStorageGroupManager.upgradeAll();
    }
  }

  /**
   * merge all storage groups.
   *
   * @throws StorageEngineException StorageEngineException
   */
  public void mergeAll(boolean isFullMerge) throws StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new StorageEngineException("Current system mode is read only, does not support merge");
    }

    for (VirtualStorageGroupManager virtualStorageGroupManager : processorMap.values()) {
      virtualStorageGroupManager.mergeAll(isFullMerge);
    }
  }

  /**
   * delete all data files (both memory data and file on disk) in a storage group. It is used when
   * there is no timeseries (which are all deleted) in this storage group)
   */
  public void deleteAllDataFilesInOneStorageGroup(PartialPath storageGroupPath) {
    if (processorMap.containsKey(storageGroupPath)) {
      syncDeleteDataFiles(storageGroupPath);
    }
  }

  private void syncDeleteDataFiles(PartialPath storageGroupPath) {
    logger.info("Force to delete the data in storage group processor {}", storageGroupPath);
    processorMap.get(storageGroupPath).syncDeleteDataFiles();
  }

  /** release all the allocated non-heap */
  public void releaseWalDirectByteBufferPoolInOneStorageGroup(PartialPath storageGroupPath) {
    if (processorMap.containsKey(storageGroupPath)) {
      processorMap.get(storageGroupPath).releaseWalDirectByteBufferPool();
    }
  }

  /** delete all data of storage groups' timeseries. */
  public synchronized boolean deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    syncCloseAllProcessor();
    for (PartialPath storageGroup : IoTDB.metaManager.getAllStorageGroupPaths()) {
      this.deleteAllDataFilesInOneStorageGroup(storageGroup);
    }
    return true;
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) {
    // storage group has no data
    if (!processorMap.containsKey(storageGroup)) {
      return;
    }

    processorMap.get(storageGroup).setTTL(dataTTL);
  }

  public void deleteStorageGroup(PartialPath storageGroupPath) {
    if (!processorMap.containsKey(storageGroupPath)) {
      return;
    }

    deleteAllDataFilesInOneStorageGroup(storageGroupPath);
    releaseWalDirectByteBufferPoolInOneStorageGroup(storageGroupPath);
    VirtualStorageGroupManager virtualStorageGroupManager = processorMap.remove(storageGroupPath);
    virtualStorageGroupManager.deleteStorageGroup(
        systemDir + File.pathSeparator + storageGroupPath);
  }

  public void loadNewTsFileForSync(TsFileResource newTsFileResource)
      throws StorageEngineException, LoadFileException, IllegalPathException {
    getProcessorDirectly(new PartialPath(getSgByEngineFile(newTsFileResource.getTsFile())))
        .loadNewTsFileForSync(newTsFileResource);
  }

  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws LoadFileException, StorageEngineException, MetadataException {
    Set<String> deviceSet = newTsFileResource.getDevices();
    if (deviceSet == null || deviceSet.isEmpty()) {
      throw new StorageEngineException("Can not get the corresponding storage group.");
    }
    String device = deviceSet.iterator().next();
    PartialPath devicePath = new PartialPath(device);
    PartialPath storageGroupPath = IoTDB.metaManager.getStorageGroupPath(devicePath);
    getProcessorDirectly(storageGroupPath).loadNewTsFile(newTsFileResource);
  }

  public boolean deleteTsfileForSync(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(deletedTsfile)))
        .deleteTsfile(deletedTsfile);
  }

  public boolean deleteTsfile(File deletedTsfile)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(deletedTsfile)))
        .deleteTsfile(deletedTsfile);
  }

  public boolean moveTsfile(File tsfileToBeMoved, File targetDir)
      throws StorageEngineException, IllegalPathException {
    return getProcessorDirectly(new PartialPath(getSgByEngineFile(tsfileToBeMoved)))
        .moveTsfile(tsfileToBeMoved, targetDir);
  }

  /**
   * The internal file means that the file is in the engine, which is different from those external
   * files which are not loaded.
   *
   * @param file internal file
   * @return sg name
   */
  public String getSgByEngineFile(File file) {
    return file.getParentFile().getParentFile().getParentFile().getName();
  }

  /** @return TsFiles (seq or unseq) grouped by their storage group and partition number. */
  public Map<PartialPath, Map<Long, List<TsFileResource>>> getAllClosedStorageGroupTsFile() {
    Map<PartialPath, Map<Long, List<TsFileResource>>> ret = new HashMap<>();
    for (Entry<PartialPath, VirtualStorageGroupManager> entry : processorMap.entrySet()) {
      entry.getValue().getAllClosedStorageGroupTsFile(entry.getKey(), ret);
    }
    return ret;
  }

  public void setFileFlushPolicy(TsFileFlushPolicy fileFlushPolicy) {
    this.fileFlushPolicy = fileFlushPolicy;
  }

  public boolean isFileAlreadyExist(
      TsFileResource tsFileResource, PartialPath storageGroup, long partitionNum) {
    VirtualStorageGroupManager virtualStorageGroupManager = processorMap.get(storageGroup);
    if (virtualStorageGroupManager == null) {
      return false;
    }

    Iterator<String> partialPathIterator = tsFileResource.getDevices().iterator();
    try {
      return getProcessor(new PartialPath(partialPathIterator.next()))
          .isFileAlreadyExist(tsFileResource, partitionNum);
    } catch (StorageEngineException | IllegalPathException e) {
      logger.error("can't find processor with: " + tsFileResource, e);
    }

    return false;
  }

  /**
   * Set the version of given partition to newMaxVersion if it is larger than the current version.
   */
  public void setPartitionVersionToMax(
      PartialPath storageGroup, long partitionId, long newMaxVersion) {
    processorMap.get(storageGroup).setPartitionVersionToMax(partitionId, newMaxVersion);
  }

  public void removePartitions(PartialPath storageGroupPath, TimePartitionFilter filter) {
    processorMap.get(storageGroupPath).removePartitions(filter);
  }

  public Map<PartialPath, VirtualStorageGroupManager> getProcessorMap() {
    return processorMap;
  }

  /**
   * Get a map indicating which storage groups have working TsFileProcessors and its associated
   * partitionId and whether it is sequence or not.
   *
   * @return storage group -> a list of partitionId-isSequence pairs
   */
  public Map<String, List<Pair<Long, Boolean>>> getWorkingStorageGroupPartitions() {
    Map<String, List<Pair<Long, Boolean>>> res = new ConcurrentHashMap<>();
    for (Entry<PartialPath, VirtualStorageGroupManager> entry : processorMap.entrySet()) {
      entry.getValue().getWorkingStorageGroupPartitions(entry.getKey().getFullPath(), res);
    }
    return res;
  }

  /**
   * Add a listener to listen flush start/end events. Notice that this addition only applies to
   * TsFileProcessors created afterwards.
   *
   * @param listener
   */
  public void registerFlushListener(FlushListener listener) {
    customFlushListeners.add(listener);
  }

  /**
   * Add a listener to listen file close events. Notice that this addition only applies to
   * TsFileProcessors created afterwards.
   *
   * @param listener
   */
  public void registerCloseFileListener(CloseFileListener listener) {
    customCloseFileListeners.add(listener);
  }

  /** get all merge lock of the storage group processor related to the query */
  public List<StorageGroupProcessor> mergeLock(List<PartialPath> pathList)
      throws StorageEngineException {
    Set<StorageGroupProcessor> set = new HashSet<>();
    for (PartialPath path : pathList) {
      set.add(getProcessor(path.getDevicePath()));
    }
    List<StorageGroupProcessor> list =
        set.stream()
            .sorted(Comparator.comparing(StorageGroupProcessor::getVirtualStorageGroupId))
            .collect(Collectors.toList());
    list.forEach(StorageGroupProcessor::readLock);
    return list;
  }

  /** unlock all merge lock of the storage group processor related to the query */
  public void mergeUnLock(List<StorageGroupProcessor> list) {
    list.forEach(StorageGroupProcessor::readUnlock);
  }

  static class InstanceHolder {

    private static final StorageEngine INSTANCE = new StorageEngine();

    private InstanceHolder() {
      // forbidding instantiation
    }
  }
}
