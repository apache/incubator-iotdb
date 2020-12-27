/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.storagegroup;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.merge.task.MergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.TEMP_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.CompactionMergeTaskPoolManager;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.task.RecoverMergeTask;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.OutOfTTLException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryFileManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.UpgradeSevice;
import org.apache.iotdb.db.utils.CopyOnReadLinkedList;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.writelog.recover.TsFileRecoverPerformer;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For sequence data, a StorageGroupProcessor has some TsFileProcessors, in which there is only one
 * TsFileProcessor in the working status. <br/>
 * <p>
 * There are two situations to set the working TsFileProcessor to closing status:<br/>
 * <p>
 * (1) when inserting data into the TsFileProcessor, and the TsFileProcessor shouldFlush() (or
 * shouldClose())<br/>
 * <p>
 * (2) someone calls syncCloseAllWorkingTsFileProcessors(). (up to now, only flush command from cli
 * will call this method)<br/>
 * <p>
 * UnSequence data has the similar process as above.
 * <p>
 * When a sequence TsFileProcessor is submitted to be flushed, the updateLatestFlushTimeCallback()
 * method will be called as a callback.<br/>
 * <p>
 * When a TsFileProcessor is closed, the closeUnsealedTsFileProcessorCallBack() method will be
 * called as a callback.
 */
public class StorageGroupProcessor {

  public static final String MERGING_MODIFICATION_FILE_NAME = "merge.mods";
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String FAIL_TO_UPGRADE_FOLDER = "Failed to move {} to upgrade folder";
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  /**
   * All newly generated chunks after merge have version number 0, so we set merged Modification
   * file version to 1 to take effect
   */
  private static final int MERGE_MOD_START_VERSION_NUM = 1;

  private static final Logger logger = LoggerFactory.getLogger(StorageGroupProcessor.class);

  private final boolean enableMemControl = config.isEnableMemControl();
  /**
   * indicating the file to be loaded already exists locally.
   */
  private static final int POS_ALREADY_EXIST = -2;
  /**
   * indicating the file to be loaded overlap with some files.
   */
  private static final int POS_OVERLAP = -3;
  /**
   * a read write lock for guaranteeing concurrent safety when accessing all fields in this class
   * (i.e., schema, (un)sequenceFileList, work(un)SequenceTsFileProcessor,
   * closing(Un)SequenceTsFileProcessor, latestTimeForEachDevice, and
   * partitionLatestFlushedTimeForEachDevice)
   */
  private final ReadWriteLock insertLock = new ReentrantReadWriteLock();
  /**
   * closeStorageGroupCondition is used to wait for all currently closing TsFiles to be done.
   */
  private final Object closeStorageGroupCondition = new Object();
  /**
   * avoid some tsfileResource is changed (e.g., from unsealed to sealed) when a query is executed.
   */
  private final ReadWriteLock closeQueryLock = new ReentrantReadWriteLock();
  /**
   * time partition id in the storage group -> tsFileProcessor for this time partition
   */
  private final TreeMap<Long, TsFileProcessor> workSequenceTsFileProcessors = new TreeMap<>();
  /**
   * time partition id in the storage group -> tsFileProcessor for this time partition
   */
  private final TreeMap<Long, TsFileProcessor> workUnsequenceTsFileProcessors = new TreeMap<>();
  /**
   * compactionMergeWorking is used to wait for last compaction to be done.
   */
  private volatile boolean compactionMergeWorking = false;
  // upgrading sequence TsFile resource list
  private List<TsFileResource> upgradeSeqFileList = new LinkedList<>();

  private CopyOnReadLinkedList<TsFileProcessor> closingSequenceTsFileProcessor = new CopyOnReadLinkedList<>();

  // upgrading unsequence TsFile resource list
  private List<TsFileResource> upgradeUnseqFileList = new LinkedList<>();

  private CopyOnReadLinkedList<TsFileProcessor> closingUnSequenceTsFileProcessor = new CopyOnReadLinkedList<>();
  /*
   * time partition id -> map, which contains
   * device -> global latest timestamp of each device latestTimeForEachDevice caches non-flushed
   * changes upon timestamps of each device, and is used to update partitionLatestFlushedTimeForEachDevice
   * when a flush is issued.
   */
  private Map<Long, Map<String, Long>> latestTimeForEachDevice = new HashMap<>();
  /**
   * time partition id -> map, which contains device -> largest timestamp of the latest memtable to
   * be submitted to asyncTryToFlush partitionLatestFlushedTimeForEachDevice determines whether a
   * data point should be put into a sequential file or an unsequential file. Data of some device
   * with timestamp less than or equals to the device's latestFlushedTime should go into an
   * unsequential file.
   */
  private Map<Long, Map<String, Long>> partitionLatestFlushedTimeForEachDevice = new HashMap<>();

  /**
   * used to record the latest flush time while upgrading and inserting
   */
  private Map<Long, Map<String, Long>> newlyFlushedPartitionLatestFlushedTimeForEachDevice = new HashMap<>();
  /**
   * global mapping of device -> largest timestamp of the latest memtable to * be submitted to
   * asyncTryToFlush, globalLatestFlushedTimeForEachDevice is utilized to maintain global
   * latestFlushedTime of devices and will be updated along with partitionLatestFlushedTimeForEachDevice
   */
  private Map<String, Long> globalLatestFlushedTimeForEachDevice = new HashMap<>();
  private String storageGroupName;
  private File storageGroupSysDir;

  // manage seqFileList and unSeqFileList
  private TsFileManagement tsFileManagement;

  /**
   * time partition id -> version controller which assigns a version for each MemTable and
   * deletion/update such that after they are persisted, the order of insertions, deletions and
   * updates can be re-determined.
   */
  private HashMap<Long, VersionController> timePartitionIdVersionControllerMap = new HashMap<>();

  /**
   * when the data in a storage group is older than dataTTL, it is considered invalid and will be
   * eventually removed.
   */
  private long dataTTL = Long.MAX_VALUE;
  private FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  private TsFileFlushPolicy fileFlushPolicy;

  /**
   * The max file versions in each partition. By recording this, if several IoTDB instances have the
   * same policy of closing file and their ingestion is identical, then files of the same version in
   * different IoTDB instance will have identical data, providing convenience for data comparison
   * across different instances. partition number -> max version number
   */
  private Map<Long, Long> partitionMaxFileVersions = new HashMap<>();

  /**
   * value of root.stats."root.sg".TOTAL_POINTS
   */
  private long monitorSeriesValue;
  private StorageGroupInfo storageGroupInfo = new StorageGroupInfo(this);

  public boolean isReady() {
    return isReady;
  }

  public void setReady(boolean ready) {
    isReady = ready;
  }

  private boolean isReady = false;

  private List<CloseFileListener> customCloseFileListeners = Collections.emptyList();
  private List<FlushListener> customFlushListeners = Collections.emptyList();

  public StorageGroupProcessor(String systemDir, String storageGroupName,
      TsFileFlushPolicy fileFlushPolicy) throws StorageGroupProcessorException {
    this.storageGroupName = storageGroupName;
    this.fileFlushPolicy = fileFlushPolicy;

    storageGroupSysDir = SystemFileFactory.INSTANCE.getFile(systemDir, storageGroupName);
    if (storageGroupSysDir.mkdirs()) {
      logger.info("Storage Group system Directory {} doesn't exist, create it",
          storageGroupSysDir.getPath());
    } else if (!storageGroupSysDir.exists()) {
      logger.error("create Storage Group system Directory {} failed",
          storageGroupSysDir.getPath());
    }
    this.tsFileManagement = IoTDBDescriptor.getInstance().getConfig().getCompactionStrategy()
        .getTsFileManagement(storageGroupName, storageGroupSysDir.getAbsolutePath());

    recover();

  }

  private Map<Long, List<TsFileResource>> splitResourcesByPartition(
      List<TsFileResource> resources) {
    Map<Long, List<TsFileResource>> ret = new HashMap<>();
    for (TsFileResource resource : resources) {
      ret.computeIfAbsent(resource.getTimePartition(), l -> new ArrayList<>()).add(resource);
    }
    return ret;
  }

  private void recover() throws StorageGroupProcessorException {
    long recoverStart = System.currentTimeMillis();
    logger.info("recovering Storage Group {}", storageGroupName);

    try {
      // collect candidate TsFiles from sequential and unsequential data directory
      Pair<List<TsFileResource>, List<TsFileResource>> seqTsFilesPair = getAllFiles(
          DirectoryManager.getInstance().getAllSequenceFileFolders());
      List<TsFileResource> tmpSeqTsFiles = seqTsFilesPair.left;
      List<TsFileResource> oldSeqTsFiles = seqTsFilesPair.right;
      upgradeSeqFileList.addAll(oldSeqTsFiles);
      Pair<List<TsFileResource>, List<TsFileResource>> unseqTsFilesPair = getAllFiles(
          DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      List<TsFileResource> tmpUnseqTsFiles = unseqTsFilesPair.left;
      List<TsFileResource> oldUnseqTsFiles = unseqTsFilesPair.right;
      upgradeUnseqFileList.addAll(oldUnseqTsFiles);

      // split by partition so that we can find the last file of each partition and decide to
      // close it or not
      Map<Long, List<TsFileResource>> partitionTmpSeqTsFiles = splitResourcesByPartition(
          tmpSeqTsFiles);
      Map<Long, List<TsFileResource>> partitionTmpUnseqTsFiles = splitResourcesByPartition(
          tmpUnseqTsFiles);
      long innerStart = System.currentTimeMillis();
      for (List<TsFileResource> value : partitionTmpSeqTsFiles.values()) {
        recoverTsFiles(value, true);
      }
      logger.info("Recovery of seq files costs {}ms", System.currentTimeMillis() - innerStart);
      innerStart = System.currentTimeMillis();
      for (List<TsFileResource> value : partitionTmpUnseqTsFiles.values()) {
        recoverTsFiles(value, false);
      }
      logger.info("Recovery of unseq files costs {}ms", System.currentTimeMillis() - innerStart);

      String taskName = storageGroupName + "-" + System.currentTimeMillis();
      File mergingMods = SystemFileFactory.INSTANCE.getFile(storageGroupSysDir,
          MERGING_MODIFICATION_FILE_NAME);
      if (mergingMods.exists()) {
        this.tsFileManagement.mergingModification = new ModificationFile(mergingMods.getPath());
      }
      RecoverMergeTask recoverMergeTask = new RecoverMergeTask(
          new ArrayList<>(tsFileManagement.getTsFileList(true)),
          tsFileManagement.getTsFileList(false), storageGroupSysDir.getPath(),
          tsFileManagement::mergeEndAction,
          taskName,
          IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(), storageGroupName);

      innerStart = System.currentTimeMillis();
      logger.info("{} a RecoverMergeTask {} starts...", storageGroupName, taskName);
      recoverMergeTask
          .recoverMerge(IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot());
      if (!IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot()) {
        mergingMods.delete();
      }
      logger.info("{} a RecoverMergeTask {} ends after {}ms", storageGroupName, taskName,
          System.currentTimeMillis() - innerStart);
      tsFileManagement.recover();
      updateLatestFlushedTime();
    } catch (IOException | MetadataException e) {
      throw new StorageGroupProcessorException(e);
    }

    for (TsFileResource resource : tsFileManagement.getTsFileList(true)) {
      long timePartitionId = resource.getTimePartition();
      Map<String, Long> endTimeMap = new HashMap<>();
      for (Entry<String, Integer> entry : resource.getDeviceToIndexMap().entrySet()) {
        String deviceId = entry.getKey();
        int index = entry.getValue();
        long endTime = resource.getEndTime(index);
        endTimeMap.put(deviceId, endTime);
      }
      latestTimeForEachDevice.computeIfAbsent(timePartitionId, l -> new HashMap<>())
          .putAll(endTimeMap);
      partitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(timePartitionId, id -> new HashMap<>())
          .putAll(endTimeMap);
      globalLatestFlushedTimeForEachDevice.putAll(endTimeMap);
    }
    logger.info("recovered Storage Group {} after {}ms", storageGroupName,
        System.currentTimeMillis() - recoverStart);
  }

  public long getMonitorSeriesValue() {
    return monitorSeriesValue;
  }

  public void setMonitorSeriesValue(long monitorSeriesValue) {
    this.monitorSeriesValue = monitorSeriesValue;
  }

  public void updateMonitorSeriesValue(int successPointsNum) {
    this.monitorSeriesValue += successPointsNum;
  }

  /**
   * use old seq file to update latestTimeForEachDevice, globalLatestFlushedTimeForEachDevice,
   * partitionLatestFlushedTimeForEachDevice and timePartitionIdVersionControllerMap
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void updateLatestFlushedTime() throws IOException {

    VersionController versionController = new SimpleFileVersionController(
        storageGroupSysDir.getPath());
    long currentVersion = versionController.currVersion();
    for (TsFileResource resource : upgradeSeqFileList) {
      for (Entry<String, Integer> entry : resource.getDeviceToIndexMap().entrySet()) {
        String deviceId = entry.getKey();
        int index = entry.getValue();
        long endTime = resource.getEndTime(index);
        long endTimePartitionId = StorageEngine.getTimePartition(endTime);
        latestTimeForEachDevice.computeIfAbsent(endTimePartitionId, l -> new HashMap<>())
            .put(deviceId, endTime);
        globalLatestFlushedTimeForEachDevice.put(deviceId, endTime);

        // set all the covered partition's LatestFlushedTime to Long.MAX_VALUE
        long partitionId = StorageEngine.getTimePartition(resource.getStartTime(index));
        while (partitionId <= endTimePartitionId) {
          partitionLatestFlushedTimeForEachDevice.computeIfAbsent(partitionId, l -> new HashMap<>())
              .put(deviceId, Long.MAX_VALUE);
          if (!timePartitionIdVersionControllerMap.containsKey(partitionId)) {
            File directory = SystemFileFactory.INSTANCE
                .getFile(storageGroupSysDir, String.valueOf(partitionId));
            if (!directory.exists()) {
              directory.mkdirs();
            }
            File versionFile = SystemFileFactory.INSTANCE
                .getFile(directory, SimpleFileVersionController.FILE_PREFIX + currentVersion);
            if (!versionFile.createNewFile()) {
              logger.warn("Version file {} has already been created ", versionFile);
            }
            timePartitionIdVersionControllerMap.put(partitionId,
                new SimpleFileVersionController(storageGroupSysDir.getPath(), partitionId));
          }
          partitionId++;
        }
      }
    }
  }

  /**
   * get version controller by time partition Id Thread-safety should be ensure by caller
   *
   * @param timePartitionId time partition Id
   * @return version controller
   */
  private VersionController getVersionControllerByTimePartitionId(long timePartitionId) {
    return timePartitionIdVersionControllerMap.computeIfAbsent(timePartitionId,
        id -> {
          try {
            return new SimpleFileVersionController(storageGroupSysDir.getPath(), timePartitionId);
          } catch (IOException e) {
            logger.error("can't build a version controller for time partition {}", timePartitionId);
            return null;
          }
        });
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private Pair<List<TsFileResource>, List<TsFileResource>> getAllFiles(List<String> folders)
      throws IOException {
    List<File> tsFiles = new ArrayList<>();
    List<File> upgradeFiles = new ArrayList<>();
    for (String baseDir : folders) {
      File fileFolder = fsFactory.getFile(baseDir, storageGroupName);
      if (!fileFolder.exists()) {
        continue;
      }

      // old version
      // some TsFileResource may be being persisted when the system crashed, try recovering such
      // resources
      continueFailedRenames(fileFolder, TEMP_SUFFIX);

      // some TsFiles were going to be replaced by the merged files when the system crashed and
      // the process was interrupted before the merged files could be named
      continueFailedRenames(fileFolder, MERGE_SUFFIX);

      File[] oldTsfileArray = fsFactory
          .listFilesBySuffix(fileFolder.getAbsolutePath(), TSFILE_SUFFIX);
      File[] oldResourceFileArray = fsFactory
          .listFilesBySuffix(fileFolder.getAbsolutePath(), TsFileResource.RESOURCE_SUFFIX);
      File[] oldModificationFileArray = fsFactory
          .listFilesBySuffix(fileFolder.getAbsolutePath(), ModificationFile.FILE_SUFFIX);
      File upgradeFolder = fsFactory.getFile(fileFolder, IoTDBConstant.UPGRADE_FOLDER_NAME);
      // move the old files to upgrade folder if exists
      if (oldTsfileArray.length != 0 || oldResourceFileArray.length != 0) {
        // create upgrade directory if not exist
        if (upgradeFolder.mkdirs()) {
          logger.info("Upgrade Directory {} doesn't exist, create it",
              upgradeFolder.getPath());
        } else if (!upgradeFolder.exists()) {
          logger.error("Create upgrade Directory {} failed",
              upgradeFolder.getPath());
        }
        // move .tsfile to upgrade folder
        for (File file : oldTsfileArray) {
          if (!file.renameTo(fsFactory.getFile(upgradeFolder, file.getName()))) {
            logger.error(FAIL_TO_UPGRADE_FOLDER, file);
          }
        }
        // move .resource to upgrade folder
        for (File file : oldResourceFileArray) {
          if (!file.renameTo(fsFactory.getFile(upgradeFolder, file.getName()))) {
            logger.error(FAIL_TO_UPGRADE_FOLDER, file);
          }
        }
        // move .mods to upgrade folder
        for (File file : oldModificationFileArray) {
          if (!file.renameTo(fsFactory.getFile(upgradeFolder, file.getName()))) {
            logger.error(FAIL_TO_UPGRADE_FOLDER, file);
          }
        }

        Collections.addAll(upgradeFiles,
            fsFactory.listFilesBySuffix(upgradeFolder.getAbsolutePath(), TSFILE_SUFFIX));
      }
      // if already move old files to upgradeFolder 
      else if (upgradeFolder.exists()) {
        Collections.addAll(upgradeFiles,
            fsFactory.listFilesBySuffix(upgradeFolder.getAbsolutePath(), TSFILE_SUFFIX));
      }

      File[] subFiles = fileFolder.listFiles();
      if (subFiles != null) {
        for (File partitionFolder : subFiles) {
          if (!partitionFolder.isDirectory()) {
            logger.warn("{} is not a directory.", partitionFolder.getAbsolutePath());
          } else if (!partitionFolder.getName().equals(IoTDBConstant.UPGRADE_FOLDER_NAME)) {
            // some TsFileResource may be being persisted when the system crashed, try recovering such
            // resources
            continueFailedRenames(partitionFolder, TEMP_SUFFIX);

            // some TsFiles were going to be replaced by the merged files when the system crashed and
            // the process was interrupted before the merged files could be named
            continueFailedRenames(partitionFolder, MERGE_SUFFIX);

            Collections.addAll(tsFiles,
                fsFactory.listFilesBySuffix(partitionFolder.getAbsolutePath(), TSFILE_SUFFIX));
          }
        }
      }

    }
    tsFiles.sort(this::compareFileName);
    List<TsFileResource> ret = new ArrayList<>();
    tsFiles.forEach(f -> ret.add(new TsFileResource(f)));
    upgradeFiles.sort(this::compareFileName);
    List<TsFileResource> upgradeRet = new ArrayList<>();
    for (File f : upgradeFiles) {
      TsFileResource fileResource = new TsFileResource(f);
      fileResource.setClosed(true);
      // make sure the flush command is called before IoTDB is down.
      fileResource.deserialize();
      upgradeRet.add(fileResource);
    }
    return new Pair<>(ret, upgradeRet);
  }

  private void continueFailedRenames(File fileFolder, String suffix) {
    File[] files = fsFactory.listFilesBySuffix(fileFolder.getAbsolutePath(), suffix);
    if (files != null) {
      for (File tempResource : files) {
        File originResource = fsFactory.getFile(tempResource.getPath().replace(suffix, ""));
        if (originResource.exists()) {
          tempResource.delete();
        } else {
          tempResource.renameTo(originResource);
        }
      }
    }
  }

  private void recoverTsFiles(List<TsFileResource> tsFiles, boolean isSeq) {
    for (int i = 0; i < tsFiles.size(); i++) {
      TsFileResource tsFileResource = tsFiles.get(i);
      long timePartitionId = tsFileResource.getTimePartition();

      TsFileRecoverPerformer recoverPerformer = new TsFileRecoverPerformer(
          storageGroupName + FILE_NAME_SEPARATOR,
          getVersionControllerByTimePartitionId(timePartitionId), tsFileResource, isSeq,
          i == tsFiles.size() - 1);

      RestorableTsFileIOWriter writer;
      try {
        long start = System.currentTimeMillis();
        // this tsfile is not zero level, no need to perform redo wal
        if (LevelCompactionTsFileManagement.getMergeLevel(tsFileResource.getTsFile()) > 0) {
          recoverPerformer.recover(false);
          tsFileResource.setClosed(true);
          tsFileManagement.add(tsFileResource, isSeq);
          continue;
        } else {
          writer = recoverPerformer.recover(true);
        }
        logger.debug("Recovery of {} costs {}ms", tsFileResource.getTsFile(),
            System.currentTimeMillis() - start);
      } catch (StorageGroupProcessorException e) {
        logger.warn("Skip TsFile: {} because of error in recover: ", tsFileResource.getTsFilePath(),
            e);
        continue;
      }



      if (i != tsFiles.size() - 1 || !writer.canWrite()) {
        // not the last file or cannot write, just close it
        tsFileResource.setClosed(true);
      } else if (writer.canWrite()) {
        // the last file is not closed, continue writing to in
        TsFileProcessor tsFileProcessor;
        if (isSeq) {
          tsFileProcessor = new TsFileProcessor(storageGroupName, storageGroupInfo, tsFileResource,
              getVersionControllerByTimePartitionId(timePartitionId),
              this::closeUnsealedTsFileProcessorCallBack, this::updateLatestFlushTimeCallback,
              true, writer);
          if (enableMemControl) {
            TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
            tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
            this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
            tsFileProcessorInfo.addTSPMemCost(tsFileProcessor
                .getTsFileResource().calculateRamSize());
          }
          workSequenceTsFileProcessors.put(timePartitionId, tsFileProcessor);
        } else {
          tsFileProcessor = new TsFileProcessor(storageGroupName, storageGroupInfo, tsFileResource,
              getVersionControllerByTimePartitionId(timePartitionId),
              this::closeUnsealedTsFileProcessorCallBack, this::unsequenceFlushCallback, false,
              writer);
          if (enableMemControl) {
            TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
            tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
            this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
            tsFileProcessorInfo.addTSPMemCost(tsFileProcessor
                .getTsFileResource().calculateRamSize());
          }
          workUnsequenceTsFileProcessors.put(timePartitionId, tsFileProcessor);
        }
        tsFileResource.setProcessor(tsFileProcessor);
        tsFileResource.removeResourceFile();
        tsFileProcessor.setTimeRangeId(timePartitionId);
        writer.makeMetadataVisible();
        if (enableMemControl) {
          // get chunkMetadata size
          long chunkMetadataSize = 0;
          for (Map<String, List<ChunkMetadata>> metaMap : writer.getMetadatasForQuery().values()) {
            for (List<ChunkMetadata> metadatas : metaMap.values()) {
              for (ChunkMetadata chunkMetadata : metadatas) {
                chunkMetadataSize += chunkMetadata.calculateRamSize();
              }
            }
          }
          tsFileProcessor.getTsFileProcessorInfo().addTSPMemCost(chunkMetadataSize);
        }
      }
      tsFileManagement.add(tsFileResource, isSeq);
    }
  }

  // ({systemTime}-{versionNum}-{mergeNum}.tsfile)
  private int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "")
        .split(FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "")
        .split(FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    } else {
      return cmp;
    }
  }

  public void insert(InsertRowPlan insertRowPlan) throws WriteProcessException {
    // reject insertions that are out of ttl
    if (!isAlive(insertRowPlan.getTime())) {
      throw new OutOfTTLException(insertRowPlan.getTime(), (System.currentTimeMillis() - dataTTL));
    }
    if (enableMemControl) {
      StorageEngine.blockInsertionIfReject();
    }
    writeLock();
    try {
      // init map
      long timePartitionId = StorageEngine.getTimePartition(insertRowPlan.getTime());

      partitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(timePartitionId, id -> new HashMap<>());

      boolean isSequence =
          insertRowPlan.getTime() > partitionLatestFlushedTimeForEachDevice.get(timePartitionId)
              .getOrDefault(insertRowPlan.getDeviceId().getFullPath(), Long.MIN_VALUE);

      //is unsequence and user set config to discard out of order data
      if (!isSequence && IoTDBDescriptor.getInstance().getConfig()
          .isEnableDiscardOutOfOrderData()) {
        return;
      }

      latestTimeForEachDevice.computeIfAbsent(timePartitionId, l -> new HashMap<>());
      // insert to sequence or unSequence file
      insertToTsFileProcessor(insertRowPlan, isSequence, timePartitionId);

    } finally {
      writeUnlock();
    }
  }

  /**
   * Insert a tablet (rows belonging to the same devices) into this storage group.
   *
   * @throws BatchProcessException if some of the rows failed to be inserted
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void insertTablet(InsertTabletPlan insertTabletPlan) throws BatchProcessException {
    if (enableMemControl) {
      try {
        StorageEngine.blockInsertionIfReject();
      } catch (WriteProcessRejectException e) {
        TSStatus[] results = new TSStatus[insertTabletPlan.getRowCount()];
        Arrays.fill(results, RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT));
        throw new BatchProcessException(results);
      }
    }

    writeLock();
    try {
      TSStatus[] results = new TSStatus[insertTabletPlan.getRowCount()];
      Arrays.fill(results, RpcUtils.SUCCESS_STATUS);
      boolean noFailure = true;

      /*
       * assume that batch has been sorted by client
       */
      int loc = 0;
      while (loc < insertTabletPlan.getRowCount()) {
        long currTime = insertTabletPlan.getTimes()[loc];
        // skip points that do not satisfy TTL
        if (!isAlive(currTime)) {
          results[loc] = RpcUtils.getStatus(TSStatusCode.OUT_OF_TTL_ERROR,
              "time " + currTime + " in current line is out of TTL: " + dataTTL);
          loc++;
          noFailure = false;
        } else {
          break;
        }
      }
      // loc pointing at first legal position
      if (loc == insertTabletPlan.getRowCount()) {
        throw new BatchProcessException(results);
      }
      // before is first start point
      int before = loc;
      // before time partition
      long beforeTimePartition = StorageEngine
          .getTimePartition(insertTabletPlan.getTimes()[before]);
      // init map
      long lastFlushTime = partitionLatestFlushedTimeForEachDevice.
          computeIfAbsent(beforeTimePartition, id -> new HashMap<>()).
          computeIfAbsent(insertTabletPlan.getDeviceId().getFullPath(), id -> Long.MIN_VALUE);
      // if is sequence
      boolean isSequence = false;
      while (loc < insertTabletPlan.getRowCount()) {
        long time = insertTabletPlan.getTimes()[loc];
        long curTimePartition = StorageEngine.getTimePartition(time);
        // start next partition
        if (curTimePartition != beforeTimePartition) {
          // insert last time partition
          if (isSequence || !IoTDBDescriptor.getInstance().getConfig()
              .isEnableDiscardOutOfOrderData()) {
            noFailure = insertTabletToTsFileProcessor(insertTabletPlan, before, loc, isSequence,
                results,
                beforeTimePartition) && noFailure;
          }
          // re initialize
          before = loc;
          beforeTimePartition = curTimePartition;
          lastFlushTime = partitionLatestFlushedTimeForEachDevice.
              computeIfAbsent(beforeTimePartition, id -> new HashMap<>()).
              computeIfAbsent(insertTabletPlan.getDeviceId().getFullPath(), id -> Long.MIN_VALUE);
          isSequence = false;
        }
        // still in this partition
        else {
          // judge if we should insert sequence
          if (!isSequence && time > lastFlushTime) {
            // insert into unsequence and then start sequence
            if (!IoTDBDescriptor.getInstance().getConfig().isEnableDiscardOutOfOrderData()) {
              noFailure =
                  insertTabletToTsFileProcessor(insertTabletPlan, before, loc, false, results,
                      beforeTimePartition) && noFailure;
            }
            before = loc;
            isSequence = true;
          }
          loc++;
        }
      }

      // do not forget last part
      if (before < loc && (isSequence || !IoTDBDescriptor.getInstance().getConfig()
          .isEnableDiscardOutOfOrderData())) {
        noFailure = insertTabletToTsFileProcessor(insertTabletPlan, before, loc, isSequence,
            results, beforeTimePartition) && noFailure;
      }
      long globalLatestFlushedTime = globalLatestFlushedTimeForEachDevice.getOrDefault(
          insertTabletPlan.getDeviceId().getFullPath(), Long.MIN_VALUE);
      tryToUpdateBatchInsertLastCache(insertTabletPlan, globalLatestFlushedTime);

      if (!noFailure) {
        throw new BatchProcessException(results);
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * @return whether the given time falls in ttl
   */
  private boolean isAlive(long time) {
    return dataTTL == Long.MAX_VALUE || (System.currentTimeMillis() - time) <= dataTTL;
  }

  /**
   * insert batch to tsfile processor thread-safety that the caller need to guarantee The rows to be
   * inserted are in the range [start, end)
   *
   * @param insertTabletPlan insert a tablet of a device
   * @param sequence         whether is sequence
   * @param start            start index of rows to be inserted in insertTabletPlan
   * @param end              end index of rows to be inserted in insertTabletPlan
   * @param results          result array
   * @param timePartitionId  time partition id
   * @return false if any failure occurs when inserting the tablet, true otherwise
   */
  private boolean insertTabletToTsFileProcessor(InsertTabletPlan insertTabletPlan,
      int start, int end, boolean sequence, TSStatus[] results, long timePartitionId) {
    // return when start >= end
    if (start >= end) {
      return true;
    }

    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);
    if (tsFileProcessor == null) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR,
            "can not create TsFileProcessor, timePartitionId: " + timePartitionId);
      }
      return false;
    }

    try {
      tsFileProcessor.insertTablet(insertTabletPlan, start, end, results);
    } catch (WriteProcessRejectException e) {
      logger.warn("insert to TsFileProcessor rejected ", e);
      return false;
    } catch (WriteProcessException e) {
      logger.error("insert to TsFileProcessor error ", e);
      return false;
    }

    latestTimeForEachDevice.computeIfAbsent(timePartitionId, t -> new HashMap<>());
    // try to update the latest time of the device of this tsRecord
    if (sequence && latestTimeForEachDevice.get(timePartitionId)
        .getOrDefault(insertTabletPlan.getDeviceId().getFullPath(), Long.MIN_VALUE)
        < insertTabletPlan.getTimes()[end - 1]) {
      latestTimeForEachDevice.get(timePartitionId)
          .put(insertTabletPlan.getDeviceId().getFullPath(), insertTabletPlan.getTimes()[end - 1]);
    }

    // check memtable size and may async try to flush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
    return true;
  }

  private void tryToUpdateBatchInsertLastCache(InsertTabletPlan plan, Long latestFlushedTime) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    MeasurementMNode[] mNodes = plan.getMeasurementMNodes();
    for (int i = 0; i < mNodes.length; i++) {
      if (plan.getColumns()[i] == null) {
        continue;
      }
      // Update cached last value with high priority
      if (mNodes[i] != null) {
        // in stand alone version, the seriesPath is not needed, just use measurementMNodes[i] to update last cache
        IoTDB.metaManager.updateLastCache(null,
            plan.composeLastTimeValuePair(i), true, latestFlushedTime, mNodes[i]);
      } else {
        // measurementMNodes[i] is null, use the path to update remote cache
        IoTDB.metaManager
            .updateLastCache(plan.getDeviceId().concatNode(plan.getMeasurements()[i]),
                plan.composeLastTimeValuePair(i), true, latestFlushedTime, null);
      }
    }
  }

  private void insertToTsFileProcessor(InsertRowPlan insertRowPlan, boolean sequence, long timePartitionId)
      throws WriteProcessException {
    TsFileProcessor tsFileProcessor = getOrCreateTsFileProcessor(timePartitionId, sequence);

    if (tsFileProcessor == null) {
      return;
    }

    tsFileProcessor.insert(insertRowPlan);

    // try to update the latest time of the device of this tsRecord
    if (latestTimeForEachDevice.get(timePartitionId)
        .getOrDefault(insertRowPlan.getDeviceId().getFullPath(), Long.MIN_VALUE) < insertRowPlan
        .getTime()) {
      latestTimeForEachDevice.get(timePartitionId)
          .put(insertRowPlan.getDeviceId().getFullPath(), insertRowPlan.getTime());
    }

    long globalLatestFlushTime = globalLatestFlushedTimeForEachDevice.getOrDefault(
        insertRowPlan.getDeviceId().getFullPath(), Long.MIN_VALUE);

    tryToUpdateInsertLastCache(insertRowPlan, globalLatestFlushTime);

    // check memtable size and may asyncTryToFlush the work memtable
    if (tsFileProcessor.shouldFlush()) {
      fileFlushPolicy.apply(this, tsFileProcessor, sequence);
    }
  }

  private void tryToUpdateInsertLastCache(InsertRowPlan plan, Long latestFlushedTime) {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    MeasurementMNode[] mNodes = plan.getMeasurementMNodes();
    for (int i = 0; i < mNodes.length; i++) {
      if (plan.getValues()[i] == null) {
        continue;
      }
      // Update cached last value with high priority
      if (mNodes[i] != null) {
        // in stand alone version, the seriesPath is not needed, just use measurementMNodes[i] to update last cache
        IoTDB.metaManager.updateLastCache(null,
            plan.composeTimeValuePair(i), true, latestFlushedTime, mNodes[i]);
      } else {
        IoTDB.metaManager
            .updateLastCache(plan.getDeviceId().concatNode(plan.getMeasurements()[i]),
                plan.composeTimeValuePair(i), true, latestFlushedTime, null);
      }
    }
  }

  public void asyncFlushMemTableInTsFileProcessor(TsFileProcessor tsFileProcessor) {
    writeLock();
    try {
      if (!closingSequenceTsFileProcessor.contains(tsFileProcessor) && 
          !closingUnSequenceTsFileProcessor.contains(tsFileProcessor)) {
        fileFlushPolicy.apply(this, tsFileProcessor, tsFileProcessor.isSequence());
      }
    } finally {
      writeUnlock();
    }
  }

  private TsFileProcessor getOrCreateTsFileProcessor(long timeRangeId, boolean sequence) {
    TsFileProcessor tsFileProcessor = null;
    try {
      if (sequence) {
        tsFileProcessor = getOrCreateTsFileProcessorIntern(timeRangeId,
            workSequenceTsFileProcessors, true);
      } else {
        tsFileProcessor = getOrCreateTsFileProcessorIntern(timeRangeId,
            workUnsequenceTsFileProcessors, false);
      }
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "disk space is insufficient when creating TsFile processor, change system mode to read-only",
          e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    } catch (IOException e) {
      logger
          .error("meet IOException when creating TsFileProcessor, change system mode to read-only",
              e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
    }
    return tsFileProcessor;
  }

  /**
   * get processor from hashmap, flush oldest processor if necessary
   *
   * @param timeRangeId            time partition range
   * @param tsFileProcessorTreeMap tsFileProcessorTreeMap
   * @param sequence               whether is sequence or not
   */
  private TsFileProcessor getOrCreateTsFileProcessorIntern(long timeRangeId,
      TreeMap<Long, TsFileProcessor> tsFileProcessorTreeMap,
      boolean sequence)
      throws IOException, DiskSpaceInsufficientException {

    TsFileProcessor res;
    // we have to ensure only one thread can change workSequenceTsFileProcessors
    writeLock();
    try {
      res = tsFileProcessorTreeMap.get(timeRangeId);
      if (res == null) {
        // we have to remove oldest processor to control the num of the memtables
        // TODO: use a method to control the number of memtables
        if (tsFileProcessorTreeMap.size()
            >= IoTDBDescriptor.getInstance().getConfig().getConcurrentWritingTimePartition()) {
          Map.Entry<Long, TsFileProcessor> processorEntry = tsFileProcessorTreeMap.firstEntry();
          logger.info(
              "will close a {} TsFile because too many active partitions ({} > {}) in the storage group {},",
              sequence, tsFileProcessorTreeMap.size(),
              IoTDBDescriptor.getInstance().getConfig().getConcurrentWritingTimePartition(),
              storageGroupName);
          asyncCloseOneTsFileProcessor(sequence, processorEntry.getValue());
        }

        // build new processor
        TsFileProcessor newProcessor = createTsFileProcessor(sequence, timeRangeId);
        tsFileProcessorTreeMap.put(timeRangeId, newProcessor);
        tsFileManagement.add(newProcessor.getTsFileResource(), sequence);
        res = newProcessor;
      }

    } finally {
      // unlock in finally
      writeUnlock();
    }

    return res;
  }


  private TsFileProcessor createTsFileProcessor(boolean sequence, long timePartitionId)
      throws IOException, DiskSpaceInsufficientException {
    String baseDir;
    if (sequence) {
      baseDir = DirectoryManager.getInstance().getNextFolderForSequenceFile();
    } else {
      baseDir = DirectoryManager.getInstance().getNextFolderForUnSequenceFile();
    }
    fsFactory.getFile(baseDir, storageGroupName).mkdirs();

    String filePath =
        baseDir + File.separator + storageGroupName + File.separator + timePartitionId
            + File.separator
            + getNewTsFileName(timePartitionId);

    TsFileProcessor tsFileProcessor;
    VersionController versionController = getVersionControllerByTimePartitionId(timePartitionId);
    if (sequence) {
      tsFileProcessor = new TsFileProcessor(storageGroupName,
          fsFactory.getFileWithParent(filePath), storageGroupInfo,
          versionController, this::closeUnsealedTsFileProcessorCallBack,
          this::updateLatestFlushTimeCallback, true);
      if (enableMemControl) {
        TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
        tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
        this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
        tsFileProcessorInfo.addTSPMemCost(tsFileProcessor
            .getTsFileResource().calculateRamSize());
      }
    } else {
      tsFileProcessor = new TsFileProcessor(storageGroupName,
          fsFactory.getFileWithParent(filePath), storageGroupInfo,
          versionController, this::closeUnsealedTsFileProcessorCallBack,
          this::unsequenceFlushCallback, false);
      if (enableMemControl) {
        TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(storageGroupInfo);
        tsFileProcessor.setTsFileProcessorInfo(tsFileProcessorInfo);
        this.storageGroupInfo.initTsFileProcessorInfo(tsFileProcessor);
        tsFileProcessorInfo.addTSPMemCost(tsFileProcessor
            .getTsFileResource().calculateRamSize());
      }
    }
    tsFileProcessor.addCloseFileListeners(customCloseFileListeners);
    tsFileProcessor.addFlushListeners(customFlushListeners);

    tsFileProcessor.setTimeRangeId(timePartitionId);
    return tsFileProcessor;
  }

  /**
   * Create a new tsfile name
   *
   * @return file name
   */
  private String getNewTsFileName(long timePartitionId) {
    long version = partitionMaxFileVersions.getOrDefault(timePartitionId, 0L) + 1;
    partitionMaxFileVersions.put(timePartitionId, version);
    return getNewTsFileName(System.currentTimeMillis(), version, 0);
  }

  private String getNewTsFileName(long time, long version, int mergeCnt) {
    return time + FILE_NAME_SEPARATOR + version
        + FILE_NAME_SEPARATOR + mergeCnt + TSFILE_SUFFIX;
  }

  public void syncCloseOneTsFileProcessor(boolean sequence, TsFileProcessor tsFileProcessor) {
    synchronized (closeStorageGroupCondition) {
      try {
        asyncCloseOneTsFileProcessor(sequence, tsFileProcessor);
        long startTime = System.currentTimeMillis();
        while (closingSequenceTsFileProcessor.contains(tsFileProcessor)
            || closingUnSequenceTsFileProcessor.contains(tsFileProcessor)) {
          closeStorageGroupCondition.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger
                .warn("{} has spent {}s to wait for closing one tsfile.", this.storageGroupName,
                    (System.currentTimeMillis() - startTime) / 1000);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger
            .error("syncCloseOneTsFileProcessor error occurs while waiting for closing the storage "
                + "group {}", storageGroupName, e);
      }
    }
  }

  /**
   * thread-safety should be ensured by caller
   */
  public void asyncCloseOneTsFileProcessor(boolean sequence, TsFileProcessor tsFileProcessor) {
    //for sequence tsfile, we update the endTimeMap only when the file is prepared to be closed.
    //for unsequence tsfile, we have maintained the endTimeMap when an insertion comes.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor) || 
        closingUnSequenceTsFileProcessor.contains(tsFileProcessor)) {
      return;
    }
    logger.info("Async close tsfile: {}",
        tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());
    if (sequence) {
      closingSequenceTsFileProcessor.add(tsFileProcessor);
      updateEndTimeMap(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workSequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if unsequence files don't contain this time range id, we should remove it's version controller
      if (!workUnsequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
      logger.info("close a sequence tsfile processor {}", storageGroupName);
    } else {
      closingUnSequenceTsFileProcessor.add(tsFileProcessor);
      tsFileProcessor.asyncClose();

      workUnsequenceTsFileProcessors.remove(tsFileProcessor.getTimeRangeId());
      // if sequence files don't contain this time range id, we should remove it's version controller
      if (!workSequenceTsFileProcessors.containsKey(tsFileProcessor.getTimeRangeId())) {
        timePartitionIdVersionControllerMap.remove(tsFileProcessor.getTimeRangeId());
      }
    }
  }

  /**
   * delete the storageGroup's own folder in folder data/system/storage_groups
   */
  public void deleteFolder(String systemDir) {
    logger.info("{} will close all files for deleting data folder {}", storageGroupName, systemDir);
    writeLock();
    syncCloseAllWorkingTsFileProcessors();
    try {
      File storageGroupFolder = SystemFileFactory.INSTANCE.getFile(systemDir, storageGroupName);
      if (storageGroupFolder.exists()) {
        org.apache.iotdb.db.utils.FileUtils.deleteDirectory(storageGroupFolder);
      }
    } catch (IOException e) {
      logger.error("Cannot delete the folder in storage group {}, because", storageGroupName, e);
    } finally {
      writeUnlock();
    }
  }

  public void closeAllResources() {
    for (TsFileResource tsFileResource : tsFileManagement.getTsFileList(false)) {
      try {
        tsFileResource.close();
      } catch (IOException e) {
        logger.error("Cannot close a TsFileResource {}", tsFileResource, e);
      }
    }
    for (TsFileResource tsFileResource : tsFileManagement.getTsFileList(true)) {
      try {
        tsFileResource.close();
      } catch (IOException e) {
        logger.error("Cannot close a TsFileResource {}", tsFileResource, e);
      }
    }
  }

  public void syncDeleteDataFiles() {
    logger.info("{} will close all files for deleting data files", storageGroupName);
    writeLock();
    syncCloseAllWorkingTsFileProcessors();
    //normally, mergingModification is just need to be closed by after a merge task is finished.
    //we close it here just for IT test.
    if (this.tsFileManagement.mergingModification != null) {
      try {
        this.tsFileManagement.mergingModification.close();
      } catch (IOException e) {
        logger.error("Cannot close the mergingMod file {}",
            this.tsFileManagement.mergingModification.getFilePath(), e);
      }

    }
    try {
      closeAllResources();
      List<String> folder = DirectoryManager.getInstance().getAllSequenceFileFolders();
      folder.addAll(DirectoryManager.getInstance().getAllUnSequenceFileFolders());
      deleteAllSGFolders(folder);

      this.workSequenceTsFileProcessors.clear();
      this.workUnsequenceTsFileProcessors.clear();
      this.tsFileManagement.clear();
      this.partitionLatestFlushedTimeForEachDevice.clear();
      this.globalLatestFlushedTimeForEachDevice.clear();
      this.latestTimeForEachDevice.clear();
    } finally {
      writeUnlock();
    }
  }

  private void deleteAllSGFolders(List<String> folder) {
    for (String tsfilePath : folder) {
      File storageGroupFolder = fsFactory.getFile(tsfilePath, storageGroupName);
      if (storageGroupFolder.exists()) {
        try {
          org.apache.iotdb.db.utils.FileUtils.deleteDirectory(storageGroupFolder);
        } catch (IOException e) {
          logger.error("Delete TsFiles failed", e);
        }
      }
    }
  }

  /**
   * Iterate each TsFile and try to lock and remove those out of TTL.
   */
  public synchronized void checkFilesTTL() {
    if (dataTTL == Long.MAX_VALUE) {
      logger.debug("{}: TTL not set, ignore the check", storageGroupName);
      return;
    }
    long timeLowerBound = System.currentTimeMillis() - dataTTL;
    if (logger.isDebugEnabled()) {
      logger.debug("{}: TTL removing files before {}", storageGroupName, new Date(timeLowerBound));
    }

    // copy to avoid concurrent modification of deletion
    List<TsFileResource> seqFiles = new ArrayList<>(tsFileManagement.getTsFileList(true));
    List<TsFileResource> unseqFiles = new ArrayList<>(tsFileManagement.getTsFileList(false));

    for (TsFileResource tsFileResource : seqFiles) {
      checkFileTTL(tsFileResource, timeLowerBound, true);
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      checkFileTTL(tsFileResource, timeLowerBound, false);
    }
  }

  private void checkFileTTL(TsFileResource resource, long timeLowerBound, boolean isSeq) {
    if (resource.isMerging() || !resource.isClosed()
        || !resource.isDeleted() && resource.stillLives(timeLowerBound)) {
      return;
    }

    writeLock();
    try {
      // prevent new merges and queries from choosing this file
      resource.setDeleted(true);
      // the file may be chosen for merge after the last check and before writeLock()
      // double check to ensure the file is not used by a merge
      if (resource.isMerging()) {
        return;
      }

      // ensure that the file is not used by any queries
      if (resource.tryWriteLock()) {
        try {
          // physical removal
          resource.remove();
          if (logger.isInfoEnabled()) {
            logger.info("Removed a file {} before {} by ttl ({}ms)", resource.getTsFilePath(),
                new Date(timeLowerBound), dataTTL);
          }
          tsFileManagement.remove(resource, isSeq);
        } finally {
          resource.writeUnlock();
        }
      }
    } finally {
      writeUnlock();
    }
  }

  /**
   * This method will be blocked until all tsfile processors are closed.
   */
  public void syncCloseAllWorkingTsFileProcessors() {
    synchronized (closeStorageGroupCondition) {
      try {
        asyncCloseAllWorkingTsFileProcessors();
        long startTime = System.currentTimeMillis();
        while (!closingSequenceTsFileProcessor.isEmpty() || !closingUnSequenceTsFileProcessor
            .isEmpty()) {
          closeStorageGroupCondition.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger.warn("{} has spent {}s to wait for closing all TsFiles.", this.storageGroupName,
                (System.currentTimeMillis() - startTime) / 1000);
          }
        }
        while (compactionMergeWorking) {
          closeStorageGroupCondition.wait(100);
          if (System.currentTimeMillis() - startTime > 60_000) {
            logger
                .warn("{} has spent {}s to wait for closing compaction.", this.storageGroupName,
                    (System.currentTimeMillis() - startTime) / 1000);
          }
        }
      } catch (InterruptedException e) {
        logger.error("CloseFileNodeCondition error occurs while waiting for closing the storage "
            + "group {}", storageGroupName, e);
        Thread.currentThread().interrupt();
      }
    }
  }

  public void asyncCloseAllWorkingTsFileProcessors() {
    writeLock();
    try {
      logger.info("async force close all files in storage group: {}", storageGroupName);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor : new ArrayList<>(
          workSequenceTsFileProcessors.values())) {
        asyncCloseOneTsFileProcessor(true, tsFileProcessor);
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor : new ArrayList<>(
          workUnsequenceTsFileProcessors.values())) {
        asyncCloseOneTsFileProcessor(false, tsFileProcessor);
      }
    } finally {
      writeUnlock();
    }
  }

  public void forceCloseAllWorkingTsFileProcessors() throws TsFileProcessorException {
    writeLock();
    try {
      logger.info("force close all processors in storage group: {}", storageGroupName);
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor : new ArrayList<>(
          workSequenceTsFileProcessors.values())) {
        tsFileProcessor.putMemTableBackAndClose();
      }
      // to avoid concurrent modification problem, we need a new array list
      for (TsFileProcessor tsFileProcessor : new ArrayList<>(
          workUnsequenceTsFileProcessors.values())) {
        tsFileProcessor.putMemTableBackAndClose();
      }
    } finally {
      writeUnlock();
    }
  }

  // TODO need a read lock, please consider the concurrency with flush manager threads.
  public QueryDataSource query(PartialPath deviceId, String measurementId, QueryContext context,
      QueryFileManager filePathsManager, Filter timeFilter) throws QueryProcessException {
    insertLock.readLock().lock();
    try {
      List<TsFileResource> seqResources = getFileResourceListForQuery(
          tsFileManagement.getTsFileList(true),
          upgradeSeqFileList, deviceId, measurementId, context, timeFilter, true);
      List<TsFileResource> unseqResources = getFileResourceListForQuery(
          tsFileManagement.getTsFileList(false),
          upgradeUnseqFileList, deviceId, measurementId, context, timeFilter, false);
      QueryDataSource dataSource = new QueryDataSource(deviceId,
          seqResources, unseqResources);
      // used files should be added before mergeLock is unlocked, or they may be deleted by
      // running merge
      // is null only in tests
      if (filePathsManager != null) {
        filePathsManager.addUsedFilesForQuery(context.getQueryId(), dataSource);
      }
      dataSource.setDataTTL(dataTTL);
      return dataSource;
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    } finally {
      insertLock.readLock().unlock();
    }
  }

  public void writeLock() {
    insertLock.writeLock().lock();
  }

  public void writeUnlock() {
    insertLock.writeLock().unlock();
  }


  /**
   * @param tsFileResources includes sealed and unsealed tsfile resources
   * @return fill unsealed tsfile resources with memory data and ChunkMetadataList of data in disk
   */
  private List<TsFileResource> getFileResourceListForQuery(
      Collection<TsFileResource> tsFileResources, List<TsFileResource> upgradeTsFileResources,
      PartialPath deviceId, String measurementId, QueryContext context, Filter timeFilter,
      boolean isSeq)
      throws MetadataException {

    if (config.isDebugOn()) {
      DEBUG_LOGGER
          .info("Path: {}.{}, get tsfile list: {} isSeq: {} timefilter: {}", deviceId.getFullPath(),
              measurementId, tsFileResources, isSeq, (timeFilter == null ? "null" : timeFilter));
    }

    MeasurementSchema schema = IoTDB.metaManager.getSeriesSchema(deviceId, measurementId);

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    long timeLowerBound = dataTTL != Long.MAX_VALUE ? System.currentTimeMillis() - dataTTL : Long
        .MIN_VALUE;
    context.setQueryTimeLowerBound(timeLowerBound);

    for (TsFileResource tsFileResource : tsFileResources) {
      if (!isTsFileResourceSatisfied(tsFileResource, deviceId.getFullPath(), timeFilter, isSeq)) {
        continue;
      }
      closeQueryLock.readLock().lock();
      try {
        if (tsFileResource.isClosed()) {
          tsfileResourcesForQuery.add(tsFileResource);
        } else {
          tsFileResource.getUnsealedFileProcessor()
              .query(deviceId.getFullPath(), measurementId, schema.getType(),
                  schema.getEncodingType(),
                  schema.getProps(), context, tsfileResourcesForQuery);
        }
      } catch (IOException e) {
        throw new MetadataException(e);
      } finally {
        closeQueryLock.readLock().unlock();
      }
    }
    // for upgrade files and old files must be closed
    for (TsFileResource tsFileResource : upgradeTsFileResources) {
      if (!isTsFileResourceSatisfied(tsFileResource, deviceId.getFullPath(), timeFilter, isSeq)) {
        continue;
      }
      closeQueryLock.readLock().lock();
      try {
        tsfileResourcesForQuery.add(tsFileResource);
      } finally {
        closeQueryLock.readLock().unlock();
      }
    }
    return tsfileResourcesForQuery;
  }

  /**
   * @return true if the device is contained in the TsFile and it lives beyond TTL
   */
  private boolean isTsFileResourceSatisfied(TsFileResource tsFileResource, String deviceId,
      Filter timeFilter, boolean isSeq) {
    if (!tsFileResource.containsDevice(deviceId)) {
      if (config.isDebugOn()) {
        DEBUG_LOGGER.info("Path: {} file {} is not satisfied because of no device!", deviceId,
            tsFileResource);
      }
      return false;
    }

    int deviceIndex = tsFileResource.getDeviceToIndexMap().get(deviceId);
    long startTime = tsFileResource.getStartTime(deviceIndex);
    long endTime = tsFileResource.isClosed() || !isSeq ? tsFileResource.getEndTime(deviceIndex)
        : Long.MAX_VALUE;

    if (!isAlive(endTime)) {
      if (config.isDebugOn()) {
        DEBUG_LOGGER
            .info("Path: {} file {} is not satisfied because of ttl!", deviceId, tsFileResource);
      }
      return false;
    }

    if (timeFilter != null) {
      boolean res = timeFilter.satisfyStartEndTime(startTime, endTime);
      if (config.isDebugOn() && !res) {
        DEBUG_LOGGER.info("Path: {} file {} is not satisfied because of time filter!", deviceId,
            tsFileResource);
      }
      return res;
    }
    return true;
  }

  /**
   * Delete data whose timestamp <= 'timestamp' and belongs to the time series
   * deviceId.measurementId.
   *
   * @param path      the timeseries path of the to be deleted.
   * @param startTime the startTime of delete range.
   * @param endTime   the endTime of delete range.
   * @param planIndex
   */
  public void delete(PartialPath path, long startTime, long endTime, long planIndex)
      throws IOException {
    // TODO: how to avoid partial deletion?
    // FIXME: notice that if we may remove a SGProcessor out of memory, we need to close all opened
    //mod files in mergingModification, sequenceFileList, and unsequenceFileList
    writeLock();
    tsFileManagement.writeLock();

    // record files which are updated so that we can roll back them in case of exception
    List<ModificationFile> updatedModFiles = new ArrayList<>();

    try {
      Set<PartialPath> devicePaths = IoTDB.metaManager.getDevices(path.getDevicePath());
      for (PartialPath device : devicePaths) {
        Long lastUpdateTime = null;
        for (Map<String, Long> latestTimeMap : latestTimeForEachDevice.values()) {
          Long curTime = latestTimeMap.get(device.getFullPath());
          if (curTime != null && (lastUpdateTime == null || lastUpdateTime < curTime)) {
            lastUpdateTime = curTime;
          }
        }
        // There is no tsfile data, the delete operation is invalid
        if (lastUpdateTime == null) {
          logger.debug("No device {} in SG {}, deletion invalid", device, storageGroupName);
          return;
        }
        // delete Last cache record if necessary
        tryToDeleteLastCache(device, path, startTime, endTime);
      }

      // write log to impacted working TsFileProcessors
      logDeletion(startTime, endTime, path);

      Deletion deletion = new Deletion(path, MERGE_MOD_START_VERSION_NUM, startTime, endTime);
      if (tsFileManagement.mergingModification != null) {
        tsFileManagement.mergingModification.write(deletion);
        updatedModFiles.add(tsFileManagement.mergingModification);
      }

      deleteDataInFiles(tsFileManagement.getTsFileList(true), deletion, devicePaths,
          updatedModFiles, planIndex);
      deleteDataInFiles(tsFileManagement.getTsFileList(false), deletion, devicePaths,
          updatedModFiles, planIndex);

    } catch (Exception e) {
      // roll back
      for (ModificationFile modFile : updatedModFiles) {
        modFile.abort();
      }
      throw new IOException(e);
    } finally {
      tsFileManagement.writeUnlock();
      writeUnlock();
    }
  }

  private void logDeletion(long startTime, long endTime, PartialPath path)
      throws IOException {
    long timePartitionStartId = StorageEngine.getTimePartition(startTime);
    long timePartitionEndId = StorageEngine.getTimePartition(endTime);
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      DeletePlan deletionPlan = new DeletePlan(startTime, endTime, path);
      for (Map.Entry<Long, TsFileProcessor> entry : workSequenceTsFileProcessors.entrySet()) {
        if (timePartitionStartId <= entry.getKey() && entry.getKey() <= timePartitionEndId) {
          entry.getValue().getLogNode().write(deletionPlan);
        }
      }

      for (Map.Entry<Long, TsFileProcessor> entry : workUnsequenceTsFileProcessors.entrySet()) {
        if (timePartitionStartId <= entry.getKey() && entry.getKey() <= timePartitionEndId) {
          entry.getValue().getLogNode().write(deletionPlan);
        }
      }
    }
  }

  private boolean canSkipDelete(TsFileResource tsFileResource, Set<PartialPath> devicePaths,
      long deleteStart, long deleteEnd) {
    for (PartialPath device : devicePaths) {
      if (tsFileResource.containsDevice(device.getFullPath()) &&
          (deleteEnd >= tsFileResource.getStartTime(device.getFullPath()) &&
              deleteStart <= tsFileResource
                  .getOrDefaultEndTime(device.getFullPath(), Long.MAX_VALUE))) {
        return false;
      }
    }
    return true;
  }

  private void deleteDataInFiles(Collection<TsFileResource> tsFileResourceList, Deletion deletion,
      Set<PartialPath> devicePaths, List<ModificationFile> updatedModFiles, long planIndex)
      throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      if (canSkipDelete(tsFileResource, devicePaths, deletion.getStartTime(),
          deletion.getEndTime())) {
        continue;
      }

      long partitionId = tsFileResource.getTimePartition();
      deletion.setVersionNum(getVersionControllerByTimePartitionId(partitionId).nextVersion());

      // write deletion into modification file
      tsFileResource.getModFile().write(deletion);
      // remember to close mod file
      tsFileResource.getModFile().close();

      tsFileResource.updatePlanIndexes(planIndex);

      // delete data in memory of unsealed file
      if (!tsFileResource.isClosed()) {
        TsFileProcessor tsfileProcessor = tsFileResource.getUnsealedFileProcessor();
        tsfileProcessor.deleteDataInMemory(deletion, devicePaths);
      }

      // add a record in case of rollback
      updatedModFiles.add(tsFileResource.getModFile());
    }
  }

  private void tryToDeleteLastCache(PartialPath deviceId, PartialPath originalPath,
      long startTime, long endTime) throws WriteProcessException {
    if (!IoTDBDescriptor.getInstance().getConfig().isLastCacheEnabled()) {
      return;
    }
    try {
      MNode node = IoTDB.metaManager.getDeviceNode(deviceId);

      for (MNode measurementNode : node.getChildren().values()) {
        if (measurementNode != null && originalPath
            .matchFullPath(measurementNode.getPartialPath())) {
          TimeValuePair lastPair = ((MeasurementMNode) measurementNode).getCachedLast();
          if (lastPair != null && startTime <= lastPair.getTimestamp()
              && lastPair.getTimestamp() <= endTime) {
            ((MeasurementMNode) measurementNode).resetCache();
          }
        }
      }
    } catch (MetadataException e) {
      throw new WriteProcessException(e);
    }
  }

  /**
   * when close an TsFileProcessor, update its EndTimeMap immediately
   *
   * @param tsFileProcessor processor to be closed
   */
  private void updateEndTimeMap(TsFileProcessor tsFileProcessor) {
    TsFileResource resource = tsFileProcessor.getTsFileResource();
    for (Entry<String, Integer> startTime : resource.getDeviceToIndexMap().entrySet()) {
      String deviceId = startTime.getKey();
      resource.forceUpdateEndTime(deviceId,
          latestTimeForEachDevice.get(tsFileProcessor.getTimeRangeId()).get(deviceId));
    }
  }

  private boolean unsequenceFlushCallback(TsFileProcessor processor) {
    return true;
  }

  private boolean updateLatestFlushTimeCallback(TsFileProcessor processor) {
    // update the largest timestamp in the last flushing memtable
    Map<String, Long> curPartitionDeviceLatestTime = latestTimeForEachDevice
        .get(processor.getTimeRangeId());

    if (curPartitionDeviceLatestTime == null) {
      logger.warn("Partition: {} does't have latest time for each device. "
              + "No valid record is written into memtable. Flushing tsfile is: {}",
          processor.getTimeRangeId(), processor.getTsFileResource().getTsFile());
      return false;
    }

    for (Entry<String, Long> entry : curPartitionDeviceLatestTime.entrySet()) {
      partitionLatestFlushedTimeForEachDevice
          .computeIfAbsent(processor.getTimeRangeId(), id -> new HashMap<>())
          .put(entry.getKey(), entry.getValue());
      updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(processor.getTimeRangeId(),
          entry.getKey(), entry.getValue());
      if (globalLatestFlushedTimeForEachDevice
          .getOrDefault(entry.getKey(), Long.MIN_VALUE) < entry.getValue()) {
        globalLatestFlushedTimeForEachDevice.put(entry.getKey(), entry.getValue());
      }
    }
    return true;
  }


  /**
   * used for upgrading
   */
  public void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(long partitionId,
      String deviceId, long time) {
    newlyFlushedPartitionLatestFlushedTimeForEachDevice
        .computeIfAbsent(partitionId, id -> new HashMap<>())
        .compute(deviceId, (k, v) -> v == null ? time : Math.max(v, time));
  }

  /**
   * put the memtable back to the MemTablePool and make the metadata in writer visible
   */
  // TODO please consider concurrency with query and insert method.
  private void closeUnsealedTsFileProcessorCallBack(
      TsFileProcessor tsFileProcessor) throws TsFileProcessorException {
    closeQueryLock.writeLock().lock();
    try {
      tsFileProcessor.close();
    } finally {
      closeQueryLock.writeLock().unlock();
    }
    //closingSequenceTsFileProcessor is a thread safety class.
    if (closingSequenceTsFileProcessor.contains(tsFileProcessor)) {
      closingSequenceTsFileProcessor.remove(tsFileProcessor);
    } else {
      closingUnSequenceTsFileProcessor.remove(tsFileProcessor);
    }
    if (!compactionMergeWorking && !CompactionMergeTaskPoolManager.getInstance()
        .isTerminated()) {
      compactionMergeWorking = true;
      logger.info("{} submit a compaction merge task", storageGroupName);
      try {
        // fork and filter current tsfile, then commit then to compaction merge
        tsFileManagement.forkCurrentFileList(tsFileProcessor.getTimeRangeId());
        CompactionMergeTaskPoolManager.getInstance()
            .submitTask(
                tsFileManagement.new CompactionMergeTask(this::closeCompactionMergeCallBack,
                    tsFileProcessor.getTimeRangeId()));
      } catch (IOException | RejectedExecutionException e) {
        this.closeCompactionMergeCallBack();
        logger.error("{} compaction submit task failed", storageGroupName);
      }
    } else {
      logger.info("{} last compaction merge task is working, skip current merge",
          storageGroupName);
    }
    synchronized (closeStorageGroupCondition) {
      closeStorageGroupCondition.notifyAll();
    }
    logger.info("signal closing storage group condition in {}", storageGroupName);
  }

  /**
   * close compaction merge callback, to release some locks
   */
  private void closeCompactionMergeCallBack() {
    this.compactionMergeWorking = false;
    synchronized (closeStorageGroupCondition) {
      closeStorageGroupCondition.notifyAll();
    }
  }

  /**
   * count all Tsfiles in the storage group which need to be upgraded
   *
   * @return total num of the tsfiles which need to be upgraded in the storage group
   */
  public int countUpgradeFiles() {
    return upgradeSeqFileList.size() + upgradeUnseqFileList.size();
  }

  public void upgrade() {
    for (TsFileResource seqTsFileResource : upgradeSeqFileList) {
      seqTsFileResource.setSeq(true);
      seqTsFileResource.setUpgradeTsFileResourceCallBack(this::upgradeTsFileResourceCallBack);
      seqTsFileResource.doUpgrade();
    }
    for (TsFileResource unseqTsFileResource : upgradeUnseqFileList) {
      unseqTsFileResource.setSeq(false);
      unseqTsFileResource.setUpgradeTsFileResourceCallBack(this::upgradeTsFileResourceCallBack);
      unseqTsFileResource.doUpgrade();
    }
  }

  private void upgradeTsFileResourceCallBack(TsFileResource tsFileResource) {
    List<TsFileResource> upgradedResources = tsFileResource.getUpgradedResources();
    for (TsFileResource resource : upgradedResources) {
      long partitionId = resource.getTimePartition();
      resource.getDeviceToIndexMap().forEach((device, index) ->
          updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(partitionId, device,
              resource.getEndTime(index))
      );
    }
    insertLock.writeLock().lock();
    tsFileManagement.writeLock();
    try {
      if (tsFileResource.isSeq()) {
        tsFileManagement.addAll(upgradedResources, true);
        upgradeSeqFileList.remove(tsFileResource);
      } else {
        tsFileManagement.addAll(upgradedResources, false);
        upgradeUnseqFileList.remove(tsFileResource);
      }
    } finally {
      tsFileManagement.writeUnlock();
      insertLock.writeLock().unlock();
    }

    // after upgrade complete, update partitionLatestFlushedTimeForEachDevice
    if (countUpgradeFiles() == 0) {
      for (Entry<Long, Map<String, Long>> entry : newlyFlushedPartitionLatestFlushedTimeForEachDevice
          .entrySet()) {
        long timePartitionId = entry.getKey();
        Map<String, Long> latestFlushTimeForPartition = partitionLatestFlushedTimeForEachDevice
            .getOrDefault(timePartitionId, new HashMap<>());
        for (Entry<String, Long> endTimeMap : entry.getValue().entrySet()) {
          String device = endTimeMap.getKey();
          long endTime = endTimeMap.getValue();
          if (latestFlushTimeForPartition.getOrDefault(device, Long.MIN_VALUE) < endTime) {
            partitionLatestFlushedTimeForEachDevice
                .computeIfAbsent(timePartitionId, id -> new HashMap<>()).put(device, endTime);
          }
        }
      }
      if (StorageEngine.getInstance().countUpgradeFiles() == 0) {
        UpgradeSevice.getINSTANCE().stop();
      }
    }
  }

  public void merge(boolean fullMerge) {
    writeLock();
    try {
      this.tsFileManagement.merge(fullMerge, tsFileManagement.getTsFileList(true),
          tsFileManagement.getTsFileList(false), dataTTL);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Load a new tsfile to storage group processor. Tne file may have overlap with other files.
   * <p>
   * or unsequence list.
   * <p>
   * Secondly, execute the loading process by the type.
   * <p>
   * Finally, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @param newTsFileResource tsfile resource
   * @UsedBy sync module.
   */
  public void loadNewTsFileForSync(TsFileResource newTsFileResource) throws LoadFileException {
    File tsfileToBeInserted = newTsFileResource.getTsFile();
    long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();
    writeLock();
    tsFileManagement.writeLock();
    try {
      if (loadTsFileByType(LoadTsFileType.LOAD_SEQUENCE, tsfileToBeInserted, newTsFileResource,
          newFilePartitionId)) {
        updateLatestTimeMap(newTsFileResource);
      }
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to storage group processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(), tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new LoadFileException(e);
    } finally {
      tsFileManagement.writeUnlock();
      writeUnlock();
    }
  }

  /**
   * Load a new tsfile to storage group processor. Tne file may have overlap with other files. <p>
   * that there has no file which is overlapping with the new file.
   * <p>
   * Firstly, determine the loading type of the file, whether it needs to be loaded in sequence list
   * or unsequence list.
   * <p>
   * Secondly, execute the loading process by the type.
   * <p>
   * Finally, update the latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @param newTsFileResource tsfile resource
   * @UsedBy load external tsfile module
   */
  public void loadNewTsFile(TsFileResource newTsFileResource)
      throws LoadFileException {
    File tsfileToBeInserted = newTsFileResource.getTsFile();
    long newFilePartitionId = newTsFileResource.getTimePartitionWithCheck();
    writeLock();
    tsFileManagement.writeLock();
    try {
      List<TsFileResource> sequenceList = tsFileManagement.getTsFileList(true);

      int insertPos = findInsertionPosition(newTsFileResource, newFilePartitionId, sequenceList);
      if (insertPos == POS_ALREADY_EXIST) {
        return;
      }

      // loading tsfile by type
      if (insertPos == POS_OVERLAP) {
        loadTsFileByType(LoadTsFileType.LOAD_UNSEQUENCE, tsfileToBeInserted, newTsFileResource,
            newFilePartitionId);
      } else {

        // check whether the file name needs to be renamed.
        if (!tsFileManagement.isEmpty(true)) {
          String newFileName = getFileNameForLoadingFile(tsfileToBeInserted.getName(), insertPos,
              newTsFileResource.getTimePartition(), sequenceList);
          if (!newFileName.equals(tsfileToBeInserted.getName())) {
            logger.info("Tsfile {} must be renamed to {} for loading into the sequence list.",
                tsfileToBeInserted.getName(), newFileName);
            newTsFileResource
                .setFile(fsFactory.getFile(tsfileToBeInserted.getParentFile(), newFileName));
          }
        }
        loadTsFileByType(LoadTsFileType.LOAD_SEQUENCE, tsfileToBeInserted, newTsFileResource,
            newFilePartitionId);
      }

      // update latest time map
      updateLatestTimeMap(newTsFileResource);
    } catch (DiskSpaceInsufficientException e) {
      logger.error(
          "Failed to append the tsfile {} to storage group processor {} because the disk space is insufficient.",
          tsfileToBeInserted.getAbsolutePath(), tsfileToBeInserted.getParentFile().getName());
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new LoadFileException(e);
    } finally {
      tsFileManagement.writeUnlock();
      writeUnlock();
    }
  }

  /**
   * Set the version in "partition" to "version" if "version" is larger than the current version.
   */
  public void setPartitionFileVersionToMax(long partition, long version) {
    partitionMaxFileVersions
        .compute(partition, (prt, oldVer) -> computeMaxVersion(oldVer, version));
  }

  private long computeMaxVersion(Long oldVersion, Long newVersion) {
    if (oldVersion == null) {
      return newVersion;
    }
    return Math.max(oldVersion, newVersion);
  }

  /**
   * Find the position of "newTsFileResource" in the sequence files if it can be inserted into
   * them.
   *
   * @return POS_ALREADY_EXIST(- 2) if some file has the same name as the one to be inserted
   * POS_OVERLAP(-3) if some file overlaps the new file an insertion position i >= -1 if the new
   * file can be inserted between [i, i+1]
   */
  private int findInsertionPosition(TsFileResource newTsFileResource, long newFilePartitionId,
      List<TsFileResource> sequenceList) {
    File tsfileToBeInserted = newTsFileResource.getTsFile();

    int insertPos = -1;

    // find the position where the new file should be inserted
    for (int i = 0; i < sequenceList.size(); i++) {
      TsFileResource localFile = sequenceList.get(i);
      if (localFile.getTsFile().getName().equals(tsfileToBeInserted.getName())) {
        return POS_ALREADY_EXIST;
      }
      long localPartitionId = Long.parseLong(localFile.getTsFile().getParentFile().getName());
      if (i == sequenceList.size() - 1 && localFile.areEndTimesEmpty()
          || newFilePartitionId > localPartitionId) {
        // skip files that are in the previous partition and the last empty file, as the all data
        // in those files must be older than the new file
        continue;
      }

      if (!localFile.isClosed() && localFile.getProcessor() != null) {
        // we cannot compare two files by TsFileResource unless they are both closed
        syncCloseOneTsFileProcessor(true, localFile.getProcessor());
      }
      int fileComparison = compareTsFileDevices(newTsFileResource, localFile);
      switch (fileComparison) {
        case 0:
          // some devices are newer but some devices are older, the two files overlap in general
          return POS_OVERLAP;
        case -1:
          // all devices in localFile are newer than the new file, the new file can be
          // inserted before localFile
          return i - 1;
        default:
          // all devices in the local file are older than the new file, proceed to the next file
          insertPos = i;
      }
    }
    return insertPos;
  }

  /**
   * Compare each device in the two files to find the time relation of them.
   *
   * @return -1 if fileA is totally older than fileB (A < B) 0 if fileA is partially older than
   * fileB and partially newer than fileB (A X B) 1 if fileA is totally newer than fileB (B < A)
   */
  private int compareTsFileDevices(TsFileResource fileA, TsFileResource fileB) {
    boolean hasPre = false, hasSubsequence = false;
    for (String device : fileA.getDeviceToIndexMap().keySet()) {
      if (!fileB.getDeviceToIndexMap().containsKey(device)) {
        continue;
      }
      long startTimeA = fileA.getStartTime(device);
      long endTimeA = fileA.getEndTime(device);
      long startTimeB = fileB.getStartTime(device);
      long endTimeB = fileB.getEndTime(device);
      if (startTimeA > endTimeB) {
        // A's data of the device is later than to the B's data
        hasPre = true;
      } else if (startTimeB > endTimeA) {
        // A's data of the device is previous to the B's data
        hasSubsequence = true;
      } else {
        // the two files overlap in the device
        return 0;
      }
    }
    if (hasPre && hasSubsequence) {
      // some devices are newer but some devices are older, the two files overlap in general
      return 0;
    }
    if (!hasPre && hasSubsequence) {
      // all devices in B are newer than those in A
      return -1;
    }
    // all devices in B are older than those in A
    return 1;
  }

  /**
   * If the historical versions of a file is a sub-set of the given file's, (close and) remove it to
   * reduce unnecessary merge. Only used when the file sender and the receiver share the same file
   * close policy. Warning: DO NOT REMOVE
   */
  @SuppressWarnings("unused")
  public void removeFullyOverlapFiles(TsFileResource resource) {
    writeLock();
    try {
      Iterator<TsFileResource> iterator = tsFileManagement.getIterator(true);
      removeFullyOverlapFiles(resource, iterator, true);

      iterator = tsFileManagement.getIterator(false);
      removeFullyOverlapFiles(resource, iterator, false);
    } finally {
      writeUnlock();
    }
  }

  private void removeFullyOverlapFiles(TsFileResource newTsFile, Iterator<TsFileResource> iterator
      , boolean isSeq) {
    while (iterator.hasNext()) {
      TsFileResource existingTsFile = iterator.next();
      if (newTsFile.isPlanRangeCovers(existingTsFile)
          && !newTsFile.getTsFile().equals(existingTsFile.getTsFile())
          && existingTsFile.tryWriteLock()) {
        logger.info("{} is covered by {}: [{}, {}], [{}, {}], remove it", existingTsFile,
            newTsFile, existingTsFile.minPlanIndex, existingTsFile.maxPlanIndex,
            newTsFile.minPlanIndex, newTsFile.maxPlanIndex);
        // if we fail to lock the file, it means it is being queried or merged and we will not
        // wait until it is free, we will just leave it to the next merge
        try {
          removeFullyOverlapFile(existingTsFile, iterator, isSeq);
        } catch (Exception e) {
          logger.error("Something gets wrong while removing FullyOverlapFiles: {}",
              existingTsFile.getTsFile().getAbsolutePath(), e);
        } finally {
          existingTsFile.writeUnlock();
        }
      }
    }
  }

  /**
   * remove the given tsFileResource. If the corresponding tsFileProcessor is in the working status,
   * close it before remove the related resource files. maybe time-consuming for closing a tsfile.
   */
  private void removeFullyOverlapFile(TsFileResource tsFileResource,
      Iterator<TsFileResource> iterator
      , boolean isSeq) {
    logger.info("Removing a covered file {}, closed: {}", tsFileResource, tsFileResource.isClosed());
    if (!tsFileResource.isClosed()) {
      try {
        // also remove the TsFileProcessor if the overlapped file is not closed
        long timePartition = tsFileResource.getTimePartition();
        Map<Long, TsFileProcessor> fileProcessorMap = isSeq ? workSequenceTsFileProcessors :
            workUnsequenceTsFileProcessors;
        TsFileProcessor tsFileProcessor = fileProcessorMap.get(timePartition);
        if (tsFileProcessor != null && tsFileProcessor.getTsFileResource() == tsFileResource) {
          //have to take some time to close the tsFileProcessor
          tsFileProcessor.syncClose();
          fileProcessorMap.remove(timePartition);
        }
      } catch (Exception e) {
        logger.error("Cannot close {}", tsFileResource, e);
      }
    }
    tsFileManagement.remove(tsFileResource, isSeq);
    iterator.remove();
    tsFileResource.remove();
  }

  /**
   * Get an appropriate filename to ensure the order between files. The tsfile is named after
   * ({systemTime}-{versionNum}-{mergeNum}.tsfile).
   * <p>
   * The sorting rules for tsfile names @see {@link this#compareFileName}, we can restore the list
   * based on the file name and ensure the correctness of the order, so there are three cases.
   * <p>
   * 1. The tsfile is to be inserted in the first place of the list. If the timestamp in the file
   * name is less than the timestamp in the file name of the first tsfile in the list, then the file
   * name is legal and the file name is returned directly. Otherwise, its timestamp can be set to
   * half of the timestamp value in the file name of the first tsfile in the list , and the version
   * number is the version number in the file name of the first tsfile in the list.
   * <p>
   * 2. The tsfile is to be inserted in the last place of the list. If the timestamp in the file
   * name is lager than the timestamp in the file name of the last tsfile  in the list, then the
   * file name is legal and the file name is returned directly. Otherwise, the file name is
   * generated by the system according to the naming rules and returned.
   * <p>
   * 3. This file is inserted between two files. If the timestamp in the name of the file satisfies
   * the timestamp between the timestamps in the name of the two files, then it is a legal name and
   * returns directly; otherwise, the time stamp is the mean of the timestamps of the two files, the
   * version number is the version number in the tsfile with a larger timestamp.
   *
   * @param tsfileName  origin tsfile name
   * @param insertIndex the new file will be inserted between the files [insertIndex, insertIndex +
   *                    1]
   * @return appropriate filename
   */
  private String getFileNameForLoadingFile(String tsfileName, int insertIndex,
      long timePartitionId, List<TsFileResource> sequenceList) {
    long currentTsFileTime = Long
        .parseLong(tsfileName.split(FILE_NAME_SEPARATOR)[0]);
    long preTime;
    if (insertIndex == -1) {
      preTime = 0L;
    } else {
      String preName = sequenceList.get(insertIndex).getTsFile().getName();
      preTime = Long.parseLong(preName.split(FILE_NAME_SEPARATOR)[0]);
    }
    if (insertIndex == tsFileManagement.size(true) - 1) {
      if (preTime < currentTsFileTime) {
        return tsfileName;
      } else {
        return getNewTsFileName(timePartitionId);
      }
    }

    String subsequenceName = sequenceList.get(insertIndex + 1).getTsFile().getName();
    long subsequenceTime = Long
        .parseLong(subsequenceName.split(FILE_NAME_SEPARATOR)[0]);
    long subsequenceVersion = Long
        .parseLong(subsequenceName.split(FILE_NAME_SEPARATOR)[1]);
    if (preTime < currentTsFileTime && currentTsFileTime < subsequenceTime) {
      return tsfileName;
    }

    return getNewTsFileName(preTime + ((subsequenceTime - preTime) >> 1), subsequenceVersion,
        0);
  }

  /**
   * Update latest time in latestTimeForEachDevice and partitionLatestFlushedTimeForEachDevice.
   *
   * @UsedBy sync module, load external tsfile module.
   */
  private void updateLatestTimeMap(TsFileResource newTsFileResource) {
    for (Entry<String, Integer> entry : newTsFileResource.getDeviceToIndexMap().entrySet()) {
      String device = entry.getKey();
      int index = entry.getValue();
      long endTime = newTsFileResource.getEndTime(index);
      long timePartitionId = StorageEngine.getTimePartition(endTime);
      if (!latestTimeForEachDevice.computeIfAbsent(timePartitionId, id -> new HashMap<>())
          .containsKey(device)
          || latestTimeForEachDevice.get(timePartitionId).get(device) < endTime) {
        latestTimeForEachDevice.get(timePartitionId).put(device, endTime);
      }

      Map<String, Long> latestFlushTimeForPartition = partitionLatestFlushedTimeForEachDevice
          .getOrDefault(timePartitionId, new HashMap<>());

      if (latestFlushTimeForPartition.getOrDefault(device, Long.MIN_VALUE) < endTime) {
        partitionLatestFlushedTimeForEachDevice
            .computeIfAbsent(timePartitionId, id -> new HashMap<>()).put(device, endTime);
      }
      if (globalLatestFlushedTimeForEachDevice.getOrDefault(device, Long.MIN_VALUE) < endTime) {
        globalLatestFlushedTimeForEachDevice.put(device, endTime);
      }
    }
  }

  /**
   * Execute the loading process by the type.
   *
   * @param type            load type
   * @param tsFileResource  tsfile resource to be loaded
   * @param filePartitionId the partition id of the new file
   * @return load the file successfully
   * @UsedBy sync module, load external tsfile module.
   */
  private boolean loadTsFileByType(LoadTsFileType type, File syncedTsFile,
      TsFileResource tsFileResource, long filePartitionId)
      throws LoadFileException, DiskSpaceInsufficientException {
    File targetFile;
    switch (type) {
      case LOAD_UNSEQUENCE:
        targetFile = fsFactory
            .getFile(DirectoryManager.getInstance().getNextFolderForUnSequenceFile(),
                storageGroupName + File.separatorChar + filePartitionId + File.separator
                    + tsFileResource.getTsFile().getName());
        tsFileResource.setFile(targetFile);
        if (tsFileManagement.contains(tsFileResource, false)) {
          logger.error("The file {} has already been loaded in unsequence list", tsFileResource);
          return false;
        }
        tsFileManagement.add(tsFileResource, false);
        logger.info("Load tsfile in unsequence list, move file from {} to {}",
            syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath());
        break;
      case LOAD_SEQUENCE:
        targetFile =
            fsFactory.getFile(DirectoryManager.getInstance().getNextFolderForSequenceFile(),
                storageGroupName + File.separatorChar + filePartitionId + File.separator
                    + tsFileResource.getTsFile().getName());
        tsFileResource.setFile(targetFile);
        if (tsFileManagement.contains(tsFileResource, true)) {
          logger.error("The file {} has already been loaded in sequence list", tsFileResource);
          return false;
        }
        tsFileManagement.add(tsFileResource, true);
        logger.info("Load tsfile in sequence list, move file from {} to {}",
            syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath());
        break;
      default:
        throw new LoadFileException(
            String.format("Unsupported type of loading tsfile : %s", type));
    }

    // move file from sync dir to data dir
    if (!targetFile.getParentFile().exists()) {
      targetFile.getParentFile().mkdirs();
    }
    try {
      FileUtils.moveFile(syncedTsFile, targetFile);
    } catch (IOException e) {
      logger.error("File renaming failed when loading tsfile. Origin: {}, Target: {}",
          syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath(), e);
      throw new LoadFileException(String.format(
          "File renaming failed when loading tsfile. Origin: %s, Target: %s, because %s",
          syncedTsFile.getAbsolutePath(), targetFile.getAbsolutePath(), e.getMessage()));
    }

    File syncedResourceFile = fsFactory.getFile(
        syncedTsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    File targetResourceFile = fsFactory.getFile(
        targetFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX);
    try {
      FileUtils.moveFile(syncedResourceFile, targetResourceFile);
    } catch (IOException e) {
      logger.error("File renaming failed when loading .resource file. Origin: {}, Target: {}",
          syncedResourceFile.getAbsolutePath(), targetResourceFile.getAbsolutePath(), e);
      throw new LoadFileException(String.format(
          "File renaming failed when loading .resource file. Origin: %s, Target: %s, because %s",
          syncedResourceFile.getAbsolutePath(), targetResourceFile.getAbsolutePath(),
          e.getMessage()));
    }
    return true;
  }

  /**
   * Delete tsfile if it exists.
   * <p>
   * Firstly, remove the TsFileResource from sequenceFileList/unSequenceFileList.
   * <p>
   * Secondly, delete the tsfile and .resource file.
   *
   * @param tsfieToBeDeleted tsfile to be deleted
   * @return whether the file to be deleted exists.
   * @UsedBy sync module, load external tsfile module.
   */
  public boolean deleteTsfile(File tsfieToBeDeleted) {
    writeLock();
    tsFileManagement.writeLock();
    TsFileResource tsFileResourceToBeDeleted = null;
    try {
      Iterator<TsFileResource> sequenceIterator = tsFileManagement.getIterator(true);
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getTsFile().getName().equals(tsfieToBeDeleted.getName())) {
          tsFileResourceToBeDeleted = sequenceResource;
          tsFileManagement.remove(tsFileResourceToBeDeleted, true);
          break;
        }
      }
      if (tsFileResourceToBeDeleted == null) {
        Iterator<TsFileResource> unsequenceIterator = tsFileManagement.getIterator(false);
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getTsFile().getName().equals(tsfieToBeDeleted.getName())) {
            tsFileResourceToBeDeleted = unsequenceResource;
            tsFileManagement.remove(tsFileResourceToBeDeleted, false);
            break;
          }
        }
      }
    } finally {
      tsFileManagement.writeUnlock();
      writeUnlock();
    }
    if (tsFileResourceToBeDeleted == null) {
      return false;
    }
    tsFileResourceToBeDeleted.writeLock();
    try {
      tsFileResourceToBeDeleted.remove();
      logger.info("Delete tsfile {} successfully.", tsFileResourceToBeDeleted.getTsFile());
    } finally {
      tsFileResourceToBeDeleted.writeUnlock();
    }
    return true;
  }


  public Collection<TsFileProcessor> getWorkSequenceTsFileProcessors() {
    return workSequenceTsFileProcessors.values();
  }

  /**
   * Move tsfile to the target directory if it exists.
   * <p>
   * Firstly, remove the TsFileResource from sequenceFileList/unSequenceFileList.
   * <p>
   * Secondly, move the tsfile and .resource file to the target directory.
   *
   * @param fileToBeMoved tsfile to be moved
   * @return whether the file to be moved exists.
   * @UsedBy load external tsfile module.
   */
  public boolean moveTsfile(File fileToBeMoved, File targetDir) {
    writeLock();
    tsFileManagement.writeLock();
    TsFileResource tsFileResourceToBeMoved = null;
    try {
      Iterator<TsFileResource> sequenceIterator = tsFileManagement.getIterator(true);
      while (sequenceIterator.hasNext()) {
        TsFileResource sequenceResource = sequenceIterator.next();
        if (sequenceResource.getTsFile().getName().equals(fileToBeMoved.getName())) {
          tsFileResourceToBeMoved = sequenceResource;
          tsFileManagement.remove(tsFileResourceToBeMoved, true);
          break;
        }
      }
      if (tsFileResourceToBeMoved == null) {
        Iterator<TsFileResource> unsequenceIterator = tsFileManagement.getIterator(false);
        while (unsequenceIterator.hasNext()) {
          TsFileResource unsequenceResource = unsequenceIterator.next();
          if (unsequenceResource.getTsFile().getName().equals(fileToBeMoved.getName())) {
            tsFileResourceToBeMoved = unsequenceResource;
            tsFileManagement.remove(tsFileResourceToBeMoved, false);
            break;
          }
        }
      }
    } finally {
      tsFileManagement.writeUnlock();
      writeUnlock();
    }
    if (tsFileResourceToBeMoved == null) {
      return false;
    }
    tsFileResourceToBeMoved.writeLock();
    try {
      tsFileResourceToBeMoved.moveTo(targetDir);
      logger
          .info("Move tsfile {} to target dir {} successfully.",
              tsFileResourceToBeMoved.getTsFile(),
              targetDir.getPath());
    } finally {
      tsFileResourceToBeMoved.writeUnlock();
    }
    return true;
  }


  public Collection<TsFileProcessor> getWorkUnsequenceTsFileProcessors() {
    return workUnsequenceTsFileProcessors.values();
  }

  public void setDataTTL(long dataTTL) {
    this.dataTTL = dataTTL;
    checkFilesTTL();
  }

  public List<TsFileResource> getSequenceFileTreeSet() {
    return tsFileManagement.getTsFileList(true);
  }

  public List<TsFileResource> getUnSequenceFileList() {
    return tsFileManagement.getTsFileList(false);
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public StorageGroupInfo getStorageGroupInfo() {
    return storageGroupInfo;
  }

  /**
   * Check if the data of "tsFileResource" all exist locally by comparing planIndexes in the
   * partition of "partitionNumber". This is available only when the IoTDB instances which generated
   * "tsFileResource" have the same plan indexes as the local one.
   *
   * @return true if any file contains plans with indexes no less than the max plan index of
   * "tsFileResource", otherwise false.
   */
  public boolean isFileAlreadyExist(TsFileResource tsFileResource, long partitionNum) {
    // examine working processor first as they have the largest plan index
    return isFileAlreadyExistInWorking(tsFileResource, partitionNum, getWorkSequenceTsFileProcessors()) ||
        isFileAlreadyExistInWorking(tsFileResource, partitionNum, getWorkUnsequenceTsFileProcessors()) ||
        isFileAlreadyExistInClosed(tsFileResource, partitionNum, getSequenceFileTreeSet()) ||
        isFileAlreadyExistInClosed(tsFileResource, partitionNum, getUnSequenceFileList());
  }

  private boolean isFileAlreadyExistInClosed(TsFileResource tsFileResource, long partitionNum,
      Collection<TsFileResource> existingFiles) {
    for (TsFileResource resource : existingFiles) {
      if (resource.getTimePartition() == partitionNum
          && resource.getMaxPlanIndex() >= tsFileResource.getMaxPlanIndex()) {
        logger.info("{} is covered by a closed file {}: [{}, {}] [{}, {}]", tsFileResource,
            resource, tsFileResource.minPlanIndex, tsFileResource.maxPlanIndex,
            resource.minPlanIndex, resource.maxPlanIndex);
        return true;
      }
    }
    return false;
  }

  private boolean isFileAlreadyExistInWorking(TsFileResource tsFileResource, long partitionNum,
      Collection<TsFileProcessor> workingProcessors) {
    for (TsFileProcessor workingProcesssor : workingProcessors) {
      if (workingProcesssor.getTimeRangeId() == partitionNum) {
        TsFileResource workResource = workingProcesssor.getTsFileResource();
        boolean isCovered =
            workResource.getMaxPlanIndex() >= tsFileResource
                .getMaxPlanIndex();
        if (isCovered) {
          logger.info("{} is covered by a working file {}: [{}, {}] [{}, {}]", tsFileResource,
              workResource, tsFileResource.minPlanIndex, tsFileResource.maxPlanIndex,
              workResource.minPlanIndex, workResource.maxPlanIndex);
        }
        return isCovered;
      }
    }
    return false;
  }

  /**
   * remove all partitions that satisfy a filter.
   */
  public void removePartitions(TimePartitionFilter filter) {
    // this requires blocking all other activities
    insertLock.writeLock().lock();
    tsFileManagement.writeLock();
    try {
      // abort ongoing merges
      MergeManager.getINSTANCE().abortMerge(storageGroupName);
      // close all working files that should be removed
      removePartitions(filter, workSequenceTsFileProcessors.entrySet());
      removePartitions(filter, workUnsequenceTsFileProcessors.entrySet());

      // remove data files
      removePartitions(filter, tsFileManagement.getIterator(true));
      removePartitions(filter, tsFileManagement.getIterator(false));

    } finally {
      tsFileManagement.writeUnlock();
      insertLock.writeLock().unlock();
    }
  }

  //may remove the processorEntrys
  private void removePartitions(TimePartitionFilter filter,
      Set<Entry<Long, TsFileProcessor>> processorEntrys) {
    for (Iterator<Entry<Long, TsFileProcessor>> iterator = processorEntrys.iterator();
        iterator.hasNext(); ) {
      Entry<Long, TsFileProcessor> longTsFileProcessorEntry = iterator.next();
      long partitionId = longTsFileProcessorEntry.getKey();
      TsFileProcessor processor = longTsFileProcessorEntry.getValue();
      if (filter.satisfy(storageGroupName, partitionId)) {
        processor.syncClose();
        iterator.remove();
        logger.debug("{} is removed during deleting partitions",
            processor.getTsFileResource().getTsFilePath());
      }
    }
  }

  //may remove the iterator's data
  private void removePartitions(TimePartitionFilter filter, Iterator<TsFileResource> iterator) {
    while (iterator.hasNext()) {
      TsFileResource tsFileResource = iterator.next();
      if (filter.satisfy(storageGroupName, tsFileResource.getTimePartition())) {
        tsFileResource.remove();
        iterator.remove();
        logger.debug("{} is removed during deleting partitions", tsFileResource.getTsFilePath());
      }
    }
  }

  public TsFileManagement getTsFileManagement() {
    return tsFileManagement;
  }

  private enum LoadTsFileType {
    LOAD_SEQUENCE, LOAD_UNSEQUENCE
  }

  @FunctionalInterface
  public interface CloseTsFileCallBack {

    void call(TsFileProcessor caller) throws TsFileProcessorException, IOException;
  }

  @FunctionalInterface
  public interface UpdateEndTimeCallBack {

    boolean call(TsFileProcessor caller);
  }

  @FunctionalInterface
  public interface UpgradeTsFileResourceCallBack {

    void call(TsFileResource caller);
  }

  @FunctionalInterface
  public interface CloseCompactionMergeCallBack {

    void call();
  }

  @FunctionalInterface
  public interface TimePartitionFilter {

    boolean satisfy(String storageGroupName, long timePartitionId);
  }

  @TestOnly
  public long getPartitionMaxFileVersions(long partitionId) {
    return partitionMaxFileVersions.getOrDefault(partitionId, -1L);
  }

  public void setCustomCloseFileListeners(
      List<CloseFileListener> customCloseFileListeners) {
    this.customCloseFileListeners = customCloseFileListeners;
  }

  public void setCustomFlushListeners(
      List<FlushListener> customFlushListeners) {
    this.customFlushListeners = customFlushListeners;
  }
}
