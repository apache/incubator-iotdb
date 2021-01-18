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

package org.apache.iotdb.db.engine.compaction.dynamic;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.compaction.no.NoCompactionTsFileManagement.compareFileName;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The TsFileManagement for LEVEL_COMPACTION, use level struct to manage TsFile list
 */
public class DynamicCompactionTsFileManagement extends TsFileManagement {

  private static final Logger logger = LoggerFactory
      .getLogger(DynamicCompactionTsFileManagement.class);

  private final int unseqLevelNum = IoTDBDescriptor.getInstance().getConfig()
      .getUnseqLevelNum();
  private final int unseqFileNumInEachLevel = IoTDBDescriptor.getInstance().getConfig()
      .getSeqFileNumInEachLevel();

  private final boolean enableUnseqCompaction = IoTDBDescriptor.getInstance().getConfig()
      .isEnableUnseqCompaction();
  private final boolean isForceFullMerge = IoTDBDescriptor.getInstance().getConfig()
      .isForceFullMerge();
  private final long queryTimeInterval = IoTDBDescriptor.getInstance().getConfig()
      .getQueryTimeInterval();
  // First map is partition list; Second list is file list in level;
  private final Map<Long, TreeSet<TsFileResource>> sequenceTsFileResources = new ConcurrentSkipListMap<>();
  private final Map<Long, List<List<TsFileResource>>> unSequenceTsFileResources = new ConcurrentSkipListMap<>();
  private final List<TsFileResource> forkedSequenceTsFileResources = new ArrayList<>();
  private final List<List<TsFileResource>> forkedUnSequenceTsFileResources = new ArrayList<>();
  private final List<TsFileResource> sequenceRecoverTsFileResources = new CopyOnWriteArrayList<>();
  private final List<TsFileResource> unSequenceRecoverTsFileResources = new CopyOnWriteArrayList<>();

  public DynamicCompactionTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
    clear();
  }

  private void deleteFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
    logger.debug("{} [compaction] mergeSeq starts to delete real file", storageGroupName);
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      deleteFile(mergeTsFile);
      logger
          .info("{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  private void deleteFilesInList(long timePartitionId, Collection<TsFileResource> mergeTsFiles) {
    logger.debug("{} [compaction] mergeSeq starts to delete file list", storageGroupName);
    if (sequenceTsFileResources.containsKey(timePartitionId)) {
      sequenceTsFileResources.get(timePartitionId).removeAll(mergeTsFiles);
    }
    if (unSequenceTsFileResources.containsKey(timePartitionId)) {
      unSequenceTsFileResources.get(timePartitionId).removeAll(mergeTsFiles);
    }
  }

  private void deleteFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  @Override
  public List<TsFileResource> getStableTsFileList(boolean sequence) {
    return getTsFileList(sequence);
  }

  @Override
  public List<TsFileResource> getTsFileList(boolean sequence) {
    List<TsFileResource> result = new ArrayList<>();
    if (sequence) {
      for (TreeSet<TsFileResource> sequenceTsFileList : sequenceTsFileResources.values()) {
        result.addAll(sequenceTsFileList);
      }
    } else {
      for (List<List<TsFileResource>> unSequenceTsFileList : unSequenceTsFileResources.values()) {
        for (int i = unSequenceTsFileList.size() - 1; i >= 0; i--) {
          result.addAll(unSequenceTsFileList.get(i));
        }
      }
    }
    return result;
  }

  @Override
  public Iterator<TsFileResource> getIterator(boolean sequence) {
    return getTsFileList(sequence).iterator();
  }

  @Override
  public void remove(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      sequenceTsFileResources.get(tsFileResource.getTimePartition()).remove(tsFileResource);
    } else {
      unSequenceTsFileResources.get(tsFileResource.getTimePartition()).remove(tsFileResource);
    }
  }

  @Override
  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    if (sequence) {
      for (TreeSet<TsFileResource> partitionSequenceTsFileResource : sequenceTsFileResources
          .values()) {
        partitionSequenceTsFileResource.removeAll(tsFileResourceList);
      }
    } else {
      for (List<List<TsFileResource>> partitionUnSequenceTsFileResource : unSequenceTsFileResources
          .values()) {
        for (List<TsFileResource> levelTsFileResource : partitionUnSequenceTsFileResource) {
          levelTsFileResource.removeAll(tsFileResourceList);
        }
      }
    }
  }

  @Override
  public void add(TsFileResource tsFileResource, boolean sequence) {
    long timePartitionId = tsFileResource.getTimePartition();
    if (sequence) {
      sequenceTsFileResources.computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
          .add(tsFileResource);
    } else {
      int level = getMergeLevel(tsFileResource.getTsFile());
      if (level <= unseqLevelNum - 1) {
        // current file has too high level
        unSequenceTsFileResources
            .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources).get(level)
            .add(tsFileResource);
      } else {
        // current file has normal level
        unSequenceTsFileResources
            .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
            .get(unseqLevelNum - 1).add(tsFileResource);
      }
    }
  }

  @Override
  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      add(tsFileResource, sequence);
    }
  }

  @Override
  public boolean contains(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      return sequenceTsFileResources
          .computeIfAbsent(tsFileResource.getTimePartition(), this::newSequenceTsFileResources)
          .contains(tsFileResource);
    } else {
      for (List<TsFileResource> unSequenceTsFileResource : unSequenceTsFileResources
          .computeIfAbsent(tsFileResource.getTimePartition(), this::newUnSequenceTsFileResources)) {
        if (unSequenceTsFileResource.contains(tsFileResource)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public void clear() {
    sequenceTsFileResources.clear();
    unSequenceTsFileResources.clear();
  }

  @Override
  @SuppressWarnings("squid:S3776")
  public boolean isEmpty(boolean sequence) {
    if (sequence) {
      for (TreeSet<TsFileResource> sequenceTsFileResource : sequenceTsFileResources.values()) {
        if (!sequenceTsFileResource.isEmpty()) {
          return false;
        }
      }
    } else {
      for (List<List<TsFileResource>> partitionUnSequenceTsFileResource : unSequenceTsFileResources
          .values()) {
        for (List<TsFileResource> unSequenceTsFileResource : partitionUnSequenceTsFileResource) {
          if (!unSequenceTsFileResource.isEmpty()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public int size(boolean sequence) {
    int result = 0;
    if (sequence) {
      for (TreeSet<TsFileResource> partitionSequenceTsFileResource : sequenceTsFileResources
          .values()) {
        result += partitionSequenceTsFileResource.size();
      }
    } else {
      for (List<List<TsFileResource>> partitionUnSequenceTsFileResource : unSequenceTsFileResources
          .values()) {
        for (int i = unseqLevelNum - 1; i >= 0; i--) {
          result += partitionUnSequenceTsFileResource.get(i).size();
        }
      }
    }
    return result;
  }

  /**
   * recover files
   */
  @Override
  @SuppressWarnings("squid:S3776")
  public void recover() {
    File logFile = FSFactoryProducer.getFSFactory()
        .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(logFile);
        logAnalyzer.analyze();
        Set<String> deviceSet = logAnalyzer.getDeviceSet();
        List<String> sourceFileList = logAnalyzer.getSourceFiles();
        long offset = logAnalyzer.getOffset();
        String targetFile = logAnalyzer.getTargetFile();
        boolean isSeq = logAnalyzer.isSeq();
        if (targetFile == null || sourceFileList.isEmpty()) {
          return;
        }
        File target = new File(targetFile);
        if (deviceSet.isEmpty()) {
          // if not in compaction, just delete the target file
          if (target.exists()) {
            Files.delete(target.toPath());
          }
          return;
        }
        // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
        TsFileResource targetResource = getRecoverTsFileResource(targetFile, isSeq);
        long timePartition = targetResource.getTimePartition();
        List<TsFileResource> sourceTsFileResources = new ArrayList<>();
        for (String file : sourceFileList) {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          sourceTsFileResources.add(getTsFileResource(file, isSeq));
        }
        int level = getMergeLevel(new File(sourceFileList.get(0)));
        RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
        // if not complete compaction, resume merge
        if (writer.hasCrashed()) {
          if (offset > 0) {
            writer.getIOWriterOut().truncate(offset - 1);
          }
          writer.close();
          CompactionLogger compactionLogger = new CompactionLogger(storageGroupDir,
              storageGroupName);
          CompactionUtils
              .merge(targetResource, sourceTsFileResources, storageGroupName,
                  compactionLogger, deviceSet,
                  isSeq);
          // complete compaction and delete source file
          writeLock();
          try {
            int targetLevel = getMergeLevel(targetResource.getTsFile());
            if (isSeq) {
              sequenceTsFileResources.get(timePartition).add(targetResource);
              sequenceRecoverTsFileResources.clear();
            } else {
              unSequenceTsFileResources.get(timePartition).get(targetLevel).add(targetResource);
              unSequenceRecoverTsFileResources.clear();
            }
            deleteLevelFilesInList(timePartition, sourceTsFileResources, level, isSeq);
          } finally {
            writeUnlock();
          }
          deleteLevelFilesInDisk(sourceTsFileResources);
          compactionLogger.close();
        } else {
          writer.close();
        }
      }
    } catch (IOException e) {
      logger.error("recover level tsfile management error ", e);
    } finally {
      if (logFile.exists()) {
        try {
          Files.delete(logFile.toPath());
        } catch (IOException e) {
          logger.error("delete level tsfile management log file error ", e);
        }
      }
    }
  }

  @Override
  public void forkCurrentFileList(long timePartition) {
    synchronized (sequenceTsFileResources) {
      selectSeqTsFileList(
          forkedSequenceTsFileResources,
          sequenceTsFileResources.computeIfAbsent(timePartition, this::newSequenceTsFileResources));
    }
    // we have to copy all unseq file
    synchronized (unSequenceTsFileResources) {
      selectUnseqTsFileList(
          forkedUnSequenceTsFileResources,
          unSequenceTsFileResources
              .computeIfAbsent(timePartition, this::newUnSequenceTsFileResources));
    }
  }

  private void selectSeqTsFileList(List<TsFileResource> forkedTsFileResources,
      TreeSet<TsFileResource> tsFileResourcesSet) {
    forkedTsFileResources.clear();
    List<TsFileResource> tsFileResources = new ArrayList<>();
    for (TsFileResource tsFileResource : tsFileResourcesSet) {
      if (tsFileResource.isClosed()) {
        tsFileResources.add(tsFileResource);
      }
    }
    Set<TsFileResource> mergeResources = selectMergeResource(tsFileResources);
    forkedTsFileResources.addAll(mergeResources);
  }

  private void selectUnseqTsFileList(
      List<List<TsFileResource>> forkedTsFileResources,
      List<List<TsFileResource>> rawTsFileResources) {
    forkedTsFileResources.clear();
    for (int i = 0; i < unseqLevelNum; i++) {
      List<TsFileResource> forkedLevelTsFileResources = new ArrayList<>();
      Collection<TsFileResource> levelRawTsFileResources = rawTsFileResources
          .get(i);
      for (TsFileResource tsFileResource : levelRawTsFileResources) {
        if (tsFileResource.isClosed()) {
          forkedLevelTsFileResources.add(tsFileResource);
        }
      }
      forkedTsFileResources.add(forkedLevelTsFileResources);
    }
  }

  @Override
  protected void merge(long timePartition) {
    mergeSeq(forkedSequenceTsFileResources, timePartition);
    if (enableUnseqCompaction && unseqLevelNum <= 1
        && !forkedUnSequenceTsFileResources.get(0).isEmpty()) {
      merge(isForceFullMerge, getTsFileList(true), forkedUnSequenceTsFileResources.get(0),
          Long.MAX_VALUE);
    } else {
      mergeUnseq(forkedUnSequenceTsFileResources, timePartition);
    }
  }

  @SuppressWarnings("squid:S3776")
  private void mergeSeq(List<TsFileResource> mergeResources, long timePartition) {
    // wait until unseq mergeSeq has finished
    while (isUnseqMerging) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error("{} [Compaction] shutdown", storageGroupName, e);
        Thread.currentThread().interrupt();
        return;
      }
    }
    long startTimeMillis = System.currentTimeMillis();
    try {
      logger.info("{} start to filter compaction condition", storageGroupName);

      CompactionLogger compactionLogger = new CompactionLogger(storageGroupDir,
          storageGroupName);
      for (TsFileResource mergeResource : mergeResources) {
        compactionLogger.logFile(SOURCE_NAME, mergeResource.getTsFile());
      }
      File firstFile = mergeResources.get(0).getTsFile();
      int currentLevel = getMergeLevel(firstFile);
      File newLevelFile = createNewTsFileName(firstFile, currentLevel + 1);
      compactionLogger.logSequence(true);
      compactionLogger.logFile(TARGET_NAME, newLevelFile);
      logger.info("{} [Dynamic Compaction] mergeSeq {} TsFiles", storageGroupName,
          mergeResources.size());
      for (TsFileResource toMergeTsFile : mergeResources) {
        logger.info("{} [Dynamic Compaction] start to mergeSeq TsFile {}", storageGroupName,
            toMergeTsFile);
      }

      TsFileResource newResource = new TsFileResource(newLevelFile);
      CompactionUtils
          .merge(newResource, mergeResources, storageGroupName, compactionLogger,
              new HashSet<>(), true);
      logger.info("{} [Dynamic Compaction] merged {} TsFiles, and start to delete old files",
          storageGroupName, mergeResources.size());
      writeLock();
      try {
        sequenceTsFileResources.get(timePartition).add(newResource);
        deleteFilesInList(timePartition, mergeResources);
      } finally {
        writeUnlock();
      }
      deleteFilesInDisk(mergeResources);
      compactionLogger.close();
      File logFile = FSFactoryProducer.getFSFactory()
          .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
      if (logFile.exists()) {
        Files.delete(logFile.toPath());
      }
    } catch (Exception e) {
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      // reset the mergeSeq working state to false
      logger
          .info("{} [Dynamic Compaction] mergeUnseq seq end time, consumption: {} ms",
              storageGroupName,
              System.currentTimeMillis() - startTimeMillis);
    }
  }

  private void mergeUnseq(List<List<TsFileResource>> mergeResources, long timePartition) {
    // wait until unseq mergeUnseq has finished
    while (isUnseqMerging) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error("{} [Compaction] shutdown", storageGroupName, e);
        Thread.currentThread().interrupt();
        return;
      }
    }
    long startTimeMillis = System.currentTimeMillis();
    try {
      logger.info("{} start to filter compaction condition", storageGroupName);
      for (int i = 0; i < unseqLevelNum - 1; i++) {
        if (unseqFileNumInEachLevel <= mergeResources.get(i).size()) {
          // level is numbered from 0
          if (enableUnseqCompaction && i == unseqLevelNum - 2) {
            // do not mergeUnseq current unseq file level to upper level and just mergeUnseq all of them to seq file
            merge(isForceFullMerge, getTsFileList(true), mergeResources.get(i), Long.MAX_VALUE);
          } else {
            CompactionLogger compactionLogger = new CompactionLogger(storageGroupDir,
                storageGroupName);
            for (TsFileResource mergeResource : mergeResources.get(i)) {
              compactionLogger.logFile(SOURCE_NAME, mergeResource.getTsFile());
            }
            File newLevelFile = createNewTsFileName(mergeResources.get(i).get(0).getTsFile(),
                i + 1);
            compactionLogger.logSequence(false);
            compactionLogger.logFile(TARGET_NAME, newLevelFile);
            List<TsFileResource> toMergeTsFiles = mergeResources.get(i);
            logger.info("{} [Compaction] mergeUnseq level-{}'s {} TsFiles to next level",
                storageGroupName, i, toMergeTsFiles.size());
            for (TsFileResource toMergeTsFile : toMergeTsFiles) {
              logger.info("{} [Compaction] start to mergeUnseq TsFile {}", storageGroupName,
                  toMergeTsFile);
            }

            TsFileResource newResource = new TsFileResource(newLevelFile);
            CompactionUtils.merge(newResource, toMergeTsFiles, storageGroupName, compactionLogger,
                new HashSet<>(), false);
            logger.info(
                "{} [Compaction] merged level-{}'s {} TsFiles to next level, and start to delete old files",
                storageGroupName, i, toMergeTsFiles.size());
            writeLock();
            try {
              unSequenceTsFileResources.get(timePartition).get(i + 1).add(newResource);
              deleteLevelFilesInList(timePartition, toMergeTsFiles, i, false);
              if (mergeResources.size() > i + 1) {
                mergeResources.get(i + 1).add(newResource);
              }
            } finally {
              writeUnlock();
            }
            deleteLevelFilesInDisk(toMergeTsFiles);
            compactionLogger.close();
            File logFile = FSFactoryProducer.getFSFactory()
                .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
            if (logFile.exists()) {
              Files.delete(logFile.toPath());
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      // reset the mergeUnseq working state to false
      logger.info("{} [Compaction] mergeUnseq end time isSeq = {}, consumption: {} ms",
          storageGroupName, false, System.currentTimeMillis() - startTimeMillis);
    }
  }

  /**
   * if level < maxLevel-1, the file need compaction else, the file can be merged later
   */
  private File createNewTsFileName(File sourceFile, int level) {
    String path = sourceFile.getAbsolutePath();
    String prefixPath = path.substring(0, path.lastIndexOf(FILE_NAME_SEPARATOR) + 1);
    return new File(prefixPath + level + TSFILE_SUFFIX);
  }

  private TreeSet<TsFileResource> newSequenceTsFileResources(Long k) {
    return new TreeSet<>(
        (o1, o2) -> {
          int rangeCompare = Long
              .compare(Long.parseLong(o1.getTsFile().getParentFile().getName()),
                  Long.parseLong(o2.getTsFile().getParentFile().getName()));
          return rangeCompare == 0 ? compareFileName(o1.getTsFile(), o2.getTsFile())
              : rangeCompare;
        });
  }

  private List<List<TsFileResource>> newUnSequenceTsFileResources(Long k) {
    List<List<TsFileResource>> newUnSequenceTsFileResources = new CopyOnWriteArrayList<>();
    for (int i = 0; i < unseqLevelNum; i++) {
      newUnSequenceTsFileResources.add(new CopyOnWriteArrayList<>());
    }
    return newUnSequenceTsFileResources;
  }

  private TsFileResource getTsFileResource(String filePath, boolean isSeq) throws IOException {
    if (isSeq) {
      for (TreeSet<TsFileResource> tsFileResources : sequenceTsFileResources.values()) {
        for (TsFileResource tsFileResource : tsFileResources) {
          if (tsFileResource.getTsFile().getAbsolutePath().equals(filePath)) {
            return tsFileResource;
          }
        }
      }
    } else {
      for (List<List<TsFileResource>> tsFileResourcesWithLevel : unSequenceTsFileResources
          .values()) {
        for (List<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
          for (TsFileResource tsFileResource : tsFileResources) {
            if (tsFileResource.getTsFile().getAbsolutePath().equals(filePath)) {
              return tsFileResource;
            }
          }
        }
      }
    }
    logger.error("cannot get tsfile resource path: {}", filePath);
    throw new IOException();
  }

  private Set<TsFileResource> selectMergeResource(List<TsFileResource> mergeResources) {
    // a list of [startIndex, endIndex, reward]
    List<long[]> candidateList = new ArrayList<>();
    List<TsFileResource> overlappedList = calculateOverlappedList(mergeResources);
    double mergeSpeed = StatMonitor.getInstance().getMergeSpeed();
    double writeSpeed = StatMonitor.getInstance().getWriteSpeed();
    long offsetTime = 0;
    for (int i = 0; i < overlappedList.size(); i++) {
      TsFileResource tsFileResource = overlappedList.get(i);
      long mergedTimeInterval = tsFileResource.getEndTime(0) - tsFileResource.getStartTime(0);
      long mergeTimeCost = (long) (tsFileResource.getTsFileSize() / mergeSpeed * writeSpeed);
      if (queryTimeInterval < (mergedTimeInterval + mergeTimeCost + offsetTime)) {
        continue;
      }

      offsetTime += mergedTimeInterval;
      for (int j = i + 1; j < overlappedList.size(); j++) {
        TsFileResource endTsFileResource = overlappedList.get(j);
        mergeTimeCost += endTsFileResource.getTsFileSize() / mergeSpeed * writeSpeed;
        long allReward = 0L;
        int maxReward = j - i;
        long fullRewardTime = queryTimeInterval - offsetTime - mergedTimeInterval - mergeTimeCost;
        allReward += maxReward * fullRewardTime;
        if (allReward > 0) {
          // calculate not full reward time, from 1 to max_reward, which is active as long as the interval of every file
          for (int k = 0; k < maxReward + 1; k++) {
            TsFileResource currTsFileResource = overlappedList.get(k);
            allReward += currTsFileResource.getEndTime(0) - currTsFileResource.getStartTime(0);
          }
        }
        candidateList.add(new long[]{i, j, allReward});
      }
    }

    if (candidateList.size() <= 0) {
      return new HashSet<>();
    }
    // get the tuple with max reward among candidate list
    long[] maxTuple = new long[]{0, 0, 0L};
    for (long[] tuple : candidateList) {
      if (tuple[2] > maxTuple[2]) {
        maxTuple = tuple;
      }
    }

    // get the select result in order
    Set<TsFileResource> result = newSequenceTsFileResources(0L);
    for (int i = (int) maxTuple[0]; i < maxTuple[1] + 1; i++) {
      result.add(overlappedList.get(i));
    }
    return result;
  }

  private List<TsFileResource> calculateOverlappedList(List<TsFileResource> tsFileResources) {
    List<TsFileResource> overlappedList = new ArrayList<>();
    long time = 0;
    for (int i = tsFileResources.size() - 1; i >= 0; i--) {
      TsFileResource tsFileResource = tsFileResources.get(i);
      if (tsFileResource.getDeviceToIndexMap().size() > 0) {
        overlappedList.add(tsFileResource);
        time += tsFileResource.getEndTime(0) - tsFileResource.getStartTime(0);
        if (time > queryTimeInterval) {
          break;
        }
      }
    }
    return overlappedList;
  }

  private TsFileResource getRecoverTsFileResource(String filePath, boolean isSeq)
      throws IOException {
    if (isSeq) {
      for (TsFileResource tsFileResource : sequenceRecoverTsFileResources) {
        if (Files.isSameFile(tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
          return tsFileResource;
        }
      }
    } else {
      for (TsFileResource tsFileResource : unSequenceRecoverTsFileResources) {
        if (Files.isSameFile(tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
          return tsFileResource;
        }
      }
    }
    logger.error("cannot get tsfile resource path: {}", filePath);
    throw new IOException();
  }

  private void deleteLevelFilesInList(long timePartitionId,
      Collection<TsFileResource> mergeTsFiles, int level, boolean sequence) {
    logger.debug("{} [compaction] merge starts to delete file list", storageGroupName);
    if (sequence) {
      if (sequenceTsFileResources.containsKey(timePartitionId)) {
        synchronized (sequenceTsFileResources) {
          sequenceTsFileResources.get(timePartitionId).removeAll(mergeTsFiles);
        }
      }
    } else {
      if (unSequenceTsFileResources.containsKey(timePartitionId)) {
        if (unSequenceTsFileResources.get(timePartitionId).size() > level) {
          synchronized (unSequenceTsFileResources) {
            unSequenceTsFileResources.get(timePartitionId).get(level).removeAll(mergeTsFiles);
          }
        }
      }
    }
  }

  private void deleteLevelFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
    logger.debug("{} [compaction] merge starts to delete real file", storageGroupName);
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      deleteLevelFile(mergeTsFile);
      logger
          .info("{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  private void deleteLevelFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }
}
