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

package org.apache.iotdb.db.engine.compaction.level;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogAnalyzer;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The TsFileManagement for LEVEL_COMPACTION, use level struct to manage TsFile list */
public class LevelCompactionTsFileManagement extends TsFileManagement {

  private static final Logger logger =
      LoggerFactory.getLogger(LevelCompactionTsFileManagement.class);

  private final int seqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getSeqLevelNum(), 1);
  private final int seqFileNumInEachLevel =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getSeqFileNumInEachLevel(), 1);
  private final int unseqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum(), 1);
  private final int unseqFileNumInEachLevel =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getUnseqFileNumInEachLevel(), 1);

  private final boolean enableUnseqCompaction =
      IoTDBDescriptor.getInstance().getConfig().isEnableUnseqCompaction();

  // First map is partition list; Second list is level list; Third list is file list in level;
  private final Map<Long, List<SortedSet<TsFileResource>>> sequenceTsFileResources =
      new ConcurrentSkipListMap<>();
  private final Map<Long, List<List<TsFileResource>>> unSequenceTsFileResources =
      new ConcurrentSkipListMap<>();
  private final List<List<TsFileResource>> forkedSequenceTsFileResources = new ArrayList<>();
  private final List<List<TsFileResource>> forkedUnSequenceTsFileResources = new ArrayList<>();
  private final List<TsFileResource> sequenceRecoverTsFileResources = new CopyOnWriteArrayList<>();
  private final List<TsFileResource> unSequenceRecoverTsFileResources =
      new CopyOnWriteArrayList<>();

  public LevelCompactionTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
    clear();
  }

  public void renameLevelFilesMods(
      Collection<Modification> filterModification,
      Collection<TsFileResource> mergeTsFiles,
      TsFileResource targetTsFile)
      throws IOException {
    logger.debug("{} [compaction] merge starts to rename real file's mod", storageGroupName);
    List<Modification> modifications = new ArrayList<>();
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      try (ModificationFile sourceModificationFile =
          new ModificationFile(mergeTsFile.getTsFilePath() + ModificationFile.FILE_SUFFIX)) {
        modifications.addAll(sourceModificationFile.getModifications());
        if (sourceModificationFile.exists()) {
          sourceModificationFile.remove();
        }
      }
    }
    modifications.removeAll(filterModification);
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile =
          new ModificationFile(targetTsFile.getTsFilePath() + ModificationFile.FILE_SUFFIX)) {
        for (Modification modification : modifications) {
          modificationFile.write(modification);
        }
      }
    }
  }

  private void deleteLevelFilesInDisk(Collection<TsFileResource> mergeTsFiles) {
    logger.debug("{} [compaction] merge starts to delete real file", storageGroupName);
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      deleteLevelFile(mergeTsFile);
      logger.debug(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
  }

  private void deleteLevelFilesInList(
      long timePartitionId, Collection<TsFileResource> mergeTsFiles, int level, boolean sequence) {
    logger.debug("{} [compaction] merge starts to delete file list", storageGroupName);
    if (sequence) {
      if (sequenceTsFileResources.containsKey(timePartitionId)) {
        if (sequenceTsFileResources.get(timePartitionId).size() > level) {
          synchronized (sequenceTsFileResources) {
            sequenceTsFileResources.get(timePartitionId).get(level).removeAll(mergeTsFiles);
          }
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

  private void deleteLevelFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      ChunkCache.getInstance().clear();
      ChunkMetadataCache.getInstance().remove(seqFile);
      TimeSeriesMetadataCache.getInstance().clear();
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setDeleted(true);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }

  @Deprecated
  @Override
  public List<TsFileResource> getTsFileList(boolean sequence) {
    List<TsFileResource> result = new ArrayList<>();
    if (sequence) {
      synchronized (sequenceTsFileResources) {
        for (long timePartition : sequenceTsFileResources.keySet()) {
          result.addAll(getTsFileListByTimePartition(true, timePartition));
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        for (long timePartition : unSequenceTsFileResources.keySet()) {
          result.addAll(getTsFileListByTimePartition(false, timePartition));
        }
      }
    }
    return result;
  }

  public List<TsFileResource> getTsFileListByTimePartition(boolean sequence, long timePartition) {
    List<TsFileResource> result = new ArrayList<>();
    if (sequence) {
      synchronized (sequenceTsFileResources) {
        List<SortedSet<TsFileResource>> sequenceTsFileList =
            sequenceTsFileResources.get(timePartition);
        for (int i = sequenceTsFileList.size() - 1; i >= 0; i--) {
          result.addAll(sequenceTsFileList.get(i));
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        List<List<TsFileResource>> unSequenceTsFileList =
            unSequenceTsFileResources.get(timePartition);
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
      synchronized (sequenceTsFileResources) {
        for (SortedSet<TsFileResource> sequenceTsFileResource :
            sequenceTsFileResources.get(tsFileResource.getTimePartition())) {
          sequenceTsFileResource.remove(tsFileResource);
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        for (List<TsFileResource> unSequenceTsFileResource :
            unSequenceTsFileResources.get(tsFileResource.getTimePartition())) {
          unSequenceTsFileResource.remove(tsFileResource);
        }
      }
    }
  }

  @Override
  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    if (sequence) {
      synchronized (sequenceTsFileResources) {
        for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
            sequenceTsFileResources.values()) {
          for (SortedSet<TsFileResource> levelTsFileResource : partitionSequenceTsFileResource) {
            levelTsFileResource.removeAll(tsFileResourceList);
          }
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
            unSequenceTsFileResources.values()) {
          for (List<TsFileResource> levelTsFileResource : partitionUnSequenceTsFileResource) {
            levelTsFileResource.removeAll(tsFileResourceList);
          }
        }
      }
    }
  }

  @Override
  public void add(TsFileResource tsFileResource, boolean sequence) {
    long timePartitionId = tsFileResource.getTimePartition();
    int level = getMergeLevel(tsFileResource.getTsFile());
    if (sequence) {
      synchronized (sequenceTsFileResources) {
        if (level <= seqLevelNum - 1) {
          // current file has normal level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          sequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
              .get(seqLevelNum - 1)
              .add(tsFileResource);
        }
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        if (level <= unseqLevelNum - 1) {
          // current file has normal level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(level)
              .add(tsFileResource);
        } else {
          // current file has too high level
          unSequenceTsFileResources
              .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
              .get(unseqLevelNum - 1)
              .add(tsFileResource);
        }
      }
    }
  }

  @Override
  public void addRecover(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      synchronized (sequenceRecoverTsFileResources) {
        sequenceRecoverTsFileResources.add(tsFileResource);
      }
    } else {
      synchronized (unSequenceTsFileResources) {
        unSequenceRecoverTsFileResources.add(tsFileResource);
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
      for (SortedSet<TsFileResource> sequenceTsFileResource :
          sequenceTsFileResources.computeIfAbsent(
              tsFileResource.getTimePartition(), this::newSequenceTsFileResources)) {
        if (sequenceTsFileResource.contains(tsFileResource)) {
          return true;
        }
      }
    } else {
      for (List<TsFileResource> unSequenceTsFileResource :
          unSequenceTsFileResources.computeIfAbsent(
              tsFileResource.getTimePartition(), this::newUnSequenceTsFileResources)) {
        if (unSequenceTsFileResource.contains(tsFileResource)) {
          return true;
        }
      }
    }
    return false;
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
      for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
          sequenceTsFileResources.values()) {
        for (SortedSet<TsFileResource> sequenceTsFileResource : partitionSequenceTsFileResource) {
          if (!sequenceTsFileResource.isEmpty()) {
            return false;
          }
        }
      }
    } else {
      for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
          unSequenceTsFileResources.values()) {
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
      for (List<SortedSet<TsFileResource>> partitionSequenceTsFileResource :
          sequenceTsFileResources.values()) {
        for (int i = seqLevelNum - 1; i >= 0; i--) {
          result += partitionSequenceTsFileResource.get(i).size();
        }
      }
    } else {
      for (List<List<TsFileResource>> partitionUnSequenceTsFileResource :
          unSequenceTsFileResources.values()) {
        for (int i = unseqLevelNum - 1; i >= 0; i--) {
          result += partitionUnSequenceTsFileResource.get(i).size();
        }
      }
    }
    return result;
  }

  /** recover files */
  @Override
  @SuppressWarnings("squid:S3776")
  public void recover() {
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(logFile);
        logAnalyzer.analyze();
        Set<String> deviceSet = logAnalyzer.getDeviceSet();
        List<String> sourceFileList = logAnalyzer.getSourceFiles();
        long offset = logAnalyzer.getOffset();
        String targetFile = logAnalyzer.getTargetFile();
        boolean fullMerge = logAnalyzer.isFullMerge();
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
        if (fullMerge) {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          TsFileResource targetTsFileResource = getRecoverTsFileResource(targetFile, isSeq);
          long timePartition = targetTsFileResource.getTimePartition();
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
          // if not complete compaction, resume merge
          if (writer.hasCrashed()) {
            if (offset > 0) {
              writer.getIOWriterOut().truncate(offset - 1);
            }
            writer.close();
            CompactionLogger compactionLogger =
                new CompactionLogger(storageGroupDir, storageGroupName);
            List<Modification> modifications = new ArrayList<>();
            CompactionUtils.merge(
                targetTsFileResource,
                getTsFileList(isSeq),
                storageGroupName,
                compactionLogger,
                deviceSet,
                isSeq,
                modifications);
            compactionLogger.close();
          } else {
            writer.close();
          }
          // complete compaction and delete source file
          deleteAllSubLevelFiles(isSeq, timePartition);
        } else {
          // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
          TsFileResource targetResource = getRecoverTsFileResource(targetFile, isSeq);
          if (targetResource == null) {
            // new file already merged but old file not deleted
            targetResource = getTsFileResource(targetFile, isSeq);
            if (targetResource == null) {
              throw new IOException();
            }
          }
          long timePartition = targetResource.getTimePartition();
          List<TsFileResource> sourceTsFileResources = new ArrayList<>();
          for (String file : sourceFileList) {
            // get tsfile resource from list, as they have been recovered in StorageGroupProcessor
            TsFileResource sourceTsFileResource = getTsFileResource(file, isSeq);
            if (sourceTsFileResource == null) {
              throw new IOException();
            }
            sourceTsFileResources.add(sourceTsFileResource);
          }
          int level = getMergeLevel(new File(sourceFileList.get(0)));
          RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(target);
          List<Modification> modifications = new ArrayList<>();
          // if not complete compaction, resume merge
          if (writer.hasCrashed()) {
            if (offset > 0) {
              writer.getIOWriterOut().truncate(offset - 1);
            }
            writer.close();
            CompactionLogger compactionLogger =
                new CompactionLogger(storageGroupDir, storageGroupName);
            CompactionUtils.merge(
                targetResource,
                sourceTsFileResources,
                storageGroupName,
                compactionLogger,
                deviceSet,
                isSeq,
                modifications);
            compactionLogger.close();
            // complete compaction and add target tsfile
            int targetLevel = getMergeLevel(targetResource.getTsFile());
            if (isSeq) {
              sequenceTsFileResources.get(timePartition).get(targetLevel).add(targetResource);
              sequenceRecoverTsFileResources.clear();
            } else {
              unSequenceTsFileResources.get(timePartition).get(targetLevel).add(targetResource);
              unSequenceRecoverTsFileResources.clear();
            }
          } else {
            // complete compaction, just close writer
            writer.close();
          }
          // complete compaction and delete source file
          writeLock();
          try {
            if (Thread.currentThread().isInterrupted()) {
              throw new InterruptedException(
                  String.format("%s [Compaction] abort", storageGroupName));
            }
            deleteLevelFilesInList(timePartition, sourceTsFileResources, level, isSeq);
          } finally {
            writeUnlock();
          }
          for (TsFileResource tsFileResource : sourceTsFileResources) {
            logger.error(
                "{} recover storage group delete source file {}",
                storageGroupName,
                tsFileResource.getTsFile().getName());
          }
          deleteLevelFilesInDisk(sourceTsFileResources);
          renameLevelFilesMods(modifications, sourceTsFileResources, targetResource);
        }
      }
    } catch (IOException | IllegalPathException | InterruptedException e) {
      logger.error("recover level tsfile management error ", e);
      restoreCompaction();
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

  private void deleteAllSubLevelFiles(boolean isSeq, long timePartition) {
    if (isSeq) {
      for (int level = 0; level < sequenceTsFileResources.get(timePartition).size(); level++) {
        SortedSet<TsFileResource> currLevelMergeFile =
            sequenceTsFileResources.get(timePartition).get(level);
        deleteLevelFilesInDisk(currLevelMergeFile);
        deleteLevelFilesInList(timePartition, currLevelMergeFile, level, isSeq);
      }
    } else {
      for (int level = 0; level < unSequenceTsFileResources.get(timePartition).size(); level++) {
        SortedSet<TsFileResource> currLevelMergeFile =
            sequenceTsFileResources.get(timePartition).get(level);
        deleteLevelFilesInDisk(currLevelMergeFile);
        deleteLevelFilesInList(timePartition, currLevelMergeFile, level, isSeq);
      }
    }
  }

  @Override
  public void forkCurrentFileList(long timePartition) {
    synchronized (sequenceTsFileResources) {
      forkTsFileList(
          forkedSequenceTsFileResources,
          sequenceTsFileResources.computeIfAbsent(timePartition, this::newSequenceTsFileResources),
          seqLevelNum);
    }
    // we have to copy all unseq file
    synchronized (unSequenceTsFileResources) {
      // we have to copy all unseq file
      forkTsFileList(
          forkedUnSequenceTsFileResources,
          unSequenceTsFileResources.computeIfAbsent(
              timePartition, this::newUnSequenceTsFileResources),
          unseqLevelNum + 1);
    }
  }

  private void forkTsFileList(
      List<List<TsFileResource>> forkedTsFileResources, List rawTsFileResources, int currMaxLevel) {
    forkedTsFileResources.clear();
    for (int i = 0; i < currMaxLevel - 1; i++) {
      List<TsFileResource> forkedLevelTsFileResources = new ArrayList<>();
      Collection<TsFileResource> levelRawTsFileResources =
          (Collection<TsFileResource>) rawTsFileResources.get(i);
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
    isMergeExecutedInCurrentTask =
        merge(
            forkedSequenceTsFileResources, true, timePartition, seqLevelNum, seqFileNumInEachLevel);
    if (enableUnseqCompaction
        && unseqLevelNum <= 1
        && forkedUnSequenceTsFileResources.get(0).size() > 0) {
      isMergeExecutedInCurrentTask =
          isMergeExecutedInCurrentTask
              || merge(
                  isForceFullMerge,
                  getTsFileListByTimePartition(true, timePartition),
                  forkedUnSequenceTsFileResources.get(0),
                  Long.MAX_VALUE);
    } else {
      isMergeExecutedInCurrentTask =
          isMergeExecutedInCurrentTask
              || merge(
                  forkedUnSequenceTsFileResources,
                  false,
                  timePartition,
                  unseqLevelNum,
                  unseqFileNumInEachLevel);
    }
  }

  @SuppressWarnings("squid:S3776")
  private boolean merge(
      List<List<TsFileResource>> mergeResources,
      boolean sequence,
      long timePartition,
      int currMaxLevel,
      int currMaxFileNumInEachLevel) {
    // wait until unseq merge has finished
    while (isUnseqMerging) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error("{} [Compaction] shutdown", storageGroupName, e);
        Thread.currentThread().interrupt();
        return false;
      }
    }
    isSeqMerging = true;
    long startTimeMillis = System.currentTimeMillis();
    // whether execute merge chunk in the loop below
    boolean isMergeExecutedInCurrentTask = false;
    CompactionLogger compactionLogger = null;
    try {
      logger.info("{} start to filter compaction condition", storageGroupName);
      for (int i = 0; i < currMaxLevel - 1; i++) {
        List<TsFileResource> currLevelTsFileResource = mergeResources.get(i);
        if (currMaxFileNumInEachLevel <= currLevelTsFileResource.size()) {
          // just merge part of the file
          isMergeExecutedInCurrentTask = true;
          // level is numbered from 0
          if (enableUnseqCompaction && !sequence && i == currMaxLevel - 2) {
            // do not merge current unseq file level to upper level and just merge all of them to
            // seq file
            isSeqMerging = false;
            isMergeExecutedInCurrentTask =
                merge(
                    isForceFullMerge,
                    getTsFileListByTimePartition(true, timePartition),
                    mergeResources.get(i),
                    Long.MAX_VALUE);
          } else {
            compactionLogger = new CompactionLogger(storageGroupDir, storageGroupName);
            // log source file list and target file for recover
            for (TsFileResource mergeResource : mergeResources.get(i)) {
              mergeResource.setMerging(true);
              compactionLogger.logFile(SOURCE_NAME, mergeResource.getTsFile());
            }
            File newLevelFile =
                createNewTsFileName(mergeResources.get(i).get(0).getTsFile(), i + 1);
            compactionLogger.logSequence(sequence);
            compactionLogger.logFile(TARGET_NAME, newLevelFile);
            List<TsFileResource> toMergeTsFiles =
                mergeResources.get(i).subList(0, currMaxFileNumInEachLevel);
            logger.info(
                "{} [Compaction] merge level-{}'s {} TsFiles to next level",
                storageGroupName,
                i,
                toMergeTsFiles.size());
            for (TsFileResource toMergeTsFile : toMergeTsFiles) {
              logger.info(
                  "{} [Compaction] start to merge TsFile {}", storageGroupName, toMergeTsFile);
            }

            TsFileResource newResource = new TsFileResource(newLevelFile);
            List<Modification> modifications = new ArrayList<>();
            // merge, read from source files and write to target file
            CompactionUtils.merge(
                newResource,
                toMergeTsFiles,
                storageGroupName,
                compactionLogger,
                new HashSet<>(),
                sequence,
                modifications);
            logger.info(
                "{} [Compaction] merged level-{}'s {} TsFiles to next level, and start to delete old files",
                storageGroupName,
                i,
                toMergeTsFiles.size());
            writeLock();
            try {
              if (sequence) {
                sequenceTsFileResources.get(timePartition).get(i + 1).add(newResource);
              } else {
                unSequenceTsFileResources.get(timePartition).get(i + 1).add(newResource);
              }
              deleteLevelFilesInList(timePartition, toMergeTsFiles, i, sequence);
              if (mergeResources.size() > i + 1) {
                mergeResources.get(i + 1).add(newResource);
              }
            } finally {
              writeUnlock();
            }
            deleteLevelFilesInDisk(toMergeTsFiles);
            renameLevelFilesMods(modifications, toMergeTsFiles, newResource);
            compactionLogger.close();
            File logFile =
                FSFactoryProducer.getFSFactory()
                    .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
            if (logFile.exists()) {
              Files.delete(logFile.toPath());
            }
          }
        }
      }
    } catch (Exception e) {
      if (compactionLogger != null) {
        try {
          compactionLogger.close();
        } catch (IOException ioException) {
          logger.error("{} Compaction log close fail", storageGroupName + COMPACTION_LOG_NAME);
        }
      }
      isMergeExecutedInCurrentTask = false;
      restoreCompaction();
      logger.error("Error occurred in Compaction Merge thread", e);
    } finally {
      isSeqMerging = false;
      // reset the merge working state to false
      if (isMergeExecutedInCurrentTask) {
        logger.info(
            "{} [Compaction] merge end time isSeq = {}, consumption: {} ms",
            storageGroupName,
            sequence,
            System.currentTimeMillis() - startTimeMillis);
      }
    }
    return isMergeExecutedInCurrentTask;
  }

  /** restore the files back to the status before the compaction task is submitted */
  private void restoreCompaction() {
    File logFile =
        FSFactoryProducer.getFSFactory()
            .getFile(storageGroupDir, storageGroupName + COMPACTION_LOG_NAME);
    try {
      if (logFile.exists()) {
        CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(logFile);
        logAnalyzer.analyze();
        String targetFilePath = logAnalyzer.getTargetFile();
        List<String> sourceFileList = logAnalyzer.getSourceFiles();
        boolean isSeq = logAnalyzer.isSeq();
        for (String file : sourceFileList) {
          TsFileResource fileResource = getTsFileResource(file, isSeq);
          if (fileResource != null) {
            fileResource.setMerging(false);
          }
        }
        if (targetFilePath != null) {
          File targetFile = new File(targetFilePath);
          if (targetFile.exists()) {
            logger.error(
                "{} restore delete target file {} ", storageGroupName, targetFile.getName());
            targetFile.delete();
          }
        }
      }
    } catch (IOException e) {
      logger.error("restore compaction failed", e);
    } finally {
      if (logFile.exists()) {
        try {
          Files.delete(logFile.toPath());
        } catch (IOException e) {
          logger.error("delete compaction log file error ", e);
        }
      }
    }
  }

  /** if level < maxLevel-1, the file need compaction else, the file can be merged later */
  private File createNewTsFileName(File sourceFile, int level) {
    String path = sourceFile.getPath();
    String prefixPath = path.substring(0, path.lastIndexOf(FILE_NAME_SEPARATOR) + 1);
    return new File(prefixPath + level + TSFILE_SUFFIX);
  }

  private List<SortedSet<TsFileResource>> newSequenceTsFileResources(Long k) {
    List<SortedSet<TsFileResource>> newSequenceTsFileResources = new CopyOnWriteArrayList<>();
    for (int i = 0; i < seqLevelNum; i++) {
      newSequenceTsFileResources.add(
          Collections.synchronizedSortedSet(
              new TreeSet<>(
                  (o1, o2) -> {
                    try {
                      int rangeCompare =
                          Long.compare(
                              Long.parseLong(o1.getTsFile().getParentFile().getName()),
                              Long.parseLong(o2.getTsFile().getParentFile().getName()));
                      return rangeCompare == 0
                          ? compareFileName(o1.getTsFile(), o2.getTsFile())
                          : rangeCompare;
                    } catch (NumberFormatException e) {
                      return compareFileName(o1.getTsFile(), o2.getTsFile());
                    }
                  })));
    }
    return newSequenceTsFileResources;
  }

  private List<List<TsFileResource>> newUnSequenceTsFileResources(Long k) {
    List<List<TsFileResource>> newUnSequenceTsFileResources = new CopyOnWriteArrayList<>();
    for (int i = 0; i < unseqLevelNum; i++) {
      newUnSequenceTsFileResources.add(new CopyOnWriteArrayList<>());
    }
    return newUnSequenceTsFileResources;
  }

  public static int getMergeLevel(File file) {
    String mergeLevelStr =
        file.getPath()
            .substring(file.getPath().lastIndexOf(FILE_NAME_SEPARATOR) + 1)
            .replaceAll(TSFILE_SUFFIX, "");
    return Integer.parseInt(mergeLevelStr);
  }

  private TsFileResource getRecoverTsFileResource(String filePath, boolean isSeq) {
    try {
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
    } catch (IOException e) {
      logger.error("cannot get tsfile resource path: {}", filePath);
    }
    return null;
  }

  private TsFileResource getTsFileResource(String filePath, boolean isSeq) {
    readLock();
    try {
      File file = new File(filePath);
      if (!file.exists()) {
        return null;
      }
      try {
        if (isSeq) {
          for (List<SortedSet<TsFileResource>> tsFileResourcesWithLevel :
              sequenceTsFileResources.values()) {
            for (SortedSet<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
              for (TsFileResource tsFileResource : tsFileResources) {
                if (Files.isSameFile(
                    tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
                  return tsFileResource;
                }
              }
            }
          }
        } else {
          for (List<List<TsFileResource>> tsFileResourcesWithLevel :
              unSequenceTsFileResources.values()) {
            for (List<TsFileResource> tsFileResources : tsFileResourcesWithLevel) {
              for (TsFileResource tsFileResource : tsFileResources) {
                if (Files.isSameFile(
                    tsFileResource.getTsFile().toPath(), new File(filePath).toPath())) {
                  return tsFileResource;
                }
              }
            }
          }
        }
      } catch (Exception e) {
        logger.error("cannot get tsfile resource path: {}", filePath, e);
        return null;
      }
      return null;
    } finally {
      readUnLock();
    }
  }

  @TestOnly
  public Map<Long, List<SortedSet<TsFileResource>>> getSequenceTsFileResources() {
    return sequenceTsFileResources;
  }
}
