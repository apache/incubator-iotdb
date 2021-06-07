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

package org.apache.iotdb.db.engine.compaction.heavyhitter;

import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;

public class HitterLevelCompactionTsFileManagement extends LevelCompactionTsFileManagement {

  private static final Logger logger =
      LoggerFactory.getLogger(HitterLevelCompactionTsFileManagement.class);

  public HitterLevelCompactionTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
  }

  @Override
  protected void merge(long timePartition) {
    isMergeExecutedInCurrentTask =
        merge(
            forkedSequenceTsFileResources, true, timePartition, seqLevelNum, seqFileNumInEachLevel);
    if (enableUnseqCompaction
        && unseqLevelNum <= 1
        && forkedUnSequenceTsFileResources.get(0).size() > 0) {
      isMergeExecutedInCurrentTask = true;
      merge(
          isForceFullMerge,
          getTsFileListByTimePartition(true, timePartition),
          forkedUnSequenceTsFileResources.get(0),
          Long.MAX_VALUE);
    } else {
      isMergeExecutedInCurrentTask =
          merge(
              forkedUnSequenceTsFileResources,
              false,
              timePartition,
              unseqLevelNum,
              unseqFileNumInEachLevel);
    }
  }

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
        logger.error("{} [Hitter Compaction] shutdown", storageGroupName, e);
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
        if (currLevelTsFileResource.size() >= currMaxFileNumInEachLevel) {
          // just merge part of the file
          isMergeExecutedInCurrentTask = true;
          // level is numbered from 0
          if (enableUnseqCompaction && !sequence && i == currMaxLevel - 2) {
            // do not merge current unseq file level to upper level and just merge all of them to
            // seq file
            isSeqMerging = false;
            merge(
                isForceFullMerge,
                getTsFileListByTimePartition(true, timePartition),
                mergeResources.get(i),
                Long.MAX_VALUE);
          } else {
            compactionLogger = new CompactionLogger(storageGroupDir, storageGroupName);
            // log source file list and target file for recover
            for (TsFileResource mergeResource : mergeResources.get(i)) {
              compactionLogger.logFile(SOURCE_NAME, mergeResource.getTsFile());
            }
            File newLevelFile =
                TsFileResource.modifyTsFileNameMergeCnt(mergeResources.get(i).get(0).getTsFile());
            compactionLogger.logSequence(sequence);
            compactionLogger.logFile(TARGET_NAME, newLevelFile);
            List<TsFileResource> toMergeTsFiles =
                mergeResources.get(i).subList(0, currMaxFileNumInEachLevel);
            logger.info(
                "{} [Hitter Compaction] merge level-{}'s {} TsFiles to next level",
                storageGroupName,
                i,
                toMergeTsFiles.size());
            for (TsFileResource toMergeTsFile : toMergeTsFiles) {
              logger.info(
                  "{} [Hitter Compaction] start to merge TsFile {}",
                  storageGroupName,
                  toMergeTsFile);
            }

            TsFileResource newResource = new TsFileResource(newLevelFile);
            List<Modification> modifications = new ArrayList<>();
            // merge, read heavy hitters time series from source files and write to target file
            List<PartialPath> unmergedPaths =
                QueryHitterManager.getInstance()
                    .getQueryHitter()
                    .getTopCompactionSeries(new PartialPath(storageGroupName));
            CompactionUtils.hitterMerge(
                newResource,
                toMergeTsFiles,
                storageGroupName,
                compactionLogger,
                new HashSet<>(),
                sequence,
                modifications,
                unmergedPaths);
            logger.info(
                "{} [Hitter Compaction] merged level-{}'s {} TsFiles to next level, and start to clean up",
                storageGroupName,
                i,
                toMergeTsFiles.size());
            writeLock();
            try {
              if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException(
                    String.format("%s [Hitter Compaction] abort", storageGroupName));
              }

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
          logger.error(
              "{} Hitter Compaction log close fail", storageGroupName + COMPACTION_LOG_NAME);
        }
      }
      restoreCompaction();
      logger.error("Error occurred in Hitter Compaction Merge thread", e);
    } finally {
      // reset the merge working state to false
      isSeqMerging = false;
      logger.info(
          "{} [Compaction] merge end time isSeq = {}, consumption: {} ms",
          storageGroupName,
          sequence,
          System.currentTimeMillis() - startTimeMillis);
    }
    return isMergeExecutedInCurrentTask;
  }
}
