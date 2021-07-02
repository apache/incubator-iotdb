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
package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask.MERGE_SUFFIX;

public class InplaceCompactionTask extends AbstractCrossSpaceCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractCrossSpaceCompactionTask.class);
  protected CrossSpaceMergeResource mergeResource;
  protected String storageGroupDir;
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected TsFileResourceList seqTsFileResourceList;
  protected TsFileResourceList unSeqTsFileResourceList;
  protected int concurrentMergeCount;
  protected String logicalStorageGroupName;
  protected String virtualStorageGroupName;

  public InplaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      CrossSpaceMergeResource mergeResource,
      String storageGroupDir,
      TsFileResourceList seqTsFileResourceList,
      TsFileResourceList unSeqTsFileResourceList,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      int concurrentMergeCount) {
    super(logicalStorageGroupName + "-" + virtualStorageGroupName, timePartitionId);
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.mergeResource = mergeResource;
    this.storageGroupDir = storageGroupDir;
    this.seqTsFileResourceList = seqTsFileResourceList;
    this.unSeqTsFileResourceList = unSeqTsFileResourceList;
    this.selectedSeqTsFileResourceList = selectedSeqTsFileResourceList;
    this.selectedUnSeqTsFileResourceList = selectedUnSeqTsFileResourceList;
    this.concurrentMergeCount = concurrentMergeCount;
  }

  @Override
  protected void doCompaction() throws Exception {
    String taskName = fullStorageGroupName + "-" + System.currentTimeMillis();
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            mergeResource,
            storageGroupDir,
            this::mergeEndAction,
            taskName,
            IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(),
            concurrentMergeCount,
            logicalStorageGroupName);
    mergeTask.call();
  }

  public void mergeEndAction(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, File mergeLog) {
    // todo: add
    LOGGER.info("{} a merge task is ending...", fullStorageGroupName);

    if (Thread.currentThread().isInterrupted() || unseqFiles.isEmpty()) {
      // merge task abort, or merge runtime exception arose, just end this merge
      LOGGER.info("{} a merge task abnormally ends", fullStorageGroupName);
      return;
    }
    removeUnseqFiles(unseqFiles);

    for (int i = 0; i < seqFiles.size(); i++) {
      TsFileResource seqFile = seqFiles.get(i);
      // get both seqFile lock and merge lock
      doubleWriteLock(seqFile);

      try {
        // if meet error(like file not found) in merge task, the .merge file may not be deleted
        File mergedFile =
            FSFactoryProducer.getFSFactory().getFile(seqFile.getTsFilePath() + MERGE_SUFFIX);
        if (mergedFile.exists()) {
          if (!mergedFile.delete()) {
            LOGGER.warn("Delete file {} failed", mergedFile);
          }
        }
        updateMergeModification(seqFile);
      } finally {
        doubleWriteUnlock(seqFile);
      }
    }

    try {
      removeMergingModification(seqFiles, unseqFiles);
      Files.delete(mergeLog.toPath());
    } catch (IOException e) {
      LOGGER.error(
          "{} a merge task ends but cannot delete log {}", fullStorageGroupName, mergeLog.toPath());
    }

    LOGGER.info("{} a merge task ends", fullStorageGroupName);
  }

  private void removeUnseqFiles(List<TsFileResource> unseqFiles) {
    unSeqTsFileResourceList.writeLock();
    try {
      for (TsFileResource unSeqFileMerged : selectedUnSeqTsFileResourceList) {
        unSeqTsFileResourceList.remove(unSeqFileMerged);
      }
      // clean cache
      if (IoTDBDescriptor.getInstance().getConfig().isMetaDataCacheEnable()) {
        ChunkCache.getInstance().clear();
        TimeSeriesMetadataCache.getInstance().clear();
      }
    } finally {
      unSeqTsFileResourceList.writeUnlock();
    }

    for (TsFileResource unseqFile : unseqFiles) {
      unseqFile.writeLock();
      try {
        unseqFile.remove();
      } finally {
        unseqFile.writeUnlock();
      }
    }
  }

  /** acquire the write locks of the resource , the merge lock and the compaction lock */
  private void doubleWriteLock(TsFileResource seqFile) {
    boolean fileLockGot;
    boolean compactionLockGot;
    while (true) {
      fileLockGot = seqFile.tryWriteLock();
      compactionLockGot = seqTsFileResourceList.tryWriteLock();

      if (fileLockGot && compactionLockGot) {
        break;
      } else {
        // did not get all of them, release the gotten one and retry
        if (compactionLockGot) {
          seqTsFileResourceList.writeUnlock();
        }
        if (fileLockGot) {
          seqFile.writeUnlock();
        }
      }
    }
  }

  private void doubleWriteUnlock(TsFileResource seqFile) {
    seqTsFileResourceList.writeUnlock();
    seqFile.writeUnlock();
  }

  private void updateMergeModification(TsFileResource seqFile) {
    try {
      // remove old modifications and write modifications generated during merge
      seqFile.removeModFile();
      ModificationFile compactionModificationFile = ModificationFile.getCompactionMods(seqFile);
      for (Modification modification : compactionModificationFile.getModifications()) {
        seqFile.getModFile().write(modification);
      }
      try {
        seqFile.getModFile().close();
      } catch (IOException e) {
        LOGGER.error("Cannot close the ModificationFile {}", seqFile.getModFile().getFilePath(), e);
      }
    } catch (IOException e) {
      LOGGER.error(
          "{} cannot clean the ModificationFile of {} after cross space merge",
          fullStorageGroupName,
          seqFile.getTsFile(),
          e);
    }
  }

  private void removeMergingModification(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    try {
      for (TsFileResource seqFile : seqFiles) {
        ModificationFile.getCompactionMods(seqFile).remove();
      }
      for (TsFileResource unseqFile : unseqFiles) {
        ModificationFile.getCompactionMods(unseqFile).remove();
      }
    } catch (IOException e) {
      LOGGER.error("{} cannot remove merging modification ", fullStorageGroupName, e);
    }
  }
}
