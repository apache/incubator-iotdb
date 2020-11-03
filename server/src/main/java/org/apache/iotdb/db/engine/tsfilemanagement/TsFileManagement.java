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

package org.apache.iotdb.db.engine.tsfilemanagement;

import static org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.MERGING_MODIFICATION_FILE_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.selector.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.selector.MaxFileMergeFileSelector;
import org.apache.iotdb.db.engine.merge.selector.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.merge.selector.MergeFileStrategy;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseHotCompactionMergeCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TsFileManagement {

  private static final Logger logger = LoggerFactory.getLogger(TsFileManagement.class);
  protected String storageGroupName;
  protected String storageGroupDir;

  /**
   * mergeLock is to be used in the merge process. Concurrent queries, deletions and merges may
   * result in losing some deletion in the merged new file, so a lock is necessary.
   */
  public final ReentrantReadWriteLock mergeLock = new ReentrantReadWriteLock();
  /**
   * hotCompactionMergeLock is used to wait for TsFile list change in hot compaction merge
   * processor.
   */
  private final ReadWriteLock hotCompactionMergeLock = new ReentrantReadWriteLock();

  public volatile boolean isUnseqMerging = false;
  /**
   * This is the modification file of the result of the current merge. Because the merged file may
   * be invisible at this moment, without this, deletion/update during merge could be lost.
   */
  public ModificationFile mergingModification;
  private long mergeStartTime;

  public TsFileManagement(String storageGroupName, String storageGroupDir) {
    this.storageGroupName = storageGroupName;
    this.storageGroupDir = storageGroupDir;
  }

  /**
   * get the TsFile list which has been completed hot compacted
   */
  public abstract List<TsFileResource> getStableTsFileList(boolean sequence);

  /**
   * get the TsFile list in sequence
   */
  public abstract List<TsFileResource> getTsFileList(boolean sequence);

  /**
   * get the TsFile list iterator in sequence
   */
  public abstract Iterator<TsFileResource> getIterator(boolean sequence);

  /**
   * remove one TsFile from list
   */
  public abstract void remove(TsFileResource tsFileResource, boolean sequence);

  /**
   * remove some TsFiles from list
   */
  public abstract void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence);

  /**
   * add one TsFile to list
   */
  public abstract void add(TsFileResource tsFileResource, boolean sequence);

  /**
   * add some TsFiles to list
   */
  public abstract void addAll(List<TsFileResource> tsFileResourceList, boolean sequence);

  /**
   * is one TsFile contained in list
   */
  public abstract boolean contains(TsFileResource tsFileResource, boolean sequence);

  /**
   * clear list
   */
  public abstract void clear();

  /**
   * is the list empty
   */
  public abstract boolean isEmpty(boolean sequence);

  /**
   * return TsFile list size
   */
  public abstract int size(boolean sequence);

  /**
   * recover TsFile list
   */
  public abstract void recover();

  /**
   * fork current TsFile list (call this before merge)
   */
  public abstract void forkCurrentFileList(long timePartition) throws IOException;

  public void readLock() {
    hotCompactionMergeLock.readLock().lock();
  }

  public void readUnLock() {
    hotCompactionMergeLock.readLock().unlock();
  }

  public void writeLock() {
    hotCompactionMergeLock.writeLock().lock();
  }

  public void writeUnlock() {
    hotCompactionMergeLock.writeLock().unlock();
  }

  public boolean tryWriteLock() {
    return hotCompactionMergeLock.writeLock().tryLock();
  }

  protected abstract void merge(long timePartition);

  public class HotCompactionMergeTask implements Runnable {

    private CloseHotCompactionMergeCallBack closeHotCompactionMergeCallBack;
    private long timePartitionId;

    public HotCompactionMergeTask(CloseHotCompactionMergeCallBack closeHotCompactionMergeCallBack,
        long timePartitionId) {
      this.closeHotCompactionMergeCallBack = closeHotCompactionMergeCallBack;
      this.timePartitionId = timePartitionId;
    }

    @Override
    public void run() {
      merge(timePartitionId);
      closeHotCompactionMergeCallBack.call();
    }
  }

  public void merge(boolean fullMerge, List<TsFileResource> seqMergeList,
      List<TsFileResource> unSeqMergeList, long dataTTL) {
    if (isUnseqMerging) {
      if (logger.isInfoEnabled()) {
        logger.info("{} Last merge is ongoing, currently consumed time: {}ms", storageGroupName,
            (System.currentTimeMillis() - mergeStartTime));
      }
      return;
    }
    logger.info("{} will close all files for starting a merge (fullmerge = {})", storageGroupName,
        fullMerge);

    if (seqMergeList.isEmpty()) {
      logger.info("{} no seq files to be merged", storageGroupName);
      return;
    }

    if (unSeqMergeList.isEmpty()) {
      logger.info("{} no unseq files to be merged", storageGroupName);
      return;
    }

    long budget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
    long timeLowerBound = System.currentTimeMillis() - dataTTL;
    MergeResource mergeResource = new MergeResource(seqMergeList, unSeqMergeList, timeLowerBound);

    IMergeFileSelector fileSelector = getMergeFileSelector(budget, mergeResource);
    try {
      List[] mergeFiles = fileSelector.select();
      if (mergeFiles.length == 0) {
        logger.info("{} cannot select merge candidates under the budget {}", storageGroupName,
            budget);
        return;
      }
      // avoid pending tasks holds the metadata and streams
      mergeResource.clear();
      String taskName = storageGroupName + "-" + System.currentTimeMillis();
      // do not cache metadata until true candidates are chosen, or too much metadata will be
      // cached during selection
      mergeResource.setCacheDeviceMeta(true);

      for (TsFileResource tsFileResource : mergeResource.getSeqFiles()) {
        tsFileResource.setMerging(true);
      }
      for (TsFileResource tsFileResource : mergeResource.getUnseqFiles()) {
        tsFileResource.setMerging(true);
      }

      MergeTask mergeTask = new MergeTask(mergeResource, storageGroupDir,
          this::mergeEndAction, taskName, fullMerge, fileSelector.getConcurrentMergeNum(),
          storageGroupName);
      mergingModification = new ModificationFile(
          storageGroupDir + File.separator + MERGING_MODIFICATION_FILE_NAME);
      MergeManager.getINSTANCE().submitMainTask(mergeTask);
      if (logger.isInfoEnabled()) {
        logger.info("{} submits a merge task {}, merging {} seqFiles, {} unseqFiles",
            storageGroupName, taskName, mergeFiles[0].size(), mergeFiles[1].size());
      }
      isUnseqMerging = true;
      mergeStartTime = System.currentTimeMillis();

    } catch (MergeException | IOException e) {
      logger.error("{} cannot select file for merge", storageGroupName, e);
    }
  }

  private IMergeFileSelector getMergeFileSelector(long budget, MergeResource resource) {
    MergeFileStrategy strategy = IoTDBDescriptor.getInstance().getConfig().getMergeFileStrategy();
    switch (strategy) {
      case MAX_FILE_NUM:
        return new MaxFileMergeFileSelector(resource, budget);
      case MAX_SERIES_NUM:
        return new MaxSeriesMergeFileSelector(resource, budget);
      default:
        throw new UnsupportedOperationException("Unknown MergeFileStrategy " + strategy);
    }
  }

  /**
   * acquire the write locks of the resource , the merge lock and the hot compaction lock
   */
  private void doubleWriteLock(TsFileResource seqFile) {
    boolean fileLockGot;
    boolean mergeLockGot;
    boolean hotCompactionLockGot;
    while (true) {
      fileLockGot = seqFile.tryWriteLock();
      mergeLockGot = mergeLock.writeLock().tryLock();
      hotCompactionLockGot = tryWriteLock();

      if (fileLockGot && mergeLockGot && hotCompactionLockGot) {
        break;
      } else {
        // did not get all of them, release the gotten one and retry
        if (hotCompactionLockGot) {
          writeUnlock();
        }
        if (mergeLockGot) {
          mergeLock.writeLock().unlock();
        }
        if (fileLockGot) {
          seqFile.writeUnlock();
        }
      }
    }
  }

  /**
   * release the write locks of the resource , the merge lock and the hot compaction lock
   */
  private void doubleWriteUnlock(TsFileResource seqFile) {
    writeUnlock();
    mergeLock.writeLock().unlock();
    seqFile.writeUnlock();
  }

  private void removeUnseqFiles(List<TsFileResource> unseqFiles) {
    mergeLock.writeLock().lock();
    writeLock();
    try {
      removeAll(unseqFiles, false);
      // clean cache
      if (IoTDBDescriptor.getInstance().getConfig().isMetaDataCacheEnable()) {
        ChunkCache.getInstance().clear();
        ChunkMetadataCache.getInstance().clear();
        TimeSeriesMetadataCache.getInstance().clear();
      }
    } finally {
      writeUnlock();
      mergeLock.writeLock().unlock();
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

  @SuppressWarnings("squid:S1141")
  private void updateMergeModification(TsFileResource seqFile) {
    try {
      // remove old modifications and write modifications generated during merge
      seqFile.removeModFile();
      if (mergingModification != null) {
        for (Modification modification : mergingModification.getModifications()) {
          seqFile.getModFile().write(modification);
        }
        try {
          seqFile.getModFile().close();
        } catch (IOException e) {
          logger
              .error("Cannot close the ModificationFile {}", seqFile.getModFile().getFilePath(), e);
        }
      }
    } catch (IOException e) {
      logger.error("{} cannot clean the ModificationFile of {} after merge", storageGroupName,
          seqFile.getTsFile(), e);
    }
  }

  private void removeMergingModification() {
    try {
      if (mergingModification != null) {
        mergingModification.remove();
        mergingModification = null;
      }
    } catch (IOException e) {
      logger.error("{} cannot remove merging modification ", storageGroupName, e);
    }
  }

  public void mergeEndAction(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles,
      File mergeLog) {
    logger.info("{} a merge task is ending...", storageGroupName);

    if (unseqFiles.isEmpty()) {
      // merge runtime exception arose, just end this merge
      isUnseqMerging = false;
      logger.info("{} a merge task abnormally ends", storageGroupName);
      return;
    }

    removeUnseqFiles(unseqFiles);

    for (int i = 0; i < seqFiles.size(); i++) {
      TsFileResource seqFile = seqFiles.get(i);
      // get both seqFile lock and merge lock
      doubleWriteLock(seqFile);

      try {
        updateMergeModification(seqFile);
        if (i == seqFiles.size() - 1) {
          //FIXME if there is an exception, the the modification file will be not closed.
          removeMergingModification();
          isUnseqMerging = false;
          Files.delete(mergeLog.toPath());
        }
      } catch (IOException e) {
        logger.error("{} a merge task ends but cannot delete log {}", storageGroupName,
            mergeLog.toPath());
      } finally {
        doubleWriteUnlock(seqFile);
      }
    }
    logger.info("{} a merge task ends", storageGroupName);
  }

}
