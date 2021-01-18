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

package org.apache.iotdb.db.engine.compaction;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.MERGING_MODIFICATION_FILE_NAME;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

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
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CloseCompactionMergeCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TsFileManagement {

  private static final Logger logger = LoggerFactory.getLogger(TsFileManagement.class);
  protected String storageGroupName;
  protected String storageGroupDir;

  /**
   * Serialize queries, delete resource files, compaction cleanup files
   */
  private final ReadWriteLock compactionMergeLock = new ReentrantReadWriteLock();

  public volatile boolean isUnseqMerging = false;
  public volatile boolean isSeqMerging = false;
  /**
   * This is the modification file of the result of the current mergeUnseq. Because the merged file may
   * be invisible at this moment, without this, deletion/update during mergeUnseq could be lost.
   */
  public ModificationFile mergingModification;
  private long mergeStartTime;

  public TsFileManagement(String storageGroupName, String storageGroupDir) {
    this.storageGroupName = storageGroupName;
    this.storageGroupDir = storageGroupDir;
  }

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
   * add one TsFile to list for recover
   */
  public abstract void addRecover(TsFileResource tsFileResource, boolean sequence);

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
   * fork current TsFile list (call this before mergeUnseq)
   */
  public abstract void forkCurrentFileList(long timePartition) throws IOException;

  public void readLock() {
    compactionMergeLock.readLock().lock();
  }

  public void readUnLock() {
    compactionMergeLock.readLock().unlock();
  }

  public void writeLock() {
    compactionMergeLock.writeLock().lock();
  }

  public void writeUnlock() {
    compactionMergeLock.writeLock().unlock();
  }

  public boolean tryWriteLock() {
    return compactionMergeLock.writeLock().tryLock();
  }

  protected abstract void merge(long timePartition);

  public class CompactionMergeTask implements Runnable {

    private CloseCompactionMergeCallBack closeCompactionMergeCallBack;
    private long timePartitionId;

    public CompactionMergeTask(CloseCompactionMergeCallBack closeCompactionMergeCallBack,
        long timePartitionId) {
      this.closeCompactionMergeCallBack = closeCompactionMergeCallBack;
      this.timePartitionId = timePartitionId;
    }

    @Override
    public void run() {
      merge(timePartitionId);
      closeCompactionMergeCallBack.call();
    }
  }

  public class CompactionRecoverTask implements Runnable {

    private CloseCompactionMergeCallBack closeCompactionMergeCallBack;

    public CompactionRecoverTask(CloseCompactionMergeCallBack closeCompactionMergeCallBack) {
      this.closeCompactionMergeCallBack = closeCompactionMergeCallBack;
    }

    @Override
    public void run() {
      recover();
      closeCompactionMergeCallBack.call();
    }
  }

  public synchronized void merge(boolean fullMerge, List<TsFileResource> seqMergeList,
      List<TsFileResource> unSeqMergeList, long dataTTL) {
    if (isUnseqMerging) {
      if (logger.isInfoEnabled()) {
        logger.info("{} Last mergeUnseq is ongoing, currently consumed time: {}ms", storageGroupName,
            (System.currentTimeMillis() - mergeStartTime));
      }
      return;
    }
    // wait until seq merge has finished
    while (isSeqMerging) {
      try {
        wait(200);
      } catch (InterruptedException e) {
        logger.error("{} [Compaction] shutdown", storageGroupName, e);
        Thread.currentThread().interrupt();
        return;
      }
    }
    isUnseqMerging = true;

    if (seqMergeList.isEmpty()) {
      logger.info("{} no seq files to be merged", storageGroupName);
      isUnseqMerging = false;
      return;
    }

    if (unSeqMergeList.isEmpty()) {
      logger.info("{} no unseq files to be merged", storageGroupName);
      isUnseqMerging = false;
      return;
    }

    long budget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
    long timeLowerBound = System.currentTimeMillis() - dataTTL;
    MergeResource mergeResource = new MergeResource(seqMergeList, unSeqMergeList, timeLowerBound);

    IMergeFileSelector fileSelector = getMergeFileSelector(budget, mergeResource);
    try {
      List[] mergeFiles = fileSelector.select();
      if (mergeFiles.length == 0) {
        logger.info("{} cannot select mergeUnseq candidates under the budget {}", storageGroupName,
            budget);
        isUnseqMerging = false;
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

      mergeStartTime = System.currentTimeMillis();
      MergeTask mergeTask = new MergeTask(mergeResource, storageGroupDir,
          this::mergeEndAction, taskName, fullMerge, fileSelector.getConcurrentMergeNum(),
          storageGroupName);
      mergingModification = new ModificationFile(
          storageGroupDir + File.separator + MERGING_MODIFICATION_FILE_NAME);
      MergeManager.getINSTANCE().submitMainTask(mergeTask);
      if (logger.isInfoEnabled()) {
        logger.info("{} submits a mergeUnseq task {}, merging {} seqFiles, {} unseqFiles",
            storageGroupName, taskName, mergeFiles[0].size(), mergeFiles[1].size());
      }

    } catch (MergeException | IOException e) {
      logger.error("{} cannot select file for mergeUnseq", storageGroupName, e);
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
   * acquire the write locks of the resource , the mergeUnseq lock and the compaction lock
   */
  private void doubleWriteLock(TsFileResource seqFile) {
    boolean fileLockGot;
    boolean compactionLockGot;
    while (true) {
      fileLockGot = seqFile.tryWriteLock();
      compactionLockGot = tryWriteLock();

      if (fileLockGot && compactionLockGot) {
        break;
      } else {
        // did not get all of them, release the gotten one and retry
        if (compactionLockGot) {
          writeUnlock();
        }
        if (fileLockGot) {
          seqFile.writeUnlock();
        }
      }
    }
  }

  /**
   * release the write locks of the resource , the mergeUnseq lock and the compaction lock
   */
  private void doubleWriteUnlock(TsFileResource seqFile) {
    writeUnlock();
    seqFile.writeUnlock();
  }

  private void removeUnseqFiles(List<TsFileResource> unseqFiles) {
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
      // remove old modifications and write modifications generated during mergeUnseq
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
      logger.error("{} cannot clean the ModificationFile of {} after mergeUnseq", storageGroupName,
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
    logger.info("{} a mergeUnseq task is ending...", storageGroupName);

    if (unseqFiles.isEmpty()) {
      // mergeUnseq runtime exception arose, just end this mergeUnseq
      isUnseqMerging = false;
      logger.info("{} a mergeUnseq task abnormally ends", storageGroupName);
      return;
    }
    removeUnseqFiles(unseqFiles);

    for (int i = 0; i < seqFiles.size(); i++) {
      TsFileResource seqFile = seqFiles.get(i);
      // get both seqFile lock and mergeUnseq lock
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
        logger.error("{} a mergeUnseq task ends but cannot delete log {}", storageGroupName,
            mergeLog.toPath());
      } finally {
        doubleWriteUnlock(seqFile);
      }
    }

    logger.info("{} a mergeUnseq task ends", storageGroupName);
  }

  public static int getMergeLevel(File file) {
    String mergeLevelStr = file.getPath()
        .substring(file.getPath().lastIndexOf(FILE_NAME_SEPARATOR) + 1)
        .replaceAll(TSFILE_SUFFIX, "");
    return Integer.parseInt(mergeLevelStr);
  }

  // ({systemTime}-{versionNum}-{mergeNum}.tsfile)
  public static int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "")
        .split(FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "")
        .split(FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      int cmpVersion = Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
      if (cmpVersion == 0) {
        return Long.compare(Long.parseLong(items1[2]), Long.parseLong(items2[2]));
      }
      return cmpVersion;
    } else {
      return cmp;
    }
  }
}
