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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.CrossSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.ICompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.merge.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.merge.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;
import org.apache.iotdb.db.exception.MergeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompactionScheduler.class);
  public static AtomicInteger currentTaskNum = new AtomicInteger(0);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public static void compactionSchedule(
      TsFileResourceManager tsFileResourceManager, long timePartition) {
    if (currentTaskNum.get() >= config.getConcurrentCompactionThread()) {
      return;
    }
    tsFileResourceManager.readLock();
    try {
      TsFileResourceList sequenceFileList =
          tsFileResourceManager.getSequenceListByTimePartition(timePartition);
      TsFileResourceList unsequenceFileList =
          tsFileResourceManager.getUnsequenceListByTimePartition(timePartition);
      CompactionPriority compactionPriority = config.getCompactionPriority();
      if (compactionPriority == CompactionPriority.BALANCE) {
        doCompactionBalancePriority(
            tsFileResourceManager.getStorageGroupName(),
            timePartition,
            sequenceFileList,
            unsequenceFileList);
      } else if (compactionPriority == CompactionPriority.INNER_CROSS) {
        doCompactionInnerCrossPriority(
            tsFileResourceManager.getStorageGroupName(),
            timePartition,
            sequenceFileList,
            unsequenceFileList);
      } else if (compactionPriority == CompactionPriority.CROSS_INNER) {
        doCompactionCrossInnerPriority(
            tsFileResourceManager.getStorageGroupName(),
            timePartition,
            sequenceFileList,
            unsequenceFileList);
      }
    } finally {
      tsFileResourceManager.readUnlock();
    }
  }

  private static void doCompactionBalancePriority(
      String storageGroup,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    boolean taskSubmitted = true;
    int concurrentCompactionThread = config.getConcurrentCompactionThread();
    while (taskSubmitted && currentTaskNum.get() < concurrentCompactionThread) {
      taskSubmitted =
          tryToSubmitInnerSpaceCompactionTask(
              storageGroup,
              timePartition,
              sequenceFileList,
              true,
              new InnerSpaceCompactionTaskFactory());
      taskSubmitted =
          tryToSubmitInnerSpaceCompactionTask(
                  storageGroup,
                  timePartition,
                  unsequenceFileList,
                  false,
                  new InnerSpaceCompactionTaskFactory())
              | taskSubmitted;
      taskSubmitted =
          tryToSubmitCrossSpaceCompactionTask(
                  storageGroup,
                  timePartition,
                  sequenceFileList,
                  unsequenceFileList,
                  new CrossSpaceCompactionTaskFactory())
              | taskSubmitted;
    }
  }

  private static void doCompactionInnerCrossPriority(
      String storageGroup,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    tryToSubmitInnerSpaceCompactionTask(
        storageGroup, timePartition, sequenceFileList, true, new InnerSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        storageGroup,
        timePartition,
        unsequenceFileList,
        false,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitCrossSpaceCompactionTask(
        storageGroup,
        timePartition,
        sequenceFileList,
        unsequenceFileList,
        new CrossSpaceCompactionTaskFactory());
  }

  private static void doCompactionCrossInnerPriority(
      String storageGroup,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    tryToSubmitCrossSpaceCompactionTask(
        storageGroup,
        timePartition,
        sequenceFileList,
        unsequenceFileList,
        new CrossSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        storageGroup, timePartition, sequenceFileList, true, new InnerSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        storageGroup,
        timePartition,
        unsequenceFileList,
        false,
        new InnerSpaceCompactionTaskFactory());
  }

  public static boolean tryToSubmitInnerSpaceCompactionTask(
      String storageGroup,
      long timePartition,
      TsFileResourceList tsFileResources,
      boolean sequence,
      ICompactionTaskFactory taskFactory) {
    boolean taskSubmitted = false;
    List<TsFileResource> selectedFileList = new ArrayList<>();
    long selectedFileSize = 0L;
    long targetCompactionFileSize = config.getTargetCompactionFileSize();
    boolean enableSeqSpaceCompaction = config.isEnableSeqSpaceCompaction();
    boolean enableUnseqSpaceCompaction = config.isEnableUnseqSpaceCompaction();
    int concurrentCompactionThread = config.getConcurrentCompactionThread();
    // this iterator traverses the list in reverse order
    tsFileResources.readLock();
    try {
      Iterator<TsFileResource> iterator = tsFileResources.reverseIterator();
      while (iterator.hasNext()) {
        TsFileResource currentFile = iterator.next();
        if ((currentTaskNum.get() >= concurrentCompactionThread)
            || (!enableSeqSpaceCompaction && sequence)
            || (!enableUnseqSpaceCompaction && !sequence)) {
          return taskSubmitted;
        }
        if (currentFile.getTsFileSize() >= targetCompactionFileSize
            || currentFile.isMerging()
            || !currentFile.isClosed()) {
          selectedFileList.clear();
          selectedFileSize = 0L;
          continue;
        }
        selectedFileList.add(currentFile);
        selectedFileSize += currentFile.getTsFileSize();
        if (selectedFileSize >= targetCompactionFileSize) {
          // submit the task
          CompactionContext context = new CompactionContext();
          context.setSequence(sequence);
          if (sequence) {
            context.setSequenceFileResourceList(tsFileResources);
            context.setSelectedSequenceFiles(selectedFileList);
          } else {
            context.setUnsequenceFileResourceList(tsFileResources);
            context.setSelectedUnsequenceFiles(selectedFileList);
          }
          AbstractCompactionTask compactionTask = taskFactory.createTask(context);
          CompactionTaskManager.getInstance()
              .submitTask(storageGroup, timePartition, compactionTask);
          selectedFileList = new ArrayList<>();
          selectedFileSize = 0L;
        }
      }
      // if some files are selected but the total size is smaller than target size, submit a task
      if (selectedFileList.size() > 0) {
        try {
          CompactionContext context = new CompactionContext();
          context.setSequence(sequence);
          if (sequence) {
            context.setSelectedSequenceFiles(selectedFileList);
            context.setSequenceFileResourceList(tsFileResources);
          } else {
            context.setSelectedUnsequenceFiles(selectedFileList);
            context.setUnsequenceFileResourceList(tsFileResources);
          }

          AbstractCompactionTask compactionTask = taskFactory.createTask(context);
          CompactionTaskManager.getInstance()
              .submitTask(storageGroup, timePartition, compactionTask);
        } catch (Exception e) {
          LOGGER.warn(e.getMessage(), e);
        }
      }
      return taskSubmitted;
    } finally {
      tsFileResources.readUnlock();
    }
  }

  private static boolean tryToSubmitCrossSpaceCompactionTask(
      String storageGroup,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList,
      ICompactionTaskFactory taskFactory) {
    boolean taskSubmitted = false;
    if ((currentTaskNum.get() >= config.getConcurrentCompactionThread())
        || (!config.isEnableCrossSpaceCompaction())) {
      return taskSubmitted;
    }
    Iterator<TsFileResource> seqIterator = sequenceFileList.iterator();
    Iterator<TsFileResource> unSeqIterator = unsequenceFileList.iterator();
    List<TsFileResource> seqFileList = new ArrayList<>();
    List<TsFileResource> unSeqFileList = new ArrayList<>();
    while (seqIterator.hasNext()) {
      seqFileList.add(seqIterator.next());
    }
    while (unSeqIterator.hasNext()) {
      unSeqFileList.add(unSeqIterator.next());
    }
    if (seqFileList.isEmpty() || unSeqFileList.isEmpty()) {
      return taskSubmitted;
    }
    if (unSeqFileList.size() > config.getMaxOpenFileNumInCrossSpaceCompaction()) {
      unSeqFileList = unSeqFileList.subList(0, config.getMaxOpenFileNumInCrossSpaceCompaction());
    }
    long budget = config.getMergeMemoryBudget();
    long timeLowerBound = System.currentTimeMillis() - Long.MAX_VALUE;
    CrossSpaceMergeResource mergeResource =
        new CrossSpaceMergeResource(seqFileList, unSeqFileList, timeLowerBound);

    ICrossSpaceMergeFileSelector fileSelector =
        CompactionUtils.getCrossSpaceFileSelector(budget, mergeResource);
    try {
      List[] mergeFiles = fileSelector.select();
      if (mergeFiles.length == 0) {
        return taskSubmitted;
      }
      // avoid pending tasks holds the metadata and streams
      mergeResource.clear();
      // do not cache metadata until true candidates are chosen, or too much metadata will be
      // cached during selection
      mergeResource.setCacheDeviceMeta(true);

      for (TsFileResource tsFileResource : mergeResource.getSeqFiles()) {
        tsFileResource.setMerging(true);
      }
      for (TsFileResource tsFileResource : mergeResource.getUnseqFiles()) {
        tsFileResource.setMerging(true);
      }

      CompactionContext context = new CompactionContext();
      context.setMergeResource(mergeResource);
      context.setSequenceFileResourceList(sequenceFileList);
      context.setSelectedSequenceFiles(mergeResource.getSeqFiles());
      context.setUnsequenceFileResourceList(unsequenceFileList);
      context.setSelectedUnsequenceFiles(mergeResource.getUnseqFiles());
      context.setConcurrentMergeCount(fileSelector.getConcurrentMergeNum());

      AbstractCompactionTask compactionTask = taskFactory.createTask(context);
      CompactionTaskManager.getInstance().submitTask(storageGroup, timePartition, compactionTask);
    } catch (MergeException | IOException e) {
      LOGGER.error("{} cannot select file for cross space compaction", storageGroup, e);
    }

    return false;
  }

  public static int getCount() {
    return currentTaskNum.get();
  }
}
