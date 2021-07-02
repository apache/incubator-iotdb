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
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.cross.CrossSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompactionScheduler.class);
  public static volatile AtomicInteger currentTaskNum = new AtomicInteger(0);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  // fullStorageGroupName -> timePartition -> compactionCount
  private static Map<String, Map<Long, Long>> compactionCountInPartition =
      new ConcurrentHashMap<>();

  public static void scheduleCompaction(
      TsFileResourceManager tsFileResourceManager, long timePartition) {
    LOGGER.warn(
        "{} [Compaction] start to schedule compaction",
        tsFileResourceManager.getStorageGroupName());
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
            tsFileResourceManager.getVirtualStorageGroup(),
            tsFileResourceManager.getStorageGroupDir(),
            timePartition,
            tsFileResourceManager,
            sequenceFileList,
            unsequenceFileList);
      } else if (compactionPriority == CompactionPriority.INNER_CROSS) {
        doCompactionInnerCrossPriority(
            tsFileResourceManager.getStorageGroupName(),
            tsFileResourceManager.getVirtualStorageGroup(),
            tsFileResourceManager.getStorageGroupDir(),
            timePartition,
            tsFileResourceManager,
            sequenceFileList,
            unsequenceFileList);
      } else if (compactionPriority == CompactionPriority.CROSS_INNER) {
        doCompactionCrossInnerPriority(
            tsFileResourceManager.getStorageGroupName(),
            tsFileResourceManager.getVirtualStorageGroup(),
            tsFileResourceManager.getStorageGroupDir(),
            timePartition,
            tsFileResourceManager,
            sequenceFileList,
            unsequenceFileList);
      }
    } finally {
      tsFileResourceManager.readUnlock();
    }
  }

  private static void doCompactionBalancePriority(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    boolean taskSubmitted = true;
    int concurrentCompactionThread = config.getConcurrentCompactionThread();
    while (taskSubmitted && currentTaskNum.get() < concurrentCompactionThread) {
      taskSubmitted =
          tryToSubmitInnerSpaceCompactionTask(
              logicalStorageGroupName,
              virtualStorageGroupName,
              timePartition,
              tsFileResourceManager,
              sequenceFileList,
              true,
              new InnerSpaceCompactionTaskFactory());
      taskSubmitted =
          tryToSubmitInnerSpaceCompactionTask(
                  logicalStorageGroupName,
                  virtualStorageGroupName,
                  timePartition,
                  tsFileResourceManager,
                  unsequenceFileList,
                  false,
                  new InnerSpaceCompactionTaskFactory())
              | taskSubmitted;
      taskSubmitted =
          tryToSubmitCrossSpaceCompactionTask(
                  logicalStorageGroupName,
                  virtualStorageGroupName,
                  storageGroupDir,
                  timePartition,
                  sequenceFileList,
                  unsequenceFileList,
                  new CrossSpaceCompactionTaskFactory())
              | taskSubmitted;
    }
  }

  private static void doCompactionInnerCrossPriority(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    tryToSubmitInnerSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileResourceManager,
        sequenceFileList,
        true,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileResourceManager,
        unsequenceFileList,
        false,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitCrossSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        storageGroupDir,
        timePartition,
        sequenceFileList,
        unsequenceFileList,
        new CrossSpaceCompactionTaskFactory());
  }

  private static void doCompactionCrossInnerPriority(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList) {
    tryToSubmitCrossSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        storageGroupDir,
        timePartition,
        sequenceFileList,
        unsequenceFileList,
        new CrossSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileResourceManager,
        sequenceFileList,
        true,
        new InnerSpaceCompactionTaskFactory());
    tryToSubmitInnerSpaceCompactionTask(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileResourceManager,
        unsequenceFileList,
        false,
        new InnerSpaceCompactionTaskFactory());
  }

  public static boolean tryToSubmitInnerSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    AbstractInnerSpaceCompactionSelector innerSpaceCompactionSelector =
        config
            .getInnerCompactionStrategy()
            .getCompactionSelector(
                logicalStorageGroupName,
                virtualStorageGroupName,
                timePartition,
                tsFileResourceManager,
                tsFileResources,
                sequence,
                taskFactory);
    return innerSpaceCompactionSelector.selectAndSubmit();
  }

  private static boolean tryToSubmitCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      String storageGroupDir,
      long timePartition,
      TsFileResourceList sequenceFileList,
      TsFileResourceList unsequenceFileList,
      CrossSpaceCompactionTaskFactory taskFactory) {
    AbstractCrossSpaceCompactionSelector crossSpaceCompactionSelector =
        config
            .getCrossCompactionStrategy()
            .getCompactionSelector(
                logicalStorageGroupName,
                virtualStorageGroupName,
                storageGroupDir,
                timePartition,
                sequenceFileList,
                unsequenceFileList,
                taskFactory);
    return crossSpaceCompactionSelector.selectAndSubmit();
  }

  public static int getCount() {
    return currentTaskNum.get();
  }

  public static Map<String, Map<Long, Long>> getCompactionCountInPartition() {
    return compactionCountInPartition;
  }

  public static void addPartitionCompaction(String fullStorageGroupName, long timePartition) {
    synchronized (compactionCountInPartition) {
      compactionCountInPartition
          .computeIfAbsent(fullStorageGroupName, l -> new HashMap<>())
          .put(
              timePartition,
              compactionCountInPartition.get(fullStorageGroupName).getOrDefault(timePartition, 0L)
                  + 1);
    }
  }

  public static void decPartitionCompaction(String fullStorageGroupName, long timePartition) {
    synchronized (compactionCountInPartition) {
      if (!compactionCountInPartition.containsKey(fullStorageGroupName)
          || !compactionCountInPartition.get(fullStorageGroupName).containsKey(timePartition)) {
        return;
      }
      compactionCountInPartition
          .get(fullStorageGroupName)
          .put(
              timePartition,
              compactionCountInPartition.get(fullStorageGroupName).get(timePartition) - 1);
    }
  }

  public static boolean isPartitionCompacting(String fullStorageGroupName, long timePartition) {
    return compactionCountInPartition
            .computeIfAbsent(fullStorageGroupName, l -> new HashMap<>())
            .getOrDefault(timePartition, 0L)
        > 0L;
  }
}
