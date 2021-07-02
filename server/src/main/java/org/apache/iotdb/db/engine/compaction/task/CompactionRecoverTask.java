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
package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.MergeLogger;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CompactionRecoverCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class CompactionRecoverTask implements Callable<Void> {
  private static final Logger logger = LoggerFactory.getLogger(CompactionRecoverTask.class);
  private CompactionRecoverCallBack compactionRecoverCallBack;
  private TsFileResourceManager tsFileResourceManager;
  private String logicalStorageGroupName;
  private String virtualStorageGroupId;

  public CompactionRecoverTask(
      CompactionRecoverCallBack compactionRecoverCallBack,
      TsFileResourceManager tsFileResourceManager,
      String logicalStorageGroupName,
      String virtualStorageGroupId) {
    this.compactionRecoverCallBack = compactionRecoverCallBack;
    this.tsFileResourceManager = tsFileResourceManager;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupId = virtualStorageGroupId;
  }

  @Override
  public Void call() throws Exception {
    logger.warn("recovering sequence inner compaction");
    recoverInnerCompaction(true);
    logger.warn("recovering unsequence inner compaction");
    recoverInnerCompaction(false);
    logger.warn("recovering cross compaction");
    recoverCrossCompaction();
    logger.warn("try to synchronize CompactionScheduler");
    CompactionScheduler.decPartitionCompaction(
        logicalStorageGroupName + "-" + virtualStorageGroupId, 0);
    CompactionScheduler.currentTaskNum.decrementAndGet();
    compactionRecoverCallBack.call();
    logger.warn(
        "recover task finish, current compaction thread is {}",
        CompactionScheduler.currentTaskNum.get());
    return null;
  }

  private void recoverInnerCompaction(boolean isSequence) throws Exception {
    Set<Long> timePartitions = tsFileResourceManager.getTimePartitions();
    List<String> dirs;
    if (isSequence) {
      dirs = DirectoryManager.getInstance().getAllSequenceFileFolders();
    } else {
      dirs = DirectoryManager.getInstance().getAllUnSequenceFileFolders();
    }
    for (String dir : dirs) {
      String storageGroupDir =
          dir + File.separator + logicalStorageGroupName + File.separator + virtualStorageGroupId;
      for (Long timePartition : timePartitions) {
        String timePartitionDir = storageGroupDir + File.separator + timePartition;
        File[] compactionLogs = CompactionUtils.findInnerSpaceCompactionLogs(timePartitionDir);
        for (File compactionLog : compactionLogs) {
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getInnerCompactionStrategy()
              .getCompactionRecoverTask(
                  tsFileResourceManager.getStorageGroupName(),
                  tsFileResourceManager.getVirtualStorageGroup(),
                  timePartition,
                  tsFileResourceManager,
                  compactionLog,
                  storageGroupDir,
                  isSequence
                      ? tsFileResourceManager.getSequenceListByTimePartition(timePartition)
                      : tsFileResourceManager.getUnsequenceListByTimePartition(timePartition),
                  isSequence
                      ? tsFileResourceManager.getSequenceRecoverTsFileResources()
                      : tsFileResourceManager.getUnsequenceRecoverTsFileResources(),
                  isSequence)
              .call();
        }
      }
    }
  }

  private void recoverCrossCompaction() throws Exception {
    Set<Long> timePartitions = tsFileResourceManager.getTimePartitions();
    List<String> sequenceDirs = DirectoryManager.getInstance().getAllSequenceFileFolders();
    for (String dir : sequenceDirs) {
      String storageGroupDir =
          dir + File.separator + logicalStorageGroupName + File.separator + virtualStorageGroupId;
      for (Long timePartition : timePartitions) {
        String timePartitionDir = storageGroupDir + File.separator + timePartition;
        File[] compactionLogs = MergeLogger.findCrossSpaceCompactionLogs(timePartitionDir);
        for (File compactionLog : compactionLogs) {
          logger.warn("calling cross compaction task");
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getCrossCompactionStrategy()
              .getCompactionRecoverTask(
                  logicalStorageGroupName,
                  virtualStorageGroupId,
                  timePartition,
                  storageGroupDir,
                  tsFileResourceManager.getSequenceListByTimePartition(timePartition),
                  tsFileResourceManager.getUnsequenceListByTimePartition(timePartition),
                  1,
                  compactionLog)
              .call();
        }
      }
    }
  }
}
