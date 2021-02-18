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
package org.apache.iotdb.db.engine.storagegroup.virtualSg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualStorageGroupManager {

  private static final Logger logger = LoggerFactory.getLogger(VirtualStorageGroupManager.class);

  /**
   * virtual storage group partitioner
   */
  VirtualPartitioner partitioner = HashVirtualPartitioner.getInstance();

  /**
   * all virtual storage group processor
   */
  StorageGroupProcessor[] virtualStorageGroupProcessor;

  /**
   * value of root.stats."root.sg".TOTAL_POINTS
   */
  private long monitorSeriesValue;

  public VirtualStorageGroupManager() {
    virtualStorageGroupProcessor = new StorageGroupProcessor[partitioner.getPartitionCount()];
  }

  /**
   * push forceCloseAllWorkingTsFileProcessors down to all sg
   */
  public void forceCloseAllWorkingTsFileProcessors() throws TsFileProcessorException {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.forceCloseAllWorkingTsFileProcessors();
      }
    }
  }

  /**
   * push syncCloseAllWorkingTsFileProcessors down to all sg
   */
  public void syncCloseAllWorkingTsFileProcessors() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.syncCloseAllWorkingTsFileProcessors();
      }
    }
  }

  /**
   * push check ttl down to all sg
   */
  public void checkTTL() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.checkFilesTTL();
      }
    }
  }

  /**
   * get processor from device id
   *
   * @param partialPath device path
   * @return virtual storage group processor
   */
  @SuppressWarnings("java:S2445")
  // actually storageGroupMNode is a unique object on the mtree, synchronize it is reasonable
  public StorageGroupProcessor getProcessor(PartialPath partialPath,
      StorageGroupMNode storageGroupMNode)
      throws StorageGroupProcessorException, StorageEngineException {
    int loc = partitioner.deviceToVirtualStorageGroupId(partialPath);

    StorageGroupProcessor processor = virtualStorageGroupProcessor[loc];
    if (processor == null) {
      // if finish recover
      if (StorageEngine.getInstance().isAllSgReady()) {
        synchronized (storageGroupMNode) {
          processor = virtualStorageGroupProcessor[loc];
          if (processor == null) {
            processor = StorageEngine.getInstance()
                .buildNewStorageGroupProcessor(storageGroupMNode.getPartialPath(),
                    storageGroupMNode, String.valueOf(loc));
            virtualStorageGroupProcessor[loc] = processor;
          }
        }
      } else {
        // not finished recover, refuse the request
        throw new StorageEngineException(
            "the sg " + partialPath + " may not ready now, please wait and retry later",
            TSStatusCode.STORAGE_GROUP_NOT_READY.getStatusCode());
      }
    }

    return processor;
  }

  /**
   * recover
   *
   * @param storageGroupMNode logical sg mnode
   */
  public void recover(StorageGroupMNode storageGroupMNode) {
    List<Thread> threadList = new ArrayList<>(partitioner.getPartitionCount());
    for (int i = 0; i < partitioner.getPartitionCount(); i++) {
      int cur = i;
      Thread recoverThread = new Thread(new Runnable() {
        @Override
        public void run() {
          StorageGroupProcessor processor = null;
          try {
            processor = StorageEngine.getInstance()
                .buildNewStorageGroupProcessor(storageGroupMNode.getPartialPath(),
                    storageGroupMNode, String.valueOf(cur));
          } catch (StorageGroupProcessorException e) {
            logger.error(
                "failed to recover storage group processor in " + storageGroupMNode.getFullPath()
                    + " virtual storage group id is " + cur);
          }
          virtualStorageGroupProcessor[cur] = processor;
        }
      });

      threadList.add(recoverThread);
    }

    Thread thread;
    for (int i = 0; i < partitioner.getPartitionCount(); i++) {
      try {
        thread = threadList.get(i);
        thread.start();
        thread.join();
      } catch (InterruptedException e) {
        logger.error(
            "failed to recover storage group processor in " + storageGroupMNode.getFullPath()
                + " virtual storage group id is " + i);
        Thread.currentThread().interrupt();
      }
    }
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
   * push closeStorageGroupProcessor operation down to all virtual storage group processors
   */
  public void closeStorageGroupProcessor(boolean isSeq, boolean isSync) {
    for (StorageGroupProcessor processor : virtualStorageGroupProcessor) {
      if (processor == null) {
        continue;
      }

      if (logger.isInfoEnabled()) {
        logger.info("{} closing sg processor is called for closing {}, seq = {}",
            isSync ? "sync" : "async",
            processor.getVirtualStorageGroupId() + "-" + processor.getLogicalStorageGroupName(),
            isSeq);
      }

      processor.writeLock();
      try {
        if (isSeq) {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor : new ArrayList<>(
              processor.getWorkSequenceTsFileProcessors())) {
            if (isSync) {
              processor.syncCloseOneTsFileProcessor(true, tsfileProcessor);
            } else {
              processor.asyncCloseOneTsFileProcessor(true, tsfileProcessor);
            }
          }
        } else {
          // to avoid concurrent modification problem, we need a new array list
          for (TsFileProcessor tsfileProcessor : new ArrayList<>(
              processor.getWorkUnsequenceTsFileProcessors())) {
            if (isSync) {
              processor.syncCloseOneTsFileProcessor(false, tsfileProcessor);
            } else {
              processor.asyncCloseOneTsFileProcessor(false, tsfileProcessor);
            }
          }
        }
      } finally {
        processor.writeUnlock();
      }
    }
  }

  /**
   * push closeStorageGroupProcessor operation down to all virtual storage group processors
   */
  public void closeStorageGroupProcessor(long partitionId,
      boolean isSeq,
      boolean isSync) {
    for (StorageGroupProcessor processor : virtualStorageGroupProcessor) {
      if (processor != null) {
        logger
            .info("async closing sg processor is called for closing {}, seq = {}, partitionId = {}",
                processor.getVirtualStorageGroupId() + "-" + processor
                    .getLogicalStorageGroupName(), isSeq, partitionId);
        processor.writeLock();
        // to avoid concurrent modification problem, we need a new array list
        List<TsFileProcessor> processors = isSeq ?
            new ArrayList<>(processor.getWorkSequenceTsFileProcessors()) :
            new ArrayList<>(processor.getWorkUnsequenceTsFileProcessors());
        try {
          for (TsFileProcessor tsfileProcessor : processors) {
            if (tsfileProcessor.getTimeRangeId() == partitionId) {
              if (isSync) {
                processor.syncCloseOneTsFileProcessor(isSeq, tsfileProcessor);
              } else {
                processor.asyncCloseOneTsFileProcessor(isSeq, tsfileProcessor);
              }
              break;
            }
          }
        } finally {
          processor.writeUnlock();
        }
      }
    }
  }

  /**
   * push delete operation down to all virtual storage group processors
   */
  public void delete(PartialPath path, long startTime, long endTime, long planIndex)
      throws IOException {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.delete(path, startTime, endTime, planIndex);
      }
    }
  }

  /**
   * push countUpgradeFiles operation down to all virtual storage group processors
   */
  public int countUpgradeFiles() {
    int totalUpgradeFileNum = 0;
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        totalUpgradeFileNum += storageGroupProcessor.countUpgradeFiles();
      }
    }

    return totalUpgradeFileNum;
  }

  /**
   * push upgradeAll operation down to all virtual storage group processors
   */
  public void upgradeAll() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.upgrade();
      }
    }
  }

  /**
   * push mergeAll operation down to all virtual storage group processors
   */
  public void mergeAll(boolean fullMerge) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.merge(fullMerge);
      }
    }
  }

  /**
   * push syncDeleteDataFiles operation down to all virtual storage group processors
   */
  public void syncDeleteDataFiles() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.syncDeleteDataFiles();
      }
    }
  }

  /**
   * push setTTL operation down to all virtual storage group processors
   */
  public void setTTL(long dataTTL) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.setDataTTL(dataTTL);
      }
    }
  }

  /**
   * push deleteStorageGroup operation down to all virtual storage group processors
   */
  public void deleteStorageGroup(String path) {
    for (StorageGroupProcessor processor : virtualStorageGroupProcessor) {
      if (processor != null) {
        processor.deleteFolder(path);
      }
    }
  }

  /**
   * push getAllClosedStorageGroupTsFile operation down to all virtual storage group processors
   */
  public void getAllClosedStorageGroupTsFile(PartialPath storageGroupName,
      Map<PartialPath, Map<Long, List<TsFileResource>>> ret) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        List<TsFileResource> allResources = storageGroupProcessor.getSequenceFileTreeSet();
        allResources.addAll(storageGroupProcessor.getUnSequenceFileList());
        for (TsFileResource tsfile : allResources) {
          if (!tsfile.isClosed()) {
            continue;
          }
          long partitionNum = tsfile.getTimePartition();
          Map<Long, List<TsFileResource>> storageGroupFiles = ret.computeIfAbsent(storageGroupName
              , n -> new HashMap<>());
          storageGroupFiles.computeIfAbsent(partitionNum, n -> new ArrayList<>())
              .add(tsfile);
        }
      }
    }
  }

  /**
   * push setPartitionVersionToMax operation down to all virtual storage group processors
   */
  public void setPartitionVersionToMax(long partitionId, long newMaxVersion) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.setPartitionFileVersionToMax(partitionId, newMaxVersion);
      }
    }
  }

  /**
   * push removePartitions operation down to all virtual storage group processors
   */
  public void removePartitions(TimePartitionFilter filter) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.removePartitions(filter);
      }
    }
  }

  /**
   * push getWorkingStorageGroupPartitions operation down to all virtual storage group processors
   */
  public void getWorkingStorageGroupPartitions(String storageGroupName,
      Map<String, List<Pair<Long, Boolean>>> res) {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        List<Pair<Long, Boolean>> partitionIdList = new ArrayList<>();
        for (TsFileProcessor tsFileProcessor : storageGroupProcessor
            .getWorkSequenceTsFileProcessors()) {
          Pair<Long, Boolean> tmpPair = new Pair<>(tsFileProcessor.getTimeRangeId(), true);
          partitionIdList.add(tmpPair);
        }

        for (TsFileProcessor tsFileProcessor : storageGroupProcessor
            .getWorkUnsequenceTsFileProcessors()) {
          Pair<Long, Boolean> tmpPair = new Pair<>(tsFileProcessor.getTimeRangeId(), false);
          partitionIdList.add(tmpPair);
        }

        res.put(storageGroupName, partitionIdList);
      }
    }
  }

  public void releaseWalDirectByteBufferPool() {
    for (StorageGroupProcessor storageGroupProcessor : virtualStorageGroupProcessor) {
      if (storageGroupProcessor != null) {
        storageGroupProcessor.releaseWalDirectByteBufferPool();
      }
    }
  }

  /**
   * only for test
   */
  public void reset() {
    Arrays.fill(virtualStorageGroupProcessor, null);
  }
}
