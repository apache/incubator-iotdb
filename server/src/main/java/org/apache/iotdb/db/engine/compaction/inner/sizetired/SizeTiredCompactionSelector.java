package org.apache.iotdb.db.engine.compaction.inner.sizetired;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SizeTiredCompactionSelector extends AbstractInnerSpaceCompactionSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SizeTiredCompactionSelector.class);
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  public SizeTiredCompactionSelector(
      String storageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    super(
        storageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileResourceManager,
        tsFileResources,
        sequence,
        taskFactory);
  }

  @Override
  public boolean selectAndSubmit() {
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
        if ((CompactionScheduler.currentTaskNum.get() >= concurrentCompactionThread)
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
          createAndSubmitTask(selectedFileList);
          taskSubmitted = true;
          selectedFileList = new ArrayList<>();
          selectedFileSize = 0L;
        }
      }
      // if some files are selected but the total size is smaller than target size, submit a task
      if (selectedFileList.size() > 1) {
        try {
          createAndSubmitTask(selectedFileList);
        } catch (Exception e) {
          LOGGER.warn(e.getMessage(), e);
        }
      }
      return taskSubmitted;
    } finally {
      tsFileResources.readUnlock();
    }
  }

  private void createAndSubmitTask(List<TsFileResource> selectedFileList) {
    AbstractCompactionTask compactionTask =
        taskFactory.createTask(
            storageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileResourceManager,
            tsFileResources,
            selectedFileList,
            sequence);
    for (TsFileResource resource : selectedFileList) {
      resource.setMerging(true);
      LOGGER.info(
          "{}-{} [Compaction] start to compact TsFile {}",
          storageGroupName,
          virtualStorageGroupName,
          resource);
    }
    CompactionTaskManager.getInstance()
        .submitTask(
            storageGroupName + "-" + virtualStorageGroupName, timePartition, compactionTask);
    LOGGER.info(
        "{}-{} [Compaction] submit a inner compaction task of {} files",
        storageGroupName,
        virtualStorageGroupName,
        selectedFileList.size());
  }
}