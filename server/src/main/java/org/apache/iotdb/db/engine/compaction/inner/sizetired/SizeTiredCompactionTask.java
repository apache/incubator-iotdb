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
package org.apache.iotdb.db.engine.compaction.inner.sizetired;

import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;
import org.apache.iotdb.db.exception.WriteLockFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.engine.compaction.inner.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.inner.utils.CompactionLogger.TARGET_NAME;

public class SizeTiredCompactionTask extends AbstractInnerSpaceCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SizeTiredCompactionTask.class);
  protected List<TsFileResource> selectedTsFileResourceList;
  protected TsFileResourceList tsFileResourceList;
  protected TsFileResourceManager tsFileResourceManager;
  protected boolean sequence;
  protected Set<String> skippedDevicesSet;

  public SizeTiredCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence) {
    super(logicalStorageGroupName + "-" + virtualStorageGroupName, timePartition);
    this.tsFileResourceList = tsFileResourceList;
    this.tsFileResourceManager = tsFileResourceManager;
    this.selectedTsFileResourceList = selectedTsFileResourceList;
    this.sequence = sequence;
    this.skippedDevicesSet = new HashSet<>();
  }

  public void renameLevelFilesMods(
      Collection<TsFileResource> mergeTsFiles, TsFileResource targetTsFile) throws IOException {
    LOGGER.debug("{} [compaction] merge starts to rename real file's mod", fullStorageGroupName);
    List<Modification> modifications = new ArrayList<>();
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      try (ModificationFile sourceCompactionModificationFile =
          ModificationFile.getCompactionMods(mergeTsFile)) {
        modifications.addAll(sourceCompactionModificationFile.getModifications());
        if (sourceCompactionModificationFile.exists()) {
          sourceCompactionModificationFile.remove();
        }
      }
      ModificationFile sourceModificationFile = ModificationFile.getNormalMods(mergeTsFile);
      if (sourceModificationFile.exists()) {
        sourceModificationFile.remove();
      }
    }
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile = ModificationFile.getNormalMods(targetTsFile)) {
        for (Modification modification : modifications) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          modificationFile.write(modification);
        }
      }
    }
  }

  @Override
  protected void doCompaction() throws Exception {
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    String targetFileName =
        TsFileNameGenerator.getInnerCompactionFileName(selectedTsFileResourceList).getName();
    TsFileResource targetTsFileResource =
        new TsFileResource(new File(dataDirectory + File.separator + targetFileName));
    LOGGER.warn(
        "{} [Compaction] starting compaction task with {} files",
        fullStorageGroupName,
        selectedTsFileResourceList.size());
    try {
      File logFile =
          new File(
              dataDirectory
                  + File.separator
                  + targetFileName
                  + CompactionLogger.COMPACTION_LOG_NAME);
      // compaction execution
      CompactionLogger compactionLogger = new CompactionLogger(logFile.getPath());
      for (TsFileResource resource : selectedTsFileResourceList) {
        compactionLogger.logFile(SOURCE_NAME, resource.getTsFile());
      }
      compactionLogger.logSequence(sequence);
      compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
      LOGGER.warn(
          "{} [Compaction] compaction with {}", fullStorageGroupName, selectedTsFileResourceList);
      CompactionUtils.compact(
          targetTsFileResource,
          selectedTsFileResourceList,
          fullStorageGroupName,
          compactionLogger,
          this.skippedDevicesSet,
          sequence);
      LOGGER.warn(
          "{} [SizeTiredCompactionTask] compact finish, close the logger", fullStorageGroupName);
      compactionLogger.close();
      if (logFile.exists()) {
        logFile.delete();
      }
    } finally {
      for (TsFileResource resource : selectedTsFileResourceList) {
        resource.setMerging(false);
      }
    }
    LOGGER.warn(
        "{} [Compaction] compaction finish, start to delete old files", fullStorageGroupName);
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException(String.format("%s [Compaction] abort", fullStorageGroupName));
    }
    // apply write lock for TsFileResource list
    try {
      tsFileResourceManager.writeLockWithTimeout("size-tired compaction", 20000);
    } catch (WriteLockFailedException e) {
      // if the thread of time partition deletion get the write lock,
      // current thread will catch a WriteLockFailException, then terminate the thread itself
      LOGGER.warn(
          "{} [SizeTiredCompactionTask] failed to get write lock, abort the task",
          fullStorageGroupName,
          e);
      throw new InterruptedException(
          String.format(
              "%s [Compaction] compaction abort because cannot acquire write lock",
              fullStorageGroupName));
    }
    try {
      // replace the old files with new file, the new is in same position as the old
      tsFileResourceList.insertBefore(selectedTsFileResourceList.get(0), targetTsFileResource);
      for (TsFileResource resource : selectedTsFileResourceList) {
        tsFileResourceList.remove(resource);
      }
      // delete the old files
      CompactionUtils.deleteTsFilesInDisk(selectedTsFileResourceList, fullStorageGroupName);
      LOGGER.warn(
          "{} [SizeTiredCompactionTask] old file deleted, start to rename mods file",
          fullStorageGroupName);
      renameLevelFilesMods(selectedTsFileResourceList, targetTsFileResource);
      LOGGER.warn("{} [SizeTiredCompactionTask] all compaction task finish", fullStorageGroupName);
    } finally {
      tsFileResourceManager.writeUnlock();
    }
  }
}
