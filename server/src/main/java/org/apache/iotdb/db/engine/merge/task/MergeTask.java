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

package org.apache.iotdb.db.engine.merge.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeTask merges given seqFiles and unseqFiles into new ones, which basically consists of three
 * steps: 1. rewrite overflowed, modified or small-sized chunks into temp mergeUnseq files
 *        2. move the merged chunks in the temp files back to the seqFiles or move the unmerged
 *        chunks in the seqFiles into temp files and replace the seqFiles with the temp files.
 *        3. remove unseqFiles
 */
public class MergeTask implements Callable<Void> {

  public static final String MERGE_SUFFIX = ".mergeUnseq";
  private static final Logger logger = LoggerFactory.getLogger(MergeTask.class);

  MergeResource resource;
  String storageGroupSysDir;
  String storageGroupName;
  MergeLogger mergeLogger;
  MergeContext mergeContext = new MergeContext();

  private MergeCallback callback;
  int concurrentMergeSeriesNum;
  String taskName;
  boolean fullMerge;

  States states = States.START;

  MergeMultiChunkTask chunkTask;
  MergeFileTask fileTask;

  MergeTask(List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles, String storageGroupSysDir, MergeCallback callback,
      String taskName, boolean fullMerge, String storageGroupName) {
    this.resource = new MergeResource(seqFiles, unseqFiles);
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
    this.concurrentMergeSeriesNum = 1;
    this.storageGroupName = storageGroupName;
  }

  public MergeTask(MergeResource mergeResource, String storageGroupSysDir, MergeCallback callback,
      String taskName, boolean fullMerge, int concurrentMergeSeriesNum, String storageGroupName) {
    this.resource = mergeResource;
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  @Override
  public Void call() throws Exception {
    try {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in mergeUnseq {}", taskName, e);
      abort();
    }
    return null;
  }

  private void abort() throws IOException {
    states = States.ABORTED;
    cleanUp(false);
    // call the callback to make sure the StorageGroup exit merging status, but passing 2
    // empty file lists to avoid files being deleted.
    callback.call(Collections.emptyList(), Collections.emptyList(),
        new File(storageGroupSysDir, MergeLogger.MERGE_LOG_NAME));
  }

  private void doMerge() throws IOException, MetadataException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to mergeUnseq {} seqFiles, {} unseqFiles", taskName,
          resource.getSeqFiles().size(), resource.getUnseqFiles().size());
    }
    long startTime = System.currentTimeMillis();
    long totalFileSize = MergeUtils.collectFileSizes(resource.getSeqFiles(),
        resource.getUnseqFiles());
    mergeLogger = new MergeLogger(storageGroupSysDir);

    mergeLogger.logFiles(resource);

    Set<PartialPath> devices = IoTDB.metaManager.getDevices(new PartialPath(storageGroupName));
    Map<PartialPath, MeasurementSchema> measurementSchemaMap = new HashMap<>();
    List<PartialPath> unmergedSeries = new ArrayList<>();
    for (PartialPath device : devices) {
      MNode deviceNode = IoTDB.metaManager.getNodeByPath(device);
      for (Entry<String, MNode> entry : deviceNode.getChildren().entrySet()) {
        PartialPath path = device.concatNode(entry.getKey());
        measurementSchemaMap.put(path, ((MeasurementMNode) entry.getValue()).getSchema());
        unmergedSeries.add(path);
      }
    }
    resource.setMeasurementSchemaMap(measurementSchemaMap);

    mergeLogger.logMergeStart();

    chunkTask = new MergeMultiChunkTask(mergeContext, taskName, mergeLogger, resource,
        fullMerge, unmergedSeries, concurrentMergeSeriesNum, storageGroupName);
    states = States.MERGE_CHUNKS;
    chunkTask.mergeSeries();
    if (Thread.interrupted()) {
      logger.info("Merge task {} aborted", taskName);
      abort();
      return;
    }

    fileTask = new MergeFileTask(taskName, mergeContext, mergeLogger, resource,
        resource.getSeqFiles());
    states = States.MERGE_FILES;
    chunkTask = null;
    fileTask.mergeFiles();
    if (Thread.interrupted()) {
      logger.info("Merge task {} aborted", taskName);
      abort();
      return;
    }

    states = States.CLEAN_UP;
    fileTask = null;
    cleanUp(true);
    if (logger.isInfoEnabled()) {
      double elapsedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;
      double byteRate = totalFileSize / elapsedTime / 1024 / 1024;
      double seriesRate = unmergedSeries.size() / elapsedTime;
      double chunkRate = mergeContext.getTotalChunkWritten() / elapsedTime;
      double fileRate =
          (resource.getSeqFiles().size() + resource.getUnseqFiles().size()) / elapsedTime;
      double ptRate = mergeContext.getTotalPointWritten() / elapsedTime;
      logger.info("{} ends after {}s, byteRate: {}MB/s, seriesRate {}/s, chunkRate: {}/s, "
              + "fileRate: {}/s, ptRate: {}/s",
          taskName, elapsedTime, byteRate, seriesRate, chunkRate, fileRate, ptRate);
    }
  }

  void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

    resource.clear();
    mergeContext.clear();

    if (mergeLogger != null) {
      mergeLogger.close();
    }

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      File mergeFile = new File(seqFile.getTsFilePath() + MERGE_SUFFIX);
      mergeFile.delete();
      seqFile.setMerging(false);
    }
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      unseqFile.setMerging(false);
    }

    File logFile = new File(storageGroupSysDir, MergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // make sure mergeUnseq.log is not deleted until unseqFiles are cleared so that when system
      // reboots, the undeleted files can be deleted again
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile);
    } else {
      logFile.delete();
    }
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  enum States {
    START,
    MERGE_CHUNKS,
    MERGE_FILES,
    CLEAN_UP,
    ABORTED
  }

  public String getProgress() {
    switch (states) {
      case ABORTED:
        return "Aborted";
      case CLEAN_UP:
        return "Cleaning up";
      case MERGE_FILES:
        return "Merging files: " + fileTask.getProgress();
      case MERGE_CHUNKS:
        return "Merging series: " + chunkTask.getProgress();
      case START:
      default:
        return "Just started";
    }
  }

  public String getTaskName() {
    return taskName;
  }
}
