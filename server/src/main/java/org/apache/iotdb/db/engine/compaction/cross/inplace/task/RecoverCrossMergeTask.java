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

package org.apache.iotdb.db.engine.compaction.cross.inplace.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.LogAnalyzer;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.LogAnalyzer.Status;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.MergeLogger;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * RecoverCrossMergeTask is an extension of MergeTask, which resumes the last merge progress by
 * scanning merge.log using LogAnalyzer and continue the unfinished merge.
 */
public class RecoverCrossMergeTask extends CrossSpaceMergeTask {

  private static final Logger logger = LoggerFactory.getLogger(RecoverCrossMergeTask.class);

  private LogAnalyzer analyzer;

  public RecoverCrossMergeTask(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      String storageGroupSysDir,
      MergeCallback callback,
      String taskName,
      boolean fullMerge,
      String storageGroupName) {
    super(
        seqFiles, unseqFiles, storageGroupSysDir, callback, taskName, fullMerge, storageGroupName);
  }

  public void recoverMerge(boolean continueMerge, File logFile)
      throws IOException, MetadataException {
    if (!logFile.exists()) {
      logger.info("{} no merge.log, merge recovery ends", taskName);
      return;
    }
    long startTime = System.currentTimeMillis();

    analyzer = new LogAnalyzer(resource, taskName, logFile, storageGroupName);
    Status status = analyzer.analyze();
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} merge recovery status determined: {} after {}ms",
          taskName,
          status,
          (System.currentTimeMillis() - startTime));
    }
    switch (status) {
      case NONE:
        logFile.delete();
        break;
      case MERGE_START:
        resumeAfterFilesLogged(continueMerge);
        break;
      case ALL_TS_MERGED:
        resumeAfterAllTsMerged(continueMerge);
        break;
      case MERGE_END:
        cleanUp(continueMerge);
        break;
      default:
        throw new UnsupportedOperationException(taskName + " found unrecognized status " + status);
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} merge recovery ends after {}ms", taskName, (System.currentTimeMillis() - startTime));
    }
  }

  private void resumeAfterFilesLogged(boolean continueMerge) throws IOException {
    if (continueMerge) {
      resumeMergeProgress();
      calculateConcurrentSeriesNum();
      if (concurrentMergeSeriesNum == 0) {
        throw new IOException(
            "Merge cannot be resumed under current memory budget, please "
                + "increase the budget or disable continueMergeAfterReboot");
      }

      MergeMultiChunkTask mergeChunkTask =
          new MergeMultiChunkTask(
              mergeContext,
              taskName,
              mergeLogger,
              resource,
              fullMerge,
              analyzer.getUnmergedPaths(),
              concurrentMergeSeriesNum,
              storageGroupName);
      analyzer.setUnmergedPaths(null);
      mergeChunkTask.mergeSeries();

      MergeFileTask mergeFileTask =
          new MergeFileTask(taskName, mergeContext, mergeLogger, resource, resource.getSeqFiles());
      mergeFileTask.mergeFiles();
    }
    cleanUp(continueMerge);
  }

  private void resumeAfterAllTsMerged(boolean continueMerge) throws IOException {
    if (continueMerge) {
      resumeMergeProgress();
      MergeFileTask mergeFileTask =
          new MergeFileTask(
              taskName, mergeContext, mergeLogger, resource, analyzer.getUnmergedFiles());
      analyzer.setUnmergedFiles(null);
      mergeFileTask.mergeFiles();
    } else {
      // NOTICE: although some of the seqFiles may have been truncated in last merge, we do not
      // recover them here because later TsFile recovery will recover them
      truncateFiles();
    }
    cleanUp(continueMerge);
  }

  private void resumeMergeProgress() throws IOException {
    mergeLogger = new MergeLogger(storageGroupSysDir);
    truncateFiles();
    recoverChunkCounts();
  }

  private void calculateConcurrentSeriesNum() throws IOException {
    long singleSeriesUnseqCost = 0;
    long maxUnseqCost = 0;
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      long[] chunkNums =
          MergeUtils.findTotalAndLargestSeriesChunkNum(
              unseqFile, resource.getFileReader(unseqFile));
      long totalChunkNum = chunkNums[0];
      long maxChunkNum = chunkNums[1];
      singleSeriesUnseqCost += unseqFile.getTsFileSize() * maxChunkNum / totalChunkNum;
      maxUnseqCost += unseqFile.getTsFileSize();
    }

    long singleSeriesSeqReadCost = 0;
    long maxSeqReadCost = 0;
    long seqWriteCost = 0;
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      long[] chunkNums =
          MergeUtils.findTotalAndLargestSeriesChunkNum(seqFile, resource.getFileReader(seqFile));
      long totalChunkNum = chunkNums[0];
      long maxChunkNum = chunkNums[1];
      long fileMetaSize = MergeUtils.getFileMetaSize(seqFile, resource.getFileReader(seqFile));
      long newSingleSeriesSeqReadCost = fileMetaSize * maxChunkNum / totalChunkNum;
      singleSeriesSeqReadCost = Math.max(newSingleSeriesSeqReadCost, singleSeriesSeqReadCost);
      maxSeqReadCost = Math.max(fileMetaSize, maxSeqReadCost);
      seqWriteCost += fileMetaSize;
    }

    long memBudget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
    int lb = 0;
    int ub = MaxSeriesMergeFileSelector.MAX_SERIES_NUM;
    int mid = (lb + ub) / 2;
    while (mid != lb) {
      long unseqCost = Math.min(singleSeriesUnseqCost * mid, maxUnseqCost);
      long seqReadCos = Math.min(singleSeriesSeqReadCost * mid, maxSeqReadCost);
      long totalCost = unseqCost + seqReadCos + seqWriteCost;
      if (totalCost <= memBudget) {
        lb = mid;
      } else {
        ub = mid;
      }
      mid = (lb + ub) / 2;
    }
    concurrentMergeSeriesNum = lb;
  }

  // scan the metadata to compute how many chunks are merged/unmerged so at last we can decide to
  // move the merged chunks or the unmerged chunks
  private void recoverChunkCounts() throws IOException {
    logger.info("{} recovering chunk counts", taskName);
    int fileCnt = 1;
    for (TsFileResource tsFileResource : resource.getSeqFiles()) {
      logger.info(
          "{} recovering {}  {}/{}",
          taskName,
          tsFileResource.getTsFile().getName(),
          fileCnt,
          resource.getSeqFiles().size());
      RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(tsFileResource);
      mergeFileWriter.makeMetadataVisible();
      mergeContext.getUnmergedChunkStartTimes().put(tsFileResource, new HashMap<>());
      List<PartialPath> pathsToRecover = analyzer.getMergedPaths();
      int cnt = 0;
      double progress = 0.0;
      for (PartialPath path : pathsToRecover) {
        recoverChunkCounts(path, tsFileResource, mergeFileWriter);
        if (logger.isInfoEnabled()) {
          cnt += 1.0;
          double newProgress = 100.0 * cnt / pathsToRecover.size();
          if (newProgress - progress >= 1.0) {
            progress = newProgress;
            logger.info(
                "{} {}% series count of {} are recovered",
                taskName, progress, tsFileResource.getTsFile().getName());
          }
        }
      }
      fileCnt++;
    }
    analyzer.setMergedPaths(null);
  }

  private void recoverChunkCounts(
      PartialPath path, TsFileResource tsFileResource, RestorableTsFileIOWriter mergeFileWriter)
      throws IOException {
    mergeContext.getUnmergedChunkStartTimes().get(tsFileResource).put(path, new ArrayList<>());

    List<ChunkMetadata> seqFileChunks = resource.queryChunkMetadata(path, tsFileResource);
    List<ChunkMetadata> mergeFileChunks =
        mergeFileWriter.getVisibleMetadataList(path.getDevice(), path.getMeasurement(), null);
    mergeContext
        .getMergedChunkCnt()
        .compute(
            tsFileResource,
            (k, v) -> v == null ? mergeFileChunks.size() : v + mergeFileChunks.size());
    int seqChunkIndex = 0;
    int mergeChunkIndex = 0;
    int unmergedCnt = 0;
    while (seqChunkIndex < seqFileChunks.size() && mergeChunkIndex < mergeFileChunks.size()) {
      ChunkMetadata seqChunk = seqFileChunks.get(seqChunkIndex);
      ChunkMetadata mergedChunk = mergeFileChunks.get(mergeChunkIndex);
      if (seqChunk.getStartTime() < mergedChunk.getStartTime()) {
        // this seqChunk is unmerged
        unmergedCnt++;
        seqChunkIndex++;
        mergeContext
            .getUnmergedChunkStartTimes()
            .get(tsFileResource)
            .get(path)
            .add(seqChunk.getStartTime());
      } else if (mergedChunk.getStartTime() <= seqChunk.getStartTime()
          && seqChunk.getStartTime() <= mergedChunk.getEndTime()) {
        // this seqChunk is merged
        seqChunkIndex++;
      } else {
        // seqChunk.startTime > mergeChunk.endTime, find next mergedChunk that may cover the
        // seqChunk
        mergeChunkIndex++;
      }
    }
    int finalUnmergedCnt = unmergedCnt;
    mergeContext
        .getUnmergedChunkCnt()
        .compute(tsFileResource, (k, v) -> v == null ? finalUnmergedCnt : v + finalUnmergedCnt);
  }

  private void truncateFiles() throws IOException {
    logger.info("{} truncating {} files", taskName, analyzer.getFileLastPositions().size());
    for (Entry<File, Long> entry : analyzer.getFileLastPositions().entrySet()) {
      File file = entry.getKey();
      Long lastPosition = entry.getValue();
      if (file.exists() && file.length() != lastPosition) {
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
          FileChannel channel = fileInputStream.getChannel();
          channel.truncate(lastPosition);
          channel.close();
        }
      }
    }
    analyzer.setFileLastPositions(null);
  }
}
