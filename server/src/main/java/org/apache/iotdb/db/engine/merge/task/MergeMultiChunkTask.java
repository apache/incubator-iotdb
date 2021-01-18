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

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.merge.selector.IMergePathSelector;
import org.apache.iotdb.db.engine.merge.selector.NaivePathSelector;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeMultiChunkTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeMultiChunkTask.class);
  private static int minChunkPointNum = IoTDBDescriptor.getInstance().getConfig()
      .getMergeChunkPointNumberThreshold();

  private MergeLogger mergeLogger;
  private List<PartialPath> unmergedSeries;

  private String taskName;
  private MergeResource resource;
  private TimeValuePair[] currTimeValuePairs;
  private boolean fullMerge;

  private MergeContext mergeContext;

  private AtomicInteger mergedChunkNum = new AtomicInteger();
  private AtomicInteger unmergedChunkNum = new AtomicInteger();
  private int mergedSeriesCnt;
  private double progress;

  private int concurrentMergeSeriesNum;
  private List<PartialPath> currMergingPaths = new ArrayList<>();

  private String storageGroupName;

  public MergeMultiChunkTask(MergeContext context, String taskName, MergeLogger mergeLogger,
      MergeResource mergeResource, boolean fullMerge, List<PartialPath> unmergedSeries,
      int concurrentMergeSeriesNum, String storageGroupName) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.fullMerge = fullMerge;
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  void mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to mergeUnseq {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      mergeContext.getUnmergedChunkStartTimes().put(seqFile, new HashMap<>());
    }
    // mergeUnseq each series and write data into each seqFile's corresponding temp mergeUnseq file
    List<List<PartialPath>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<PartialPath> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      IMergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        currMergingPaths = pathSelector.next();
        mergePaths();
        resource.clearChunkWriterCache();
        if (Thread.interrupted()) {
          logger.info("MergeMultiChunkTask {} aborted", taskName);
          Thread.currentThread().interrupt();
          return;
        }
        mergedSeriesCnt += currMergingPaths.size();
        logMergeProgress();
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }
    mergeLogger.logAllTsEnd();
  }

  private void logMergeProgress() {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * mergedSeriesCnt / (double) (unmergedSeries.size());
      if (newProgress - progress >= 10.0) {
        progress = newProgress;
        logger.info("{} has merged {}% series", taskName, progress);
      }
    }
  }

  public String getProgress() {
    return String.format("Processed %d/%d series", mergedSeriesCnt, unmergedSeries.size());
  }

  private void mergePaths() throws IOException {
    mergeLogger.logTSStart(currMergingPaths);
    IPointReader[] unseqReaders = resource.getUnseqReaders(currMergingPaths);
    currTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (unseqReaders[i].hasNextTimeValuePair()) {
        currTimeValuePairs[i] = unseqReaders[i].currentTimeValuePair();
      }
    }

    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      pathsMergeOneFile(i, unseqReaders);

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    mergeLogger.logTSEnd();
  }

  private void pathsMergeOneFile(int seqFileIdx, IPointReader[] unseqReaders)
      throws IOException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    String deviceId = currMergingPaths.get(0).getDevice();
    long currDeviceMinTime = currTsFile.getStartTime(deviceId);
    // all paths in one call are from the same device
    if (currDeviceMinTime == Long.MAX_VALUE) {
      return;
    }

    for (PartialPath path : currMergingPaths) {
      mergeContext.getUnmergedChunkStartTimes().get(currTsFile).put(path, new ArrayList<>());
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    for (TimeValuePair timeValuePair : currTimeValuePairs) {
      if (timeValuePair != null && timeValuePair.getTimestamp() < currDeviceMinTime) {
        currDeviceMinTime = timeValuePair.getTimestamp();
      }
    }
    boolean isLastFile = seqFileIdx + 1 == resource.getSeqFiles().size();

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification>[] modifications = new List[currMergingPaths.size()];
    List<ChunkMetadata>[] seqChunkMeta = new List[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      modifications[i] = resource.getModifications(currTsFile, currMergingPaths.get(i));
      seqChunkMeta[i] = resource.queryChunkMetadata(currMergingPaths.get(i), currTsFile);
      modifyChunkMetaData(seqChunkMeta[i], modifications[i]);

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    List<Integer> unskippedPathIndices = filterNoDataPaths(seqChunkMeta, seqFileIdx);
    if (unskippedPathIndices.isEmpty()) {
      return;
    }

    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile);
    for (PartialPath path : currMergingPaths) {
      MeasurementSchema schema = resource.getSchema(path);
      mergeFileWriter.addSchema(path, schema);
    }
    // mergeUnseq unseq data with seq data in this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(deviceId);
    boolean dataWritten = mergeChunks(seqChunkMeta, isLastFile, fileSequenceReader, unseqReaders,
        mergeFileWriter, currTsFile);
    if (dataWritten) {
      mergeFileWriter.endChunkGroup();
      mergeLogger.logFilePosition(mergeFileWriter.getFile());
      currTsFile.updateStartTime(deviceId, currDeviceMinTime);
    }
  }

  private List<Integer> filterNoDataPaths(List[] seqChunkMeta, int seqFileIdx) {
    // if the last seqFile does not contains this series but the unseqFiles do, data of this
    // series should also be written into a new chunk
    List<Integer> ret = new ArrayList<>();
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (seqChunkMeta[i].isEmpty()
          && !(seqFileIdx + 1 == resource.getSeqFiles().size() && currTimeValuePairs[i] != null)) {
        continue;
      }
      ret.add(i);
    }
    return ret;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean mergeChunks(List<ChunkMetadata>[] seqChunkMeta, boolean isLastFile,
      TsFileSequenceReader reader, IPointReader[] unseqReaders,
      RestorableTsFileIOWriter mergeFileWriter, TsFileResource currFile)
      throws IOException {
    int[] ptWrittens = new int[seqChunkMeta.length];
    int mergeChunkSubTaskNum = IoTDBDescriptor.getInstance().getConfig()
        .getMergeChunkSubThreadNum();
    MetaListEntry[] metaListEntries = new MetaListEntry[currMergingPaths.size()];
    PriorityQueue<Integer>[] chunkIdxHeaps = new PriorityQueue[mergeChunkSubTaskNum];
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      chunkIdxHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    for (int i = 0; i < currMergingPaths.size(); i++) {
      chunkIdxHeaps[idx % mergeChunkSubTaskNum].add(i);
      if (seqChunkMeta[i].isEmpty()) {
        continue;
      }

      MetaListEntry entry = new MetaListEntry(i, seqChunkMeta[i]);
      entry.next();
      metaListEntries[i] = entry;
      idx++;
      ptWrittens[i] = 0;
    }

    mergedChunkNum.set(0);
    unmergedChunkNum.set(0);

    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      futures.add(MergeManager.getINSTANCE()
          .submitChunkSubTask(new MergeChunkHeapTask(chunkIdxHeaps[i],
              metaListEntries, ptWrittens,
              reader,
              mergeFileWriter, unseqReaders,
              currFile,
              isLastFile, i)));

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      try {
        futures.get(i).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    // add mergeUnseq and unmerged chunk statistic
    mergeContext.getMergedChunkCnt().compute(currFile, (tsFileResource, anInt) -> anInt == null ?
        mergedChunkNum.get() : anInt + mergedChunkNum.get());
    mergeContext.getUnmergedChunkCnt().compute(currFile, (tsFileResource, anInt) -> anInt == null ?
        unmergedChunkNum.get() : anInt + unmergedChunkNum.get());

    return mergedChunkNum.get() > 0;
  }


  /**
   * mergeUnseq a sequence chunk SK
   * <p>
   * 1. no need to write the chunk to .mergeUnseq file when: isn't full mergeUnseq & there isn't unclosed
   * chunk before & SK is big enough & SK isn't overflowed & SK isn't modified
   * <p>
   * <p>
   * 2. write SK to .mergeUnseq.file without compressing when: is full mergeUnseq & there isn't unclosed chunk
   * before & SK is big enough & SK isn't overflowed & SK isn't modified
   * <p>
   * 3. other cases: need to unCompress the chunk and write 3.1 SK isn't overflowed 3.2 SK is
   * overflowed
   */
  @SuppressWarnings("java:S2445") // avoid writing the same writer concurrently
  private int mergeChunkV2(ChunkMetadata currMeta, boolean chunkOverflowed,
      boolean chunkTooSmall, Chunk chunk, int lastUnclosedChunkPoint, int pathIdx,
      TsFileIOWriter mergeFileWriter, IPointReader unseqReader,
      IChunkWriter chunkWriter, TsFileResource currFile) throws IOException {

    int unclosedChunkPoint = lastUnclosedChunkPoint;
    boolean chunkModified = (currMeta.getDeleteIntervalList() != null &&
        !currMeta.getDeleteIntervalList().isEmpty());

    // no need to write the chunk to .mergeUnseq file
    if (!fullMerge && lastUnclosedChunkPoint == 0 && !chunkTooSmall && !chunkOverflowed
        && !chunkModified) {
      unmergedChunkNum.incrementAndGet();
      mergeContext.getUnmergedChunkStartTimes().get(currFile).get(currMergingPaths.get(pathIdx))
          .add(currMeta.getStartTime());
      return 0;
    }

    // write SK to .mergeUnseq.file without compressing
    if (fullMerge && lastUnclosedChunkPoint == 0 && !chunkTooSmall && !chunkOverflowed
        && !chunkModified) {
      synchronized (mergeFileWriter) {
        mergeFileWriter.writeChunk(chunk, currMeta);
      }
      mergeContext.incTotalPointWritten(currMeta.getNumOfPoints());
      mergeContext.incTotalChunkWritten();
      mergedChunkNum.incrementAndGet();
      return 0;
    }

    // 3.1 SK isn't overflowed, just uncompress and write sequence chunk
    if (!chunkOverflowed) {
      unclosedChunkPoint += MergeUtils.writeChunkWithoutUnseq(chunk, chunkWriter);
      mergedChunkNum.incrementAndGet();
    } else {
      // 3.2 SK is overflowed, uncompress sequence chunk and mergeUnseq with unseq chunk, then write
      unclosedChunkPoint += writeChunkWithUnseq(chunk, chunkWriter, unseqReader,
          currMeta.getEndTime(), pathIdx);
      mergedChunkNum.incrementAndGet();
    }

    // update points written statistics
    mergeContext.incTotalPointWritten((long) unclosedChunkPoint - lastUnclosedChunkPoint);
    if (minChunkPointNum > 0 && unclosedChunkPoint >= minChunkPointNum
        || unclosedChunkPoint > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      synchronized (mergeFileWriter) {
        chunkWriter.writeToFileWriter(mergeFileWriter);
      }
      unclosedChunkPoint = 0;
    }
    return unclosedChunkPoint;
  }

  private int writeRemainingUnseq(IChunkWriter chunkWriter,
      IPointReader unseqReader, long timeLimit, int pathIdx) throws IOException {
    int ptWritten = 0;
    while (currTimeValuePairs[pathIdx] != null
        && currTimeValuePairs[pathIdx].getTimestamp() < timeLimit) {
      writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
      ptWritten++;
      unseqReader.nextTimeValuePair();
      currTimeValuePairs[pathIdx] =
          unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
    }
    return ptWritten;
  }

  private int writeChunkWithUnseq(Chunk chunk, IChunkWriter chunkWriter, IPointReader unseqReader,
      long chunkLimitTime, int pathIdx) throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      cnt += mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
    cnt += writeRemainingUnseq(chunkWriter, unseqReader, chunkLimitTime, pathIdx);
    return cnt;
  }

  private int mergeWriteBatch(BatchData batchData, IChunkWriter chunkWriter,
      IPointReader unseqReader, int pathIdx) throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // mergeUnseq data in batch and data in unseqReader

      boolean overwriteSeqPoint = false;
      // unseq point.time <= sequence point.time, write unseq point
      while (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].getTimestamp() <= time) {
        writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
        if (currTimeValuePairs[pathIdx].getTimestamp() == time) {
          overwriteSeqPoint = true;
        }
        unseqReader.nextTimeValuePair();
        currTimeValuePairs[pathIdx] =
            unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
        cnt++;
      }
      // unseq point.time > sequence point.time, write seq point
      if (!overwriteSeqPoint) {
        writeBatchPoint(batchData, i, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }

  public class MergeChunkHeapTask implements Callable<Void> {

    private PriorityQueue<Integer> chunkIdxHeap;
    private MetaListEntry[] metaListEntries;
    private int[] ptWrittens;
    private TsFileSequenceReader reader;
    private RestorableTsFileIOWriter mergeFileWriter;
    private IPointReader[] unseqReaders;
    private TsFileResource currFile;
    private boolean isLastFile;
    private int taskNum;

    private int totalSeriesNum;

    public MergeChunkHeapTask(PriorityQueue<Integer> chunkIdxHeap,
        MetaListEntry[] metaListEntries, int[] ptWrittens,
        TsFileSequenceReader reader,
        RestorableTsFileIOWriter mergeFileWriter,
        IPointReader[] unseqReaders, TsFileResource currFile, boolean isLastFile, int taskNum) {
      this.chunkIdxHeap = chunkIdxHeap;
      this.metaListEntries = metaListEntries;
      this.ptWrittens = ptWrittens;
      this.reader = reader;
      this.mergeFileWriter = mergeFileWriter;
      this.unseqReaders = unseqReaders;
      this.currFile = currFile;
      this.isLastFile = isLastFile;
      this.taskNum = taskNum;
      this.totalSeriesNum = chunkIdxHeap.size();
    }

    @Override
    public Void call() throws Exception {
      mergeChunkHeap();
      return null;
    }

    @SuppressWarnings("java:S2445") // avoid reading the same reader concurrently
    private void mergeChunkHeap() throws IOException {
      while (!chunkIdxHeap.isEmpty()) {
        int pathIdx = chunkIdxHeap.poll();
        PartialPath path = currMergingPaths.get(pathIdx);
        MeasurementSchema measurementSchema = resource.getSchema(path);
        IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        if (metaListEntries[pathIdx] != null) {
          MetaListEntry metaListEntry = metaListEntries[pathIdx];
          ChunkMetadata currMeta = metaListEntry.current();
          boolean isLastChunk = !metaListEntry.hasNext();
          boolean chunkOverflowed = MergeUtils
              .isChunkOverflowed(currTimeValuePairs[pathIdx], currMeta);
          boolean chunkTooSmall = MergeUtils
              .isChunkTooSmall(ptWrittens[pathIdx], currMeta, isLastChunk, minChunkPointNum);

          Chunk chunk;
          synchronized (reader) {
            chunk = reader.readMemChunk(currMeta);
          }
          ptWrittens[pathIdx] = mergeChunkV2(currMeta, chunkOverflowed, chunkTooSmall, chunk,
              ptWrittens[pathIdx], pathIdx, mergeFileWriter, unseqReaders[pathIdx], chunkWriter,
              currFile);

          if (!isLastChunk) {
            metaListEntry.next();
            chunkIdxHeap.add(pathIdx);
            continue;
          }
        }
        // this only happens when the seqFiles do not contain this series, otherwise the remaining
        // data will be merged with the last chunk in the seqFiles
        if (isLastFile && currTimeValuePairs[pathIdx] != null) {
          ptWrittens[pathIdx] += writeRemainingUnseq(chunkWriter, unseqReaders[pathIdx],
              Long.MAX_VALUE,
              pathIdx);
          mergedChunkNum.incrementAndGet();
        }
        // the last merged chunk may still be smaller than the threshold, flush it anyway
        if (ptWrittens[pathIdx] > 0) {
          synchronized (mergeFileWriter) {
            chunkWriter.writeToFileWriter(mergeFileWriter);
          }
        }
      }
    }

    public String getStorageGroupName() {
      return storageGroupName;
    }

    public String getTaskName() {
      return taskName + "_" + taskNum;
    }

    public String getProgress() {
      return String.format("Processed %d/%d series", totalSeriesNum - chunkIdxHeap.size(),
          totalSeriesNum);
    }
  }
}
