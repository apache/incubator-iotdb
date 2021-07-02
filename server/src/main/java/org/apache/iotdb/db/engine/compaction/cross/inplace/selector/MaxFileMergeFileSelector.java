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

package org.apache.iotdb.db.engine.compaction.cross.inplace.selector;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.MergeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * MaxFileMergeFileSelector selects the most files from given seqFiles and unseqFiles which can be
 * merged without exceeding given memory budget. It always assume the number of timeseries being
 * queried at the same time is 1 to maximize the number of file merged.
 */
public class MaxFileMergeFileSelector implements ICrossSpaceMergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(MaxFileMergeFileSelector.class);
  private static final String LOG_FILE_COST = "Memory cost of file {} is {}";

  CrossSpaceMergeResource resource;

  long totalCost;
  private long memoryBudget;
  private long maxSeqFileCost;

  // the number of timeseries being queried at the same time
  int concurrentMergeNum = 1;

  /** Total metadata size of each file. */
  private Map<TsFileResource, Long> fileMetaSizeMap = new HashMap<>();
  /** Maximum memory cost of querying a timeseries in each file. */
  private Map<TsFileResource, Long> maxSeriesQueryCostMap = new HashMap<>();

  List<TsFileResource> selectedUnseqFiles;
  List<TsFileResource> selectedSeqFiles;

  private Collection<Integer> tmpSelectedSeqFiles;
  private long tempMaxSeqFileCost;

  private boolean[] seqSelected;
  private int seqSelectedNum;

  public MaxFileMergeFileSelector(CrossSpaceMergeResource resource, long memoryBudget) {
    this.resource = resource;
    this.memoryBudget = memoryBudget;
  }

  /**
   * Select merge candidates from seqFiles and unseqFiles under the given memoryBudget. This process
   * iteratively adds the next unseqFile from unseqFiles and its overlapping seqFiles as newly-added
   * candidates and computes their estimated memory cost. If the current cost pluses the new cost is
   * still under the budget, accept the unseqFile and the seqFiles as candidates, otherwise go to
   * the next iteration. The memory cost of a file is calculated in two ways: The rough estimation:
   * for a seqFile, the size of its metadata is used for estimation. Since in the worst case, the
   * file only contains one timeseries and all its metadata will be loaded into memory with at most
   * one actual data chunk (which is negligible) and writing the timeseries into a new file generate
   * metadata of the similar size, so the size of all seqFiles' metadata (generated when writing new
   * chunks) pluses the largest one (loaded when reading a timeseries from the seqFiles) is the
   * total estimation of all seqFiles; for an unseqFile, since the merge reader may read all chunks
   * of a series to perform a merge read, the whole file may be loaded into memory, so we use the
   * file's length as the maximum estimation. The tight estimation: based on the rough estimation,
   * we scan the file's metadata to count the number of chunks for each series, find the series
   * which have the most chunks in the file and use its chunk proportion to refine the rough
   * estimation. The rough estimation is performed first, if no candidates can be found using rough
   * estimation, we run the selection again with tight estimation.
   *
   * @return two lists of TsFileResource, the former is selected seqFiles and the latter is selected
   *     unseqFiles or an empty array if there are no proper candidates by the budget.
   */
  @Override
  public List[] select() throws MergeException {
    long startTime = System.currentTimeMillis();
    try {
      logger.info(
          "Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(),
          resource.getUnseqFiles().size());
      select(false);
      if (selectedUnseqFiles.isEmpty()) {
        select(true);
      }
      resource.setSeqFiles(selectedSeqFiles);
      resource.setUnseqFiles(selectedUnseqFiles);
      resource.removeOutdatedSeqReaders();
      if (selectedUnseqFiles.isEmpty()) {
        logger.info("No merge candidates are found");
        return new List[0];
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "time consumption {}ms",
          selectedSeqFiles.size(),
          selectedUnseqFiles.size(),
          totalCost,
          System.currentTimeMillis() - startTime);
    }
    return new List[] {selectedSeqFiles, selectedUnseqFiles};
  }

  void select(boolean useTightBound) throws IOException {
    tmpSelectedSeqFiles = new HashSet<>();
    seqSelected = new boolean[resource.getSeqFiles().size()];
    seqSelectedNum = 0;
    selectedSeqFiles = new ArrayList<>();
    selectedUnseqFiles = new ArrayList<>();
    maxSeqFileCost = 0;
    tempMaxSeqFileCost = 0;

    totalCost = 0;

    int unseqIndex = 0;
    long startTime = System.currentTimeMillis();
    long timeConsumption = 0;
    long timeLimit = IoTDBDescriptor.getInstance().getConfig().getMergeFileSelectionTimeBudget();
    if (timeLimit < 0) {
      timeLimit = Long.MAX_VALUE;
    }
    while (unseqIndex < resource.getUnseqFiles().size() && timeConsumption < timeLimit) {
      // select next unseq files
      TsFileResource unseqFile = resource.getUnseqFiles().get(unseqIndex);

      if (seqSelectedNum != resource.getSeqFiles().size()) {
        selectOverlappedSeqFiles(unseqFile);
      }
      boolean isClosed = checkClosedAndNotMerging(unseqFile);
      if (!isClosed) {
        tmpSelectedSeqFiles.clear();
        unseqIndex++;
        timeConsumption = System.currentTimeMillis() - startTime;
        continue;
      }

      tempMaxSeqFileCost = maxSeqFileCost;
      long newCost =
          useTightBound
              ? calculateTightMemoryCost(unseqFile, tmpSelectedSeqFiles, startTime, timeLimit)
              : calculateLooseMemoryCost(unseqFile, tmpSelectedSeqFiles, startTime, timeLimit);
      if (!updateSelectedFiles(newCost, unseqFile)) {
        // older unseq files must be merged before newer ones
        break;
      }

      tmpSelectedSeqFiles.clear();
      unseqIndex++;
      timeConsumption = System.currentTimeMillis() - startTime;
    }
    for (int i = 0; i < seqSelected.length; i++) {
      if (seqSelected[i]) {
        selectedSeqFiles.add(resource.getSeqFiles().get(i));
      }
    }
  }

  private boolean updateSelectedFiles(long newCost, TsFileResource unseqFile) {
    if (totalCost + newCost < memoryBudget) {
      selectedUnseqFiles.add(unseqFile);
      maxSeqFileCost = tempMaxSeqFileCost;

      for (Integer seqIdx : tmpSelectedSeqFiles) {
        seqSelected[seqIdx] = true;
        seqSelectedNum++;
      }
      totalCost += newCost;
      logger.debug(
          "Adding a new unseqFile {} and seqFiles {} as candidates, new cost {}, total"
              + " cost {}",
          unseqFile,
          tmpSelectedSeqFiles,
          newCost,
          totalCost);
      return true;
    }
    return false;
  }

  private boolean checkClosedAndNotMerging(TsFileResource unseqFile) {
    boolean isClosedAndNotMerging = unseqFile.isClosed() && !unseqFile.isMerging();
    if (!isClosedAndNotMerging) {
      return false;
    }
    for (Integer seqIdx : tmpSelectedSeqFiles) {
      if (!resource.getSeqFiles().get(seqIdx).isClosed()
          || resource.getSeqFiles().get(seqIdx).isMerging()) {
        isClosedAndNotMerging = false;
        break;
      }
    }
    return isClosedAndNotMerging;
  }

  private void selectOverlappedSeqFiles(TsFileResource unseqFile) {
    int tmpSelectedNum = 0;
    for (String deviceId : unseqFile.getDevices()) {
      long unseqStartTime = unseqFile.getStartTime(deviceId);
      long unseqEndTime = unseqFile.getEndTime(deviceId);

      boolean noMoreOverlap = false;
      for (int i = 0; i < resource.getSeqFiles().size() && !noMoreOverlap; i++) {
        TsFileResource seqFile = resource.getSeqFiles().get(i);
        if (seqSelected[i] || !seqFile.getDevices().contains(deviceId)) {
          continue;
        }
        // the open file's endTime is Long.MIN_VALUE, this will make the file be filtered below
        long seqEndTime = seqFile.isClosed() ? seqFile.getEndTime(deviceId) : Long.MAX_VALUE;
        if (unseqEndTime <= seqEndTime) {
          // the unseqFile overlaps current seqFile
          tmpSelectedSeqFiles.add(i);
          tmpSelectedNum++;
          // the device of the unseqFile can not merge with later seqFiles
          noMoreOverlap = true;
        } else if (unseqStartTime <= seqEndTime) {
          // the device of the unseqFile may merge with later seqFiles
          // and the unseqFile overlaps current seqFile
          tmpSelectedSeqFiles.add(i);
          tmpSelectedNum++;
        }
      }
      if (tmpSelectedNum + seqSelectedNum == resource.getSeqFiles().size()) {
        break;
      }
    }
  }

  private long calculateMemoryCost(
      TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFiles,
      IFileQueryMemMeasurement unseqMeasurement,
      IFileQueryMemMeasurement seqMeasurement,
      long startTime,
      long timeLimit)
      throws IOException {
    long cost = 0;
    Long fileCost = unseqMeasurement.measure(tmpSelectedUnseqFile);
    cost += fileCost;

    for (Integer seqFileIdx : tmpSelectedSeqFiles) {
      TsFileResource seqFile = resource.getSeqFiles().get(seqFileIdx);
      fileCost = seqMeasurement.measure(seqFile);
      if (fileCost > tempMaxSeqFileCost) {
        // only one file will be read at the same time, so only the largest one is recorded here
        cost -= tempMaxSeqFileCost;
        cost += fileCost;
        tempMaxSeqFileCost = fileCost;
      }
      // but writing data into a new file may generate the same amount of metadata in memory
      cost += calculateMetadataSize(seqFile);
      long timeConsumption = System.currentTimeMillis() - startTime;
      if (timeConsumption > timeLimit) {
        return Long.MAX_VALUE;
      }
    }
    return cost;
  }

  private long calculateLooseMemoryCost(
      TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFiles,
      long startTime,
      long timeLimit)
      throws IOException {
    return calculateMemoryCost(
        tmpSelectedUnseqFile,
        tmpSelectedSeqFiles,
        TsFileResource::getTsFileSize,
        this::calculateMetadataSize,
        startTime,
        timeLimit);
  }

  private long calculateTightMemoryCost(
      TsFileResource tmpSelectedUnseqFile,
      Collection<Integer> tmpSelectedSeqFiles,
      long startTime,
      long timeLimit)
      throws IOException {
    return calculateMemoryCost(
        tmpSelectedUnseqFile,
        tmpSelectedSeqFiles,
        this::calculateTightUnseqMemoryCost,
        this::calculateTightSeqMemoryCost,
        startTime,
        timeLimit);
  }

  private long calculateMetadataSize(TsFileResource seqFile) throws IOException {
    Long cost = fileMetaSizeMap.get(seqFile);
    if (cost == null) {
      cost = MergeUtils.getFileMetaSize(seqFile, resource.getFileReader(seqFile));
      fileMetaSizeMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  private long calculateTightFileMemoryCost(
      TsFileResource seqFile, IFileQueryMemMeasurement measurement) throws IOException {
    Long cost = maxSeriesQueryCostMap.get(seqFile);
    if (cost == null) {
      long[] chunkNums =
          MergeUtils.findTotalAndLargestSeriesChunkNum(seqFile, resource.getFileReader(seqFile));
      long totalChunkNum = chunkNums[0];
      long maxChunkNum = chunkNums[1];
      cost = measurement.measure(seqFile) * maxChunkNum / totalChunkNum;
      maxSeriesQueryCostMap.put(seqFile, cost);
      logger.debug(LOG_FILE_COST, seqFile, cost);
    }
    return cost;
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion to all series to get a maximum estimation
  private long calculateTightSeqMemoryCost(TsFileResource seqFile) throws IOException {
    long singleSeriesCost = calculateTightFileMemoryCost(seqFile, this::calculateMetadataSize);
    long multiSeriesCost = concurrentMergeNum * singleSeriesCost;
    long maxCost = calculateMetadataSize(seqFile);
    return Math.min(multiSeriesCost, maxCost);
  }

  // this method traverses all ChunkMetadata to find out which series has the most chunks and uses
  // its proportion among all series to get a maximum estimation
  private long calculateTightUnseqMemoryCost(TsFileResource unseqFile) throws IOException {
    long singleSeriesCost = calculateTightFileMemoryCost(unseqFile, TsFileResource::getTsFileSize);
    long multiSeriesCost = concurrentMergeNum * singleSeriesCost;
    long maxCost = unseqFile.getTsFileSize();
    return Math.min(multiSeriesCost, maxCost);
  }

  @Override
  public int getConcurrentMergeNum() {
    return concurrentMergeNum;
  }
}
