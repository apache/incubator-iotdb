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

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * MaxSeriesMergeFileSelector is an extension of IMergeFileSelector which tries to maximize the
 * number of timeseries that can be merged at the same time.
 */
public class MaxSeriesMergeFileSelector extends MaxFileMergeFileSelector {

  public static final int MAX_SERIES_NUM = 1024;
  private static final Logger logger = LoggerFactory.getLogger(MaxSeriesMergeFileSelector.class);

  private List<TsFileResource> lastSelectedSeqFiles = Collections.emptyList();
  private List<TsFileResource> lastSelectedUnseqFiles = Collections.emptyList();
  private long lastTotalMemoryCost;

  public MaxSeriesMergeFileSelector(CrossSpaceMergeResource mergeResource, long memoryBudget) {
    super(mergeResource, memoryBudget);
  }

  @Override
  public List[] select() throws MergeException {
    long startTime = System.currentTimeMillis();
    try {
      logger.info(
          "Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(),
          resource.getUnseqFiles().size());

      searchMaxSeriesNum();
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
              + "concurrent merge num {}"
              + "time consumption {}ms",
          selectedSeqFiles.size(),
          selectedUnseqFiles.size(),
          totalCost,
          concurrentMergeNum,
          System.currentTimeMillis() - startTime);
    }
    return new List[] {selectedSeqFiles, selectedUnseqFiles};
  }

  private void searchMaxSeriesNum() throws IOException {
    binSearch();
  }

  private void binSearch() throws IOException {
    int lb = 0;
    int ub = MAX_SERIES_NUM + 1;
    while (true) {
      int mid = (lb + ub) / 2;
      if (mid == lb) {
        break;
      }
      concurrentMergeNum = mid;
      select(false);
      if (selectedUnseqFiles.isEmpty()) {
        select(true);
      }
      if (selectedUnseqFiles.isEmpty()) {
        ub = mid;
      } else {
        lastSelectedSeqFiles = selectedSeqFiles;
        lastSelectedUnseqFiles = selectedUnseqFiles;
        lastTotalMemoryCost = totalCost;
        lb = mid;
      }
    }
    selectedUnseqFiles = lastSelectedUnseqFiles;
    selectedSeqFiles = lastSelectedSeqFiles;
    concurrentMergeNum = lb;
    totalCost = lastTotalMemoryCost;
  }
}
