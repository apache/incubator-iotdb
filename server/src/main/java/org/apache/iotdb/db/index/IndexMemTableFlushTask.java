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
package org.apache.iotdb.db.index;

import org.apache.iotdb.db.index.common.IndexProcessorStruct;
import org.apache.iotdb.db.index.router.IIndexRouter;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;

/**
 * IndexMemTableFlushTask is responsible for the index insertion when a TsFileProcessor flushes.
 * IoTDB creates a MemTableFlushTask when a memtable is flushed to disk. After MemTableFlushTask
 * sorts a series, it will be passed to a IndexMemTableFlushTask. The IndexMemTableFlushTask will
 * detect whether one or more indexes have been created on this series, and pass its data to
 * corresponding IndexProcessors and insert it into the corresponding indexes.
 *
 * <p>IndexMemTableFlushTask may cover more than one index processor.
 */
public class IndexMemTableFlushTask {

  private final IIndexRouter router;
  private final boolean sequence;

  /** it should be immutable. */
  IndexMemTableFlushTask(IIndexRouter router, boolean sequence) {
    // check all processors
    this.router = router;
    this.sequence = sequence;
    // in current version, we don't build index for unsequence block
    if (sequence) {
      for (IndexProcessorStruct p : router.getAllIndexProcessorsAndInfo()) {
        p.processor.startFlushMemTable();
      }
    }
  }

  /**
   * insert sorted time series.
   *
   * @param path the path of time series
   * @param tvList the sorted data
   */
  public void buildIndexForOneSeries(PartialPath path, TVList tvList) {
    // in current version, we don't build index for unsequence block, but only update the index
    // usability range.
    if (sequence) {
      router.getIndexProcessorByPath(path).forEach(p -> p.buildIndexForOneSeries(path, tvList));
    } else {
      router.getIndexProcessorByPath(path).forEach(p -> p.updateUnsequenceData(path, tvList));
    }
  }

  /** wait for all IndexProcessors to finish building indexes. */
  public void endFlush() {
    if (sequence) {
      router.getAllIndexProcessorsAndInfo().forEach(p -> p.processor.endFlushMemTable());
    }
  }
}
