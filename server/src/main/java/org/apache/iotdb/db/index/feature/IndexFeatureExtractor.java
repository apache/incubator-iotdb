/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.feature;

import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.read.common.BatchData;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.iotdb.db.index.common.IndexConstant.NON_IMPLEMENTED_MSG;

/**
 * For all indexes, the raw input sequence has to be pre-processed before it's organized by indexes.
 * In general, index structure needn't maintain all of original data, but only pointers (e.g. the
 * identifier [start_time, end_time, series_path] can identify a time sequence uniquely).
 *
 * <p>By and large, similarity index supposes the input data are ideal: fixed dimension, equal
 * interval, not missing values and even not outliers. However, in real scenario, the input series
 * may contain missing values (thus it's not dimension-fixed) and the point's timestamp may contain
 * slight offset (thus they are not equal-interval). IndexFeatureExtractor need to preprocess the
 * series and obtain clean series to insert.
 *
 * <p>Many indexes will further extract features of alignment sequences, such as PAA, SAX, FFT, etc.
 *
 * <p>In summary, the IndexFeatureExtractor can provide three-level information:
 *
 * <ul>
 *   <li>L1: a triplet to identify a series: {@code {StartTime, EndTime, Length}} (not submitted in
 *       this pr)
 *   <li>L2: aligned sequence: {@code {a1, a2, ..., an}}
 *   <li>L3: feature: {@code {C1, C2, ..., Cm}}
 * </ul>
 */
public abstract class IndexFeatureExtractor {

  /** In the BUILD and QUERY modes, the IndexFeatureExtractor may work differently. */
  protected boolean inQueryMode;

  IndexFeatureExtractor(boolean inQueryMode) {
    this.inQueryMode = inQueryMode;
  }

  /**
   * Input a list of new data into the FeatureProcessor.
   *
   * @param newData new coming data.
   */
  public abstract void appendNewSrcData(TVList newData);

  /**
   * Input a list of new data into the FeatureProcessor. Due to the exist codes, IoTDB generate
   * TVList in the insert phase, but obtain BatchData in the query phase.
   *
   * @param newData new coming data.
   */
  public abstract void appendNewSrcData(BatchData newData);

  /** Having done {@code appendNewSrcData}, the index framework will check {@code hasNext}. */
  public abstract boolean hasNext();

  /**
   * If {@code hasNext} returns true, the index framework will call {@code processNext} which
   * prepares a new series item (ready for insert).
   */
  public abstract void processNext();

  /**
   * After processing a batch of data, the index framework may call {@code clearProcessedSrcData} to
   * clean out the processed data for releasing memory.
   */
  public abstract void clearProcessedSrcData();

  /**
   * close this extractor and release all allocated resources (TVList and PrimitiveList). It also
   * returns the data saved for next open.
   *
   * @return the data saved for next open.
   */
  public abstract ByteBuffer closeAndRelease() throws IOException;

  /** @return current L2: aligned sequence. */
  public Object getCurrent_L2_AlignedSequence() {
    throw new UnsupportedOperationException(NON_IMPLEMENTED_MSG);
  }

  /** @return current L3: costumed feature. */
  public Object getCurrent_L3_Feature() {
    throw new UnsupportedOperationException(NON_IMPLEMENTED_MSG);
  }
}
