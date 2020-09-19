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

package org.apache.iotdb.db.query.aggregation.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.DescBatchData;

public class MaxTimeAggrResult extends AggregateResult {

  public MaxTimeAggrResult() {
    super(TSDataType.INT64, AggregationType.MAX_TIME);
    reset();
    this.ascending = false;
  }

  @Override
  public Long getResult() {
    return hasResult() ? getLongValue() : null;
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    if (hasResult()) {
      return;
    }
    long maxTimestamp = statistics.getEndTime();
    setLongValue(maxTimestamp);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage) {
    updateResultFromPageData(dataInThisPage, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound) {
    if (hasResult()) {
      return;
    }
    if (dataInThisPage instanceof DescBatchData && dataInThisPage.isFromMergeReader()) {
      setLongValue(dataInThisPage.getTimeByIndex(0));
    } else if (dataInThisPage instanceof DescBatchData) {
      if (dataInThisPage.hasCurrent()
          && dataInThisPage.currentTime() < maxBound
          && dataInThisPage.currentTime() >= minBound) {
        setLongValue(dataInThisPage.currentTime());
      }
    } else {
      while (dataInThisPage.hasCurrent()
          && dataInThisPage.currentTime() < maxBound
          && dataInThisPage.currentTime() >= minBound) {
        updateMaxTimeResult(dataInThisPage.currentTime());
        dataInThisPage.next();
      }
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    long time = -1;
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        time = timestamps[i];
      }
    }

    if (time == -1) {
      return;
    }
    updateMaxTimeResult(time);
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return hasResult();
  }

  @Override
  public void merge(AggregateResult another) {
    MaxTimeAggrResult anotherMaxTime = (MaxTimeAggrResult) another;
    if (anotherMaxTime.getResult() != null) {
      this.updateMaxTimeResult(anotherMaxTime.getResult());
    }
  }

  @Override
  protected void deserializeSpecificFields(ByteBuffer buffer) {

  }

  @Override
  protected void serializeSpecificFields(OutputStream outputStream) throws IOException {

  }

  private void updateMaxTimeResult(long value) {
    if (!hasResult() || value >= getLongValue()) {
      setLongValue(value);
    }
  }
}
