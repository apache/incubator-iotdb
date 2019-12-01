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
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class MaxTimeAggrFunc extends AggregateFunction {

  public MaxTimeAggrFunc() {
    super(TSDataType.INT64);
  }

  @Override
  public void init() {
    resultData.reset();
  }

  @Override
  public AggreResultData getResult() {
    return resultData;
  }

  @Override
  public void calculateValueFromPageHeader(PageHeader pageHeader) {
    long maxTimestamp = pageHeader.getEndTime();
    updateMaxTimeResult(0, maxTimestamp);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader) {

    int maxIndex = dataInThisPage.length() - 1;
    if (maxIndex < 0) {
      return;
    }
    long time = dataInThisPage.getTimeByIndex(maxIndex);
    updateMaxTimeResult(0, time);
  }

  @Override
  public void calculateValueFromPageData(BatchData dataInThisPage, IPointReader unsequenceReader,
      long bound) {
    long time = -1;
    while (dataInThisPage.hasNext()) {
      if (dataInThisPage.currentTime() < bound) {
        time = dataInThisPage.currentTime();
        dataInThisPage.next();
      } else {
        break;
      }
    }
    if (time != -1) {
      updateMaxTimeResult(0, time);
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader)
      throws IOException {
    TimeValuePair pair = null;
    while (unsequenceReader.hasNext()) {
      pair = unsequenceReader.next();
    }
    if (pair != null) {
      updateMaxTimeResult(0, pair.getTimestamp());
    }
  }

  @Override
  public void calculateValueFromUnsequenceReader(IPointReader unsequenceReader, long bound)
      throws IOException {
    TimeValuePair pair = null;
    while (unsequenceReader.hasNext() && unsequenceReader.current().getTimestamp() < bound) {
      pair = unsequenceReader.next();
    }
    if (pair != null) {
      updateMaxTimeResult(0, pair.getTimestamp());
    }
  }

  //TODO Consider how to reverse order in dataReader(IReaderByTimeStamp)
  @Override
  public void calcAggregationUsingTimestamps(long[] timestamps, int length,
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
    updateMaxTimeResult(0, time);
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  @Override
  public void calculateValueFromChunkData(ChunkMetaData chunkMetaData) {
    long maxTimestamp = chunkMetaData.getEndTime();
    updateMaxTimeResult(0, maxTimestamp);
  }

  private void updateMaxTimeResult(long time, long value) {
    if (!resultData.isSetValue() || value >= resultData.getLongRet()) {
      resultData.setTimestamp(time);
      resultData.setLongRet(value);
    }
  }
}
