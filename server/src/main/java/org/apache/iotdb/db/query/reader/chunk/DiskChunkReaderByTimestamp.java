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
package org.apache.iotdb.db.query.reader.chunk;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.TimeColumn;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * To read chunk data on disk by timestamp, this class implements an interface {@link
 * IReaderByTimestamp} based on the data reader {@link ChunkReaderByTimestamp}.
 * <p>
 */
public class DiskChunkReaderByTimestamp implements IReaderByTimestamp {

  private ChunkReaderByTimestamp chunkReaderByTimestamp;
  private BatchData data;
  private long currentTime = Long.MIN_VALUE;

  public DiskChunkReaderByTimestamp(ChunkReaderByTimestamp chunkReaderByTimestamp) {
    this.chunkReaderByTimestamp = chunkReaderByTimestamp;
  }

  @Override
  public Object[] getValuesInTimestamps(TimeColumn timestamps, long bound) throws IOException {
    int position = timestamps.position();
    int size = 0;
    while (timestamps.hasCurrent()) {
      if (timestamps.currentTime() >= bound) {
        size = timestamps.position() - position;
        break;
      }
      timestamps.next();
    }
    timestamps.position(position);
    Object[] result = new Object[size];

    for (int i = 0; i < timestamps.size(); i++) {
      if (timestamps.currentTime() < currentTime) {
        throw new IOException("time must be increasing when use ReaderByTimestamp");
      }
      if (timestamps.currentTime() >= bound) {
        return result;
      }
      currentTime = timestamps.currentTime();
      timestamps.next();
      while (hasNext()) {
        data = next();
        if (data.getMaxTimestamp() > currentTime) {
          result[i] = null;
          break;
        }
        result[i] = data.getValueInTimestamp(currentTime);
        //fill cache
        if (!data.hasCurrent() && chunkReaderByTimestamp.hasNextSatisfiedPage()) {
          data = next();
        }
      }
    }
    return result;
  }

  private boolean hasCacheData() {
    return data != null && data.hasCurrent();
  }

  private boolean hasNext() {
    return hasCacheData() || chunkReaderByTimestamp.hasNextSatisfiedPage();
  }

  private BatchData next() throws IOException {
    if (hasCacheData()) {
      return data;
    }
    return chunkReaderByTimestamp.nextPageData();
  }
}