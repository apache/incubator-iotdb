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
package org.apache.iotdb.db.query.externalsort;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.query.reader.chunk.ChunkReaderWrap;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.IPointReader;


public interface ExternalSortJobEngine {

  /**
   * Receive a list of ChunkReaderWraps and judge whether it should be processed using external
   * sort. If needed, do the mergeUnseq sort for all ChunkReaderWraps using specific strategy.
   *
   * @param queryId query job id
   * @param chunkReaderWraps A list of ChunkReaderWrap
   */
  List<IPointReader> executeForIPointReader(long queryId, List<ChunkReaderWrap>
      chunkReaderWraps) throws IOException;

  /**
   * Receive a list of chunkReaderWraps and judge whether it should be processed using external
   * sort. If needed, do the mergeUnseq sort for all ChunkReaderWraps using specific strategy.
   *
   * @param chunkReaderWraps A list of ChunkReaderWrap
   */
  List<IReaderByTimestamp> executeForByTimestampReader(long queryId, List<ChunkReaderWrap>
      chunkReaderWraps) throws IOException;

  /**
   * Create an external sort job which merges many chunks.
   */
  ExternalSortJob createJob(long queryId, List<ChunkReaderWrap> timeValuePairReaderList)
      throws IOException;

}
