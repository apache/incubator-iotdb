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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public interface IChunkMetadata {

  Statistics getStatistics();

  boolean isModified();

  void setModified(boolean modified);

  boolean isSeq();

  void setSeq(boolean seq);

  long getVersion();

  void setVersion(long version);

  long getOffsetOfChunkHeader();

  long getStartTime();

  long getEndTime();

  boolean isFromOldTsFile();

  IChunkLoader getChunkLoader();

  void setChunkLoader(IChunkLoader chunkLoader);

  void setFile(File file);

  void setClosed(boolean closed);

  TSDataType getDataType();

  String getMeasurementUid();

  void insertIntoSortedDeletions(long startTime, long endTime);

  List<TimeRange> getDeleteIntervalList();

  int serializeTo(OutputStream outputStream, boolean serializeStatistic) throws IOException;

  byte getMask();

  boolean isTimeColumn();

  boolean isValueColumn();
}
