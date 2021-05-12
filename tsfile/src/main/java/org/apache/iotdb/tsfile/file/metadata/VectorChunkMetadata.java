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
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class VectorChunkMetadata implements IChunkMetadata {

  private final IChunkMetadata timeChunkMetadata;
  private final List<IChunkMetadata> valueChunkMetadataList;

  public VectorChunkMetadata(
      IChunkMetadata timeChunkMetadata, List<IChunkMetadata> valueChunkMetadataList) {
    this.timeChunkMetadata = timeChunkMetadata;
    this.valueChunkMetadataList = valueChunkMetadataList;
  }

  @Override
  public Statistics getStatistics() {
    return valueChunkMetadataList.size() == 1
        ? valueChunkMetadataList.get(0).getStatistics()
        : timeChunkMetadata.getStatistics();
  }

  @Override
  public boolean isModified() {
    return timeChunkMetadata.isModified();
  }

  @Override
  public void setModified(boolean modified) {
    timeChunkMetadata.setModified(modified);
  }

  @Override
  public boolean isSeq() {
    return timeChunkMetadata.isSeq();
  }

  @Override
  public void setSeq(boolean seq) {
    timeChunkMetadata.setSeq(seq);
  }

  @Override
  public long getVersion() {
    return timeChunkMetadata.getVersion();
  }

  @Override
  public void setVersion(long version) {
    timeChunkMetadata.setVersion(version);
  }

  @Override
  public long getOffsetOfChunkHeader() {
    return timeChunkMetadata.getOffsetOfChunkHeader();
  }

  @Override
  public long getStartTime() {
    return timeChunkMetadata.getStartTime();
  }

  @Override
  public long getEndTime() {
    return timeChunkMetadata.getEndTime();
  }

  @Override
  public boolean isFromOldTsFile() {
    return false;
  }

  @Override
  public IChunkLoader getChunkLoader() {
    return timeChunkMetadata.getChunkLoader();
  }

  @Override
  public void setChunkLoader(IChunkLoader chunkLoader) {
    timeChunkMetadata.setChunkLoader(chunkLoader);
    for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
      chunkMetadata.setChunkLoader(chunkLoader);
    }
  }

  @Override
  public void setFile(File file) {
    timeChunkMetadata.setFile(file);
    for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
      chunkMetadata.setFile(file);
    }
  }

  @Override
  public void setClosed(boolean closed) {
    timeChunkMetadata.setClosed(closed);
    for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
      chunkMetadata.setClosed(closed);
    }
  }

  @Override
  public TSDataType getDataType() {
    return timeChunkMetadata.getDataType();
  }

  @Override
  public String getMeasurementUid() {
    return timeChunkMetadata.getMeasurementUid();
  }

  @Override
  public void insertIntoSortedDeletions(long startTime, long endTime) {
    timeChunkMetadata.insertIntoSortedDeletions(startTime, endTime);
  }

  @Override
  public List<TimeRange> getDeleteIntervalList() {
    return timeChunkMetadata.getDeleteIntervalList();
  }

  @Override
  public int serializeTo(OutputStream outputStream, boolean serializeStatistic) {
    throw new UnsupportedOperationException("VectorChunkMetadata doesn't support serial method");
  }

  @Override
  public byte getMask() {
    return 0;
  }

  @Override
  public boolean isTimeColumn() {
    return false;
  }

  @Override
  public boolean isValueColumn() {
    return false;
  }

  public Chunk getTimeChunk() throws IOException {
    return timeChunkMetadata.getChunkLoader().loadChunk((ChunkMetadata) timeChunkMetadata);
  }

  public List<Chunk> getValueChunkList() throws IOException {
    List<Chunk> valueChunkList = new ArrayList<>();
    for (IChunkMetadata chunkMetadata : valueChunkMetadataList) {
      valueChunkList.add(chunkMetadata.getChunkLoader().loadChunk((ChunkMetadata) chunkMetadata));
    }
    return valueChunkList;
  }

  public IChunkMetadata getTimeChunkMetadata() {
    return timeChunkMetadata;
  }

  public List<IChunkMetadata> getValueChunkMetadataList() {
    return valueChunkMetadataList;
  }
}
