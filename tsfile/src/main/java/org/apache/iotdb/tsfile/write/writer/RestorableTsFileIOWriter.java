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

package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.exception.NotCompatibleTsFileException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileCheckStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.FSUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This writer is for opening and recover a TsFile
 *
 * <p>(1) If the TsFile is closed normally, hasCrashed()=false and canWrite()=false
 *
 * <p>(2) Otherwise, the writer generates metadata for already flushed Chunks and truncate crashed
 * data. The hasCrashed()=true and canWrite()=true
 *
 * <p>Notice!!! If you want to query this file through the generated metadata, remember to call the
 * makeMetadataVisible()
 */
public class RestorableTsFileIOWriter extends TsFileIOWriter {

  private static final Logger logger = LoggerFactory.getLogger("FileMonitor");
  private long truncatedSize = -1;
  private Map<Path, IMeasurementSchema> knownSchemas = new HashMap<>();

  private int lastFlushedChunkGroupIndex = 0;

  private boolean crashed;

  private long minPlanIndex = Long.MAX_VALUE;
  private long maxPlanIndex = Long.MIN_VALUE;

  /** all chunk group metadata which have been serialized on disk. */
  private Map<String, Map<String, List<ChunkMetadata>>> metadatasForQuery = new HashMap<>();

  /**
   * @param file a given tsfile path you want to (continue to) write
   * @throws IOException if write failed, or the file is broken but autoRepair==false.
   */
  public RestorableTsFileIOWriter(File file) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{} is opened.", file.getName());
    }
    this.file = file;
    this.out =
        FSFactoryProducer.getFileOutputFactory(FSUtils.getFSType(this.file))
            .getTsFileOutput(file.getPath(), true);

    // file doesn't exist
    if (file.length() == 0) {
      startFile();
      crashed = true;
      canWrite = true;
      return;
    }

    if (file.exists()) {
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file, false)) {

        truncatedSize = reader.selfCheck(knownSchemas, chunkGroupMetadataList, true);
        minPlanIndex = reader.getMinPlanIndex();
        maxPlanIndex = reader.getMaxPlanIndex();
        if (truncatedSize == TsFileCheckStatus.COMPLETE_FILE) {
          crashed = false;
          canWrite = false;
          out.close();
        } else if (truncatedSize == TsFileCheckStatus.INCOMPATIBLE_FILE) {
          out.close();
          throw new NotCompatibleTsFileException(
              String.format("%s is not in TsFile format.", file.getAbsolutePath()));
        } else {
          crashed = true;
          canWrite = true;
          // remove broken data
          out.truncate(truncatedSize);
        }
      }
    }
  }

  /**
   * Given a TsFile, generate a writable RestorableTsFileIOWriter. That is, for a complete TsFile,
   * the function erases all FileMetadata and supports writing new data; For a incomplete TsFile,
   * the function supports writing new data directly. However, it is more efficient using the
   * construction function of RestorableTsFileIOWriter, if the tsfile is incomplete.
   *
   * @param file a TsFile
   * @return a writable RestorableTsFileIOWriter
   */
  public static RestorableTsFileIOWriter getWriterForAppendingDataOnCompletedTsFile(File file)
      throws IOException {
    long position = file.length();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file, false)) {
      // this tsfile is complete
      if (reader.isComplete()) {
        reader.loadMetadataSize();
        position = reader.getFileMetadataPos();
      }
    }

    if (position != file.length()) {
      // if the file is complete, we will remove all file metadatas
      try (FileChannel channel =
          FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE)) {
        channel.truncate(position - 1); // remove the last marker.
      }
    }
    return new RestorableTsFileIOWriter(file);
  }

  long getTruncatedSize() {
    return truncatedSize;
  }

  public Map<Path, IMeasurementSchema> getKnownSchema() {
    return knownSchemas;
  }

  /**
   * For query.
   *
   * <p>get chunks' metadata from memory.
   *
   * @param deviceId the device id
   * @param measurementId the measurement id
   * @param dataType the value type
   * @return chunks' metadata
   */
  public List<ChunkMetadata> getVisibleMetadataList(
      String deviceId, String measurementId, TSDataType dataType) {
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
    if (metadatasForQuery.containsKey(deviceId)
        && metadatasForQuery.get(deviceId).containsKey(measurementId)) {
      for (IChunkMetadata chunkMetaData : metadatasForQuery.get(deviceId).get(measurementId)) {
        // filter: if a device'measurement is defined as float type, and data has been persistent.
        // Then someone deletes the timeseries and recreate it with Int type. We have to ignore
        // all the stale data.
        if (dataType == null || dataType.equals(chunkMetaData.getDataType())) {
          chunkMetadataList.add((ChunkMetadata) chunkMetaData);
        }
      }
    }
    return chunkMetadataList;
  }

  public Map<String, Map<String, List<ChunkMetadata>>> getMetadatasForQuery() {
    return metadatasForQuery;
  }

  /**
   * add all appendChunkMetadatas into memory. After calling this method, other classes can read
   * these metadata.
   */
  public void makeMetadataVisible() {
    List<ChunkGroupMetadata> newlyFlushedMetadataList = getAppendedRowMetadata();
    if (!newlyFlushedMetadataList.isEmpty()) {
      for (ChunkGroupMetadata chunkGroupMetadata : newlyFlushedMetadataList) {
        List<ChunkMetadata> rowMetaDataList = chunkGroupMetadata.getChunkMetadataList();

        String device = chunkGroupMetadata.getDevice();
        for (IChunkMetadata chunkMetaData : rowMetaDataList) {
          String measurementId = chunkMetaData.getMeasurementUid();
          if (!metadatasForQuery.containsKey(device)) {
            metadatasForQuery.put(device, new HashMap<>());
          }
          if (!metadatasForQuery.get(device).containsKey(measurementId)) {
            metadatasForQuery.get(device).put(measurementId, new ArrayList<>());
          }
          metadatasForQuery.get(device).get(measurementId).add((ChunkMetadata) chunkMetaData);
        }
      }
    }
  }

  public boolean hasCrashed() {
    return crashed;
  }

  /**
   * get all the chunk's metadata which are appended after the last calling of this method, or after
   * the class instance is initialized if this is the first time to call the method.
   *
   * @return a list of Device ChunkMetadataList Pair
   */
  private List<ChunkGroupMetadata> getAppendedRowMetadata() {
    List<ChunkGroupMetadata> append = new ArrayList<>();
    if (lastFlushedChunkGroupIndex < chunkGroupMetadataList.size()) {
      append.addAll(
          chunkGroupMetadataList.subList(
              lastFlushedChunkGroupIndex, chunkGroupMetadataList.size()));
      lastFlushedChunkGroupIndex = chunkGroupMetadataList.size();
    }
    return append;
  }

  public void addSchema(Path path, IMeasurementSchema schema) {
    knownSchemas.put(path, schema);
  }

  @Override
  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  @Override
  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }
}
