/**
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
package org.apache.iotdb.tsfile.read.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class MetadataQuerierByFileImpl implements MetadataQuerier {

  private static final int CHUNK_METADATA_CACHE_SIZE = 100000;

  private TsFileMetaData fileMetaData;

  private LRUCache<Path, List<ChunkMetaData>> chunkMetaDataCache;

  private TsFileSequenceReader tsFileReader;

  private LoadMode mode; // be the default mode (NoPartition) most of the time

  private long partitionStartOffset = 0L;
  private long partitionEndOffset = 0L;

  /**
   * Constructor of MetadataQuerierByFileImpl.
   */
  public MetadataQuerierByFileImpl(TsFileSequenceReader tsFileReader) throws IOException {
    this.tsFileReader = tsFileReader;
    this.fileMetaData = tsFileReader.readFileMetadata();
    // default mode
    this.mode = LoadMode.NoPartition;
    chunkMetaDataCache = new LRUCache<Path, List<ChunkMetaData>>(CHUNK_METADATA_CACHE_SIZE) {
      @Override
      public List<ChunkMetaData> loadObjectByKey(Path key) throws IOException {
        return loadChunkMetadata(key);
      }
    };
  }

  @Override
  public List<ChunkMetaData> getChunkMetaDataList(Path path) throws IOException {
    return chunkMetaDataCache.get(path);
  }

  @Override
  public Map<Path, List<ChunkMetaData>> getChunkMetaDataMap(List<Path> paths) throws IOException {
    Map<Path, List<ChunkMetaData>> chunkMetaDatas = new HashMap<>();
    for (Path path : paths) {
      if (!chunkMetaDatas.containsKey(path)) {
        chunkMetaDatas.put(path, new ArrayList<>());
      }
      chunkMetaDatas.get(path).addAll(getChunkMetaDataList(path));
    }
    return chunkMetaDatas;
  }

  @Override
  public TsFileMetaData getWholeFileMetadata() {
    return fileMetaData;
  }

  @Override
  public void loadChunkMetaDatas(List<Path> paths) throws IOException {

    // group measurements by device
    TreeMap<String, Set<String>> deviceMeasurementsMap = new TreeMap<>();
    for (Path path : paths) {
      if (!deviceMeasurementsMap.containsKey(path.getDevice())) {
        deviceMeasurementsMap.put(path.getDevice(), new HashSet<>());
      }
      deviceMeasurementsMap.get(path.getDevice()).add(path.getMeasurement());
    }

    Map<Path, List<ChunkMetaData>> tempChunkMetaDatas = new HashMap<>();

    int count = 0;
    boolean enough = false;

    // get all TsDeviceMetadataIndex by string order
    for (Map.Entry<String, Set<String>> deviceMeasurements : deviceMeasurementsMap.entrySet()) {

      if (enough) {
        break;
      }

      // d1
      String selectedDevice = deviceMeasurements.getKey();
      // s1, s2, s3
      Set<String> selectedMeasurements = deviceMeasurements.getValue();

      // get the index information of TsDeviceMetadata
      TsDeviceMetadataIndex index = fileMetaData.getDeviceMetadataIndex(selectedDevice);
      TsDeviceMetadata tsDeviceMetadata = tsFileReader.readTsDeviceMetaData(index);

      // d1
      for (ChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata
          .getChunkGroupMetaDataList()) {
        // better

        if (enough) {
          break;
        }

        // s1, s2
        for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {

          String currentMeasurement = chunkMetaData.getMeasurementUid();

          // s1
          if (selectedMeasurements.contains(currentMeasurement)) {

            // d1.s1
            Path path = new Path(selectedDevice, currentMeasurement);

            // add into tempChunkMetaDatas
            if (!tempChunkMetaDatas.containsKey(path)) {
              tempChunkMetaDatas.put(path, new ArrayList<>());
            }
            tempChunkMetaDatas.get(path).add(chunkMetaData);

            // check cache size, stop when reading enough
            count++;
            if (count == CHUNK_METADATA_CACHE_SIZE) {
              enough = true;
              break;
            }
          }
        }
      }
    }

    for (Map.Entry<Path, List<ChunkMetaData>> entry : tempChunkMetaDatas.entrySet()) {
      chunkMetaDataCache.put(entry.getKey(), entry.getValue());
    }

  }

  @Override
  public TSDataType getDataType(String measurement) throws NoMeasurementException {
    MeasurementSchema measurementSchema = fileMetaData.getMeasurementSchema().get(measurement);
    if (measurementSchema != null) {
      return measurementSchema.getType();
    }
    throw new NoMeasurementException(String.format("%s not found.", measurement));
  }

  /**
   * Return the chunk metadata of a given path under the control of the checkAccess function.
   *
   * @param path the given path
   * @return the accessible chunk metadata of a given path
   */
  private List<ChunkMetaData> loadChunkMetadata(Path path) throws IOException {

    if (!fileMetaData.containsDevice(path.getDevice())) {
      return new ArrayList<>();
    }

    // get the index information of TsDeviceMetadata
    TsDeviceMetadataIndex index = fileMetaData.getDeviceMetadataIndex(path.getDevice());

    // read TsDeviceMetadata from file
    TsDeviceMetadata tsDeviceMetadata = tsFileReader.readTsDeviceMetaData(index);

    // get all ChunkMetaData of this path included in all ChunkGroups of this device
    List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
    for (ChunkGroupMetaData chunkGroupMetaData : tsDeviceMetadata.getChunkGroupMetaDataList()) {
      if (checkAccess(chunkGroupMetaData)) {
        List<ChunkMetaData> chunkMetaDataListInOneChunkGroup = chunkGroupMetaData
            .getChunkMetaDataList();
        for (ChunkMetaData chunkMetaData : chunkMetaDataListInOneChunkGroup) {
          if (path.getMeasurement().equals(chunkMetaData.getMeasurementUid())) {
            chunkMetaData.setVersion(chunkGroupMetaData.getVersion());
            chunkMetaDataList.add(chunkMetaData);
          }
        }
      }
    }
    return chunkMetaDataList;
  }

  public ArrayList<TimeRange> getTimeRangeInOrPrev(List<Path> paths, LoadMode targetMode,
      long partitionStartOffset, long partitionEndOffset) throws IOException {
    if (targetMode == LoadMode.NoPartition) {
      throw new IllegalArgumentException(
          "The target mode should be either InPartition or PrevPartition");
    }
    // switch to the target mode temporarily to control loadChunkMetadata's checkAccess behavior
    this.mode = targetMode;
    this.partitionStartOffset = partitionStartOffset;
    this.partitionEndOffset = partitionEndOffset;

    ArrayList<TimeRange> unionCandidates = new ArrayList<>();
    for (Path path : paths) {
      //TODO 优先用cache 多个device只查一次
      List<ChunkMetaData> chunkMetaDataList = loadChunkMetadata(path);
      for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
        unionCandidates
            .add(new TimeRange(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()));
      }
    }
    Collections.sort(unionCandidates);

    // union the increasingly sorted candidates
    ArrayList<TimeRange> unionResult = new ArrayList<>(TimeRange.getUnions(unionCandidates));

    // restore the default mode. DO NOT REMOVE THIS LINE.
    this.mode = LoadMode.NoPartition;
    return unionResult;
  }

  /**
   * Check if the given chunk group can be accessed under the current load mode and possible
   * partition constraints. Used to control the behavior of the loadChunkMetaData function.
   *
   * @param chunkGroupMetaData a chunk group's metadata
   * @return True if the chunk group can be accessed under the current load mode. False otherwise.
   * @throws IOException illegal mode
   */
  private boolean checkAccess(ChunkGroupMetaData chunkGroupMetaData) throws IOException {
    if (mode == LoadMode.NoPartition) {
      // always true
      return true;
    }
    long startOffsetOfChunkGroup = chunkGroupMetaData.getStartOffsetOfChunkGroup();
    long endOffsetOfChunkGroup = chunkGroupMetaData.getEndOffsetOfChunkGroup();
    long middleOffsetOfChunkGroup = (startOffsetOfChunkGroup + endOffsetOfChunkGroup) / 2;
    switch (mode) {
      case InPartition:
        return (partitionStartOffset <= middleOffsetOfChunkGroup
            && middleOffsetOfChunkGroup < partitionEndOffset);
      case PrevPartition:
        return (middleOffsetOfChunkGroup < partitionStartOffset);
      default:
        throw new IOException(
            "Illegal mode! It should be one of {NoPartition, InPartition, PrevPartition}.");
    }
  }
}
