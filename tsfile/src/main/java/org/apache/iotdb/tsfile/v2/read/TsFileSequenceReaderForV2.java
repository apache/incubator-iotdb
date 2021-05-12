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
package org.apache.iotdb.tsfile.v2.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.v2.file.footer.ChunkGroupFooterV2;
import org.apache.iotdb.tsfile.v2.file.header.ChunkHeaderV2;
import org.apache.iotdb.tsfile.v2.file.header.PageHeaderV2;
import org.apache.iotdb.tsfile.v2.file.metadata.ChunkMetadataV2;
import org.apache.iotdb.tsfile.v2.file.metadata.MetadataIndexNodeV2;
import org.apache.iotdb.tsfile.v2.file.metadata.TimeseriesMetadataV2;
import org.apache.iotdb.tsfile.v2.file.metadata.TsFileMetadataV2;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TsFileSequenceReaderForV2 extends TsFileSequenceReader implements AutoCloseable {

  private List<Pair<Long, Long>> versionInfo;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
   *
   * @param file the data file
   * @throws IOException If some I/O error occurs
   */
  public TsFileSequenceReaderForV2(File file) throws IOException {
    this(file, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -whether load meta data size
   */
  public TsFileSequenceReaderForV2(File file, boolean loadMetadataSize) throws IOException {
    super(file, loadMetadataSize);
  }

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
   *
   * @param input given input
   */
  public TsFileSequenceReaderForV2(TsFileInput input) throws IOException {
    this(input, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input -given input
   * @param loadMetadataSize -load meta data size
   */
  public TsFileSequenceReaderForV2(TsFileInput input, boolean loadMetadataSize) throws IOException {
    super(input, loadMetadataSize);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input the input of a tsfile. The current position should be a markder and then a chunk
   *     Header, rather than the magic number
   * @param fileMetadataPos the position of the file metadata in the TsFileInput from the beginning
   *     of the input to the current position
   * @param fileMetadataSize the byte size of the file metadata in the input
   */
  public TsFileSequenceReaderForV2(TsFileInput input, long fileMetadataPos, int fileMetadataSize) {
    super(input, fileMetadataPos, fileMetadataSize);
    this.fileMetadataPos = fileMetadataPos;
    this.fileMetadataSize = fileMetadataSize;
  }

  /** whether the file is a complete TsFile: only if the head magic and tail magic string exists. */
  @Override
  public boolean isComplete() throws IOException {
    return tsFileInput.size()
            >= TSFileConfig.MAGIC_STRING.getBytes().length * 2
                + TSFileConfig.VERSION_NUMBER_V2.getBytes().length
        && (readTailMagic().equals(readHeadMagic()));
  }

  /** this function reads version number and checks compatibility of TsFile. */
  public String readVersionNumberV2() throws IOException {
    ByteBuffer versionNumberBytes =
        ByteBuffer.allocate(TSFileConfig.VERSION_NUMBER_V2.getBytes().length);
    tsFileInput.read(versionNumberBytes, TSFileConfig.MAGIC_STRING.getBytes().length);
    versionNumberBytes.flip();
    return new String(versionNumberBytes.array());
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  @Override
  public TsFileMetadata readFileMetadata() throws IOException {
    if (tsFileMetaData == null) {
      versionInfo = new ArrayList<>();
      tsFileMetaData =
          TsFileMetadataV2.deserializeFrom(
              readData(fileMetadataPos, fileMetadataSize), versionInfo);
    }
    return tsFileMetaData;
  }

  @Override
  public TimeseriesMetadata readTimeseriesMetadata(Path path, boolean ignoreNotExists)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetV2(
            deviceMetadataIndexNode, path.getDevice(), MetadataIndexNodeType.INTERNAL_DEVICE, true);
    if (metadataIndexPair == null) {
      if (ignoreNotExists) {
        return null;
      }
      return null;
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      metadataIndexNode = MetadataIndexNodeV2.deserializeFrom(buffer);
      metadataIndexPair =
          getMetadataAndEndOffsetV2(
              metadataIndexNode,
              path.getMeasurement(),
              MetadataIndexNodeType.INTERNAL_MEASUREMENT,
              false);
    }
    if (metadataIndexPair == null) {
      return null;
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetadata timeseriesMetadata = TimeseriesMetadataV2.deserializeFrom(buffer);
      ArrayList<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetadata);
      timeseriesMetadata.setChunkMetadataList(chunkMetadataList);
      timeseriesMetadataList.add(timeseriesMetadata);
    }
    // return null if path does not exist in the TsFile
    int searchResult =
        binarySearchInTimeseriesMetadataList(timeseriesMetadataList, path.getMeasurement());
    return searchResult >= 0 ? timeseriesMetadataList.get(searchResult) : null;
  }

  /*Find the leaf node that contains path, return all the sensors in that leaf node which are also
  in allSensors set */
  @SuppressWarnings("squid:S3776")
  @Override
  public List<TimeseriesMetadata> readTimeseriesMetadata(Path path, Set<String> allSensors)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, path.getDevice(), true, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      metadataIndexNode = MetadataIndexNodeV2.deserializeFrom(buffer);
      metadataIndexPair =
          getMetadataAndEndOffset(metadataIndexNode, path.getMeasurement(), false, false);
    }
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetadata timeseriesMetadata;
      timeseriesMetadata = TimeseriesMetadataV2.deserializeFrom(buffer);
      ArrayList<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetadata);
      timeseriesMetadata.setChunkMetadataList(chunkMetadataList);
      if (allSensors.contains(timeseriesMetadata.getMeasurementId())) {
        timeseriesMetadataList.add(timeseriesMetadata);
      }
    }
    return timeseriesMetadataList;
  }

  @SuppressWarnings("squid:S3776")
  @Override
  public List<TimeseriesMetadata> readTimeseriesMetadata(String device, Set<String> measurements)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetV2(
            deviceMetadataIndexNode, device, MetadataIndexNodeType.INTERNAL_DEVICE, false);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    List<TimeseriesMetadata> resultTimeseriesMetadataList = new ArrayList<>();
    List<String> measurementList = new ArrayList<>(measurements);
    Set<String> measurementsHadFound = new HashSet<>();
    for (int i = 0; i < measurementList.size(); i++) {
      if (measurementsHadFound.contains(measurementList.get(i))) {
        continue;
      }
      ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
      Pair<MetadataIndexEntry, Long> measurementMetadataIndexPair = metadataIndexPair;
      List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
      MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
      if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        metadataIndexNode = MetadataIndexNodeV2.deserializeFrom(buffer);
        measurementMetadataIndexPair =
            getMetadataAndEndOffsetV2(
                metadataIndexNode,
                measurementList.get(i),
                MetadataIndexNodeType.INTERNAL_MEASUREMENT,
                false);
      }
      if (measurementMetadataIndexPair == null) {
        return Collections.emptyList();
      }
      buffer =
          readData(
              measurementMetadataIndexPair.left.getOffset(), measurementMetadataIndexPair.right);
      while (buffer.hasRemaining()) {
        TimeseriesMetadata timeseriesMetadata = TimeseriesMetadataV2.deserializeFrom(buffer);
        ArrayList<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetadata);
        timeseriesMetadata.setChunkMetadataList(chunkMetadataList);
        timeseriesMetadataList.add(timeseriesMetadata);
      }
      for (int j = i; j < measurementList.size(); j++) {
        String current = measurementList.get(j);
        if (!measurementsHadFound.contains(current)) {
          int searchResult = binarySearchInTimeseriesMetadataList(timeseriesMetadataList, current);
          if (searchResult >= 0) {
            resultTimeseriesMetadataList.add(timeseriesMetadataList.get(searchResult));
            measurementsHadFound.add(current);
          }
        }
        if (measurementsHadFound.size() == measurements.size()) {
          return resultTimeseriesMetadataList;
        }
      }
    }
    return resultTimeseriesMetadataList;
  }

  @Override
  public List<String> getAllDevices() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    return getAllDevicesV2(tsFileMetaData.getMetadataIndex());
  }

  private List<String> getAllDevicesV2(MetadataIndexNode metadataIndexNode) throws IOException {
    List<String> deviceList = new ArrayList<>();
    int metadataIndexListSize = metadataIndexNode.getChildren().size();
    if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.INTERNAL_MEASUREMENT)) {
      for (MetadataIndexEntry index : metadataIndexNode.getChildren()) {
        deviceList.add(index.getName());
      }
    } else {
      for (int i = 0; i < metadataIndexListSize; i++) {
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexListSize - 1) {
          endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
        }
        ByteBuffer buffer = readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
        MetadataIndexNode node = MetadataIndexNodeV2.deserializeFrom(buffer);
        if (node.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
          // if node in next level is LEAF_DEVICE, put all devices in node entry into the set
          deviceList.addAll(
              node.getChildren().stream()
                  .map(MetadataIndexEntry::getName)
                  .collect(Collectors.toList()));
        } else {
          // keep traversing
          deviceList.addAll(getAllDevicesV2(node));
        }
      }
    }
    return deviceList;
  }

  /**
   * read all ChunkMetaDatas of given device
   *
   * @param device name
   * @return measurement -> ChunkMetadata list
   * @throws IOException io error
   */
  @Override
  public Map<String, List<ChunkMetadata>> readChunkMetadataInDevice(String device)
      throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }

    long start = 0;
    int size = 0;
    List<TimeseriesMetadata> timeseriesMetadataMap = getDeviceTimeseriesMetadataV2(device);
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap) {
      if (start == 0) {
        start = timeseriesMetadata.getOffsetOfChunkMetaDataList();
      }
      size += timeseriesMetadata.getDataSizeOfChunkMetaDataList();
    }
    // read buffer of all ChunkMetadatas of this device
    ByteBuffer buffer = readData(start, size);
    Map<String, List<ChunkMetadata>> seriesMetadata = new HashMap<>();
    while (buffer.hasRemaining()) {
      ChunkMetadata chunkMetadata = ChunkMetadataV2.deserializeFrom(buffer);
      seriesMetadata
          .computeIfAbsent(chunkMetadata.getMeasurementUid(), key -> new ArrayList<>())
          .add(chunkMetadata);
    }
    // set version in ChunkMetadata
    for (Entry<String, List<ChunkMetadata>> entry : seriesMetadata.entrySet()) {
      applyVersion(entry.getValue());
    }
    return seriesMetadata;
  }

  /**
   * Traverse the metadata index from MetadataIndexEntry to get TimeseriesMetadatas
   *
   * @param metadataIndex MetadataIndexEntry
   * @param buffer byte buffer
   * @param deviceId String
   * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
   */
  private void generateMetadataIndexV2(
      MetadataIndexEntry metadataIndex,
      ByteBuffer buffer,
      String deviceId,
      MetadataIndexNodeType type,
      Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap)
      throws IOException {
    switch (type) {
      case INTERNAL_DEVICE:
      case LEAF_DEVICE:
      case INTERNAL_MEASUREMENT:
        deviceId = metadataIndex.getName();
        MetadataIndexNode metadataIndexNode = MetadataIndexNodeV2.deserializeFrom(buffer);
        int metadataIndexListSize = metadataIndexNode.getChildren().size();
        for (int i = 0; i < metadataIndexListSize; i++) {
          long endOffset = metadataIndexNode.getEndOffset();
          if (i != metadataIndexListSize - 1) {
            endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
          }
          ByteBuffer nextBuffer =
              readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
          generateMetadataIndexV2(
              metadataIndexNode.getChildren().get(i),
              nextBuffer,
              deviceId,
              metadataIndexNode.getNodeType(),
              timeseriesMetadataMap);
        }
        break;
      case LEAF_MEASUREMENT:
        List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
        while (buffer.hasRemaining()) {
          TimeseriesMetadata timeseriesMetadata = TimeseriesMetadataV2.deserializeFrom(buffer);
          ArrayList<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetadata);
          timeseriesMetadata.setChunkMetadataList(chunkMetadataList);
          timeseriesMetadataList.add(timeseriesMetadata);
        }
        timeseriesMetadataMap
            .computeIfAbsent(deviceId, k -> new ArrayList<>())
            .addAll(timeseriesMetadataList);
        break;
    }
  }

  @Override
  public Map<String, List<TimeseriesMetadata>> getAllTimeseriesMetadata() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new HashMap<>();
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    List<MetadataIndexEntry> metadataIndexEntryList = metadataIndexNode.getChildren();
    for (int i = 0; i < metadataIndexEntryList.size(); i++) {
      MetadataIndexEntry metadataIndexEntry = metadataIndexEntryList.get(i);
      long endOffset = tsFileMetaData.getMetadataIndex().getEndOffset();
      if (i != metadataIndexEntryList.size() - 1) {
        endOffset = metadataIndexEntryList.get(i + 1).getOffset();
      }
      ByteBuffer buffer = readData(metadataIndexEntry.getOffset(), endOffset);
      generateMetadataIndexV2(
          metadataIndexEntry, buffer, null, metadataIndexNode.getNodeType(), timeseriesMetadataMap);
    }
    return timeseriesMetadataMap;
  }

  private List<TimeseriesMetadata> getDeviceTimeseriesMetadataV2(String device) throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffsetV2(
            metadataIndexNode, device, MetadataIndexNodeType.INTERNAL_DEVICE, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndexV2(
        metadataIndexPair.left,
        buffer,
        device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT,
        timeseriesMetadataMap);
    List<TimeseriesMetadata> deviceTimeseriesMetadata = new ArrayList<>();
    for (List<TimeseriesMetadata> timeseriesMetadataList : timeseriesMetadataMap.values()) {
      deviceTimeseriesMetadata.addAll(timeseriesMetadataList);
    }
    return deviceTimeseriesMetadata;
  }

  /**
   * Get target MetadataIndexEntry and its end offset
   *
   * @param metadataIndex given MetadataIndexNode
   * @param name target device / measurement name
   * @param type target MetadataIndexNodeType, either INTERNAL_DEVICE or INTERNAL_MEASUREMENT. When
   *     searching for a device node, return when it is not INTERNAL_DEVICE. Likewise, when
   *     searching for a measurement node, return when it is not INTERNAL_MEASUREMENT. This works
   *     for the situation when the index tree does NOT have the device level and ONLY has the
   *     measurement level.
   * @param exactSearch if is in exact search mode, return null when there is no entry with name; or
   *     else return the nearest MetadataIndexEntry before it (for deeper search)
   * @return target MetadataIndexEntry, endOffset pair
   */
  private Pair<MetadataIndexEntry, Long> getMetadataAndEndOffsetV2(
      MetadataIndexNode metadataIndex, String name, MetadataIndexNodeType type, boolean exactSearch)
      throws IOException {
    if (!metadataIndex.getNodeType().equals(type)) {
      return metadataIndex.getChildIndexEntry(name, exactSearch);
    } else {
      Pair<MetadataIndexEntry, Long> childIndexEntry =
          metadataIndex.getChildIndexEntry(name, false);
      ByteBuffer buffer = readData(childIndexEntry.left.getOffset(), childIndexEntry.right);
      return getMetadataAndEndOffsetV2(
          MetadataIndexNodeV2.deserializeFrom(buffer), name, type, false);
    }
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupHeader readChunkGroupFooter() throws IOException {
    return ChunkGroupFooterV2.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader() throws IOException {
    return ChunkHeaderV2.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read the chunk's header.
   *
   * @param position the file offset of this chunk's header
   * @param chunkHeaderSize the size of chunk's header
   * @param markerRead true if the offset does not contains the marker , otherwise false
   */
  private ChunkHeader readChunkHeader(long position, int chunkHeaderSize, boolean markerRead)
      throws IOException {
    return ChunkHeaderV2.deserializeFrom(tsFileInput, position, chunkHeaderSize, markerRead);
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param dataSize the size of chunkdata
   * @param position the offset of the chunk data
   * @return the pages of this chunk
   */
  private ByteBuffer readChunkV2(long position, int dataSize) throws IOException {
    return readData(position, dataSize);
  }

  /**
   * read memory chunk.
   *
   * @param metaData -given chunk meta data
   * @return -chunk
   */
  @Override
  public Chunk readMemChunk(ChunkMetadata metaData) throws IOException {
    int chunkHeadSize = ChunkHeaderV2.getSerializedSize(metaData.getMeasurementUid());
    ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), chunkHeadSize, false);
    ByteBuffer buffer =
        readChunkV2(
            metaData.getOffsetOfChunkHeader() + header.getSerializedSize(), header.getDataSize());
    Chunk chunk =
        new Chunk(header, buffer, metaData.getDeleteIntervalList(), metaData.getStatistics());
    chunk.setFromOldFile(true);
    return chunk;
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type) throws IOException {
    return PageHeaderV2.deserializeFrom(tsFileInput.wrapAsInputStream(), type);
  }

  public ByteBuffer readCompressedPage(PageHeader header) throws IOException {
    return readData(-1, header.getCompressedSize());
  }

  public long readVersion() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    buffer.flip();
    return buffer.getLong();
  }

  /**
   * get ChunkMetaDatas in given TimeseriesMetaData
   *
   * @return List of ChunkMetaData
   */
  public ArrayList<ChunkMetadata> readChunkMetaDataList(TimeseriesMetadata timeseriesMetaData)
      throws IOException {
    readFileMetadata();
    ArrayList<ChunkMetadata> chunkMetadataList = new ArrayList<>();
    long startOffsetOfChunkMetadataList = timeseriesMetaData.getOffsetOfChunkMetaDataList();
    int dataSizeOfChunkMetadataList = timeseriesMetaData.getDataSizeOfChunkMetaDataList();

    ByteBuffer buffer = readData(startOffsetOfChunkMetadataList, dataSizeOfChunkMetadataList);
    while (buffer.hasRemaining()) {
      chunkMetadataList.add(ChunkMetadataV2.deserializeFrom(buffer));
    }

    // minimize the storage of an ArrayList instance.
    chunkMetadataList.trimToSize();
    applyVersion(chunkMetadataList);
    return chunkMetadataList;
  }

  private void applyVersion(List<ChunkMetadata> chunkMetadataList) {
    if (versionInfo == null || versionInfo.isEmpty()) {
      return;
    }
    int versionIndex = 0;
    for (ChunkMetadata chunkMetadata : chunkMetadataList) {

      while (chunkMetadata.getOffsetOfChunkHeader() >= versionInfo.get(versionIndex).left) {
        versionIndex++;
      }

      chunkMetadata.setVersion(versionInfo.get(versionIndex).right);
    }
  }
}
