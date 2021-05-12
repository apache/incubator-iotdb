/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.FSUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class TsFileSequenceReader implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileSequenceReader.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final String METADATA_INDEX_NODE_DESERIALIZE_ERROR =
      "Something error happened while deserializing MetadataIndexNode of file {}";
  protected File file;
  protected TsFileInput tsFileInput;
  protected long fileMetadataPos;
  protected int fileMetadataSize;
  private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);
  protected TsFileMetadata tsFileMetaData;
  // device -> measurement -> TimeseriesMetadata
  private Map<String, Map<String, TimeseriesMetadata>> cachedDeviceMetadata =
      new ConcurrentHashMap<>();
  private static final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
  private boolean cacheDeviceMetadata;
  private long minPlanIndex = Long.MAX_VALUE;
  private long maxPlanIndex = Long.MIN_VALUE;

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
   *
   * @param file the data file
   * @throws IOException If some I/O error occurs
   */
  public TsFileSequenceReader(File file) throws IOException {
    this(file, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param file -given file name
   * @param loadMetadataSize -whether load meta data size
   */
  public TsFileSequenceReader(File file, boolean loadMetadataSize) throws IOException {
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} reader is opened. {}", file, getClass().getName());
    }
    this.file = file;
    tsFileInput =
        FSFactoryProducer.getFileInputFactory(FSUtils.getFSType(file))
            .getTsFileInput(file.getAbsolutePath());
    try {
      if (loadMetadataSize) {
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  // used in merge resource
  public TsFileSequenceReader(File file, boolean loadMetadata, boolean cacheDeviceMetadata)
      throws IOException {
    this(file, loadMetadata);
    this.cacheDeviceMetadata = cacheDeviceMetadata;
  }

  /**
   * Create a file reader of the given file. The reader will read the tail of the file to get the
   * file metadata size.Then the reader will skip the first
   * TSFileConfig.MAGIC_STRING.getBytes().length + TSFileConfig.NUMBER_VERSION.getBytes().length
   * bytes of the file for preparing reading real data.
   *
   * @param input given input
   */
  public TsFileSequenceReader(TsFileInput input) throws IOException {
    this(input, true);
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input -given input
   * @param loadMetadataSize -load meta data size
   */
  public TsFileSequenceReader(TsFileInput input, boolean loadMetadataSize) throws IOException {
    this.tsFileInput = input;
    try {
      if (loadMetadataSize) { // NOTE no autoRepair here
        loadMetadataSize();
      }
    } catch (Throwable e) {
      tsFileInput.close();
      throw e;
    }
  }

  /**
   * construct function for TsFileSequenceReader.
   *
   * @param input the input of a tsfile. The current position should be a marker and then a chunk
   *     Header, rather than the magic number
   * @param fileMetadataPos the position of the file metadata in the TsFileInput from the beginning
   *     of the input to the current position
   * @param fileMetadataSize the byte size of the file metadata in the input
   */
  public TsFileSequenceReader(TsFileInput input, long fileMetadataPos, int fileMetadataSize) {
    this.tsFileInput = input;
    this.fileMetadataPos = fileMetadataPos;
    this.fileMetadataSize = fileMetadataSize;
  }

  public void loadMetadataSize() throws IOException {
    ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
    if (readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      tsFileInput.read(
          metadataSize,
          tsFileInput.size() - TSFileConfig.MAGIC_STRING.getBytes().length - Integer.BYTES);
      metadataSize.flip();
      // read file metadata size and position
      fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
      fileMetadataPos =
          tsFileInput.size()
              - TSFileConfig.MAGIC_STRING.getBytes().length
              - Integer.BYTES
              - fileMetadataSize;
    }
  }

  public long getFileMetadataPos() {
    return fileMetadataPos;
  }

  public int getFileMetadataSize() {
    return fileMetadataSize;
  }

  /** this function does not modify the position of the file reader. */
  public String readTailMagic() throws IOException {
    long totalSize = tsFileInput.size();
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.getBytes().length);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /** whether the file is a complete TsFile: only if the head magic and tail magic string exists. */
  public boolean isComplete() throws IOException {
    long size = tsFileInput.size();
    // TSFileConfig.MAGIC_STRING.getBytes().length * 2 for two magic string
    // Byte.BYTES for the file version number
    if (size >= TSFileConfig.MAGIC_STRING.getBytes().length * 2 + Byte.BYTES) {
      String tailMagic = readTailMagic();
      String headMagic = readHeadMagic();
      return tailMagic.equals(headMagic);
    } else {
      return false;
    }
  }

  /** this function does not modify the position of the file reader. */
  public String readHeadMagic() throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.getBytes().length);
    tsFileInput.read(magicStringBytes, 0);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  /** this function reads version number and checks compatibility of TsFile. */
  public byte readVersionNumber() throws IOException {
    ByteBuffer versionNumberByte = ByteBuffer.allocate(Byte.BYTES);
    tsFileInput.read(versionNumberByte, TSFileConfig.MAGIC_STRING.getBytes().length);
    versionNumberByte.flip();
    return versionNumberByte.get();
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  public TsFileMetadata readFileMetadata() throws IOException {
    try {
      if (tsFileMetaData == null) {
        tsFileMetaData =
            TsFileMetadata.deserializeFrom(readData(fileMetadataPos, fileMetadataSize));
      }
    } catch (BufferOverflowException e) {
      logger.error("Something error happened while reading file metadata of file {}", file);
      throw e;
    }
    return tsFileMetaData;
  }

  /**
   * this function does not modify the position of the file reader.
   *
   * @throws IOException io error
   */
  public BloomFilter readBloomFilter() throws IOException {
    readFileMetadata();
    return tsFileMetaData.getBloomFilter();
  }

  /**
   * this function reads measurements and TimeseriesMetaDatas in given device Thread Safe
   *
   * @param device name
   * @return the map measurementId -> TimeseriesMetaData in one device
   * @throws IOException io error
   */
  public Map<String, TimeseriesMetadata> readDeviceMetadata(String device) throws IOException {
    if (!cacheDeviceMetadata) {
      return readDeviceMetadataFromDisk(device);
    }

    cacheLock.readLock().lock();
    try {
      if (cachedDeviceMetadata.containsKey(device)) {
        return cachedDeviceMetadata.get(device);
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    cacheLock.writeLock().lock();
    try {
      if (cachedDeviceMetadata.containsKey(device)) {
        return cachedDeviceMetadata.get(device);
      }
      readFileMetadata();
      Map<String, TimeseriesMetadata> deviceMetadata = readDeviceMetadataFromDisk(device);
      cachedDeviceMetadata.put(device, deviceMetadata);
      return deviceMetadata;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  private Map<String, TimeseriesMetadata> readDeviceMetadataFromDisk(String device)
      throws IOException {
    readFileMetadata();
    List<TimeseriesMetadata> timeseriesMetadataList =
        getDeviceTimeseriesMetadataWithoutChunkMetadata(device);
    Map<String, TimeseriesMetadata> deviceMetadata = new HashMap<>();
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
      deviceMetadata.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata);
    }
    return deviceMetadata;
  }

  public TimeseriesMetadata readTimeseriesMetadata(Path path, boolean ignoreNotExists)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, path.getDevice(), true, true);
    if (metadataIndexPair == null) {
      if (ignoreNotExists) {
        return null;
      }
      throw new IOException("Device {" + path.getDevice() + "} is not in tsFileMetaData");
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      try {
        metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      } catch (BufferOverflowException e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
      metadataIndexPair =
          getMetadataAndEndOffset(metadataIndexNode, path.getMeasurement(), false, false);
    }
    if (metadataIndexPair == null) {
      return null;
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      try {
        timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
      } catch (BufferOverflowException e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
    }
    // return null if path does not exist in the TsFile
    int searchResult =
        binarySearchInTimeseriesMetadataList(timeseriesMetadataList, path.getMeasurement());
    return searchResult >= 0 ? timeseriesMetadataList.get(searchResult) : null;
  }

  /**
   * Find the leaf node that contains this vector, return all the needed subSensor and time column
   *
   * @param path path with time column
   * @param subSensorList value columns that needed
   * @return TimeseriesMetadata for the time column and all the needed subSensor, the order of the
   *     element in this list should be the same as subSensorList
   */
  public List<TimeseriesMetadata> readTimeseriesMetadata(Path path, List<String> subSensorList)
      throws IOException {
    Pair<MetadataIndexEntry, Long> metadataIndexPair = getLeafMetadataIndexPair(path);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    Map<String, TimeseriesMetadata> timeseriesMetadataMap = new HashMap<>();
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetadata timeseriesMetadata;
      try {
        timeseriesMetadata = TimeseriesMetadata.deserializeFrom(buffer, true);
      } catch (BufferOverflowException e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
      timeseriesMetadataMap.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata);
    }

    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
    for (String subSensor : subSensorList) {
      timeseriesMetadataList.add(timeseriesMetadataMap.get(subSensor));
    }
    return timeseriesMetadataList;
  }

  /* Find the leaf node that contains path, return all the sensors in that leaf node which are also in allSensors set */
  public List<TimeseriesMetadata> readTimeseriesMetadata(Path path, Set<String> allSensors)
      throws IOException {
    Pair<MetadataIndexEntry, Long> metadataIndexPair = getLeafMetadataIndexPair(path);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();

    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    while (buffer.hasRemaining()) {
      TimeseriesMetadata timeseriesMetadata;
      try {
        timeseriesMetadata = TimeseriesMetadata.deserializeFrom(buffer, true);
      } catch (BufferOverflowException e) {
        logger.error(
            "Something error happened while deserializing TimeseriesMetadata of file {}", file);
        throw e;
      }
      if (allSensors.contains(timeseriesMetadata.getMeasurementId())) {
        timeseriesMetadataList.add(timeseriesMetadata);
      }
    }
    return timeseriesMetadataList;
  }

  /* Get leaf MetadataIndexPair which contains path */
  private Pair<MetadataIndexEntry, Long> getLeafMetadataIndexPair(Path path) throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, path.getDevice(), true, true);
    if (metadataIndexPair == null) {
      return null;
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    MetadataIndexNode metadataIndexNode = deviceMetadataIndexNode;
    if (!metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
      try {
        metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      } catch (BufferOverflowException e) {
        logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
        throw e;
      }
      metadataIndexPair =
          getMetadataAndEndOffset(metadataIndexNode, path.getMeasurement(), false, false);
    }
    return metadataIndexPair;
  }

  public List<TimeseriesMetadata> readTimeseriesMetadata(String device, Set<String> measurements)
      throws IOException {
    readFileMetadata();
    MetadataIndexNode deviceMetadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(deviceMetadataIndexNode, device, true, false);
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
        try {
          metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
        } catch (BufferOverflowException e) {
          logger.error(METADATA_INDEX_NODE_DESERIALIZE_ERROR, file);
          throw e;
        }
        measurementMetadataIndexPair =
            getMetadataAndEndOffset(metadataIndexNode, measurementList.get(i), false, false);
      }
      if (measurementMetadataIndexPair == null) {
        return Collections.emptyList();
      }
      buffer =
          readData(
              measurementMetadataIndexPair.left.getOffset(), measurementMetadataIndexPair.right);
      while (buffer.hasRemaining()) {
        try {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, true));
        } catch (BufferOverflowException e) {
          logger.error(
              "Something error happened while deserializing TimeseriesMetadata of file {}", file);
          throw e;
        }
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

  protected int binarySearchInTimeseriesMetadataList(
      List<TimeseriesMetadata> timeseriesMetadataList, String key) {
    int low = 0;
    int high = timeseriesMetadataList.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      TimeseriesMetadata midVal = timeseriesMetadataList.get(mid);
      int cmp = midVal.getMeasurementId().compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -1; // key not found
  }

  public List<String> getAllDevices() throws IOException {
    if (tsFileMetaData == null) {
      readFileMetadata();
    }
    return getAllDevices(tsFileMetaData.getMetadataIndex());
  }

  private List<String> getAllDevices(MetadataIndexNode metadataIndexNode) throws IOException {
    List<String> deviceList = new ArrayList<>();
    int metadataIndexListSize = metadataIndexNode.getChildren().size();

    // if metadataIndexNode is LEAF_DEVICE, put all devices in node entry into the list
    if (metadataIndexNode.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
      deviceList.addAll(
          metadataIndexNode.getChildren().stream()
              .map(MetadataIndexEntry::getName)
              .collect(Collectors.toList()));
      return deviceList;
    }

    for (int i = 0; i < metadataIndexListSize; i++) {
      long endOffset = metadataIndexNode.getEndOffset();
      if (i != metadataIndexListSize - 1) {
        endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
      }
      ByteBuffer buffer = readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
      MetadataIndexNode node = MetadataIndexNode.deserializeFrom(buffer);
      if (node.getNodeType().equals(MetadataIndexNodeType.LEAF_DEVICE)) {
        // if node in next level is LEAF_DEVICE, put all devices in node entry into the list
        deviceList.addAll(
            node.getChildren().stream()
                .map(MetadataIndexEntry::getName)
                .collect(Collectors.toList()));
      } else {
        // keep traversing
        deviceList.addAll(getAllDevices(node));
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
  public Map<String, List<ChunkMetadata>> readChunkMetadataInDevice(String device)
      throws IOException {
    readFileMetadata();
    List<TimeseriesMetadata> timeseriesMetadataMap = getDeviceTimeseriesMetadata(device);
    if (timeseriesMetadataMap.isEmpty()) {
      return new HashMap<>();
    }
    Map<String, List<ChunkMetadata>> seriesMetadata = new HashMap<>();
    for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap) {
      seriesMetadata.put(
          timeseriesMetadata.getMeasurementId(),
          timeseriesMetadata.getChunkMetadataList().stream()
              .map(chunkMetadata -> ((ChunkMetadata) chunkMetadata))
              .collect(Collectors.toList()));
    }
    return seriesMetadata;
  }

  /**
   * this function return all timeseries names in this file
   *
   * @return list of Paths
   * @throws IOException io error
   */
  public List<Path> getAllPaths() throws IOException {
    List<Path> paths = new ArrayList<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (String measurementId : timeseriesMetadataMap.keySet()) {
        paths.add(new Path(device, measurementId));
      }
    }
    return paths;
  }

  /**
   * Traverse the metadata index from MetadataIndexEntry to get TimeseriesMetadatas
   *
   * @param metadataIndex MetadataIndexEntry
   * @param buffer byte buffer
   * @param deviceId String
   * @param timeseriesMetadataMap map: deviceId -> timeseriesMetadata list
   * @param needChunkMetadata deserialize chunk metadata list or not
   */
  private void generateMetadataIndex(
      MetadataIndexEntry metadataIndex,
      ByteBuffer buffer,
      String deviceId,
      MetadataIndexNodeType type,
      Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap,
      boolean needChunkMetadata)
      throws IOException {
    try {
      if (type.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
        List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
        while (buffer.hasRemaining()) {
          timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(buffer, needChunkMetadata));
        }
        timeseriesMetadataMap
            .computeIfAbsent(deviceId, k -> new ArrayList<>())
            .addAll(timeseriesMetadataList);
      } else {
        deviceId = metadataIndex.getName();
        MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
        int metadataIndexListSize = metadataIndexNode.getChildren().size();
        for (int i = 0; i < metadataIndexListSize; i++) {
          long endOffset = metadataIndexNode.getEndOffset();
          if (i != metadataIndexListSize - 1) {
            endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
          }
          ByteBuffer nextBuffer =
              readData(metadataIndexNode.getChildren().get(i).getOffset(), endOffset);
          generateMetadataIndex(
              metadataIndexNode.getChildren().get(i),
              nextBuffer,
              deviceId,
              metadataIndexNode.getNodeType(),
              timeseriesMetadataMap,
              needChunkMetadata);
        }
      }
    } catch (BufferOverflowException e) {
      logger.error("Something error happened while generating MetadataIndex of file {}", file);
      throw e;
    }
  }

  /* TimeseriesMetadata don't need deserialize chunk metadata list */
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
      generateMetadataIndex(
          metadataIndexEntry,
          buffer,
          null,
          metadataIndexNode.getNodeType(),
          timeseriesMetadataMap,
          false);
    }
    return timeseriesMetadataMap;
  }

  /* This method will only deserialize the TimeseriesMetadata, not including chunk metadata list */
  private List<TimeseriesMetadata> getDeviceTimeseriesMetadataWithoutChunkMetadata(String device)
      throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndex(
        metadataIndexPair.left,
        buffer,
        device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT,
        timeseriesMetadataMap,
        false);
    List<TimeseriesMetadata> deviceTimeseriesMetadata = new ArrayList<>();
    for (List<TimeseriesMetadata> timeseriesMetadataList : timeseriesMetadataMap.values()) {
      deviceTimeseriesMetadata.addAll(timeseriesMetadataList);
    }
    return deviceTimeseriesMetadata;
  }

  /* This method will not only deserialize the TimeseriesMetadata, but also all the chunk metadata list meanwhile. */
  private List<TimeseriesMetadata> getDeviceTimeseriesMetadata(String device) throws IOException {
    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);
    if (metadataIndexPair == null) {
      return Collections.emptyList();
    }
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    Map<String, List<TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();
    generateMetadataIndex(
        metadataIndexPair.left,
        buffer,
        device,
        MetadataIndexNodeType.INTERNAL_MEASUREMENT,
        timeseriesMetadataMap,
        true);
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
   * @param isDeviceLevel whether target MetadataIndexNode is device level
   * @param exactSearch whether is in exact search mode, return null when there is no entry with
   *     name; or else return the nearest MetadataIndexEntry before it (for deeper search)
   * @return target MetadataIndexEntry, endOffset pair
   */
  protected Pair<MetadataIndexEntry, Long> getMetadataAndEndOffset(
      MetadataIndexNode metadataIndex, String name, boolean isDeviceLevel, boolean exactSearch)
      throws IOException {
    try {
      // When searching for a device node, return when it is not INTERNAL_DEVICE
      // When searching for a measurement node, return when it is not INTERNAL_MEASUREMENT
      if ((isDeviceLevel
              && !metadataIndex.getNodeType().equals(MetadataIndexNodeType.INTERNAL_DEVICE))
          || (!isDeviceLevel
              && !metadataIndex.getNodeType().equals(MetadataIndexNodeType.INTERNAL_MEASUREMENT))) {
        return metadataIndex.getChildIndexEntry(name, exactSearch);
      } else {
        Pair<MetadataIndexEntry, Long> childIndexEntry =
            metadataIndex.getChildIndexEntry(name, false);
        ByteBuffer buffer = readData(childIndexEntry.left.getOffset(), childIndexEntry.right);
        return getMetadataAndEndOffset(
            MetadataIndexNode.deserializeFrom(buffer), name, isDeviceLevel, false);
      }
    } catch (BufferOverflowException e) {
      logger.error("Something error happened while deserializing MetadataIndex of file {}", file);
      throw e;
    }
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupHeader readChunkGroupHeader() throws IOException {
    return ChunkGroupHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_GROUP_FOOTER.
   *
   * @param position the offset of the chunk group footer in the file
   * @param markerRead true if the offset does not contains the marker , otherwise false
   * @return a CHUNK_GROUP_FOOTER
   * @throws IOException io error
   */
  public ChunkGroupHeader readChunkGroupHeader(long position, boolean markerRead)
      throws IOException {
    return ChunkGroupHeader.deserializeFrom(tsFileInput, position, markerRead);
  }

  public void readPlanIndex() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    buffer.flip();
    minPlanIndex = buffer.getLong();
    buffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    buffer.flip();
    maxPlanIndex = buffer.getLong();
  }

  /**
   * read data from current position of the input, and deserialize it to a CHUNK_HEADER. <br>
   * This method is not threadsafe.
   *
   * @return a CHUNK_HEADER
   * @throws IOException io error
   */
  public ChunkHeader readChunkHeader(byte chunkType) throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), chunkType);
  }

  /**
   * read the chunk's header.
   *
   * @param position the file offset of this chunk's header
   * @param chunkHeaderSize the size of chunk's header
   */
  private ChunkHeader readChunkHeader(long position, int chunkHeaderSize) throws IOException {
    return ChunkHeader.deserializeFrom(tsFileInput, position, chunkHeaderSize);
  }

  /**
   * notice, this function will modify channel's position.
   *
   * @param dataSize the size of chunkdata
   * @param position the offset of the chunk data
   * @return the pages of this chunk
   */
  private ByteBuffer readChunk(long position, int dataSize) throws IOException {
    return readData(position, dataSize);
  }

  /**
   * read memory chunk.
   *
   * @param metaData -given chunk meta data
   * @return -chunk
   */
  public Chunk readMemChunk(ChunkMetadata metaData) throws IOException {
    int chunkHeadSize = ChunkHeader.getSerializedSize(metaData.getMeasurementUid());
    ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader(), chunkHeadSize);
    ByteBuffer buffer =
        readChunk(
            metaData.getOffsetOfChunkHeader() + header.getSerializedSize(), header.getDataSize());
    return new Chunk(header, buffer, metaData.getDeleteIntervalList(), metaData.getStatistics());
  }

  /**
   * not thread safe.
   *
   * @param type given tsfile data type
   */
  public PageHeader readPageHeader(TSDataType type, boolean hasStatistic) throws IOException {
    return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type, hasStatistic);
  }

  public long position() throws IOException {
    return tsFileInput.position();
  }

  public void position(long offset) throws IOException {
    tsFileInput.position(offset);
  }

  public void skipPageData(PageHeader header) throws IOException {
    tsFileInput.position(tsFileInput.position() + header.getCompressedSize());
  }

  public ByteBuffer readPage(PageHeader header, CompressionType type) throws IOException {
    ByteBuffer buffer = readData(-1, header.getCompressedSize());
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(type);
    ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
    if (type == CompressionType.UNCOMPRESSED) {
      return buffer;
    } // FIXME if the buffer is not array-implemented.
    unCompressor.uncompress(
        buffer.array(), buffer.position(), buffer.remaining(), uncompressedBuffer.array(), 0);
    return uncompressedBuffer;
  }

  /**
   * read one byte from the input. <br>
   * this method is not thread safe
   */
  public byte readMarker() throws IOException {
    markerBuffer.clear();
    if (ReadWriteIOUtils.readAsPossible(tsFileInput, markerBuffer) == 0) {
      throw new IOException("reach the end of the file.");
    }
    markerBuffer.flip();
    return markerBuffer.get();
  }

  @Override
  public void close() throws IOException {
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} reader is closed.", file);
    }
    this.tsFileInput.close();
  }

  public String getFileName() {
    return this.file.getAbsolutePath();
  }

  public File getFile() {
    return file;
  }

  public long fileSize() throws IOException {
    return tsFileInput.size();
  }

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position. <br>
   * if position = -1, the tsFileInput's position will be changed to the current position + real
   * data size that been read. Other wise, the tsFileInput's position is not changed.
   *
   * @param position the start position of data in the tsFileInput, or the current position if
   *     position = -1
   * @param size the size of data that want to read
   * @return data that been read.
   */
  protected ByteBuffer readData(long position, int size) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    if (position < 0) {
      if (ReadWriteIOUtils.readAsPossible(tsFileInput, buffer) != size) {
        throw new IOException("reach the end of the data");
      }
    } else {
      long actualReadSize = ReadWriteIOUtils.readAsPossible(tsFileInput, buffer, position, size);
      if (actualReadSize != size) {
        throw new IOException(
            String.format(
                "reach the end of the data. Size of data that want to read: %s,"
                    + "actual read size: %s, position: %s",
                size, actualReadSize, position));
      }
    }
    buffer.flip();
    return buffer;
  }

  /**
   * read data from tsFileInput, from the current position (if position = -1), or the given
   * position.
   *
   * @param start the start position of data in the tsFileInput, or the current position if position
   *     = -1
   * @param end the end position of data that want to read
   * @return data that been read.
   */
  protected ByteBuffer readData(long start, long end) throws IOException {
    return readData(start, (int) (end - start));
  }

  /** notice, the target bytebuffer are not flipped. */
  public int readRaw(long position, int length, ByteBuffer target) throws IOException {
    return ReadWriteIOUtils.readAsPossible(tsFileInput, target, position, length);
  }

  /**
   * Self Check the file and return the position before where the data is safe.
   *
   * @param newSchema the schema on each time series in the file
   * @param chunkGroupMetadataList ChunkGroupMetadata List
   * @param fastFinish if true and the file is complete, then newSchema and chunkGroupMetadataList
   *     parameter will be not modified.
   * @return the position of the file that is fine. All data after the position in the file should
   *     be truncated.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public long selfCheck(
      Map<Path, IMeasurementSchema> newSchema,
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      boolean fastFinish)
      throws IOException {
    File checkFile = this.file;
    long fileSize;
    if (!checkFile.exists()) {
      return TsFileCheckStatus.FILE_NOT_FOUND;
    } else {
      fileSize = checkFile.length();
    }
    ChunkMetadata currentChunk;
    String measurementID;
    TSDataType dataType;
    long fileOffsetOfChunk;

    // ChunkMetadata of current ChunkGroup
    List<ChunkMetadata> chunkMetadataList = new ArrayList<>();

    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
    if (fileSize < headerLength) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }
    if (!TSFileConfig.MAGIC_STRING.equals(readHeadMagic())
        || (TSFileConfig.VERSION_NUMBER != readVersionNumber())) {
      return TsFileCheckStatus.INCOMPATIBLE_FILE;
    }

    tsFileInput.position(headerLength);
    if (fileSize == headerLength) {
      return headerLength;
    } else if (isComplete()) {
      loadMetadataSize();
      if (fastFinish) {
        return TsFileCheckStatus.COMPLETE_FILE;
      }
    }
    // not a complete file, we will recover it...
    long truncatedSize = headerLength;
    byte marker;
    String lastDeviceId = null;
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while ((marker = this.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | 0x80):
          case (byte) (MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | 0x40):
            fileOffsetOfChunk = this.position() - 1;
            // if there is something wrong with a chunk, we will drop the whole ChunkGroup
            // as different chunks may be created by the same insertions(sqls), and partial
            // insertion is not tolerable
            ChunkHeader chunkHeader = this.readChunkHeader(marker);
            measurementID = chunkHeader.getMeasurementID();
            IMeasurementSchema measurementSchema =
                new MeasurementSchema(
                    measurementID,
                    chunkHeader.getDataType(),
                    chunkHeader.getEncodingType(),
                    chunkHeader.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            dataType = chunkHeader.getDataType();
            Statistics<?> chunkStatistics = Statistics.getStatsByType(dataType);
            int dataSize = chunkHeader.getDataSize();
            if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.CHUNK_HEADER) {
              while (dataSize > 0) {
                // a new Page
                PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), true);
                chunkStatistics.mergeStatistics(pageHeader.getStatistics());
                this.skipPageData(pageHeader);
                dataSize -= pageHeader.getSerializedPageSize();
                chunkHeader.increasePageNums(1);
              }
            } else {
              // only one page without statistic, we need to iterate each point to generate
              // statistic
              PageHeader pageHeader = this.readPageHeader(chunkHeader.getDataType(), false);
              Decoder valueDecoder =
                  Decoder.getDecoderByType(
                      chunkHeader.getEncodingType(), chunkHeader.getDataType());
              ByteBuffer pageData = readPage(pageHeader, chunkHeader.getCompressionType());
              Decoder timeDecoder =
                  Decoder.getDecoderByType(
                      TSEncoding.valueOf(
                          TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                      TSDataType.INT64);
              PageReader reader =
                  new PageReader(
                      pageHeader,
                      pageData,
                      chunkHeader.getDataType(),
                      valueDecoder,
                      timeDecoder,
                      null);
              BatchData batchData = reader.getAllSatisfiedPageData();
              while (batchData.hasCurrent()) {
                switch (dataType) {
                  case INT32:
                    chunkStatistics.update(batchData.currentTime(), batchData.getInt());
                    break;
                  case INT64:
                    chunkStatistics.update(batchData.currentTime(), batchData.getLong());
                    break;
                  case FLOAT:
                    chunkStatistics.update(batchData.currentTime(), batchData.getFloat());
                    break;
                  case DOUBLE:
                    chunkStatistics.update(batchData.currentTime(), batchData.getDouble());
                    break;
                  case BOOLEAN:
                    chunkStatistics.update(batchData.currentTime(), batchData.getBoolean());
                    break;
                  case TEXT:
                    chunkStatistics.update(batchData.currentTime(), batchData.getBinary());
                    break;
                  default:
                    throw new IOException("Unexpected type " + dataType);
                }
                batchData.next();
              }
              chunkHeader.increasePageNums(1);
            }
            currentChunk =
                new ChunkMetadata(measurementID, dataType, fileOffsetOfChunk, chunkStatistics);
            chunkMetadataList.add(currentChunk);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            // if there is something wrong with the ChunkGroup Header, we will drop this ChunkGroup
            // because we can not guarantee the correctness of the deviceId.
            truncatedSize = this.position() - 1;
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (newSchema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  newSchema.putIfAbsent(
                      new Path(lastDeviceId, tsSchema.getMeasurementId()), tsSchema);
                }
              }
              measurementSchemaList = new ArrayList<>();
              // last chunk group Metadata
              chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
            }
            // this is a chunk group
            chunkMetadataList = new ArrayList<>();
            ChunkGroupHeader chunkGroupHeader = this.readChunkGroupHeader();
            lastDeviceId = chunkGroupHeader.getDeviceID();
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            truncatedSize = this.position() - 1;
            if (lastDeviceId != null) {
              // schema of last chunk group
              if (newSchema != null) {
                for (IMeasurementSchema tsSchema : measurementSchemaList) {
                  newSchema.putIfAbsent(
                      new Path(lastDeviceId, tsSchema.getMeasurementId()), tsSchema);
                }
              }
              measurementSchemaList = new ArrayList<>();
              // last chunk group Metadata
              chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
              lastDeviceId = null;
            }
            readPlanIndex();
            truncatedSize = this.position();
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unexpected marker " + marker);
        }
      }
      // now we read the tail of the data section, so we are sure that the last
      // ChunkGroupFooter is complete.
      if (lastDeviceId != null) {
        // schema of last chunk group
        if (newSchema != null) {
          for (IMeasurementSchema tsSchema : measurementSchemaList) {
            newSchema.putIfAbsent(new Path(lastDeviceId, tsSchema.getMeasurementId()), tsSchema);
          }
        }
        // last chunk group Metadata
        chunkGroupMetadataList.add(new ChunkGroupMetadata(lastDeviceId, chunkMetadataList));
      }
      truncatedSize = this.position() - 1;
    } catch (Exception e) {
      logger.info(
          "TsFile {} self-check cannot proceed at position {} " + "recovered, because : {}",
          file,
          this.position(),
          e.getMessage());
    }
    // Despite the completeness of the data section, we will discard current FileMetadata
    // so that we can continue to write data into this tsfile.
    return truncatedSize;
  }

  /**
   * get ChunkMetaDatas of given path, and throw exception if path not exists
   *
   * @param path timeseries path
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> getChunkMetadataList(Path path, boolean ignoreNotExists)
      throws IOException {
    TimeseriesMetadata timeseriesMetaData = readTimeseriesMetadata(path, ignoreNotExists);
    if (timeseriesMetaData == null) {
      return Collections.emptyList();
    }
    List<ChunkMetadata> chunkMetadataList = readChunkMetaDataList(timeseriesMetaData);
    chunkMetadataList.sort(Comparator.comparingLong(IChunkMetadata::getStartTime));
    return chunkMetadataList;
  }

  public List<ChunkMetadata> getChunkMetadataList(Path path) throws IOException {
    return getChunkMetadataList(path, false);
  }

  /**
   * get ChunkMetaDatas in given TimeseriesMetaData
   *
   * @return List of ChunkMetaData
   */
  public List<ChunkMetadata> readChunkMetaDataList(TimeseriesMetadata timeseriesMetaData)
      throws IOException {
    return timeseriesMetaData.getChunkMetadataList().stream()
        .map(chunkMetadata -> (ChunkMetadata) chunkMetadata)
        .collect(Collectors.toList());
  }

  /**
   * get all measurements in this file
   *
   * @return measurement -> datatype
   */
  public Map<String, TSDataType> getAllMeasurements() throws IOException {
    Map<String, TSDataType> result = new HashMap<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result.put(timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTSDataType());
      }
    }
    return result;
  }

  public Map<String, List<String>> getDeviceMeasurementsMap() throws IOException {
    Map<String, List<String>> result = new HashMap<>();
    for (String device : getAllDevices()) {
      Map<String, TimeseriesMetadata> timeseriesMetadataMap = readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap.values()) {
        result
            .computeIfAbsent(device, d -> new ArrayList<>())
            .add(timeseriesMetadata.getMeasurementId());
      }
    }
    return result;
  }

  /**
   * get device names which has valid chunks in [start, end)
   *
   * @param start start of the partition
   * @param end end of the partition
   * @return device names in range
   */
  public List<String> getDeviceNameInRange(long start, long end) throws IOException {
    List<String> res = new ArrayList<>();
    for (String device : getAllDevices()) {
      Map<String, List<ChunkMetadata>> seriesMetadataMap = readChunkMetadataInDevice(device);
      if (hasDataInPartition(seriesMetadataMap, start, end)) {
        res.add(device);
      }
    }
    return res;
  }

  /**
   * Check if the device has at least one Chunk in this partition
   *
   * @param seriesMetadataMap chunkMetaDataList of each measurement
   * @param start the start position of the space partition
   * @param end the end position of the space partition
   */
  private boolean hasDataInPartition(
      Map<String, List<ChunkMetadata>> seriesMetadataMap, long start, long end) {
    for (List<ChunkMetadata> chunkMetadataList : seriesMetadataMap.values()) {
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        LocateStatus location =
            MetadataQuerierByFileImpl.checkLocateStatus(chunkMetadata, start, end);
        if (location == LocateStatus.in) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * The location of a chunkGroupMetaData with respect to a space partition constraint.
   *
   * <p>in - the middle point of the chunkGroupMetaData is located in the current space partition.
   * before - the middle point of the chunkGroupMetaData is located before the current space
   * partition. after - the middle point of the chunkGroupMetaData is located after the current
   * space partition.
   */
  public enum LocateStatus {
    in,
    before,
    after
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  /**
   * @return An iterator of linked hashmaps ( measurement -> chunk metadata list ). When traversing
   *     the linked hashmap, you will get chunk metadata lists according to the lexicographic order
   *     of the measurements. The first measurement of the linked hashmap of each iteration is
   *     always larger than the last measurement of the linked hashmap of the previous iteration in
   *     lexicographic order.
   */
  public Iterator<Map<String, List<ChunkMetadata>>> getMeasurementChunkMetadataListMapIterator(
      String device) throws IOException {
    readFileMetadata();

    MetadataIndexNode metadataIndexNode = tsFileMetaData.getMetadataIndex();
    Pair<MetadataIndexEntry, Long> metadataIndexPair =
        getMetadataAndEndOffset(metadataIndexNode, device, true, true);

    if (metadataIndexPair == null) {
      return new Iterator<Map<String, List<ChunkMetadata>>>() {

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public LinkedHashMap<String, List<ChunkMetadata>> next() {
          throw new NoSuchElementException();
        }
      };
    }

    Queue<Pair<Long, Long>> queue = new LinkedList<>();
    ByteBuffer buffer = readData(metadataIndexPair.left.getOffset(), metadataIndexPair.right);
    collectEachLeafMeasurementNodeOffsetRange(buffer, queue);

    return new Iterator<Map<String, List<ChunkMetadata>>>() {

      @Override
      public boolean hasNext() {
        return !queue.isEmpty();
      }

      @Override
      public LinkedHashMap<String, List<ChunkMetadata>> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        Pair<Long, Long> startEndPair = queue.remove();
        LinkedHashMap<String, List<ChunkMetadata>> measurementChunkMetadataList =
            new LinkedHashMap<>();
        try {
          List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
          ByteBuffer nextBuffer = readData(startEndPair.left, startEndPair.right);
          while (nextBuffer.hasRemaining()) {
            timeseriesMetadataList.add(TimeseriesMetadata.deserializeFrom(nextBuffer, true));
          }
          for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataList) {
            List<ChunkMetadata> list =
                measurementChunkMetadataList.computeIfAbsent(
                    timeseriesMetadata.getMeasurementId(), m -> new ArrayList<>());
            for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
              list.add((ChunkMetadata) chunkMetadata);
            }
          }
          return measurementChunkMetadataList;
        } catch (IOException e) {
          throw new TsFileRuntimeException(
              "Error occurred while reading a time series metadata block.");
        }
      }
    };
  }

  private void collectEachLeafMeasurementNodeOffsetRange(
      ByteBuffer buffer, Queue<Pair<Long, Long>> queue) throws IOException {
    try {
      final MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer);
      final MetadataIndexNodeType metadataIndexNodeType = metadataIndexNode.getNodeType();
      final int metadataIndexListSize = metadataIndexNode.getChildren().size();
      for (int i = 0; i < metadataIndexListSize; ++i) {
        long startOffset = metadataIndexNode.getChildren().get(i).getOffset();
        long endOffset = metadataIndexNode.getEndOffset();
        if (i != metadataIndexListSize - 1) {
          endOffset = metadataIndexNode.getChildren().get(i + 1).getOffset();
        }
        if (metadataIndexNodeType.equals(MetadataIndexNodeType.LEAF_MEASUREMENT)) {
          queue.add(new Pair<>(startOffset, endOffset));
          continue;
        }
        collectEachLeafMeasurementNodeOffsetRange(readData(startOffset, endOffset), queue);
      }
    } catch (BufferOverflowException e) {
      logger.error(
          "Error occurred while collecting offset ranges of measurement nodes of file {}", file);
      throw e;
    }
  }
}
