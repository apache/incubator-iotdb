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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.footer.ChunkGroupFooter;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexConstructor;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexNode;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.VersionUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TsFileIOWriter is used to construct metadata and write data stored in memory to output stream.
 */
public class TsFileIOWriter {

  public static final byte[] magicStringBytes;
  public static final byte[] versionNumberBytes;
  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TsFileIOWriter.class);
  private static final Logger resourceLogger = LoggerFactory.getLogger("FileMonitor");

  static {
    magicStringBytes = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    versionNumberBytes = TSFileConfig.VERSION_NUMBER.getBytes();
  }

  protected TsFileOutput out;
  protected boolean canWrite = true;
  protected int totalChunkNum = 0;
  protected int invalidChunkNum;
  protected File file;

  // current flushed Chunk
  private ChunkMetadata currentChunkMetadata;
  // current flushed ChunkGroup
  protected List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
  // all flushed ChunkGroups
  protected List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();

  private long markedPosition;
  private String currentChunkGroupDeviceId;
  private long currentChunkGroupStartOffset;
  protected List<Pair<Long, Long>> versionInfo = new ArrayList<>();
  
  // for upgrade tool
  Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap;

  // the two longs marks the index range of operations in current MemTable
  // and are serialized after MetaMarker.OPERATION_INDEX_RANGE to recover file-level range
  private long minPlanIndex;
  private long maxPlanIndex;

  /**
   * empty construct function.
   */
  protected TsFileIOWriter() {

  }

  /**
   * for writing a new tsfile.
   *
   * @param file be used to output written data
   * @throws IOException if I/O error occurs
   */
  public TsFileIOWriter(File file) throws IOException {
    this.out = FSFactoryProducer.getFileOutputFactory().getTsFileOutput(file.getPath(), false);
    this.file = file;
    if (resourceLogger.isDebugEnabled()) {
      resourceLogger.debug("{} writer is opened.", file.getName());
    }
    startFile();
  }

  /**
   * for writing a new tsfile.
   *
   * @param output be used to output written data
   */
  public TsFileIOWriter(TsFileOutput output) throws IOException {
    this.out = output;
    startFile();
  }

  /**
   * Writes given bytes to output stream. This method is called when total memory size exceeds the
   * chunk group size threshold.
   *
   * @param bytes - data of several pages which has been packed
   * @throws IOException if an I/O error occurs.
   */
  public void writeBytesToStream(PublicBAOS bytes) throws IOException {
    bytes.writeTo(out.wrapAsStream());
  }

  protected void startFile() throws IOException {
    out.write(magicStringBytes);
    out.write(versionNumberBytes);
  }

  public void startChunkGroup(String deviceId) throws IOException {
    this.currentChunkGroupDeviceId = deviceId;
    currentChunkGroupStartOffset = out.getPosition();
    if (logger.isTraceEnabled()) {
      logger.trace("start chunk group:{}, file position {}", deviceId, out.getPosition());
    }
    chunkMetadataList = new ArrayList<>();
  }

  /**
   * end chunk and write some log. If there is no data in the chunk group, nothing will be flushed.
   */
  public void endChunkGroup() throws IOException {
    if (currentChunkGroupDeviceId == null || chunkMetadataList.isEmpty()) {
      return;
    }
    long dataSize = out.getPosition() - currentChunkGroupStartOffset;
    ChunkGroupFooter chunkGroupFooter = new ChunkGroupFooter(currentChunkGroupDeviceId, dataSize,
        chunkMetadataList.size());
    chunkGroupFooter.serializeTo(out.wrapAsStream());
    chunkGroupMetadataList
        .add(new ChunkGroupMetadata(currentChunkGroupDeviceId, chunkMetadataList));
    currentChunkGroupDeviceId = null;
    chunkMetadataList = null;
    out.flush();
  }

  /**
   * start a {@linkplain ChunkMetadata ChunkMetaData}.
   *
   * @param measurementSchema - schema of this time series
   * @param compressionCodecName - compression name of this time series
   * @param tsDataType - data type
   * @param statistics - Chunk statistics
   * @param dataSize - the serialized size of all pages
   * @throws IOException if I/O error occurs
   */
  public void startFlushChunk(MeasurementSchema measurementSchema,
      CompressionType compressionCodecName, TSDataType tsDataType, TSEncoding encodingType,
      Statistics<?> statistics, int dataSize, int numOfPages) throws IOException {

    currentChunkMetadata = new ChunkMetadata(measurementSchema.getMeasurementId(), tsDataType,
        out.getPosition(), statistics);

    ChunkHeader header = new ChunkHeader(measurementSchema.getMeasurementId(), dataSize, tsDataType,
        compressionCodecName, encodingType, numOfPages);
    header.serializeTo(out.wrapAsStream());

  }

  /**
   * Write a whole chunk in another file into this file. Providing fast merge for IoTDB.
   */
  public void writeChunk(Chunk chunk, ChunkMetadata chunkMetadata) throws IOException {
    ChunkHeader chunkHeader = chunk.getHeader();
    currentChunkMetadata = new ChunkMetadata(chunkHeader.getMeasurementID(),
        chunkHeader.getDataType(),
        out.getPosition(), chunkMetadata.getStatistics());
    chunkHeader.serializeTo(out.wrapAsStream());
    out.write(chunk.getData());
    endCurrentChunk();
    if (logger.isDebugEnabled()) {
      logger.debug("end flushing a chunk:{}, totalvalue:{}", chunkHeader.getMeasurementID(),
          chunkMetadata.getNumOfPoints());
    }
  }

  /**
   * end chunk and write some log.
   */
  public void endCurrentChunk() {
    chunkMetadataList.add(currentChunkMetadata);
    currentChunkMetadata = null;
    totalChunkNum++;
  }

  /**
   * write {@linkplain TsFileMetadata TSFileMetaData} to output stream and close it.
   *
   * @throws IOException if I/O error occurs
   */
  public void endFile() throws IOException {
    long metaOffset = out.getPosition();

    // serialize the SEPARATOR of MetaData
    ReadWriteIOUtils.write(MetaMarker.SEPARATOR, out.wrapAsStream());

    // group ChunkMetadata by series
    Map<Path, List<ChunkMetadata>> chunkMetadataListMap = new TreeMap<>();
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        Path series = new Path(chunkGroupMetadata.getDevice(), chunkMetadata.getMeasurementUid());
        chunkMetadataListMap.computeIfAbsent(series, k -> new ArrayList<>()).add(chunkMetadata);
      }
    }

    MetadataIndexNode metadataIndex = flushMetadataIndex(chunkMetadataListMap);
    TsFileMetadata tsFileMetaData = new TsFileMetadata();
    tsFileMetaData.setMetadataIndex(metadataIndex);
    tsFileMetaData.setVersionInfo(versionInfo);
    tsFileMetaData.setTotalChunkNum(totalChunkNum);
    tsFileMetaData.setInvalidChunkNum(invalidChunkNum);
    tsFileMetaData.setMetaOffset(metaOffset);

    long footerIndex = out.getPosition();
    if (logger.isDebugEnabled()) {
      logger.debug("start to flush the footer,file pos:{}", footerIndex);
    }

    // write TsFileMetaData
    int size = tsFileMetaData.serializeTo(out.wrapAsStream());
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the footer {}, file pos:{}", tsFileMetaData, out.getPosition());
    }

    // write bloom filter
    size += tsFileMetaData.serializeBloomFilter(out.wrapAsStream(), chunkMetadataListMap.keySet());
    if (logger.isDebugEnabled()) {
      logger.debug("finish flushing the bloom filter file pos:{}", out.getPosition());
    }

    // write TsFileMetaData size
    ReadWriteIOUtils.write(size, out.wrapAsStream());// write the size of the file metadata.

    // write magic string
    out.write(magicStringBytes);

    // close file
    out.close();
    if (resourceLogger.isDebugEnabled() && file != null) {
      resourceLogger.debug("{} writer is closed.", file.getName());
    }
    canWrite = false;
  }

  /**
   * Flush TsFileMetadata, including ChunkMetadataList and TimeseriesMetaData
   *
   * @return MetadataIndexEntry list in TsFileMetadata
   */
  private MetadataIndexNode flushMetadataIndex(
      Map<Path, List<ChunkMetadata>> chunkMetadataListMap) throws IOException {

    // convert ChunkMetadataList to this field
    deviceTimeseriesMetadataMap = new LinkedHashMap<>();
    // create device -> TimeseriesMetaDataList Map
    for (Map.Entry<Path, List<ChunkMetadata>> entry : chunkMetadataListMap.entrySet()) {
      Path path = entry.getKey();
      String device = path.getDevice();

      // create TimeseriesMetaData
      TSDataType dataType = entry.getValue().get(entry.getValue().size() - 1).getDataType();
      long offsetOfChunkMetadataList = out.getPosition();
      Statistics seriesStatistics = Statistics.getStatsByType(dataType);

      int chunkMetadataListLength = 0;
      // flush chunkMetadataList one by one
      for (ChunkMetadata chunkMetadata : entry.getValue()) {
        if (!chunkMetadata.getDataType().equals(dataType)) {
          continue;
        }
        chunkMetadataListLength += chunkMetadata.serializeTo(out.wrapAsStream());
        seriesStatistics.mergeStatistics(chunkMetadata.getStatistics());
      }
      TimeseriesMetadata timeseriesMetadata = new TimeseriesMetadata(offsetOfChunkMetadataList,
          chunkMetadataListLength, path.getMeasurement(), dataType, seriesStatistics);
      deviceTimeseriesMetadataMap.computeIfAbsent(device, k -> new ArrayList<>())
          .add(timeseriesMetadata);
    }

    // construct TsFileMetadata and return
    return MetadataIndexConstructor.constructMetadataIndex(deviceTimeseriesMetadataMap, out);
  }

  /**
   * get the length of normal OutputStream.
   *
   * @return - length of normal OutputStream
   * @throws IOException if I/O error occurs
   */
  public long getPos() throws IOException {
    return out.getPosition();
  }

  // device -> ChunkMetadataList
  public Map<String, List<ChunkMetadata>> getDeviceChunkMetadataMap() {
    Map<String, List<ChunkMetadata>> deviceChunkMetadataMap = new HashMap<>();

    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      VersionUtils.applyVersion(chunkGroupMetadata.getChunkMetadataList(), versionInfo);
      deviceChunkMetadataMap.computeIfAbsent(chunkGroupMetadata.getDevice(), k -> new ArrayList<>())
          .addAll(chunkGroupMetadata.getChunkMetadataList());
    }
    return deviceChunkMetadataMap;
  }

  public boolean canWrite() {
    return canWrite;
  }

  public void mark() throws IOException {
    markedPosition = getPos();
  }

  public void reset() throws IOException {
    out.truncate(markedPosition);
  }

  /**
   * close the outputStream or file channel without writing FileMetadata. This is just used for
   * Testing.
   */
  public void close() throws IOException {
    canWrite = false;
    out.close();
  }

  void writeSeparatorMaskForTest() throws IOException {
    out.write(new byte[]{MetaMarker.SEPARATOR});
  }

  void writeChunkMaskForTest() throws IOException {
    out.write(new byte[]{MetaMarker.CHUNK_HEADER});
  }

  public int getTotalChunkNum() {
    return totalChunkNum;
  }

  public int getInvalidChunkNum() {
    return invalidChunkNum;
  }

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  /**
   * Remove such ChunkMetadata that its startTime is not in chunkStartTimes
   */
  public void filterChunks(Map<Path, List<Long>> chunkStartTimes) {
    Map<Path, Integer> startTimeIdxes = new HashMap<>();
    chunkStartTimes.forEach((p, t) -> startTimeIdxes.put(p, 0));

    Iterator<ChunkGroupMetadata> chunkGroupMetaDataIterator = chunkGroupMetadataList.iterator();
    while (chunkGroupMetaDataIterator.hasNext()) {
      ChunkGroupMetadata chunkGroupMetaData = chunkGroupMetaDataIterator.next();
      String deviceId = chunkGroupMetaData.getDevice();
      int chunkNum = chunkGroupMetaData.getChunkMetadataList().size();
      Iterator<ChunkMetadata> chunkMetaDataIterator = chunkGroupMetaData.getChunkMetadataList()
          .iterator();
      while (chunkMetaDataIterator.hasNext()) {
        ChunkMetadata chunkMetaData = chunkMetaDataIterator.next();
        Path path = new Path(deviceId, chunkMetaData.getMeasurementUid());
        int startTimeIdx = startTimeIdxes.get(path);

        List<Long> pathChunkStartTimes = chunkStartTimes.get(path);
        boolean chunkValid = startTimeIdx < pathChunkStartTimes.size()
            && pathChunkStartTimes.get(startTimeIdx) == chunkMetaData.getStartTime();
        if (!chunkValid) {
          chunkMetaDataIterator.remove();
          chunkNum--;
          invalidChunkNum++;
        } else {
          startTimeIdxes.put(path, startTimeIdx + 1);
        }
      }
      if (chunkNum == 0) {
        chunkGroupMetaDataIterator.remove();
      }
    }
  }

  /**
   * write MetaMarker.VERSION with version Then, cache offset-version in versionInfo
   */
  public void writeVersion(long version) throws IOException {
    ReadWriteIOUtils.write(MetaMarker.VERSION, out.wrapAsStream());
    ReadWriteIOUtils.write(version, out.wrapAsStream());
    versionInfo.add(new Pair<>(getPos(), version));
  }

  public void writePlanIndices() throws IOException {
    ReadWriteIOUtils.write(MetaMarker.OPERATION_INDEX_RANGE, out.wrapAsStream());
    ReadWriteIOUtils.write(minPlanIndex, out.wrapAsStream());
    ReadWriteIOUtils.write(maxPlanIndex, out.wrapAsStream());
  }

  public void setDefaultVersionPair() {
    // only happen when using tsfile module write api
    if (versionInfo.isEmpty()) {
      versionInfo.add(new Pair<>(Long.MAX_VALUE, 0L));
    }
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileOutput
   */
  public TsFileOutput getIOWriterOut() {
    return out;
  }

  /**
   * this function is only for Upgrade Tool.
   *
   * @return DeviceTimeseriesMetadataMap
   */
  public Map<String, List<TimeseriesMetadata>> getDeviceTimeseriesMetadataMap() {
    return deviceTimeseriesMetadataMap;
  }

  public long getMinPlanIndex() {
    return minPlanIndex;
  }

  public void setMinPlanIndex(long minPlanIndex) {
    this.minPlanIndex = minPlanIndex;
  }

  public long getMaxPlanIndex() {
    return maxPlanIndex;
  }

  public void setMaxPlanIndex(long maxPlanIndex) {
    this.maxPlanIndex = maxPlanIndex;
  }
}
