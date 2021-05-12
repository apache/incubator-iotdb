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
package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.FSUtils;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TsFileRewriteTool implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRewriteTool.class);

  protected TsFileSequenceReader reader;
  protected File oldTsFile;
  protected List<Modification> oldModification;
  protected Iterator<Modification> modsIterator;

  /** new tsFile writer -> list of new modification */
  protected Map<TsFileIOWriter, ModificationFile> fileModificationMap;

  protected Deletion currentMod;
  protected Decoder defaultTimeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);
  protected Decoder valueDecoder;

  /** PartitionId -> TsFileIOWriter */
  protected Map<Long, TsFileIOWriter> partitionWriterMap;

  /** Maximum index of plans executed within this TsFile. */
  protected long maxPlanIndex = Long.MIN_VALUE;

  /** Minimum index of plans executed within this TsFile. */
  protected long minPlanIndex = Long.MAX_VALUE;

  /**
   * Create a file reader of the given file. The reader will read the real data and rewrite to some
   * new tsFiles.
   *
   * @throws IOException If some I/O error occurs
   */
  public TsFileRewriteTool(TsFileResource resourceToBeRewritten) throws IOException {
    oldTsFile = resourceToBeRewritten.getTsFile();
    String file = oldTsFile.getAbsolutePath();
    reader = new TsFileSequenceReader(oldTsFile);
    partitionWriterMap = new HashMap<>();
    if (FSFactoryProducer.getFSFactory(FSUtils.getFSType(oldTsFile))
        .getFile(file + ModificationFile.FILE_SUFFIX)
        .exists()) {
      oldModification = (List<Modification>) resourceToBeRewritten.getModFile().getModifications();
      modsIterator = oldModification.iterator();
      fileModificationMap = new HashMap<>();
    }
  }

  /**
   * Rewrite an old file to the latest version
   *
   * @param resourceToBeRewritten the tsfile which to be rewrite
   * @param rewrittenResources the rewritten files
   */
  public static void rewriteTsFile(
      TsFileResource resourceToBeRewritten, List<TsFileResource> rewrittenResources)
      throws IOException, WriteProcessException {
    try (TsFileRewriteTool rewriteTool = new TsFileRewriteTool(resourceToBeRewritten)) {
      rewriteTool.parseAndRewriteFile(rewrittenResources);
    }
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }

  /**
   * Parse the old files and generate some new files according to the time partition interval.
   *
   * @throws IOException WriteProcessException
   */
  @SuppressWarnings({"squid:S3776", "deprecation"}) // Suppress high Cognitive Complexity warning
  public void parseAndRewriteFile(List<TsFileResource> rewrittenResources)
      throws IOException, WriteProcessException {
    // check if the TsFile has correct header
    if (!fileCheck()) {
      return;
    }
    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
    reader.position(headerLength);
    // start to scan chunks and chunkGroups
    List<List<PageHeader>> pageHeadersInChunkGroup = new ArrayList<>();
    List<List<ByteBuffer>> pageDataInChunkGroup = new ArrayList<>();
    List<List<Boolean>> needToDecodeInfoInChunkGroup = new ArrayList<>();
    byte marker;
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    String lastChunkGroupDeviceId = null;
    try {
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            MeasurementSchema measurementSchema =
                new MeasurementSchema(
                    header.getMeasurementID(),
                    header.getDataType(),
                    header.getEncodingType(),
                    header.getCompressionType());
            measurementSchemaList.add(measurementSchema);
            TSDataType dataType = header.getDataType();
            TSEncoding encoding = header.getEncodingType();
            List<PageHeader> pageHeadersInChunk = new ArrayList<>();
            List<ByteBuffer> dataInChunk = new ArrayList<>();
            List<Boolean> needToDecodeInfo = new ArrayList<>();
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              // a new Page
              PageHeader pageHeader =
                  reader.readPageHeader(dataType, header.getChunkType() == MetaMarker.CHUNK_HEADER);
              boolean needToDecode = checkIfNeedToDecode(dataType, encoding, pageHeader);
              needToDecodeInfo.add(needToDecode);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              pageHeadersInChunk.add(pageHeader);
              dataInChunk.add(pageData);
              dataSize -= pageHeader.getSerializedPageSize();
            }
            pageHeadersInChunkGroup.add(pageHeadersInChunk);
            pageDataInChunkGroup.add(dataInChunk);
            needToDecodeInfoInChunkGroup.add(needToDecodeInfo);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            String deviceId = chunkGroupHeader.getDeviceID();
            if (lastChunkGroupDeviceId != null && !measurementSchemaList.isEmpty()) {
              rewrite(
                  lastChunkGroupDeviceId,
                  measurementSchemaList,
                  pageHeadersInChunkGroup,
                  pageDataInChunkGroup,
                  needToDecodeInfoInChunkGroup);
              pageHeadersInChunkGroup.clear();
              pageDataInChunkGroup.clear();
              measurementSchemaList.clear();
              needToDecodeInfoInChunkGroup.clear();
            }
            lastChunkGroupDeviceId = deviceId;
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            // write plan indices for ending memtable
            for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
              long tmpMinPlanIndex = reader.getMinPlanIndex();
              if (tmpMinPlanIndex < minPlanIndex) {
                minPlanIndex = tmpMinPlanIndex;
              }

              long tmpMaxPlanIndex = reader.getMaxPlanIndex();
              if (tmpMaxPlanIndex < maxPlanIndex) {
                maxPlanIndex = tmpMaxPlanIndex;
              }

              tsFileIOWriter.setMaxPlanIndex(tmpMinPlanIndex);
              tsFileIOWriter.setMaxPlanIndex(tmpMaxPlanIndex);
              tsFileIOWriter.writePlanIndices();
            }
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }

      if (!measurementSchemaList.isEmpty()) {
        rewrite(
            lastChunkGroupDeviceId,
            measurementSchemaList,
            pageHeadersInChunkGroup,
            pageDataInChunkGroup,
            needToDecodeInfoInChunkGroup);
        pageHeadersInChunkGroup.clear();
        pageDataInChunkGroup.clear();
        measurementSchemaList.clear();
        needToDecodeInfoInChunkGroup.clear();
      }
      // close upgraded tsFiles and generate resources for them
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
        rewrittenResources.add(endFileAndGenerateResource(tsFileIOWriter));
      }
      // write the remain modification for new file
      if (oldModification != null) {
        while (currentMod != null || modsIterator.hasNext()) {
          if (currentMod == null) {
            currentMod = (Deletion) modsIterator.next();
          }
          for (Entry<TsFileIOWriter, ModificationFile> entry : fileModificationMap.entrySet()) {
            TsFileIOWriter tsFileIOWriter = entry.getKey();
            ModificationFile newMods = entry.getValue();
            newMods.write(
                new Deletion(
                    currentMod.getPath(),
                    tsFileIOWriter.getFile().length(),
                    currentMod.getStartTime(),
                    currentMod.getEndTime()));
          }
          currentMod = null;
        }
      }
    } catch (IOException e2) {
      throw new IOException(
          "TsFile rewrite process cannot proceed at position "
              + reader.position()
              + "because: "
              + e2.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * If the page have no statistics or crosses multi partitions, will return true, otherwise return
   * false.
   */
  protected boolean checkIfNeedToDecode(
      TSDataType dataType, TSEncoding encoding, PageHeader pageHeader) {
    if (pageHeader.getStatistics() == null) {
      return true;
    }
    return StorageEngine.getTimePartition(pageHeader.getStartTime())
        != StorageEngine.getTimePartition(pageHeader.getEndTime());
  }

  /**
   * This method is for rewriting the ChunkGroup which data is in the different time partitions. In
   * this case, we have to decode the data to points, and then rewrite the data points to different
   * chunkWriters, finally write chunks to their own upgraded TsFiles.
   */
  protected void rewrite(
      String deviceId,
      List<IMeasurementSchema> schemas,
      List<List<PageHeader>> pageHeadersInChunkGroup,
      List<List<ByteBuffer>> dataInChunkGroup,
      List<List<Boolean>> needToDecodeInfoInChunkGroup)
      throws IOException, PageException {
    Map<Long, Map<IMeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup = new HashMap<>();
    for (int i = 0; i < schemas.size(); i++) {
      IMeasurementSchema schema = schemas.get(i);
      List<ByteBuffer> pageDataInChunk = dataInChunkGroup.get(i);
      List<PageHeader> pageHeadersInChunk = pageHeadersInChunkGroup.get(i);
      List<Boolean> needToDecodeInfoInChunk = needToDecodeInfoInChunkGroup.get(i);
      valueDecoder = Decoder.getDecoderByType(schema.getEncodingType(), schema.getType());
      boolean isOnlyOnePageChunk = pageDataInChunk.size() == 1;
      for (int j = 0; j < pageDataInChunk.size(); j++) {
        if (Boolean.TRUE.equals(needToDecodeInfoInChunk.get(j))) {
          decodeAndWritePageInToFiles(schema, pageDataInChunk.get(j), chunkWritersInChunkGroup);
        } else {
          writePageInToFile(
              schema,
              pageHeadersInChunk.get(j),
              pageDataInChunk.get(j),
              chunkWritersInChunkGroup,
              isOnlyOnePageChunk);
        }
      }
    }

    for (Entry<Long, Map<IMeasurementSchema, ChunkWriterImpl>> entry :
        chunkWritersInChunkGroup.entrySet()) {
      long partitionId = entry.getKey();
      TsFileIOWriter tsFileIOWriter = partitionWriterMap.get(partitionId);
      tsFileIOWriter.startChunkGroup(deviceId);
      // write chunks to their own upgraded tsFiles
      for (IChunkWriter chunkWriter : entry.getValue().values()) {
        chunkWriter.writeToFileWriter(tsFileIOWriter);
      }
      tsFileIOWriter.endChunkGroup();
    }
  }

  public String upgradeTsFileName(String oldTsFileName) {
    return oldTsFileName;
  }

  protected TsFileIOWriter getOrDefaultTsFileIOWriter(File oldTsFile, long partition) {
    return partitionWriterMap.computeIfAbsent(
        partition,
        k -> {
          FSFactory fsFactory = FSFactoryProducer.getFSFactory(FSUtils.getFSType(oldTsFile));
          File partitionDir = fsFactory.getFile(oldTsFile.getParent() + File.separator + partition);
          if (!partitionDir.exists()) {
            partitionDir.mkdirs();
          }
          File newFile =
              fsFactory.getFile(
                  partitionDir + File.separator + upgradeTsFileName(oldTsFile.getName()));
          try {
            if (newFile.exists()) {
              logger.debug("delete uncomplated file {}", newFile);
              Files.delete(newFile.toPath());
            }
            if (!newFile.createNewFile()) {
              logger.error("Create new TsFile {} failed because it exists", newFile);
            }
            TsFileIOWriter writer = new TsFileIOWriter(newFile);
            if (oldModification != null) {
              fileModificationMap.put(
                  writer,
                  new ModificationFile(fsFactory.getFile(newFile + ModificationFile.FILE_SUFFIX)));
            }
            return writer;
          } catch (IOException e) {
            logger.error("Create new TsFile {} failed ", newFile, e);
            return null;
          }
        });
  }

  protected void writePageInToFile(
      IMeasurementSchema schema,
      PageHeader pageHeader,
      ByteBuffer pageData,
      Map<Long, Map<IMeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup,
      boolean isOnlyOnePageChunk)
      throws PageException {
    long partitionId = StorageEngine.getTimePartition(pageHeader.getStartTime());
    getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
    Map<IMeasurementSchema, ChunkWriterImpl> chunkWriters =
        chunkWritersInChunkGroup.getOrDefault(partitionId, new HashMap<>());
    ChunkWriterImpl chunkWriter = chunkWriters.getOrDefault(schema, new ChunkWriterImpl(schema));
    chunkWriter.writePageHeaderAndDataIntoBuff(pageData, pageHeader, isOnlyOnePageChunk);
    chunkWriters.put(schema, chunkWriter);
    chunkWritersInChunkGroup.put(partitionId, chunkWriters);
  }

  protected void decodeAndWritePageInToFiles(
      IMeasurementSchema schema,
      ByteBuffer pageData,
      Map<Long, Map<IMeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup)
      throws IOException {
    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, schema.getType(), valueDecoder, defaultTimeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    rewritePageIntoFiles(batchData, schema, chunkWritersInChunkGroup);
  }

  protected void rewritePageIntoFiles(
      BatchData batchData,
      IMeasurementSchema schema,
      Map<Long, Map<IMeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup) {
    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      Object value = batchData.currentValue();
      long partitionId = StorageEngine.getTimePartition(time);

      Map<IMeasurementSchema, ChunkWriterImpl> chunkWriters =
          chunkWritersInChunkGroup.getOrDefault(partitionId, new HashMap<>());
      ChunkWriterImpl chunkWriter = chunkWriters.getOrDefault(schema, new ChunkWriterImpl(schema));
      getOrDefaultTsFileIOWriter(oldTsFile, partitionId);
      switch (schema.getType()) {
        case INT32:
          chunkWriter.write(time, (int) value, false);
          break;
        case INT64:
          chunkWriter.write(time, (long) value, false);
          break;
        case FLOAT:
          chunkWriter.write(time, (float) value, false);
          break;
        case DOUBLE:
          chunkWriter.write(time, (double) value, false);
          break;
        case BOOLEAN:
          chunkWriter.write(time, (boolean) value, false);
          break;
        case TEXT:
          chunkWriter.write(time, (Binary) value, false);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", schema.getType()));
      }
      batchData.next();
      chunkWriters.put(schema, chunkWriter);
      chunkWritersInChunkGroup.put(partitionId, chunkWriters);
    }
  }

  /** check if the file has correct magic strings and version number */
  protected boolean fileCheck() throws IOException {
    String magic = reader.readHeadMagic();
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    byte versionNumber = reader.readVersionNumber();
    if (versionNumber != TSFileConfig.VERSION_NUMBER) {
      logger.error("the file's Version Number is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file is not closed correctly, file path: {}", reader.getFileName());
      return false;
    }
    return true;
  }

  protected TsFileResource endFileAndGenerateResource(TsFileIOWriter tsFileIOWriter)
      throws IOException {
    tsFileIOWriter.endFile();
    TsFileResource tsFileResource = new TsFileResource(tsFileIOWriter.getFile());
    Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
        tsFileIOWriter.getDeviceTimeseriesMetadataMap();
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {
      String device = entry.getKey();
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());
      }
    }
    tsFileResource.setMinPlanIndex(minPlanIndex);
    tsFileResource.setMaxPlanIndex(maxPlanIndex);
    tsFileResource.setClosed(true);
    tsFileResource.serialize();
    return tsFileResource;
  }
}
