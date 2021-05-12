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
package org.apache.iotdb.db.tools.upgrade;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.v2.read.TsFileSequenceReaderForV2;
import org.apache.iotdb.tsfile.v2.read.reader.page.PageReaderV2;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TsFileOnlineUpgradeTool extends TsFileRewriteTool {

  private static final Logger logger = LoggerFactory.getLogger(TsFileOnlineUpgradeTool.class);

  /**
   * Create a file reader of the given file. This reader will read the old file and rewrite it to a
   * new format(v3) file
   *
   * @param resourceToBeUpgraded the old tsfile resource which need to be upgrade
   * @throws IOException If some I/O error occurs
   */
  public TsFileOnlineUpgradeTool(TsFileResource resourceToBeUpgraded) throws IOException {
    super(resourceToBeUpgraded);
    String file = oldTsFile.getAbsolutePath();
    reader = new TsFileSequenceReaderForV2(oldTsFile);
  }

  /**
   * upgrade a single TsFile.
   *
   * @param resourceToBeUpgraded the old file's resource which need to be upgrade.
   * @param upgradedResources new version tsFiles' resources
   */
  public static void upgradeOneTsFile(
      TsFileResource resourceToBeUpgraded, List<TsFileResource> upgradedResources)
      throws IOException, WriteProcessException {
    try (TsFileOnlineUpgradeTool updater = new TsFileOnlineUpgradeTool(resourceToBeUpgraded)) {
      updater.upgradeFile(upgradedResources);
    }
  }

  /** upgrade file resource */
  @SuppressWarnings({"squid:S3776", "deprecation"}) // Suppress high Cognitive Complexity warning
  private void upgradeFile(List<TsFileResource> upgradedResources)
      throws IOException, WriteProcessException {

    // check if the old TsFile has correct header
    if (!fileCheck()) {
      return;
    }

    int headerLength =
        TSFileConfig.MAGIC_STRING.getBytes().length
            + TSFileConfig.VERSION_NUMBER_V2.getBytes().length;
    reader.position(headerLength);
    List<List<PageHeader>> pageHeadersInChunkGroup = new ArrayList<>();
    List<List<ByteBuffer>> pageDataInChunkGroup = new ArrayList<>();
    List<List<Boolean>> needToDecodeInfoInChunkGroup = new ArrayList<>();
    byte marker;
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    try {
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            ChunkHeader header = ((TsFileSequenceReaderForV2) reader).readChunkHeader();
            IMeasurementSchema measurementSchema =
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
              PageHeader pageHeader = ((TsFileSequenceReaderForV2) reader).readPageHeader(dataType);
              boolean needToDecode = checkIfNeedToDecode(dataType, encoding, pageHeader);
              needToDecodeInfo.add(needToDecode);
              ByteBuffer pageData =
                  !needToDecode
                      ? ((TsFileSequenceReaderForV2) reader).readCompressedPage(pageHeader)
                      : reader.readPage(pageHeader, header.getCompressionType());
              pageHeadersInChunk.add(pageHeader);
              dataInChunk.add(pageData);
              dataSize -=
                  (Integer.BYTES * 2 // the bytes size of uncompressedSize and compressedSize
                      // count, startTime, endTime bytes size in old statistics
                      + 24
                      // statistics bytes size
                      // new boolean StatsSize is 8 bytes larger than old one
                      + (pageHeader.getStatistics().getStatsSize()
                          - (dataType == TSDataType.BOOLEAN ? 8 : 0))
                      // page data bytes
                      + pageHeader.getCompressedSize());
            }
            pageHeadersInChunkGroup.add(pageHeadersInChunk);
            pageDataInChunkGroup.add(dataInChunk);
            needToDecodeInfoInChunkGroup.add(needToDecodeInfo);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            // this is the footer of a ChunkGroup in TsFileV2.
            ChunkGroupHeader chunkGroupFooter =
                ((TsFileSequenceReaderForV2) reader).readChunkGroupFooter();
            String deviceID = chunkGroupFooter.getDeviceID();
            rewrite(
                deviceID,
                measurementSchemaList,
                pageHeadersInChunkGroup,
                pageDataInChunkGroup,
                needToDecodeInfoInChunkGroup);
            pageHeadersInChunkGroup.clear();
            pageDataInChunkGroup.clear();
            measurementSchemaList.clear();
            needToDecodeInfoInChunkGroup.clear();
            break;
          case MetaMarker.VERSION:
            long version = ((TsFileSequenceReaderForV2) reader).readVersion();
            // convert old Modification to new
            if (oldModification != null && modsIterator.hasNext()) {
              if (currentMod == null) {
                currentMod = (Deletion) modsIterator.next();
              }
              if (currentMod.getFileOffset() <= version) {
                for (Entry<TsFileIOWriter, ModificationFile> entry :
                    fileModificationMap.entrySet()) {
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
            // write plan indices for ending memtable
            for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
              tsFileIOWriter.writePlanIndices();
            }
            break;
          default:
            // the disk file is corrupted, using this file may be dangerous
            throw new IOException("Unrecognized marker detected, " + "this file may be corrupted");
        }
      }
      // close upgraded tsFiles and generate resources for them
      for (TsFileIOWriter tsFileIOWriter : partitionWriterMap.values()) {
        upgradedResources.add(endFileAndGenerateResource(tsFileIOWriter));
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
          "TsFile upgrade process cannot proceed at position "
              + reader.position()
              + "because: "
              + e2.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /** TsFileName is changing like from 1610635230693-1-0.tsfile to 1610635230693-1-0-0.tsfile */
  @Override
  public String upgradeTsFileName(String oldTsFileName) {
    String[] name = oldTsFileName.split(TsFileConstant.TSFILE_SUFFIX);
    return name[0] + "-0" + TsFileConstant.TSFILE_SUFFIX;
  }

  /**
   * Due to TsFile version-3 changed the serialize way of integer in TEXT data and INT32 data with
   * PLAIN encoding, and also add a sum statistic for BOOLEAN data, these types of data need to
   * decode to points and rewrite in new TsFile.
   */
  @Override
  protected boolean checkIfNeedToDecode(
      TSDataType dataType, TSEncoding encoding, PageHeader pageHeader) {
    return dataType == TSDataType.BOOLEAN
        || dataType == TSDataType.TEXT
        || (dataType == TSDataType.INT32 && encoding == TSEncoding.PLAIN)
        || StorageEngine.getTimePartition(pageHeader.getStartTime())
            != StorageEngine.getTimePartition(pageHeader.getEndTime());
  }

  @Override
  protected void decodeAndWritePageInToFiles(
      IMeasurementSchema schema,
      ByteBuffer pageData,
      Map<Long, Map<IMeasurementSchema, ChunkWriterImpl>> chunkWritersInChunkGroup)
      throws IOException {
    valueDecoder.reset();
    PageReaderV2 pageReader =
        new PageReaderV2(pageData, schema.getType(), valueDecoder, defaultTimeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    rewritePageIntoFiles(batchData, schema, chunkWritersInChunkGroup);
  }

  /** check if the file to be upgraded has correct magic strings and version number */
  @Override
  protected boolean fileCheck() throws IOException {
    String magic = reader.readHeadMagic();
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    String versionNumber = ((TsFileSequenceReaderForV2) reader).readVersionNumberV2();
    if (!versionNumber.equals(TSFileConfig.VERSION_NUMBER_V2)) {
      logger.error("the file's Version Number is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file is not closed correctly, file path: {}", reader.getFileName());
      return false;
    }
    return true;
  }
}
