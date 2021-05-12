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
package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TsFileSequenceRead {
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory(Constant.DEFAULT_FS);

  @SuppressWarnings({
    "squid:S3776",
    "squid:S106"
  }) // Suppress high Cognitive Complexity and Standard outputs warning
  public static void main(String[] args) throws IOException {
    String filename = "test.tsfile";
    if (args.length >= 1) {
      filename = args[0];
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(filename))) {
      System.out.println("file length: " + fsFactory.getFile(filename).length());
      System.out.println("file magic head: " + reader.readHeadMagic());
      System.out.println("file magic tail: " + reader.readTailMagic());
      System.out.println("Level 1 metadata position: " + reader.getFileMetadataPos());
      System.out.println("Level 1 metadata size: " + reader.getFileMetadataSize());
      // Sequential reading of one ChunkGroup now follows this order:
      // first the CHUNK_GROUP_HEADER, then SeriesChunks (headers and data) in one ChunkGroup
      // Because we do not know how many chunks a ChunkGroup may have, we should read one byte (the
      // marker) ahead and judge accordingly.
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      System.out.println("[Chunk Group]");
      System.out.println("position: " + reader.position());
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            System.out.println("\t[Chunk]");
            System.out.println("\tchunk type: " + marker);
            System.out.println("\tposition: " + reader.position());
            ChunkHeader header = reader.readChunkHeader(marker);
            System.out.println("\tMeasurement: " + header.getMeasurementID());
            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              valueDecoder.reset();
              System.out.println("\t\t[Page]\n \t\tPage head position: " + reader.position());
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
              System.out.println("\t\tPage data position: " + reader.position());
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              System.out.println(
                  "\t\tUncompressed page data size: " + pageHeader.getUncompressedSize());
              PageReader reader1 =
                  new PageReader(
                      pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
              BatchData batchData = reader1.getAllSatisfiedPageData();
              if (header.getChunkType() == MetaMarker.CHUNK_HEADER) {
                System.out.println("\t\tpoints in the page: " + pageHeader.getNumOfValues());
              } else {
                System.out.println("\t\tpoints in the page: " + batchData.length());
              }
              while (batchData.hasCurrent()) {
                System.out.println(
                    "\t\t\ttime, value: "
                        + batchData.currentTime()
                        + ", "
                        + batchData.currentValue());
                batchData.next();
              }
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            System.out.println("Chunk Group Header position: " + reader.position());
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            System.out.println("device: " + chunkGroupHeader.getDeviceID());
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            System.out.println("minPlanIndex: " + reader.getMinPlanIndex());
            System.out.println("maxPlanIndex: " + reader.getMaxPlanIndex());
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      System.out.println("[Metadata]");
      for (String device : reader.getAllDevices()) {
        Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
        System.out.printf(
            "\t[Device]Device %s, Number of Measurements %d%n", device, seriesMetaData.size());
        for (Map.Entry<String, List<ChunkMetadata>> serie : seriesMetaData.entrySet()) {
          System.out.println("\t\tMeasurement:" + serie.getKey());
          for (ChunkMetadata chunkMetadata : serie.getValue()) {
            System.out.println("\t\tFile offset:" + chunkMetadata.getOffsetOfChunkHeader());
          }
        }
      }
    }
  }
}
