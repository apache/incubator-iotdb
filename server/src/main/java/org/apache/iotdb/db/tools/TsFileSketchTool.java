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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.MetadataIndexEntry;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TsFileSketchTool {

  public static void main(String[] args) throws IOException {
    Pair<String, String> fileNames = checkArgs(args);
    String filename = fileNames.left;
    String outFile = fileNames.right;
    System.out.println("TsFile path:" + filename);
    System.out.println("Sketch save path:" + outFile);
    try (PrintWriter pw = new PrintWriter(new FileWriter(outFile))) {
      File file = FSPath.parse(filename).toFile();
      long length = file.length();
      printlnBoth(
          pw, "-------------------------------- TsFile Sketch --------------------------------");
      printlnBoth(pw, "file path: " + filename);
      printlnBoth(pw, "file length: " + length);

      // get metadata information
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file)) {
        TsFileMetadata tsFileMetaData = reader.readFileMetadata();
        List<ChunkGroupMetadata> allChunkGroupMetadata = new ArrayList<>();
        reader.selfCheck(null, allChunkGroupMetadata, false);

        // begin print
        StringBuilder str1 = new StringBuilder();
        for (int i = 0; i < 21; i++) {
          str1.append("|");
        }

        printlnBoth(pw, "");
        printlnBoth(pw, String.format("%20s", "POSITION") + "|\tCONTENT");
        printlnBoth(pw, String.format("%20s", "--------") + " \t-------");
        printlnBoth(pw, String.format("%20d", 0) + "|\t[magic head] " + reader.readHeadMagic());
        printlnBoth(
            pw,
            String.format("%20d", TSFileConfig.MAGIC_STRING.getBytes().length)
                + "|\t[version number] "
                + reader.readVersionNumber());
        long nextChunkGroupHeaderPos =
            (long) TSFileConfig.MAGIC_STRING.getBytes().length + Byte.BYTES;
        // ChunkGroup begins
        for (ChunkGroupMetadata chunkGroupMetadata : allChunkGroupMetadata) {
          printlnBoth(
              pw,
              str1
                  + "\t[Chunk Group] of "
                  + chunkGroupMetadata.getDevice()
                  + ", num of Chunks:"
                  + chunkGroupMetadata.getChunkMetadataList().size());
          // chunkGroupHeader begins
          printlnBoth(
              pw, String.format("%20s", nextChunkGroupHeaderPos) + "|\t[Chunk Group Header]");
          ChunkGroupHeader chunkGroupHeader =
              reader.readChunkGroupHeader(nextChunkGroupHeaderPos, false);
          printlnBoth(pw, String.format("%20s", "") + "|\t\t[marker] 0");
          printlnBoth(
              pw, String.format("%20s", "") + "|\t\t[deviceID] " + chunkGroupHeader.getDeviceID());
          // chunk begins
          for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
            Chunk chunk = reader.readMemChunk(chunkMetadata);
            printlnBoth(
                pw,
                String.format("%20d", chunkMetadata.getOffsetOfChunkHeader())
                    + "|\t[Chunk] of "
                    + chunkMetadata.getMeasurementUid()
                    + ", numOfPoints:"
                    + chunkMetadata.getNumOfPoints()
                    + ", time range:["
                    + chunkMetadata.getStartTime()
                    + ","
                    + chunkMetadata.getEndTime()
                    + "], tsDataType:"
                    + chunkMetadata.getDataType()
                    + ", \n"
                    + String.format("%20s", "")
                    + " \t"
                    + chunkMetadata.getStatistics());
            printlnBoth(
                pw,
                String.format("%20s", "") + "|\t\t[marker] " + chunk.getHeader().getChunkType());
            nextChunkGroupHeaderPos =
                chunkMetadata.getOffsetOfChunkHeader()
                    + chunk.getHeader().getSerializedSize()
                    + chunk.getHeader().getDataSize()
                    + 17; // skip the PlanIndex
          }

          printlnBoth(pw, str1 + "\t[Chunk Group] of " + chunkGroupMetadata.getDevice() + " ends");
        }

        // metadata begins
        if (tsFileMetaData.getMetadataIndex().getChildren().isEmpty()) {
          printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos() - 1) + "|\t[marker] 2");
        } else {
          printlnBoth(
              pw,
              String.format("%20s", reader.readFileMetadata().getMetaOffset()) + "|\t[marker] 2");
        }

        Map<String, List<TimeseriesMetadata>> allTimeseriesMetadata =
            reader.getAllTimeseriesMetadata();
        Map<String, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap = new TreeMap<>();

        for (Map.Entry<String, List<TimeseriesMetadata>> entry : allTimeseriesMetadata.entrySet()) {
          String device = entry.getKey();
          List<TimeseriesMetadata> seriesMetadataList = entry.getValue();
          for (TimeseriesMetadata seriesMetadata : seriesMetadataList) {
            timeseriesMetadataMap.put(
                seriesMetadata.getMeasurementId(),
                new Pair<>(new Path(device, seriesMetadata.getMeasurementId()), seriesMetadata));
          }
        }
        for (Map.Entry<String, Pair<Path, TimeseriesMetadata>> entry :
            timeseriesMetadataMap.entrySet()) {
          printlnBoth(
              pw,
              entry.getKey()
                  + "|\t[ChunkMetadataList] of "
                  + entry.getValue().left
                  + ", tsDataType:"
                  + entry.getValue().right.getTSDataType());
          printlnBoth(
              pw,
              String.format("%20s", "") + "|\t[" + entry.getValue().right.getStatistics() + "] ");
        }

        for (MetadataIndexEntry metadataIndex : tsFileMetaData.getMetadataIndex().getChildren()) {
          printlnBoth(
              pw,
              String.format("%20s", metadataIndex.getOffset())
                  + "|\t[MetadataIndex] of "
                  + metadataIndex.getName());
        }

        printlnBoth(pw, String.format("%20s", reader.getFileMetadataPos()) + "|\t[TsFileMetadata]");
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[num of devices] "
                + tsFileMetaData.getMetadataIndex().getChildren().size());
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t"
                + tsFileMetaData.getMetadataIndex().getChildren().size()
                + " key&TsMetadataIndex");

        // bloom filter
        BloomFilter bloomFilter = tsFileMetaData.getBloomFilter();
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[bloom filter bit vector byte array length] "
                + bloomFilter.serialize().length);
        printlnBoth(pw, String.format("%20s", "") + "|\t\t[bloom filter bit vector byte array] ");
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[bloom filter number of bits] "
                + bloomFilter.getSize());
        printlnBoth(
            pw,
            String.format("%20s", "")
                + "|\t\t[bloom filter number of hash functions] "
                + bloomFilter.getHashFunctionSize());

        printlnBoth(
            pw,
            String.format("%20s", (reader.getFileMetadataPos() + reader.getFileMetadataSize()))
                + "|\t[TsFileMetadataSize] "
                + reader.getFileMetadataSize());

        printlnBoth(
            pw,
            String.format("%20s", reader.getFileMetadataPos() + reader.getFileMetadataSize() + 4)
                + "|\t[magic tail] "
                + reader.readTailMagic());

        printlnBoth(pw, String.format("%20s", length) + "|\tEND of TsFile");

        printlnBoth(pw, "");

        printlnBoth(
            pw,
            "---------------------------------- TsFile Sketch End ----------------------------------");
      }
    }
  }

  private static void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }

  private static Pair<String, String> checkArgs(String[] args) {
    String filename = "test.tsfile";
    String outFile = "TsFile_sketch_view.txt";
    if (args.length == 1) {
      filename = args[0];
    } else if (args.length == 2) {
      filename = args[0];
      outFile = args[1];
    }
    return new Pair<>(filename, outFile);
  }
}
