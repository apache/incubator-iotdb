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

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** (1) print the total size of valid Chunks (2) print the percentage of valid bytes in a TsFile. */
public class TsFileCheckTool {
  public static void main(String[] args) throws IOException {
    // file path
    String path = "test.tsfile";
    if (args.length >= 1) {
      path = args[0];
    }

    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    List<String> devices = reader.getAllDevices();
    long totalSize = 0;
    for (String device : devices) {
      Map<String, List<ChunkMetadata>> sensorMeasurementMap =
          reader.readChunkMetadataInDevice(device);
      System.out.println("start to restore device: " + device);
      for (Entry<String, List<ChunkMetadata>> sensorMeasurementEntry :
          sensorMeasurementMap.entrySet()) {
        for (ChunkMetadata chunkMetadata : sensorMeasurementEntry.getValue()) {
          Chunk chunk = reader.readMemChunk(chunkMetadata);
          int dataSize = chunk.getHeader().getDataSize();
          totalSize += chunkMetadata.calculateRamSize();
          totalSize += dataSize;
          System.out.println(
              "read chunk points num: "
                  + chunkMetadata.getNumOfPoints()
                  + " startTime: "
                  + chunkMetadata.getStartTime()
                  + " endTime: "
                  + chunkMetadata.getEndTime()
                  + " dataSize: "
                  + dataSize);
        }
      }
    }
    System.out.println("valid chunk size: " + totalSize * 1.0 / 1024 / 1024 / 1024 + "GB");
    System.out.println(
        "valid chunk percentage: " + totalSize * 1.0 / new File(path).length() + "%");
    System.out.println("check file done");
  }
}
