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

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The class is to restore the big file with broken page data (1) generate a new TsFile from an old
 * TsFile (2) remove the old TsFile (3) rename the new TsFile to the old TsFile's name.
 */
public class BigFileRestoreTool {
  public static void main(String[] args) throws IOException {
    // file path
    String path = "test.tsfile";
    if (args.length >= 1) {
      path = args[0];
    }

    File restoreFile = new File(path + ".restore");
    RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(restoreFile);
    TsFileResource restoreResource = new TsFileResource(restoreFile);
    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    List<String> devices = reader.getAllDevices();
    for (String device : devices) {
      writer.startChunkGroup(device);
      Map<String, List<ChunkMetadata>> sensorMeasurementMap =
          reader.readChunkMetadataInDevice(device);
      System.out.println("start to restore device: " + device);
      for (Entry<String, List<ChunkMetadata>> sensorMeasurementEntry :
          sensorMeasurementMap.entrySet()) {
        for (ChunkMetadata chunkMetadata : sensorMeasurementEntry.getValue()) {
          Chunk chunk = reader.readMemChunk(chunkMetadata);
          writer.writeChunk(chunk, chunkMetadata);
          restoreResource.updateStartTime(device, chunkMetadata.getStartTime());
          restoreResource.updateEndTime(device, chunkMetadata.getEndTime());
          System.out.println(
              "restore chunk points num: "
                  + chunkMetadata.getNumOfPoints()
                  + " startTime: "
                  + chunkMetadata.getStartTime()
                  + " endTime: "
                  + chunkMetadata.getEndTime()
                  + " dataSize: "
                  + chunk.getHeader().getDataSize());
        }
      }
      writer.endChunkGroup();
    }
    restoreResource.setMinPlanIndex(reader.getMinPlanIndex());
    restoreResource.setMaxPlanIndex(reader.getMaxPlanIndex());
    restoreResource.serialize();
    writer.endFile();
    restoreResource.close();

    System.out.println("start to move file");
    File originFile = new File(path);
    originFile.delete();
    restoreFile.renameTo(originFile);
    File originFileResource = new File(path + ".resource");
    originFileResource.delete();
    File restoreFileResource = new File(restoreFile + ".resource");
    restoreFileResource.renameTo(originFileResource);

    System.out.println("restore file done");
  }
}
