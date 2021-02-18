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

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.query.reader.chunk.ChunkDataIterator;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;

public class TsFileSinglePathRead {

  public static void main(String[] args) throws IOException {
    Pair<String, String> params = checkArgs(args);
    printTsFileSinglePath(params);
  }

  public static int printTsFileSinglePath(Pair<String, String> params) throws IOException{
    String filename = params.left;
    String path = params.right;
    int numCnt = 0;
    try (TsFileSequenceReader reader = new TsFileSequenceReader(filename)) {
      if (!reader.isComplete()){
        throw new RuntimeException("The analyzed data file is incomplete, process will stop");
      }
      // get the chunkMetaList of the specific path
      List<ChunkMetadata> chunkMetadataList = reader
          .getChunkMetadataList(new Path(path, true), false);
      for (ChunkMetadata metadata : chunkMetadataList) {
        System.out.println("|--[Chunk]");
        ChunkReader chunkReader = new ChunkReader(reader.readMemChunk(metadata), null);
        ChunkDataIterator chunkDataIterator = new ChunkDataIterator(chunkReader);
        while (chunkDataIterator.hasNextTimeValuePair()) {
          TimeValuePair pair = chunkDataIterator.nextTimeValuePair();
          System.out.println(
              "\t\t\ttime, value: " + pair.getTimestamp() + ", " + pair.getValue());
          numCnt++;
        }
      }
    }
    return numCnt;
  }

  private static Pair<String, String> checkArgs(String[] args) {
    String filename;
    String path;
    if (args.length >= 2) {
      filename = args[0];
      path = args[1];
    } else {
      throw new RuntimeException("Input args length less than two. Example usage: "
          + "tools/print-tsfile-specific-measurement.sh file_path timeseries_path");
    }
    return new Pair<>(filename, path);
  }
}



