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

import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ForceAppendTsFileWriter opens a COMPLETE TsFile, reads and truncate its metadata to support
 * appending new data.
 */
public class ForceAppendTsFileWriter extends TsFileIOWriter {

  private long truncatePosition;
  private static Logger logger = LoggerFactory.getLogger(ForceAppendTsFileWriter.class);

  public ForceAppendTsFileWriter(File file) throws IOException {
    if (logger.isDebugEnabled()) {
      logger.debug("{} writer is opened.", file.getName());
    }
    this.file = file;
    this.out =
        FSFactoryProducer.getFileOutputFactory(FSUtils.getFSType(this.file))
            .getTsFileOutput(file.getPath(), true);

    // file doesn't exist
    if (file.length() == 0 || !file.exists()) {
      throw new TsFileNotCompleteException("File " + file.getPath() + " is not a complete TsFile");
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file, true)) {

      // this tsfile is not complete
      if (!reader.isComplete()) {
        throw new TsFileNotCompleteException(
            "File " + file.getPath() + " is not a complete TsFile");
      }
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      // truncate metadata and marker
      truncatePosition = tsFileMetadata.getMetaOffset();

      canWrite = true;
      List<String> devices = reader.getAllDevices();
      for (String device : devices) {
        List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
        reader.readChunkMetadataInDevice(device).values().forEach(chunkMetadataList::addAll);
        ChunkGroupMetadata chunkGroupMetadata = new ChunkGroupMetadata(device, chunkMetadataList);
        chunkGroupMetadataList.add(chunkGroupMetadata);
      }
    }
  }

  public void doTruncate() throws IOException {
    out.truncate(truncatePosition);
  }

  public long getTruncatePosition() {
    return truncatePosition;
  }
}
