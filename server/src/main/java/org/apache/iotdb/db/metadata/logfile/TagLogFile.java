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
package org.apache.iotdb.db.metadata.logfile;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;

public class TagLogFile implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TagLogFile.class);
  private FileChannel fileChannel;
  private static final String LENGTH_EXCEED_MSG =
      "Tag/Attribute exceeds the max length limit. "
          + "Please enlarge tag_attribute_total_size in iotdb-engine.properties";

  private static final int MAX_LENGTH =
      IoTDBDescriptor.getInstance().getConfig().getTagAttributeTotalSize();

  private static final byte FILL_BYTE = 0;

  public TagLogFile(String schemaDir, String logFileName) throws IOException {

    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      if (metadataDir.mkdirs()) {
        logger.info("create schema folder {}.", metadataDir);
      } else {
        logger.info("create schema folder {} failed.", metadataDir);
      }
    }

    File logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);

    this.fileChannel =
        FileChannel.open(
            logFile.toPath(),
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.DSYNC);
    // move the current position to the tail of the file
    this.fileChannel.position(fileChannel.size());
  }

  /** @return tags map, attributes map */
  public Pair<Map<String, String>, Map<String, String>> read(int size, long position)
      throws IOException {
    if (position < 0) {
      return new Pair<>(Collections.emptyMap(), Collections.emptyMap());
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    fileChannel.read(byteBuffer, position);
    byteBuffer.flip();
    return new Pair<>(ReadWriteIOUtils.readMap(byteBuffer), ReadWriteIOUtils.readMap(byteBuffer));
  }

  public Map<String, String> readTag(int size, long position) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    fileChannel.read(byteBuffer, position);
    byteBuffer.flip();
    return ReadWriteIOUtils.readMap(byteBuffer);
  }

  public long write(Map<String, String> tagMap, Map<String, String> attributeMap)
      throws IOException, MetadataException {
    long offset = fileChannel.position();
    ByteBuffer byteBuffer = convertMapToByteBuffer(tagMap, attributeMap);
    fileChannel.write(byteBuffer);
    return offset;
  }

  /** This method does not modify this file's current position. */
  public void write(Map<String, String> tagMap, Map<String, String> attributeMap, long position)
      throws IOException, MetadataException {
    ByteBuffer byteBuffer = convertMapToByteBuffer(tagMap, attributeMap);
    fileChannel.write(byteBuffer, position);
  }

  private ByteBuffer convertMapToByteBuffer(
      Map<String, String> tagMap, Map<String, String> attributeMap) throws MetadataException {
    ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_LENGTH);
    int length = serializeMap(tagMap, byteBuffer, 0);
    length = serializeMap(attributeMap, byteBuffer, length);

    // fill the remaining space
    for (int i = length + 1; i <= MAX_LENGTH; i++) {
      byteBuffer.put(FILL_BYTE);
    }

    // persist to the disk
    byteBuffer.flip();
    return byteBuffer;
  }

  private int serializeMap(Map<String, String> map, ByteBuffer byteBuffer, int length)
      throws MetadataException {
    if (map == null) {
      length += Integer.BYTES;
      if (length > MAX_LENGTH) {
        throw new MetadataException(LENGTH_EXCEED_MSG);
      }
      ReadWriteIOUtils.write(0, byteBuffer);
      return length;
    }
    length += Integer.BYTES;
    if (length > MAX_LENGTH) {
      throw new MetadataException(LENGTH_EXCEED_MSG);
    }
    ReadWriteIOUtils.write(map.size(), byteBuffer);
    byte[] bytes;
    for (Map.Entry<String, String> entry : map.entrySet()) {
      // serialize key
      bytes = entry.getKey().getBytes();
      length += (4 + bytes.length);
      if (length > MAX_LENGTH) {
        throw new MetadataException(LENGTH_EXCEED_MSG);
      }
      ReadWriteIOUtils.write(bytes.length, byteBuffer);
      byteBuffer.put(bytes);

      // serialize value
      bytes = entry.getValue().getBytes();
      length += (4 + bytes.length);
      if (length > MAX_LENGTH) {
        throw new MetadataException(LENGTH_EXCEED_MSG);
      }
      ReadWriteIOUtils.write(bytes.length, byteBuffer);
      byteBuffer.put(bytes);
    }
    return length;
  }

  @Override
  public void close() throws IOException {
    fileChannel.force(true);
    fileChannel.close();
    fileChannel = null;
  }
}
