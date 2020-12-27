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
package org.apache.iotdb.db.writelog.io;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SingleFileLogReader reads binarized WAL logs from a file through a DataInputStream by scanning
 * the file from head to tail.
 */
public class SingleFileLogReader implements ILogReader {

  private static final Logger logger = LoggerFactory.getLogger(SingleFileLogReader.class);
  public static final int LEAST_LOG_SIZE = 12; // size + checksum

  private DataInputStream logStream;
  private String filepath;

  private byte[] buffer;
  private CRC32 checkSummer = new CRC32();

  // used to indicate the position of the broken log
  private int idx;

  private BatchLogReader batchLogReader;

  private boolean fileCorrupted = false;

  public SingleFileLogReader(File logFile) throws FileNotFoundException {
    open(logFile);
  }

  @Override
  public boolean hasNext() {
    try {
      if (batchLogReader != null && batchLogReader.hasNext()) {
        return true;
      }

      if (logStream.available() < LEAST_LOG_SIZE) {
        return false;
      }

      int logSize = logStream.readInt();
      if (logSize <= 0) {
        return false;
      }
      buffer = new byte[logSize];

      int readLen = logStream.read(buffer, 0, logSize);
      if (readLen < logSize) {
        throw new IOException("Reach eof");
      }

      final long checkSum = logStream.readLong();
      checkSummer.reset();
      checkSummer.update(buffer, 0, logSize);
      if (checkSummer.getValue() != checkSum) {
        throw new IOException(String.format("The check sum of the No.%d log batch is incorrect! In "
            + "file: "
            + "%d Calculated: %d.", idx, checkSum, checkSummer.getValue()));
      }

      batchLogReader = getBatchLogReader(ByteBuffer.wrap(buffer));
      fileCorrupted = fileCorrupted || batchLogReader.isFileCorrupted();
    } catch (Exception e) {
      logger.error("Cannot read more PhysicalPlans from {} because", filepath, e);
      fileCorrupted = true;
      return false;
    }
    return true;
  }

  BatchLogReader getBatchLogReader(ByteBuffer byteBuffer) {
    return new BatchLogReader(byteBuffer);
  }

  @Override
  public PhysicalPlan next() {
    if (!hasNext()){
      throw new NoSuchElementException();
    }

    idx ++;
    return batchLogReader.next();
  }

  @Override
  public void close() {
    if (logStream != null) {
      try {
        logStream.close();
      } catch (IOException e) {
        logger.error("Cannot close log file {}", filepath, e);
      }
    }
  }

  public void open(File logFile) throws FileNotFoundException {
    close();
    logStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    this.filepath = logFile.getPath();
    idx = 0;
  }

  public boolean isFileCorrupted() {
    return fileCorrupted;
  }

  public interface Factory {

    SingleFileLogReader create(File file) throws FileNotFoundException;
  }
}
