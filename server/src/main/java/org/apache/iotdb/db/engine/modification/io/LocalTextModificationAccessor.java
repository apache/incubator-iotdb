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

package org.apache.iotdb.db.engine.modification.io;

import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * LocalTextModificationAccessor uses a file on local file system to store the modifications in text
 * format, and writes modifications by appending to the tail of the file.
 */
public class LocalTextModificationAccessor
    implements ModificationReader, ModificationWriter, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(LocalTextModificationAccessor.class);
  private static final String SEPARATOR = ",";
  private static final String ABORT_MARK = "aborted";

  private File file;
  private BufferedWriter writer;
  private final FSFactory fsFactory;

  /**
   * Construct a LocalTextModificationAccessor using a file specified by filePath.
   *
   * @param file the file that is used for storing modifications.
   */
  public LocalTextModificationAccessor(File file) {
    this.file = file;
    this.fsFactory = FSFactoryProducer.getFSFactory(FSUtils.getFSType(file));
  }

  @Override
  public Collection<Modification> read() {
    if (!file.exists()) {
      logger.debug("No modification has been written to this file");
      return new ArrayList<>();
    }

    String line;
    List<Modification> modificationList = new ArrayList<>();
    try (BufferedReader reader = fsFactory.getBufferedReader(file.getPath())) {
      while ((line = reader.readLine()) != null) {
        if (line.equals(ABORT_MARK) && !modificationList.isEmpty()) {
          modificationList.remove(modificationList.size() - 1);
        } else {
          modificationList.add(decodeModification(line));
        }
      }
    } catch (IOException e) {
      logger.error(
          "An error occurred when reading modifications, and the remaining modifications "
              + "were ignored.",
          e);
    }
    return modificationList;
  }

  @Override
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }

  @Override
  public void abort() throws IOException {
    if (writer == null) {
      writer = fsFactory.getBufferedWriter(file.getPath(), true);
    }
    writer.write(ABORT_MARK);
    writer.newLine();
    writer.flush();
  }

  @Override
  public void write(Modification mod) throws IOException {
    if (writer == null) {
      writer = fsFactory.getBufferedWriter(file.getPath(), true);
    }
    writer.write(encodeModification(mod));
    writer.newLine();
    writer.flush();
  }

  private static String encodeModification(Modification mod) {
    if (mod instanceof Deletion) return encodeDeletion((Deletion) mod);
    return null;
  }

  private static Modification decodeModification(String src) throws IOException {
    String[] fields = src.split(SEPARATOR);
    if (Modification.Type.DELETION.name().equals(fields[0])) {
      return decodeDeletion(fields);
    }
    throw new IOException("Unknown modification type: " + fields[0]);
  }

  private static String encodeDeletion(Deletion del) {
    return del.getType()
        + SEPARATOR
        + del.getPathString()
        + SEPARATOR
        + del.getFileOffset()
        + SEPARATOR
        + del.getStartTime()
        + SEPARATOR
        + del.getEndTime();
  }

  /**
   * Decode a range deletion record. E.g. "DELETION,root.ln.wf01.wt01.temperature,111,100,300" the
   * index of field endTimestamp is length - 1, startTimestamp is length - 2, TsFile offset is
   * length - 3. Fields in index range [1, length -3) all belong to a timeseries path in case when
   * the path contains comma.
   */
  private static Deletion decodeDeletion(String[] fields) throws IOException {
    if (fields.length < 4) {
      throw new IOException("Incorrect deletion fields number: " + fields.length);
    }

    String path = "";
    long startTimestamp;
    long endTimestamp;
    long tsFileOffset;
    try {
      tsFileOffset = Long.parseLong(fields[fields.length - 3]);
    } catch (NumberFormatException e) {
      return decodePointDeletion(fields);
    }

    try {
      endTimestamp = Long.parseLong(fields[fields.length - 1]);
      startTimestamp = Long.parseLong(fields[fields.length - 2]);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid timestamp: " + e.getMessage());
    }
    try {
      String[] pathArray = Arrays.copyOfRange(fields, 1, fields.length - 3);
      path = String.join(SEPARATOR, pathArray);
      return new Deletion(new PartialPath(path), tsFileOffset, startTimestamp, endTimestamp);
    } catch (IllegalPathException e) {
      throw new IOException("Invalid series path: " + path);
    }
  }

  /**
   * Decode a point deletion record. E.g. "DELETION,root.ln.wf01.wt01.temperature,111,300" the index
   * of field endTimestamp is length - 1, versionNum is length - 2. Fields in index range [1, length
   * - 2) compose timeseries path.
   */
  private static Deletion decodePointDeletion(String[] fields) throws IOException {
    String path = "";
    long versionNum;
    long endTimestamp;
    try {
      endTimestamp = Long.parseLong(fields[fields.length - 1]);
      versionNum = Long.parseLong(fields[fields.length - 2]);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid timestamp: " + e.getMessage());
    }
    try {
      String[] pathArray = Arrays.copyOfRange(fields, 1, fields.length - 2);
      path = String.join(SEPARATOR, pathArray);
      return new Deletion(new PartialPath(path), versionNum, endTimestamp);
    } catch (IllegalPathException e) {
      throw new IOException("Invalid series path: " + path);
    }
  }
}
