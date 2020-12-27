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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.NoSuchElementException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

/**
 * MultiFileLogReader constructs SingleFileLogReaders for a list of WAL files, and retrieve logs
 * from the files one-by-one.
 */
public class MultiFileLogReader implements ILogReader {

  private SingleFileLogReader currentReader;
  private File[] files;
  private int fileIdx = 0;
  private SingleFileLogReader.Factory fileLogReaderFactory;

  public MultiFileLogReader(File[] files, SingleFileLogReader.Factory fileLogReaderFactory) {
    this.files = files;
    this.fileLogReaderFactory = fileLogReaderFactory;
  }

  @Override
  public void close() {
    if (currentReader != null) {
      currentReader.close();
    }
  }

  @Override
  public boolean hasNext() throws FileNotFoundException {
    if (files == null || files.length == 0) {
      return false;
    }
    if (currentReader == null) {
      currentReader = fileLogReaderFactory.create(files[fileIdx++]);
    }
    if (currentReader.hasNext()) {
      return true;
    }
    while (fileIdx < files.length) {
      currentReader.open(files[fileIdx++]);
      if (currentReader.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public PhysicalPlan next() throws FileNotFoundException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return currentReader.next();
  }
}
