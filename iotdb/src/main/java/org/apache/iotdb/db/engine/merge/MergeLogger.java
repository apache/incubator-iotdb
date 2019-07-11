/**
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

package org.apache.iotdb.db.engine.merge;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.Path;

public class MergeLogger {

  public static final String MERGE_LOG_NAME = "merge.log";

  public static final String STR_SEQ_FILES = "seqFiles";
  public static final String STR_UNSEQ_FILES = "unseqFiles";
  public static final String STR_START = "start";
  public static final String STR_END = "end";
  public static final String STR_ALL_TS_END = "all ts end";
  public static final String STR_MERGE_START = "merge start";
  public static final String STR_MERGE_END = "merge end";

  private BufferedWriter logStream;

  public MergeLogger(String storageGroupDir) throws IOException {
    logStream = new BufferedWriter(new FileWriter(new File(storageGroupDir, MERGE_LOG_NAME), true));
  }

  public void close() throws IOException {
    logStream.close();
  }

  public void logSeqFiles(List<TsFileResource> seqFiles) throws IOException {
    logStream.write(STR_SEQ_FILES);
    logStream.newLine();
    for (TsFileResource tsFileResource : seqFiles) {
      logStream.write(tsFileResource.getFile().getAbsolutePath());
    }
    logStream.flush();
  }

  public void logUnseqFiles(List<TsFileResource> unseqFiles) throws IOException {
    logStream.write(STR_UNSEQ_FILES);
    logStream.newLine();
    for (TsFileResource tsFileResource : unseqFiles) {
      logStream.write(tsFileResource.getFile().getAbsolutePath());
    }
    logStream.flush();
  }

  public void logMergeStart() throws IOException {
    logStream.write(STR_MERGE_START);
    logStream.newLine();
    logStream.flush();
  }

  public void logTSStart(Path path) throws IOException {
    logStream.write(path.getFullPath() + " " + STR_START);
    logStream.newLine();
    logStream.flush();
  }

  public void logFilePositionUpdate(File file) throws IOException {
    logStream.write(String.format("%s %d", file.getAbsolutePath(), file.length()));
    logStream.newLine();
    logStream.flush();
  }

  public void logTSEnd(Path path) throws IOException {
    logStream.write(path.getFullPath() + " " + STR_END);
    logStream.newLine();
    logStream.flush();
  }

  public void logAllTsEnd() throws IOException {
    logStream.write(STR_ALL_TS_END);
    logStream.newLine();
    logStream.flush();
  }

  public void logFileMergeStart(File file, long position) throws IOException {
    logStream.write(String.format("%s %d", file.getAbsolutePath(), position));
    logStream.newLine();
    logStream.flush();
  }

  public void logFileMergeEnd(File file) throws IOException {
    logStream.write(file.getAbsolutePath() + " " + STR_END);
    logStream.newLine();
    logStream.flush();
  }

  public void logMergeEnd() throws IOException {
    logStream.write(STR_MERGE_END);
    logStream.newLine();
    logStream.flush();
  }
}
