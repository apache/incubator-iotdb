/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.compaction.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;

public class CompactionLogger {

  public static final String COMPACTION_LOG_NAME = ".compaction.log";
  public static final String SOURCE_NAME = "source";
  public static final String TARGET_NAME = "target";
  public static final String SEQUENCE_NAME = "sequence";
  public static final String UNSEQUENCE_NAME = "unsequence";
  public static final String FULL_MERGE = "full mergeUnseq";
  public static final String MERGE_FINISHED = "mergeUnseq finished";

  private BufferedWriter logStream;

  public CompactionLogger(String storageGroupDir, String storageGroupName) throws IOException {
    logStream = new BufferedWriter(
        new FileWriter(SystemFileFactory.INSTANCE.getFile(storageGroupDir,
            storageGroupName + COMPACTION_LOG_NAME), true));
  }

  public void close() throws IOException {
    logStream.close();
  }

  public void logDevice(String device, long offset) throws IOException {
    logStream.write(device + CompactionLogAnalyzer.STR_DEVICE_OFFSET_SEPARATOR + offset);
    logStream.newLine();
    logStream.flush();
  }

  public void logFile(String prefix, File file) throws IOException {
    logStream.write(prefix);
    logStream.newLine();
    logStream.write(file.getPath());
    logStream.newLine();
    logStream.flush();
  }

  public void logSequence(boolean isSeq) throws IOException {
    if (isSeq) {
      logStream.write(SEQUENCE_NAME);
    } else {
      logStream.write(UNSEQUENCE_NAME);
    }
    logStream.newLine();
    logStream.flush();
  }
}
