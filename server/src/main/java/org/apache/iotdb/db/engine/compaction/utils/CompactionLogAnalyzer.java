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

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.FULL_MERGE;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SEQUENCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.UNSEQUENCE_NAME;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CompactionLogAnalyzer {

  public static final String STR_DEVICE_OFFSET_SEPARATOR = " ";

  private File logFile;
  private Set<String> deviceSet = new HashSet<>();
  private long offset = 0;
  private List<String> sourceFiles = new ArrayList<>();
  private String targetFile = null;
  private boolean isSeq = false;
  private boolean fullMerge = false;

  public CompactionLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  /**
   * @return analyze (written device set, last offset, source file list, target file , is contains
   * mergeUnseq finished)
   */
  public void analyze() throws IOException {
    String currLine;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      while ((currLine = bufferedReader.readLine()) != null) {
        switch (currLine) {
          case SOURCE_NAME:
            currLine = bufferedReader.readLine();
            sourceFiles.add(currLine);
            break;
          case TARGET_NAME:
            currLine = bufferedReader.readLine();
            targetFile = currLine;
            break;
          case FULL_MERGE:
            fullMerge = true;
            break;
          case SEQUENCE_NAME:
            isSeq = true;
            break;
          case UNSEQUENCE_NAME:
            isSeq = false;
            break;
          default:
            int separatorIndex = currLine.lastIndexOf(STR_DEVICE_OFFSET_SEPARATOR);
            deviceSet.add(currLine.substring(0, separatorIndex));
            offset = Long.parseLong(currLine.substring(separatorIndex + 1));
            break;
        }
      }
    }
  }

  public Set<String> getDeviceSet() {
    return deviceSet;
  }

  public long getOffset() {
    return offset;
  }

  public List<String> getSourceFiles() {
    return sourceFiles;
  }

  public String getTargetFile() {
    return targetFile;
  }

  public boolean isSeq() {
    return isSeq;
  }

  public boolean isFullMerge() {
    return fullMerge;
  }
}
