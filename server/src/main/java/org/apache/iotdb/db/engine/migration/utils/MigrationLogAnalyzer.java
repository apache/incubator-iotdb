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

package org.apache.iotdb.db.engine.migration.utils;

import org.apache.iotdb.db.engine.migration.task.IMigrationTask.FileMigrationStatus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.FILE_COPY_END;
import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.FILE_END;
import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.FILE_MOVE_END;
import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.FILE_START;
import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.MIGRATION_END;
import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.MIGRATION_START;
import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.SEQUENCE_SOURCE;
import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.TARGET_DIR;
import static org.apache.iotdb.db.engine.migration.utils.MigrationLogger.UNSEQUENCE_SOURCE;

public class MigrationLogAnalyzer {
  private final File logFile;
  /** migration status of files */
  private final Map<String, FileMigrationStatus> fileStatusByPath = new LinkedHashMap<>();

  private String targetDir;
  private boolean sequence;

  public MigrationLogAnalyzer(File logFile) {
    this.logFile = logFile;
  }

  public void analyze() throws IOException {
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      String currLine;
      String currStatus = "";
      String currFile = "";
      while ((currLine = bufferedReader.readLine()) != null) {
        // resolve status line
        switch (currLine) {
          case SEQUENCE_SOURCE:
            sequence = true;
            currStatus = SEQUENCE_SOURCE;
            break;
          case UNSEQUENCE_SOURCE:
            sequence = false;
            currStatus = UNSEQUENCE_SOURCE;
            break;
          case TARGET_DIR:
            currStatus = TARGET_DIR;
            break;
          case MIGRATION_START:
            currStatus = MIGRATION_START;
            break;
          case MIGRATION_END:
            currStatus = MIGRATION_END;
            break;
          case FILE_START:
            currStatus = FILE_START;
            break;
          case FILE_COPY_END:
            currStatus = FILE_COPY_END;
            fileStatusByPath.put(currFile, FileMigrationStatus.COPY_END);
            break;
          case FILE_MOVE_END:
            currStatus = FILE_MOVE_END;
            fileStatusByPath.put(currFile, FileMigrationStatus.MOVE_END);
            break;
          case FILE_END:
            currStatus = FILE_END;
            fileStatusByPath.put(currFile, FileMigrationStatus.END);
            break;
          default:
            // resolve path line
            switch (currStatus) {
              case SEQUENCE_SOURCE:
              case UNSEQUENCE_SOURCE:
                fileStatusByPath.put(currLine, FileMigrationStatus.NONE);
                break;
              case TARGET_DIR:
                targetDir = currLine;
                break;
              case FILE_START:
                currFile = currLine;
                fileStatusByPath.put(currFile, FileMigrationStatus.START);
                break;
              default:
                break;
            }
            break;
        }
      }
    }
  }

  public boolean isSequence() {
    return sequence;
  }

  public List<String> getFiles() {
    return new ArrayList<>(fileStatusByPath.keySet());
  }

  public FileMigrationStatus getMigrationStatus(String filename) {
    return fileStatusByPath.get(filename);
  }

  public String getTargetDir() {
    return targetDir;
  }
}
