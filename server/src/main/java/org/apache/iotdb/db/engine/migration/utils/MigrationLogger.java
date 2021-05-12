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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class MigrationLogger {
  public static final String MIGRATION_FOLDER = "migration";
  public static final String MIGRATION_LOG_EXTENSION = ".migration.log";

  static final String SEQUENCE_SOURCE = "sequence files";
  static final String UNSEQUENCE_SOURCE = "unsequence files";
  static final String TARGET_DIR = "target dir";
  static final String MIGRATION_START = "migration start";
  static final String MIGRATION_END = "migration end";
  static final String FILE_START = "start";
  static final String FILE_COPY_END = "copy end";
  static final String FILE_MOVE_END = "move end";
  static final String FILE_END = "end";

  private File logFile;
  private BufferedWriter logStream;

  public MigrationLogger(String storageGroupSysDir, long timPartitionId) throws IOException {
    logFile =
        SystemFileFactory.INSTANCE.getFile(
            storageGroupSysDir,
            MIGRATION_FOLDER
                + File.separator
                + timPartitionId
                + IoTDBConstant.FILE_NAME_SEPARATOR
                + System.currentTimeMillis()
                + MIGRATION_LOG_EXTENSION);
    if (!logFile.getParentFile().exists()) {
      logFile.getParentFile().mkdirs();
    }
    logStream = new BufferedWriter(new FileWriter(logFile));
  }

  public void close() throws IOException {
    logStream.close();
  }

  public void logSourceFiles(List<File> sourceFiles, boolean sequence) throws IOException {
    logStream.write(sequence ? SEQUENCE_SOURCE : UNSEQUENCE_SOURCE);
    logStream.newLine();
    for (File file : sourceFiles) {
      logStream.write(FSPath.parse(file).getAbsoluteFSPath().getRawFSPath());
      logStream.newLine();
    }
    logStream.flush();
  }

  public void logTargetDir(File targetDir) throws IOException {
    logStream.write(TARGET_DIR);
    logStream.newLine();
    logStream.write(FSPath.parse(targetDir).getAbsoluteFSPath().getRawFSPath());
    logStream.newLine();
    logStream.flush();
  }

  public void startMigration() throws IOException {
    logStream.write(MIGRATION_START);
    logStream.newLine();
    logStream.flush();
  }

  public void endMigration() throws IOException {
    logStream.write(MIGRATION_END);
    logStream.newLine();
    logStream.flush();
  }

  public void startMigrateTsFile(File file) throws IOException {
    logStream.write(FILE_START);
    logStream.newLine();
    logStream.write(FSPath.parse(file).getAbsoluteFSPath().getRawFSPath());
    logStream.newLine();
    logStream.flush();
  }

  public void endCopyTsFile() throws IOException {
    logStream.write(FILE_COPY_END);
    logStream.newLine();
    logStream.flush();
  }

  public void endMoveTsFile() throws IOException {
    logStream.write(FILE_MOVE_END);
    logStream.newLine();
    logStream.flush();
  }

  public void endMigrateTsFile() throws IOException {
    logStream.write(FILE_END);
    logStream.newLine();
    logStream.flush();
  }

  public File getLogFile() {
    return logFile;
  }
}
