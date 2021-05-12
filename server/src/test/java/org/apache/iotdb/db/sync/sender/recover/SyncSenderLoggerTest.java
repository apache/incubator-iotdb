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
package org.apache.iotdb.db.sync.sender.recover;

import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SyncSenderLoggerTest {

  private SyncSenderLogger senderLogger;
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();
  private FSPath dataDir;

  @Before
  public void setUp()
      throws IOException, InterruptedException, StartupException, DiskSpaceInsufficientException {
    EnvironmentUtils.envSetUp();
    FSPath seqDir = TierManager.getInstance().getAllSequenceFileFolders().get(0);
    dataDir = new FSPath(seqDir.getFsType(), seqDir.toFile().getParentFile().getAbsolutePath());
    config.update(dataDir);
  }

  @After
  public void tearDown() throws InterruptedException, IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSyncSenderLogger() throws IOException {
    senderLogger =
        new SyncSenderLogger(config.getSenderFolderPath().getChildFile(SyncConstant.SYNC_LOG_NAME));
    Set<String> deletedFileNames = new HashSet<>();
    Set<String> deletedFileNamesTest = new HashSet<>();
    senderLogger.startSyncDeletedFilesName();
    for (int i = 0; i < 100; i++) {
      File file = config.getSenderFolderPath().getChildFile("deleted" + i);
      senderLogger.finishSyncDeletedFileName(file);
      deletedFileNames.add(FSPath.parse(file).getAbsoluteFSPath().getRawFSPath());
    }
    Set<String> toBeSyncedFiles = new HashSet<>();
    Set<String> toBeSyncedFilesTest = new HashSet<>();
    senderLogger.startSyncTsFiles();
    for (int i = 0; i < 100; i++) {
      File file = config.getSenderFolderPath().getChildFile("new" + i);
      senderLogger.finishSyncTsfile(file);
      toBeSyncedFiles.add(FSPath.parse(file).getAbsoluteFSPath().getRawFSPath());
    }
    senderLogger.close();
    int count = 0;
    int mode = 0;
    try (BufferedReader br =
        new BufferedReader(
            new FileReader(
                config.getSenderFolderPath().getChildFile(SyncConstant.SYNC_LOG_NAME)))) {
      String line;
      while ((line = br.readLine()) != null) {
        count++;
        if (line.equals(SyncSenderLogger.SYNC_DELETED_FILE_NAME_START)) {
          mode = -1;
        } else if (line.equals(SyncSenderLogger.SYNC_TSFILE_START)) {
          mode = 1;
        } else {
          if (mode == -1) {
            deletedFileNamesTest.add(line);
          } else if (mode == 1) {
            toBeSyncedFilesTest.add(line);
          }
        }
      }
    }
    assertEquals(202, count);
    assertEquals(deletedFileNames.size(), deletedFileNamesTest.size());
    assertEquals(toBeSyncedFiles.size(), toBeSyncedFilesTest.size());
    assertTrue(deletedFileNames.containsAll(deletedFileNamesTest));
    assertTrue(toBeSyncedFiles.containsAll(toBeSyncedFilesTest));
  }
}
