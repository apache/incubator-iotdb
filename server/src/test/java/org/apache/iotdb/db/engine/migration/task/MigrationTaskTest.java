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

package org.apache.iotdb.db.engine.migration.task;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class MigrationTaskTest extends MigrationTest {
  private final boolean sequence = true;
  private final int timePartitionId = 0;

  @Test
  public void testMigrate() {
    MigrationTask task =
        new MigrationTask(
            srcResources,
            targetDir,
            sequence,
            timePartitionId,
            this::migrationCallback,
            testSgName,
            testSgSysDir);
    task.run();

    for (File src : srcFiles) {
      File srcResource = fsFactory.getFile(src.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertFalse(src.exists());
      Assert.assertFalse(srcResource.exists());

      File target = fsFactory.getFile(testSgTier2Dir, src.getName());
      File targetResource = fsFactory.getFile(target.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertTrue(target.exists());
      Assert.assertTrue(targetResource.exists());
    }
  }

  @Test
  public void testMigrateNotSynced() throws IOException {
    File tier1SyncFolder =
        fsFactory.getFile(
            TestConstant.OUTPUT_DATA_DIR.concat(
                "tier1"
                    + File.separator
                    + SyncConstant.SYNC_SENDER
                    + File.separator
                    + "127.0.01_5555"));
    File tier2SyncFolder =
        fsFactory.getFile(
            TestConstant.OUTPUT_DATA_DIR.concat(
                "tier2"
                    + File.separator
                    + SyncConstant.SYNC_SENDER
                    + File.separator
                    + "127.0.01_5555"));
    tier1SyncFolder.mkdirs();
    tier2SyncFolder.mkdirs();

    MigrationTask task =
        new MigrationTask(
            srcResources,
            targetDir,
            sequence,
            timePartitionId,
            this::migrationCallback,
            testSgName,
            testSgSysDir);
    task.run();

    for (File src : srcFiles) {
      File srcResource = fsFactory.getFile(src.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertFalse(src.exists());
      Assert.assertFalse(srcResource.exists());

      File target = fsFactory.getFile(testSgTier2Dir, src.getName());
      File targetResource = fsFactory.getFile(target.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertTrue(target.exists());
      Assert.assertTrue(targetResource.exists());
    }

    // verify deleted_files_blacklist
    File deletedBlacklistFile =
        fsFactory.getFile(tier1SyncFolder, SyncConstant.DELETED_BLACKLIST_FILE_NAME);
    Assert.assertFalse(deletedBlacklistFile.exists());

    // verify to_be_synced_files_blacklist
    File toBeSyncedBlacklistFile =
        fsFactory.getFile(tier2SyncFolder, SyncConstant.TO_BE_SYNCED_BLACKLIST_FILE_NAME);
    Assert.assertFalse(toBeSyncedBlacklistFile.exists());
  }

  @Test
  public void testMigrateAlreadySynced() throws IOException {
    File tier1SyncFolder =
        fsFactory.getFile(
            TestConstant.OUTPUT_DATA_DIR.concat(
                "tier1"
                    + File.separator
                    + SyncConstant.SYNC_SENDER
                    + File.separator
                    + "127.0.01_5555"));
    File tier2SyncFolder =
        fsFactory.getFile(
            TestConstant.OUTPUT_DATA_DIR.concat(
                "tier2"
                    + File.separator
                    + SyncConstant.SYNC_SENDER
                    + File.separator
                    + "127.0.01_5555"));
    tier1SyncFolder.mkdirs();
    tier2SyncFolder.mkdirs();
    File lastLocalFileInfo = fsFactory.getFile(tier1SyncFolder, SyncConstant.LAST_LOCAL_FILE_NAME);
    try (BufferedWriter bw =
        fsFactory.getBufferedWriter(lastLocalFileInfo.getAbsolutePath(), false)) {
      for (File file : srcFiles) {
        bw.write(FSPath.parse(file).getAbsoluteFSPath().getRawFSPath());
        bw.newLine();
      }
      bw.flush();
    }

    MigrationTask task =
        new MigrationTask(
            srcResources,
            targetDir,
            sequence,
            timePartitionId,
            this::migrationCallback,
            testSgName,
            testSgSysDir);
    task.run();

    List<File> targetFiles = new ArrayList<>();
    for (File src : srcFiles) {
      File srcResource = fsFactory.getFile(src.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertFalse(src.exists());
      Assert.assertFalse(srcResource.exists());

      File target = fsFactory.getFile(testSgTier2Dir, src.getName());
      File targetResource = fsFactory.getFile(target.getPath() + TsFileResource.RESOURCE_SUFFIX);
      Assert.assertTrue(target.exists());
      Assert.assertTrue(targetResource.exists());

      targetFiles.add(target.getAbsoluteFile());
    }

    // verify deleted_files_blacklist
    List<File> deletedFilesBlacklist = new ArrayList<>();
    File deletedBlacklistFile =
        fsFactory.getFile(tier1SyncFolder, SyncConstant.DELETED_BLACKLIST_FILE_NAME);
    Assert.assertTrue(deletedBlacklistFile.exists());
    try (BufferedReader reader =
        fsFactory.getBufferedReader(deletedBlacklistFile.getAbsolutePath())) {
      String filePath;
      while ((filePath = reader.readLine()) != null) {
        File file = FSPath.parse(filePath).toFile();
        deletedFilesBlacklist.add(file);
      }
    }
    Assert.assertEquals(srcFiles.size(), deletedFilesBlacklist.size());
    Assert.assertTrue(srcFiles.containsAll(deletedFilesBlacklist));

    // verify to_be_synced_files_blacklist
    List<File> toBeSyncedFilesBlacklist = new ArrayList<>();
    File toBeSyncedBlacklistFile =
        fsFactory.getFile(tier2SyncFolder, SyncConstant.TO_BE_SYNCED_BLACKLIST_FILE_NAME);
    Assert.assertTrue(toBeSyncedBlacklistFile.exists());
    try (BufferedReader reader =
        fsFactory.getBufferedReader(toBeSyncedBlacklistFile.getAbsolutePath())) {
      String filePath;
      while ((filePath = reader.readLine()) != null) {
        File file = FSPath.parse(filePath).toFile();
        toBeSyncedFilesBlacklist.add(file);
      }
    }
    Assert.assertEquals(targetFiles.size(), toBeSyncedFilesBlacklist.size());
    Assert.assertTrue(targetFiles.containsAll(toBeSyncedFilesBlacklist));
  }

  private void migrationCallback(
      File tsFileToDelete,
      File tsFileToLoad,
      boolean sequence,
      BiConsumer<File, File> opsToBothFiles) {
    Assert.assertTrue(tsFileToDelete.exists());
    Assert.assertFalse(tsFileToLoad.exists());

    if (opsToBothFiles != null) {
      opsToBothFiles.accept(tsFileToDelete, tsFileToLoad);
    }

    Assert.assertFalse(tsFileToDelete.exists());
    Assert.assertTrue(tsFileToLoad.exists());
  }
}
