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
package org.apache.iotdb.db.sync.receiver.recover;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.receiver.load.FileLoader;
import org.apache.iotdb.db.sync.receiver.load.FileLoaderManager;
import org.apache.iotdb.db.sync.receiver.load.FileLoaderTest;
import org.apache.iotdb.db.sync.receiver.load.IFileLoader;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SyncReceiverLogAnalyzerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoaderTest.class);
  private static final String SG_NAME = "root.sg";
  private FSPath dataDir;
  private IFileLoader fileLoader;
  private ISyncReceiverLogAnalyzer logAnalyze;
  private ISyncReceiverLogger receiverLogger;

  @Before
  public void setUp() throws DiskSpaceInsufficientException, MetadataException {
    IoTDBDescriptor.getInstance().getConfig().setSyncEnable(true);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    FSPath seqDir = TierManager.getInstance().getAllSequenceFileFolders().get(0);
    dataDir = new FSPath(seqDir.getFsType(), seqDir.toFile().getParentFile().getAbsolutePath());
    logAnalyze = SyncReceiverLogAnalyzer.getInstance();
    initMetadata();
  }

  private void initMetadata() throws MetadataException {
    MManager mmanager = IoTDB.metaManager;
    mmanager.init();
    mmanager.setStorageGroup(new PartialPath("root.sg0"));
    mmanager.setStorageGroup(new PartialPath("root.sg1"));
    mmanager.setStorageGroup(new PartialPath("root.sg2"));
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setSyncEnable(false);
  }

  @Test
  public void recover()
      throws IOException, StorageEngineException, InterruptedException, IllegalPathException {
    receiverLogger =
        new SyncReceiverLogger(new File(getReceiverFolderFile(), SyncConstant.SYNC_LOG_NAME));
    fileLoader = FileLoader.createFileLoader(getReceiverFolderFile());
    Map<String, Set<File>> allFileList = new HashMap<>();
    Map<String, Set<File>> correctSequenceLoadedFileMap = new HashMap<>();

    // add some new tsfiles
    Random r = new Random(0);
    receiverLogger.startSyncTsFiles();
    Set<String> toBeSyncedFiles = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 10; j++) {
        allFileList.putIfAbsent(SG_NAME + i, new HashSet<>());
        correctSequenceLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000) + i * j);
        String fileName =
            getSnapshotFolder()
                + File.separator
                + SG_NAME
                + i
                + File.separator
                + "0"
                + File.separator
                + "0"
                + File.separator
                + System.currentTimeMillis()
                + IoTDBConstant.FILE_NAME_SEPARATOR
                + rand
                + IoTDBConstant.FILE_NAME_SEPARATOR
                + "0.tsfile";
        Thread.sleep(1);
        File syncFile = new File(fileName);
        receiverLogger.finishSyncTsfile(syncFile);
        toBeSyncedFiles.add(FSPath.parse(syncFile).getAbsoluteFSPath().getRawFSPath());
        File dataFile =
            TierManager.getInstance()
                .getAllSequenceFileFolders()
                .get(0)
                .getChildFile(
                    syncFile.getParentFile().getName() + File.separatorChar + syncFile.getName());
        correctSequenceLoadedFileMap.get(SG_NAME + i).add(dataFile);
        allFileList.get(SG_NAME + i).add(syncFile);
        if (!syncFile.getParentFile().exists()) {
          syncFile.getParentFile().mkdirs();
        }
        if (!syncFile.exists() && !syncFile.createNewFile()) {
          LOGGER.error("Can not create new file {}", syncFile.getPath());
        }
        if (!new File(syncFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(syncFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX)
                .createNewFile()) {
          LOGGER.error("Can not create new file {}", syncFile.getPath());
        }
        TsFileResource tsFileResource = new TsFileResource(syncFile);
        tsFileResource.updateStartTime(String.valueOf(i), (long) j * 10);
        tsFileResource.updateEndTime(String.valueOf(i), (long) j * 10 + 5);
        tsFileResource.serialize();
      }
    }

    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor =
          StorageEngine.getInstance().getProcessor(new PartialPath(SG_NAME + i));
      assertTrue(processor.getSequenceFileTreeSet().isEmpty());
      assertTrue(processor.getUnSequenceFileList().isEmpty());
    }

    assertTrue(getReceiverFolderFile().exists());
    for (Set<File> set : allFileList.values()) {
      for (File newTsFile : set) {
        if (!newTsFile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          fileLoader.addTsfile(newTsFile);
        }
      }
    }

    receiverLogger.close();
    assertTrue(new File(getReceiverFolderFile(), SyncConstant.LOAD_LOG_NAME).exists());
    assertTrue(new File(getReceiverFolderFile(), SyncConstant.SYNC_LOG_NAME).exists());
    assertTrue(
        FileLoaderManager.getInstance().containsFileLoader(getReceiverFolderFile().getName()));
    int mode = 0;
    Set<String> toBeSyncedFilesTest = new HashSet<>();
    try (BufferedReader br =
        new BufferedReader(
            new FileReader(new File(getReceiverFolderFile(), SyncConstant.SYNC_LOG_NAME)))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (line.equals(SyncReceiverLogger.SYNC_DELETED_FILE_NAME_START)) {
          mode = -1;
        } else if (line.equals(SyncReceiverLogger.SYNC_TSFILE_START)) {
          mode = 1;
        } else {
          if (mode == 1) {
            toBeSyncedFilesTest.add(line);
          }
        }
      }
    }
    assertEquals(toBeSyncedFilesTest.size(), toBeSyncedFiles.size());
    assertTrue(toBeSyncedFilesTest.containsAll(toBeSyncedFiles));

    logAnalyze.recover(getReceiverFolderFile().getName());

    try {
      long waitTime = 0;
      while (FileLoaderManager.getInstance()
          .containsFileLoader(getReceiverFolderFile().getName())) {
        Thread.sleep(100);
        waitTime += 100;
        LOGGER.info("Has waited for loading new tsfiles {}ms", waitTime);
      }
    } catch (InterruptedException e) {
      LOGGER.error("Fail to wait for loading new tsfiles", e);
      Thread.currentThread().interrupt();
      throw e;
    }

    assertFalse(new File(getReceiverFolderFile(), SyncConstant.LOAD_LOG_NAME).exists());
    assertFalse(new File(getReceiverFolderFile(), SyncConstant.SYNC_LOG_NAME).exists());
  }

  private File getReceiverFolderFile() {
    return dataDir
        .postConcat(
            File.separatorChar + SyncConstant.SYNC_RECEIVER + File.separatorChar + "127.0.0.1_5555")
        .toFile();
  }

  private File getSnapshotFolder() {
    return new File(getReceiverFolderFile(), SyncConstant.RECEIVER_DATA_FOLDER_NAME);
  }
}
