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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.sync.sender.manage.ISyncFileManager;
import org.apache.iotdb.db.sync.sender.manage.SyncFileManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SyncUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SyncSenderLogAnalyzerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncSenderLogAnalyzerTest.class);
  private ISyncSenderLogger senderLogger;
  private ISyncSenderLogAnalyzer senderLogAnalyzer;
  private ISyncFileManager manager = SyncFileManager.getInstance();
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();
  private FSPath dataDir;

  @Before
  public void setUp()
      throws IOException, InterruptedException, StartupException, DiskSpaceInsufficientException {
    EnvironmentUtils.envSetUp();
    FSPath seqDir = TierManager.getInstance().getAllSequenceFileFolders().get(0);
    dataDir = new FSPath(seqDir.getFsType(), seqDir.toFile().getParentFile().getAbsolutePath());
    config.update(dataDir);
    senderLogger =
        new SyncSenderLogger(config.getSenderFolderPath().getChildFile(SyncConstant.SYNC_LOG_NAME));
    senderLogAnalyzer = new SyncSenderLogAnalyzer(config.getSenderFolderPath());
  }

  @After
  public void tearDown() throws InterruptedException, IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void recover() throws IOException, MetadataException {
    Map<String, Map<Long, Map<Long, Set<File>>>> allFileList = new HashMap<>();

    for (int i = 0; i < 3; i++) {
      IoTDB.metaManager.setStorageGroup(new PartialPath(getSgName(i)));
    }
    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        allFileList
            .computeIfAbsent(getSgName(i), k -> new HashMap<>())
            .computeIfAbsent(0L, k -> new HashMap<>())
            .computeIfAbsent(0L, k -> new HashSet<>());
        String rand = r.nextInt(10000) + TSFILE_SUFFIX;
        String fileName =
            FilePathUtils.regularizePath(dataDir.getPath())
                + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator
                + getSgName(i)
                + File.separator
                + "0"
                + File.separator
                + "0"
                + File.separator
                + rand;
        File file = new FSPath(dataDir.getFsType(), fileName).toFile();
        allFileList.get(getSgName(i)).get(0L).get(0L).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          LOGGER.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          LOGGER.error("Can not create new file {}", file.getPath());
        }
      }
    }
    manager.getValidFiles(dataDir);
    assertTrue(SyncUtils.isEmpty(manager.getLastLocalFilesMap()));
    senderLogger.startSyncTsFiles();
    for (Map<Long, Map<Long, Set<File>>> map : allFileList.values()) {
      for (Map<Long, Set<File>> vgMap : map.values()) {
        for (Set<File> newTsFiles : vgMap.values()) {
          for (File file : newTsFiles) {
            senderLogger.finishSyncTsfile(file);
          }
        }
      }
    }
    senderLogger.close();

    // recover log
    senderLogAnalyzer.recover();
    manager.getValidFiles(dataDir);
    assertFalse(SyncUtils.isEmpty(manager.getLastLocalFilesMap()));
    Map<String, Map<Long, Map<Long, Set<File>>>> lastFilesMap = manager.getLastLocalFilesMap();
    assertFileMap(allFileList, lastFilesMap);

    // delete some files
    assertFalse(config.getSenderFolderPath().getChildFile(SyncConstant.SYNC_LOG_NAME).exists());
    senderLogger =
        new SyncSenderLogger(config.getSenderFolderPath().getChildFile(SyncConstant.SYNC_LOG_NAME));
    manager.getValidFiles(dataDir);
    assertFalse(SyncUtils.isEmpty(manager.getLastLocalFilesMap()));
    senderLogger.startSyncDeletedFilesName();
    for (Map<Long, Map<Long, Set<File>>> map : allFileList.values()) {
      for (Map<Long, Set<File>> vgMap : map.values()) {
        for (Set<File> newTsFiles : vgMap.values()) {
          for (File file : newTsFiles) {
            senderLogger.finishSyncDeletedFileName(file);
          }
        }
      }
    }
    senderLogger.close();
    // recover log
    senderLogAnalyzer.recover();
    manager.getValidFiles(dataDir);
    assertTrue(SyncUtils.isEmpty(manager.getLastLocalFilesMap()));
    assertTrue(SyncUtils.isEmpty(manager.getDeletedFilesMap()));
    Map<String, Map<Long, Map<Long, Set<File>>>> toBeSyncedFilesMap =
        manager.getToBeSyncedFilesMap();
    assertFileMap(allFileList, toBeSyncedFilesMap);
  }

  private void assertFileMap(
      Map<String, Map<Long, Map<Long, Set<File>>>> correctMap,
      Map<String, Map<Long, Map<Long, Set<File>>>> curMap) {
    for (Entry<String, Map<Long, Map<Long, Set<File>>>> entry : correctMap.entrySet()) {
      assertTrue(curMap.containsKey(entry.getKey()));
      for (Entry<Long, Map<Long, Set<File>>> vgEntry : entry.getValue().entrySet()) {
        assertTrue(curMap.get(entry.getKey()).containsKey(vgEntry.getKey()));
        for (Entry<Long, Set<File>> innerEntry : vgEntry.getValue().entrySet()) {
          assertTrue(
              curMap
                  .get(entry.getKey())
                  .get(vgEntry.getKey())
                  .get(innerEntry.getKey())
                  .containsAll(innerEntry.getValue()));
        }
      }
    }
  }

  private String getSgName(int i) {
    return IoTDBConstant.PATH_ROOT + IoTDBConstant.PATH_SEPARATOR + i;
  }

  private boolean isEmpty(Map<String, Set<File>> sendingFileList) {
    for (Entry<String, Set<File>> entry : sendingFileList.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        return false;
      }
    }
    return true;
  }
}
