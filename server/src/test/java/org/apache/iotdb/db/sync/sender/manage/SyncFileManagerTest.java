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
package org.apache.iotdb.db.sync.sender.manage;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SyncUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;
import static org.junit.Assert.assertTrue;

public class SyncFileManagerTest {

  private static final Logger logger = LoggerFactory.getLogger(SyncFileManagerTest.class);
  private ISyncFileManager manager = SyncFileManager.getInstance();
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();
  private FSPath dataDir;

  @Before
  public void setUp() throws DiskSpaceInsufficientException {
    EnvironmentUtils.envSetUp();
    FSPath seqDir = TierManager.getInstance().getAllSequenceFileFolders().get(0);
    dataDir = new FSPath(seqDir.getFsType(), seqDir.toFile().getParentFile().getAbsolutePath());
    config.update(dataDir);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testGetValidFiles() throws IOException, MetadataException {
    Map<String, Map<Long, Map<Long, Set<File>>>> allFileList = new HashMap<>();

    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      IoTDB.metaManager.setStorageGroup(new PartialPath(getSgName(i)));
    }
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
          logger.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }

    // lastFileList is empty
    manager.getValidFiles(dataDir);
    assertTrue(SyncUtils.isEmpty(manager.getLastLocalFilesMap()));

    updateLastLocalFiles(allFileList);

    manager.getValidFiles(dataDir);
    Map<String, Map<Long, Map<Long, Set<File>>>> lastFileMap = manager.getLastLocalFilesMap();
    assertFileMap(allFileList, lastFileMap);

    // add some files
    Map<String, Map<Long, Map<Long, Set<File>>>> correctToBeSyncedFiles = new HashMap<>();
    r = new Random(1);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        allFileList
            .computeIfAbsent(getSgName(i), k -> new HashMap<>())
            .computeIfAbsent(0L, k -> new HashMap<>())
            .computeIfAbsent(0L, k -> new HashSet<>());
        correctToBeSyncedFiles
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
        correctToBeSyncedFiles.get(getSgName(i)).get(0L).get(0L).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }
    manager.getValidFiles(dataDir);
    Map<String, Map<Long, Map<Long, Set<File>>>> curFileMap =
        manager.getCurrentSealedLocalFilesMap();
    Map<String, Map<Long, Map<Long, Set<File>>>> toBeSyncedFilesMap =
        manager.getToBeSyncedFilesMap();
    assertFileMap(allFileList, curFileMap);
    assertFileMap(correctToBeSyncedFiles, toBeSyncedFilesMap);

    updateLastLocalFiles(allFileList);
    manager.getValidFiles(dataDir);
    lastFileMap = manager.getLastLocalFilesMap();

    assertFileMap(allFileList, lastFileMap);

    // add some files and delete some files
    correctToBeSyncedFiles.clear();
    r = new Random(2);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        allFileList
            .computeIfAbsent(getSgName(i), k -> new HashMap<>())
            .computeIfAbsent(0L, k -> new HashMap<>())
            .computeIfAbsent(0L, k -> new HashSet<>());
        correctToBeSyncedFiles
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
                + File.separator
                + rand;
        File file = new FSPath(dataDir.getFsType(), fileName).toFile();
        allFileList.get(getSgName(i)).get(0L).get(0L).add(file);
        correctToBeSyncedFiles.get(getSgName(i)).get(0L).get(0L).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
        if (!new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
            && !new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }
    int count = 0;
    Map<String, Map<Long, Map<Long, Set<File>>>> correctDeleteFile = new HashMap<>();
    for (Entry<String, Map<Long, Map<Long, Set<File>>>> entry : allFileList.entrySet()) {
      correctDeleteFile.put(entry.getKey(), new HashMap<>());
      for (Entry<Long, Map<Long, Set<File>>> vgEntry : entry.getValue().entrySet()) {
        correctDeleteFile.get(entry.getKey()).putIfAbsent(vgEntry.getKey(), new HashMap<>());
        for (Entry<Long, Set<File>> innerEntry : vgEntry.getValue().entrySet()) {
          Set<File> files = innerEntry.getValue();
          correctDeleteFile
              .get(entry.getKey())
              .get(vgEntry.getKey())
              .putIfAbsent(innerEntry.getKey(), new HashSet<>());
          for (File file : files) {
            count++;
            if (count % 3 == 0 && lastFileMap.get(entry.getKey()).get(0L).get(0L).contains(file)) {
              correctDeleteFile.get(entry.getKey()).get(0L).get(0L).add(file);
            }
          }
        }
      }
    }
    for (Entry<String, Map<Long, Map<Long, Set<File>>>> entry : correctDeleteFile.entrySet()) {
      correctDeleteFile.put(entry.getKey(), new HashMap<>());
      for (Entry<Long, Map<Long, Set<File>>> vgEntry : entry.getValue().entrySet()) {
        correctDeleteFile.get(entry.getKey()).putIfAbsent(vgEntry.getKey(), new HashMap<>());
        for (Entry<Long, Set<File>> innerEntry : vgEntry.getValue().entrySet()) {
          Set<File> files = innerEntry.getValue();
          correctDeleteFile
              .get(entry.getKey())
              .get(vgEntry.getKey())
              .putIfAbsent(innerEntry.getKey(), new HashSet<>());
          for (File file : innerEntry.getValue()) {
            file.delete();
            new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).delete();
            allFileList.get(entry.getKey()).get(0L).get(0L).remove(file);
          }
        }
      }
    }
    manager.getValidFiles(dataDir);
    curFileMap = manager.getCurrentSealedLocalFilesMap();
    Map<String, Map<Long, Map<Long, Set<File>>>> deletedFilesMap = manager.getDeletedFilesMap();
    toBeSyncedFilesMap = manager.getToBeSyncedFilesMap();
    assertFileMap(allFileList, curFileMap);
    assertFileMap(correctDeleteFile, deletedFilesMap);
    assertFileMap(correctToBeSyncedFiles, toBeSyncedFilesMap);

    // add some invalid files
    r = new Random(3);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        allFileList
            .computeIfAbsent(getSgName(i), k -> new HashMap<>())
            .computeIfAbsent(0L, k -> new HashMap<>())
            .computeIfAbsent(0L, k -> new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000));
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
                + File.separator
                + rand;
        File file = new FSPath(dataDir.getFsType(), fileName).toFile();
        allFileList.get(getSgName(i)).get(0L).get(0L).add(file);
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        if (!file.exists() && !file.createNewFile()) {
          logger.error("Can not create new file {}", file.getPath());
        }
      }
    }
    manager.getValidFiles(dataDir);
    curFileMap = manager.getCurrentSealedLocalFilesMap();
    deletedFilesMap = manager.getDeletedFilesMap();
    toBeSyncedFilesMap = manager.getToBeSyncedFilesMap();

    assertFileMap(curFileMap, allFileList);
    assertFileMap(curFileMap, allFileList);
    assertFileMap(correctDeleteFile, deletedFilesMap);
    assertFileMap(correctToBeSyncedFiles, toBeSyncedFilesMap);
  }

  private void assertFileMap(
      Map<String, Map<Long, Map<Long, Set<File>>>> correctMap,
      Map<String, Map<Long, Map<Long, Set<File>>>> curMap) {
    for (Entry<String, Map<Long, Map<Long, Set<File>>>> entry : correctMap.entrySet()) {
      assertTrue(curMap.containsKey(entry.getKey()));
      for (Entry<Long, Map<Long, Set<File>>> innerEntry : entry.getValue().entrySet()) {
        assertTrue(curMap.get(entry.getKey()).containsKey(innerEntry.getKey()));
        for (Entry<Long, Set<File>> fileEntry : innerEntry.getValue().entrySet()) {
          assertTrue(
              curMap
                  .get(entry.getKey())
                  .get(innerEntry.getKey())
                  .get(fileEntry.getKey())
                  .containsAll(fileEntry.getValue()));
        }
      }
    }
  }

  private String getSgName(int i) {
    return IoTDBConstant.PATH_ROOT + IoTDBConstant.PATH_SEPARATOR + i;
  }

  private void updateLastLocalFiles(
      Map<String, Map<Long, Map<Long, Set<File>>>> lastLocalFilesMap) {
    try (BufferedWriter bw =
        new BufferedWriter(new FileWriter(config.getLastFileInfoPath().toFile()))) {
      for (Map<Long, Map<Long, Set<File>>> currentLocalFiles : lastLocalFilesMap.values()) {
        for (Map<Long, Set<File>> vgFiles : currentLocalFiles.values()) {
          for (Set<File> files : vgFiles.values()) {
            for (File file : files) {
              bw.write(file.getAbsolutePath());
              bw.newLine();
            }
            bw.flush();
          }
        }
      }
    } catch (IOException e) {
      logger.error("Can not clear sync log {}", config.getLastFileInfoPath(), e);
    }
  }
}
