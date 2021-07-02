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
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class SyncFileManager implements ISyncFileManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncFileManager.class);

  /**
   * All storage groups on the disk where the current sync task is executed logicalSg -> <virtualSg,
   * timeRangeId>
   */
  private Map<String, Map<Long, Set<Long>>> allSGs;

  /**
   * Key is storage group, value is all sealed tsfiles in the storage group. Inner key is time range
   * id, inner value is the set of current sealed tsfiles. logicalSg -> <virtualSg, <timeRangeId,
   * tsfiles>>
   */
  private Map<String, Map<Long, Map<Long, Set<File>>>> currentSealedLocalFilesMap;

  /**
   * Key is storage group, value is all last local tsfiles in the storage group, which doesn't
   * contains those tsfiles which are not synced successfully. Inner key is time range id, inner
   * value is the set of last local tsfiles. logicalSg -> <virtualSg, <timeRangeId, tsfiles>>
   */
  private Map<String, Map<Long, Map<Long, Set<File>>>> lastLocalFilesMap;

  /**
   * Key is storage group, value is all deleted tsfiles which need to be synced to receiver end in
   * the storage group. Inner key is time range id, inner value is the valid set of sealed tsfiles.
   * logicalSg -> <virtualSg, <timeRangeId, tsfiles>>
   */
  private Map<String, Map<Long, Map<Long, Set<File>>>> deletedFilesMap;

  /**
   * Key is storage group, value is all new tsfiles which need to be synced to receiver end in the
   * storage group. Inner key is time range id, inner value is the valid set of new tsfiles.
   * logicalSg -> <virtualSg, <timeRangeId, tsfiles>>
   */
  private Map<String, Map<Long, Map<Long, Set<File>>>> toBeSyncedFilesMap;

  private SyncFileManager() {
    IoTDB.metaManager.init();
  }

  public static SyncFileManager getInstance() {
    return SyncFileManagerHolder.INSTANCE;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public void getCurrentLocalFiles(String dataDir) {
    LOGGER.info("Start to get current local files in data folder {}", dataDir);

    currentSealedLocalFilesMap = new HashMap<>();
    // get all files in data dir sequence folder
    Map<String, Map<Long, Map<Long, Set<File>>>> currentAllLocalFiles = new HashMap<>();
    if (!new File(dataDir + File.separatorChar + IoTDBConstant.SEQUENCE_FLODER_NAME).exists()) {
      return;
    }
    File[] allSgFolders =
        new File(dataDir + File.separatorChar + IoTDBConstant.SEQUENCE_FLODER_NAME).listFiles();
    for (File sgFolder : allSgFolders) {
      if (!sgFolder.getName().startsWith(IoTDBConstant.PATH_ROOT)
          || sgFolder.getName().equals(TsFileConstant.TMP_SUFFIX)) {
        continue;
      }
      allSGs.putIfAbsent(sgFolder.getName(), new HashMap<>());
      currentAllLocalFiles.putIfAbsent(sgFolder.getName(), new HashMap<>());
      for (File virtualSgFolder : sgFolder.listFiles()) {
        try {
          Long vgId = Long.parseLong(virtualSgFolder.getName());
          allSGs.get(sgFolder.getName()).putIfAbsent(vgId, new HashSet<>());
          currentAllLocalFiles.get(sgFolder.getName()).putIfAbsent(vgId, new HashMap<>());

          for (File timeRangeFolder : virtualSgFolder.listFiles()) {
            Long timeRangeId = Long.parseLong(timeRangeFolder.getName());
            currentAllLocalFiles
                .get(sgFolder.getName())
                .get(vgId)
                .putIfAbsent(timeRangeId, new HashSet<>());
            File[] files = timeRangeFolder.listFiles();
            Arrays.stream(files)
                .forEach(
                    file ->
                        currentAllLocalFiles
                            .get(sgFolder.getName())
                            .get(vgId)
                            .get(timeRangeId)
                            .add(new File(timeRangeFolder.getAbsolutePath(), file.getName())));
          }
        } catch (Exception e) {
          LOGGER.error(
              "Invalid virtual storage group folder: {}", virtualSgFolder.getAbsolutePath(), e);
        }
      }
    }

    // get sealed tsfiles
    for (Entry<String, Map<Long, Map<Long, Set<File>>>> entry : currentAllLocalFiles.entrySet()) {
      String sgName = entry.getKey();
      currentSealedLocalFilesMap.putIfAbsent(sgName, new HashMap<>());
      for (Entry<Long, Map<Long, Set<File>>> vgEntry : entry.getValue().entrySet()) {
        Long vgId = vgEntry.getKey();
        currentSealedLocalFilesMap.get(sgName).putIfAbsent(vgId, new HashMap<>());
        for (Entry<Long, Set<File>> innerEntry : vgEntry.getValue().entrySet()) {
          Long timeRangeId = innerEntry.getKey();
          currentSealedLocalFilesMap
              .get(sgName)
              .get(vgId)
              .putIfAbsent(timeRangeId, new HashSet<>());
          for (File file : innerEntry.getValue()) {
            if (!file.getName().endsWith(TSFILE_SUFFIX)) {
              continue;
            }
            if (checkFileValidity(file)) {
              currentSealedLocalFilesMap.get(sgName).get(vgId).get(timeRangeId).add(file);
            }
          }
        }
      }
    }
  }

  private boolean checkFileValidity(File file) {
    return new File(file.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).exists()
        && !new File(file.getAbsolutePath() + ModificationFile.FILE_SUFFIX).exists()
        && !new File(file.getAbsolutePath() + CrossSpaceMergeTask.MERGE_SUFFIX).exists();
  }

  @Override
  public void getLastLocalFiles(File lastLocalFileInfo) throws IOException {
    LOGGER.info(
        "Start to get last local files from last local file info {}",
        lastLocalFileInfo.getAbsoluteFile());
    lastLocalFilesMap = new HashMap<>();
    if (!lastLocalFileInfo.exists()) {
      return;
    }
    try (BufferedReader reader = new BufferedReader(new FileReader(lastLocalFileInfo))) {
      String filePath;
      while ((filePath = reader.readLine()) != null) {
        File file = new File(filePath);
        Long timeRangeId = Long.parseLong(file.getParentFile().getName());
        Long vgId = Long.parseLong(file.getParentFile().getParentFile().getName());
        String sgName = file.getParentFile().getParentFile().getParentFile().getName();
        allSGs.putIfAbsent(sgName, new HashMap<>());
        allSGs.get(sgName).putIfAbsent(vgId, new HashSet<>());
        lastLocalFilesMap
            .computeIfAbsent(sgName, k -> new HashMap<>())
            .computeIfAbsent(vgId, k -> new HashMap<>())
            .computeIfAbsent(timeRangeId, k -> new HashSet<>())
            .add(file);
      }
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public void getValidFiles(String dataDir) throws IOException {
    allSGs = new HashMap<>();
    getCurrentLocalFiles(dataDir);
    getLastLocalFiles(
        new File(SyncSenderDescriptor.getInstance().getConfig().getLastFileInfoPath()));
    toBeSyncedFilesMap = new HashMap<>();
    deletedFilesMap = new HashMap<>();
    for (String sgName : allSGs.keySet()) {
      toBeSyncedFilesMap.putIfAbsent(sgName, new HashMap<>());
      deletedFilesMap.putIfAbsent(sgName, new HashMap<>());

      for (Entry<Long, Map<Long, Set<File>>> entry :
          currentSealedLocalFilesMap.getOrDefault(sgName, Collections.emptyMap()).entrySet()) {
        Long vgId = entry.getKey();
        toBeSyncedFilesMap.get(sgName).putIfAbsent(vgId, new HashMap<>());
        allSGs.get(sgName).putIfAbsent(vgId, new HashSet<>());

        for (Entry<Long, Set<File>> innerEntry : entry.getValue().entrySet()) {
          Long timeRangeId = innerEntry.getKey();
          toBeSyncedFilesMap.get(sgName).get(vgId).putIfAbsent(timeRangeId, new HashSet<>());
          allSGs.get(sgName).get(vgId).add(timeRangeId);
          for (File newFile : innerEntry.getValue()) {
            if (!lastLocalFilesMap
                .getOrDefault(sgName, Collections.emptyMap())
                .getOrDefault(vgId, Collections.emptyMap())
                .getOrDefault(timeRangeId, Collections.emptySet())
                .contains(newFile)) {
              toBeSyncedFilesMap.get(sgName).get(vgId).get(timeRangeId).add(newFile);
            }
          }
        }
      }

      for (Entry<Long, Map<Long, Set<File>>> entry :
          lastLocalFilesMap.getOrDefault(sgName, Collections.emptyMap()).entrySet()) {
        Long vgId = entry.getKey();
        deletedFilesMap.get(sgName).putIfAbsent(vgId, new HashMap<>());
        allSGs.get(sgName).putIfAbsent(vgId, new HashSet<>());

        for (Entry<Long, Set<File>> innerEntry : entry.getValue().entrySet()) {
          Long timeRangeId = innerEntry.getKey();
          deletedFilesMap.get(sgName).get(vgId).putIfAbsent(timeRangeId, new HashSet<>());
          allSGs.get(sgName).get(vgId).add(timeRangeId);

          for (File oldFile : innerEntry.getValue()) {
            if (!currentSealedLocalFilesMap
                .getOrDefault(sgName, Collections.emptyMap())
                .getOrDefault(vgId, Collections.emptyMap())
                .getOrDefault(timeRangeId, Collections.emptySet())
                .contains(oldFile)) {
              deletedFilesMap.get(sgName).get(vgId).get(timeRangeId).add(oldFile);
            }
          }
        }
      }
    }
  }

  @Override
  public Map<String, Map<Long, Map<Long, Set<File>>>> getCurrentSealedLocalFilesMap() {
    return currentSealedLocalFilesMap;
  }

  @Override
  public Map<String, Map<Long, Map<Long, Set<File>>>> getLastLocalFilesMap() {
    return lastLocalFilesMap;
  }

  @Override
  public Map<String, Map<Long, Map<Long, Set<File>>>> getDeletedFilesMap() {
    return deletedFilesMap;
  }

  @Override
  public Map<String, Map<Long, Map<Long, Set<File>>>> getToBeSyncedFilesMap() {
    return toBeSyncedFilesMap;
  }

  @Override
  public Map<String, Map<Long, Set<Long>>> getAllSGs() {
    return allSGs;
  }

  private static class SyncFileManagerHolder {

    private static final SyncFileManager INSTANCE = new SyncFileManager();

    private SyncFileManagerHolder() {}
  }
}
