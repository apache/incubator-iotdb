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
package org.apache.iotdb.db.sync.receiver.load;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.HashVirtualPartitioner;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileLoaderTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoaderTest.class);
  private static final String SG_NAME = "root.sg";
  private FSPath dataDir;
  private IFileLoader fileLoader;

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setSyncEnable(true);
    HashVirtualPartitioner.getInstance().setStorageGroupNum(1);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    FSPath seqDir = TierManager.getInstance().getAllSequenceFileFolders().get(0);
    dataDir = new FSPath(seqDir.getFsType(), seqDir.toFile().getParentFile().getAbsolutePath());
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
    HashVirtualPartitioner.getInstance()
        .setStorageGroupNum(IoTDBDescriptor.getInstance().getConfig().getVirtualStorageGroupNum());
  }

  @Test
  public void loadNewTsfiles()
      throws IOException, StorageEngineException, IllegalPathException, InterruptedException {
    fileLoader = FileLoader.createFileLoader(getReceiverFolderFile());
    Map<String, List<File>> allFileList = new HashMap<>();
    Map<String, Set<String>> correctSequenceLoadedFileMap = new HashMap<>();

    // add some new tsfiles
    Random r = new Random(0);
    long time = System.currentTimeMillis();
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 10; j++) {
        allFileList.putIfAbsent(SG_NAME + i, new ArrayList<>());
        correctSequenceLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000));
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
                + (time + i * 100 + j)
                + IoTDBConstant.FILE_NAME_SEPARATOR
                + rand
                + IoTDBConstant.FILE_NAME_SEPARATOR
                + "0.tsfile";

        File syncFile = new File(fileName);
        File dataFile =
            new File(
                syncFile
                    .getParentFile()
                    .getParentFile()
                    .getParentFile()
                    .getParentFile()
                    .getParentFile()
                    .getParentFile(),
                IoTDBConstant.SEQUENCE_FLODER_NAME
                    + File.separatorChar
                    + syncFile.getParentFile().getParentFile().getName()
                    + File.separatorChar
                    + syncFile.getParentFile().getName()
                    + File.separatorChar
                    + fromTimeToTimePartition(i)
                    + File.separator
                    + syncFile.getName());
        correctSequenceLoadedFileMap.get(SG_NAME + i).add(dataFile.getAbsolutePath());
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
        tsFileResource.setMaxPlanIndex(j);
        tsFileResource.setMinPlanIndex(j);

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
    for (List<File> set : allFileList.values()) {
      for (File newTsFile : set) {
        if (!newTsFile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          fileLoader.addTsfile(newTsFile);
        }
      }
    }
    fileLoader.endSync();

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

    assertFalse(new File(getReceiverFolderFile(), SyncConstant.RECEIVER_DATA_FOLDER_NAME).exists());
    Map<String, Set<String>> sequenceLoadedFileMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor =
          StorageEngine.getInstance().getProcessor(new PartialPath(SG_NAME + i));
      sequenceLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
      assertEquals(10, processor.getSequenceFileTreeSet().size());
      for (TsFileResource tsFileResource : processor.getSequenceFileTreeSet()) {
        sequenceLoadedFileMap.get(SG_NAME + i).add(tsFileResource.getTsFile().getAbsolutePath());
      }
      assertTrue(processor.getUnSequenceFileList().isEmpty());
    }

    assertEquals(sequenceLoadedFileMap.size(), correctSequenceLoadedFileMap.size());
    for (Entry<String, Set<String>> entry : correctSequenceLoadedFileMap.entrySet()) {
      String sg = entry.getKey();
      assertEquals(entry.getValue().size(), sequenceLoadedFileMap.get(sg).size());
    }
  }

  @Test
  public void loadDeletedFileName()
      throws IOException, StorageEngineException, InterruptedException, IllegalPathException {
    fileLoader = FileLoader.createFileLoader(getReceiverFolderFile());
    Map<String, List<File>> allFileList = new HashMap<>();
    Map<String, Set<String>> correctLoadedFileMap = new HashMap<>();

    // add some tsfiles
    Random r = new Random(0);
    long time = System.currentTimeMillis();
    FSPath seqFolder = TierManager.getInstance().getAllSequenceFileFolders().get(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 25; j++) {
        allFileList.putIfAbsent(SG_NAME + i, new ArrayList<>());
        correctLoadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
        String rand = String.valueOf(r.nextInt(10000));
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
                + (time + i * 100 + j)
                + IoTDBConstant.FILE_NAME_SEPARATOR
                + rand
                + IoTDBConstant.FILE_NAME_SEPARATOR
                + "0.tsfile";

        File syncFile = new File(fileName);
        File dataFile =
            seqFolder.getChildFile(
                syncFile.getParentFile().getName() + File.separator + syncFile.getName());
        File loadDataFile =
            seqFolder.getChildFile(
                syncFile.getParentFile().getParentFile().getParentFile().getName()
                    + File.separator
                    + "0"
                    + File.separator
                    + fromTimeToTimePartition(i)
                    + File.separator
                    + syncFile.getName());
        correctLoadedFileMap.get(SG_NAME + i).add(loadDataFile.getAbsolutePath());
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
        tsFileResource.setMinPlanIndex(j);
        tsFileResource.setMaxPlanIndex(j);
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
    for (List<File> set : allFileList.values()) {
      for (File newTsFile : set) {
        if (!newTsFile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          fileLoader.addTsfile(newTsFile);
        }
      }
    }
    fileLoader.endSync();

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

    assertFalse(new File(getReceiverFolderFile(), SyncConstant.RECEIVER_DATA_FOLDER_NAME).exists());
    Map<String, Set<String>> loadedFileMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor =
          StorageEngine.getInstance().getProcessor(new PartialPath(SG_NAME + i));
      loadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
      assertEquals(25, processor.getSequenceFileTreeSet().size());
      for (TsFileResource tsFileResource : processor.getSequenceFileTreeSet()) {
        loadedFileMap.get(SG_NAME + i).add(tsFileResource.getTsFile().getAbsolutePath());
      }
      assertTrue(processor.getUnSequenceFileList().isEmpty());
    }

    assertEquals(loadedFileMap.size(), correctLoadedFileMap.size());
    for (Entry<String, Set<String>> entry : correctLoadedFileMap.entrySet()) {
      String sg = entry.getKey();
      assertEquals(entry.getValue().size(), loadedFileMap.get(sg).size());
    }

    // delete some tsfiles
    fileLoader = FileLoader.createFileLoader(getReceiverFolderFile());
    for (Entry<String, List<File>> entry : allFileList.entrySet()) {
      String sg = entry.getKey();
      List<File> files = entry.getValue();
      int cnt = 0;
      for (File snapFile : files) {
        if (!snapFile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
          File dataFile =
              seqFolder.getChildFile(
                  snapFile.getParentFile().getParentFile().getParentFile().getName()
                      + File.separator
                      + "0"
                      + File.separator
                      + "0"
                      + File.separator
                      + snapFile.getName());
          correctLoadedFileMap.get(sg).remove(dataFile.getAbsolutePath());
          snapFile.delete();
          fileLoader.addDeletedFileName(snapFile);
          new File(snapFile + TsFileResource.RESOURCE_SUFFIX).delete();
          if (++cnt == 15) {
            break;
          }
        }
      }
    }
    fileLoader.endSync();

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

    loadedFileMap.clear();
    for (int i = 0; i < 3; i++) {
      StorageGroupProcessor processor =
          StorageEngine.getInstance().getProcessor(new PartialPath(SG_NAME + i));
      loadedFileMap.putIfAbsent(SG_NAME + i, new HashSet<>());
      for (TsFileResource tsFileResource : processor.getSequenceFileTreeSet()) {
        loadedFileMap.get(SG_NAME + i).add(tsFileResource.getTsFile().getAbsolutePath());
      }
      assertTrue(processor.getUnSequenceFileList().isEmpty());
    }

    assertEquals(loadedFileMap.size(), correctLoadedFileMap.size());
    for (Entry<String, Set<String>> entry : correctLoadedFileMap.entrySet()) {
      String sg = entry.getKey();
      assertEquals(entry.getValue().size(), loadedFileMap.get(sg).size());
      assertTrue(entry.getValue().containsAll(loadedFileMap.get(sg)));
    }
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

  private long fromTimeToTimePartition(long time) {
    return time / IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
  }
}
