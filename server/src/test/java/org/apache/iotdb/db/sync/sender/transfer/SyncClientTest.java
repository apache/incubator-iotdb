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
package org.apache.iotdb.db.sync.sender.transfer;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.sync.conf.SyncSenderConfig;
import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.db.sync.sender.recover.ISyncSenderLogAnalyzer;
import org.apache.iotdb.db.sync.sender.recover.SyncSenderLogAnalyzer;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SyncClientTest {

  private static final Logger logger = LoggerFactory.getLogger(SyncClientTest.class);
  private ISyncClient manager = SyncClient.getInstance();
  private SyncSenderConfig config = SyncSenderDescriptor.getInstance().getConfig();
  private FSPath dataDir;
  private ISyncSenderLogAnalyzer senderLogAnalyzer;

  @Before
  public void setUp() throws DiskSpaceInsufficientException {
    EnvironmentUtils.envSetUp();
    FSPath seqDir = TierManager.getInstance().getAllSequenceFileFolders().get(0);
    dataDir = new FSPath(seqDir.getFsType(), seqDir.toFile().getParentFile().getAbsolutePath());
    config.update(dataDir);
    senderLogAnalyzer = new SyncSenderLogAnalyzer(config.getSenderFolderPath());
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void makeFileSnapshot() throws IOException {
    Map<String, Map<Long, Map<Long, Set<File>>>> allFileList = new HashMap<>();

    Random r = new Random(0);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 5; j++) {
        if (!allFileList.containsKey(String.valueOf(i))) {
          allFileList
              .computeIfAbsent(String.valueOf(i), k -> new HashMap<>())
              .computeIfAbsent(0L, k -> new HashMap<>())
              .computeIfAbsent(0L, k -> new HashSet<>());
        }
        String rand = String.valueOf(r.nextInt(10000));
        String fileName =
            FilePathUtils.regularizePath(dataDir.getPath())
                + IoTDBConstant.SEQUENCE_FLODER_NAME
                + File.separator
                + i
                + File.separator
                + "0"
                + File.separator
                + "0"
                + File.separator
                + rand;
        File file = new FSPath(dataDir.getFsType(), fileName).toFile();
        allFileList.get(String.valueOf(i)).get(0L).get(0L).add(file);
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

    Map<String, Set<String>> dataFileMap = new HashMap<>();
    File sequenceFile = dataDir.getChildFile(IoTDBConstant.SEQUENCE_FLODER_NAME);
    for (File sgFile : sequenceFile.listFiles()) {
      dataFileMap.putIfAbsent(sgFile.getName(), new HashSet<>());
      for (File vgFile : sgFile.listFiles()) {
        for (File trFile : vgFile.listFiles()) {
          for (File tsfile : trFile.listFiles()) {
            if (!tsfile.getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
              ((SyncClient) manager).makeFileSnapshot(tsfile);
            }
            dataFileMap.get(sgFile.getName()).add(tsfile.getName());
          }
        }
      }
    }

    assertTrue(config.getSenderFolderPath().toFile().exists());
    assertTrue(config.getSnapshotPath().toFile().exists());

    Map<String, Set<String>> snapFileMap = new HashMap<>();
    for (File sgFile : config.getSnapshotPath().toFile().listFiles()) {
      snapFileMap.putIfAbsent(sgFile.getName(), new HashSet<>());
      for (File vgFile : sgFile.listFiles()) {
        for (File trFile : vgFile.listFiles()) {
          for (File snapshotTsfile : trFile.listFiles()) {
            snapFileMap.get(sgFile.getName()).add(snapshotTsfile.getName());
          }
        }
      }
    }

    assertEquals(dataFileMap.size(), snapFileMap.size());
    for (Entry<String, Set<String>> entry : dataFileMap.entrySet()) {
      String sg = entry.getKey();
      Set<String> tsfiles = entry.getValue();
      assertTrue(snapFileMap.containsKey(sg));
      assertEquals(snapFileMap.get(sg).size(), tsfiles.size());
      assertTrue(snapFileMap.get(sg).containsAll(tsfiles));
    }

    assertFalse(config.getLastFileInfoPath().toFile().exists());
    senderLogAnalyzer.recover();
    assertFalse(config.getSnapshotPath().toFile().exists());
    assertTrue(config.getLastFileInfoPath().toFile().exists());
  }
}
