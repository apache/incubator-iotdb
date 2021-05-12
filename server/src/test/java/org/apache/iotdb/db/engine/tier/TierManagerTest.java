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

package org.apache.iotdb.db.engine.tier;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.tier.directories.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.db.engine.tier.directories.strategy.RandomOnDiskUsableSpaceStrategy;
import org.apache.iotdb.db.engine.tier.directories.strategy.SequenceStrategy;
import org.apache.iotdb.db.engine.tier.migration.IMigrationStrategy;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadConfigurationException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.*;

import java.io.File;
import java.util.List;

public class TierManagerTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final TierManager tierManager = TierManager.getInstance();
  private static FSPath[][] originDataDirs;
  private static FSPath[][] dataDirs;
  private String[] originMultiDirStrategies;
  private String[] originTierMigrationStrategyClassNames;

  @BeforeClass
  public static void beforeClass() throws Exception {
    originDataDirs = config.getDataDirs();
    dataDirs = new FSPath[3][2];
    for (int tierLevel = 0; tierLevel < 3; ++tierLevel) {
      for (int i = 0; i < 2; i++) {
        FSPath folder =
            new FSPath(
                TestConstant.DEFAULT_TEST_FS,
                TestConstant.OUTPUT_DATA_DIR + tierLevel + IoTDBConstant.FILE_NAME_SEPARATOR + i);
        dataDirs[tierLevel][i] = folder;
      }
    }
    config.setDataDirs(dataDirs);
    tierManager.updateFileFolders();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    config.setDataDirs(originDataDirs);
    tierManager.updateFileFolders();
    EnvironmentUtils.cleanAllDir();
  }

  @Before
  public void setUp() throws Exception {
    originMultiDirStrategies = config.getMultiDirStrategyClassNames();
    originTierMigrationStrategyClassNames = config.getTierMigrationStrategyClassNames();
  }

  @After
  public void tearDown() throws Exception {
    config.setMultiDirStrategyClassNames(originMultiDirStrategies);
    tierManager.updateDirectoryStrategies();
    config.setTierMigrationStrategyClassNames(originTierMigrationStrategyClassNames);
    tierManager.updateDirectoryStrategies();
  }

  @Test(expected = LoadConfigurationException.class)
  public void testUpdateDirectoryStrategiesWithWrongNum() throws LoadConfigurationException {
    String[] multiDirStrategies =
        new String[] {
          MinFolderOccupiedSpaceFirstStrategy.class.getName(),
          RandomOnDiskUsableSpaceStrategy.class.getName()
        };
    config.setMultiDirStrategyClassNames(multiDirStrategies);
    tierManager.updateDirectoryStrategies();
  }

  @Test
  public void testUpdateDirectoryStrategies()
      throws LoadConfigurationException, DiskSpaceInsufficientException {
    String[] multiDirStrategies =
        new String[] {
          SequenceStrategy.class.getName(),
          SequenceStrategy.class.getName(),
          SequenceStrategy.class.getName()
        };
    config.setMultiDirStrategyClassNames(multiDirStrategies);
    tierManager.updateDirectoryStrategies();

    for (int tierLevel = 0; tierLevel < 3; ++tierLevel) {
      for (int i = 0; i < 2; i++) {
        FSPath seqFolder = dataDirs[tierLevel][i].postConcat(File.separator, "sequence");
        Assert.assertEquals(
            seqFolder,
            tierManager.getTierDirectoryManager(tierLevel).getNextFolderForSequenceFile());
      }
    }
  }

  @Test(expected = LoadConfigurationException.class)
  public void testUpdateMigrationStrategiesWithWrongNum() throws LoadConfigurationException {
    String[] tierMigrationStrategyClassNames =
        new String[] {
          IMigrationStrategy.PINNED_STRATEGY_CLASS_NAME,
          IMigrationStrategy.TIME2LIVE_STRATEGY_CLASS_NAME + "(500)"
        };
    config.setTierMigrationStrategyClassNames(tierMigrationStrategyClassNames);
    tierManager.updateMigrationStrategies();
  }

  @Test
  public void testGetAllSequenceFileFolders() {
    List<FSPath> seqFolders = tierManager.getAllSequenceFileFolders();
    Assert.assertEquals(6, seqFolders.size());
    for (int tierLevel = 0; tierLevel < 3; ++tierLevel) {
      for (int i = 0; i < 2; i++) {
        Assert.assertEquals(
            dataDirs[tierLevel][i].postConcat(File.separator, "sequence"),
            seqFolders.get(tierLevel * 2 + i));
      }
    }
  }

  @Test
  public void testGetAllUnSequenceFileFolders() {
    List<FSPath> unSeqFolders = tierManager.getAllUnSequenceFileFolders();
    Assert.assertEquals(6, unSeqFolders.size());
    for (int tierLevel = 0; tierLevel < 3; ++tierLevel) {
      for (int i = 0; i < 2; i++) {
        Assert.assertEquals(
            dataDirs[tierLevel][i].postConcat(File.separator, "unsequence"),
            unSeqFolders.get(tierLevel * 2 + i));
      }
    }
  }

  @Test
  public void testGetNextFolderForSequenceFile() throws DiskSpaceInsufficientException {
    FSPath seqFolder = tierManager.getNextFolderForSequenceFile();
    Assert.assertEquals(dataDirs[0][0].postConcat(File.separator, "sequence"), seqFolder);
  }

  @Test
  public void testGetNextFolderForUnSequenceFile() throws DiskSpaceInsufficientException {
    FSPath unSeqFolder = tierManager.getNextFolderForUnSequenceFile();
    Assert.assertEquals(dataDirs[0][0].postConcat(File.separator, "unsequence"), unSeqFolder);
  }

  @Test
  public void testGetTierLevel() throws DiskSpaceInsufficientException {
    File tsFile =
        tierManager
            .getTierDirectoryManager(1)
            .getNextFolderForSequenceFile()
            .getChildFile(
                "root.sg"
                    + File.separator
                    + 0
                    + File.separator
                    + 0
                    + File.separator
                    + "test.tsfile");
    Assert.assertEquals(1, tierManager.getTierLevel(tsFile));
  }

  @Test
  public void testGetTiersNum() {
    Assert.assertEquals(3, tierManager.getTiersNum());
  }
}
