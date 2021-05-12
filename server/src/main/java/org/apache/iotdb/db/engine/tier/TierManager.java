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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.tier.directories.DirectoryManager;
import org.apache.iotdb.db.engine.tier.directories.strategy.MaxDiskUsableSpaceFirstStrategy;
import org.apache.iotdb.db.engine.tier.migration.IMigrationStrategy;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadConfigurationException;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is used to maintain the relationship between storage tiers and data directories. It
 * also specifies the multi-directory selecting strategy and default migration strategy of each
 * tier.
 */
public class TierManager {

  private static final Logger logger = LoggerFactory.getLogger(TierManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final String DEFAULT_MULTI_DIR_STRATEGY =
      MaxDiskUsableSpaceFirstStrategy.class.getName();
  private static final String DEFAULT_TIER_MIGRATION_STRATEGY =
      IMigrationStrategy.PINNED_STRATEGY_CLASS_NAME;

  /** directory manager of each tier, managing both data directories and multi-dir strategy */
  private List<DirectoryManager> tiers;
  /** default migration strategy of each tier */
  private List<IMigrationStrategy> migrationStrategies;
  /** file folder's rawFsPath path -> tier level */
  private final Map<String, Integer> directoryTierLevel = new HashMap<>();

  public static TierManager getInstance() {
    return TierManagerHolder.INSTANCE;
  }

  private TierManager() {
    // init tiers and multi-dir strategies
    tiers = new ArrayList<>();
    FSPath[][] dataDirs = config.getDataDirs();
    String[] dirStrategyClassNames = config.getMultiDirStrategyClassNames();
    if (dirStrategyClassNames.length != dataDirs.length) {
      logger.error(
          "Multi-dir strategy amount {} doesn't match tier amount {}.",
          dirStrategyClassNames.length,
          dataDirs.length);
      // reset multiDirStrategyClassNames
      String[] finalDirStrategyClassNames = new String[dataDirs.length];
      for (int i = 0; i < dataDirs.length; ++i) {
        finalDirStrategyClassNames[i] = DEFAULT_MULTI_DIR_STRATEGY;
        if (i < dirStrategyClassNames.length) {
          finalDirStrategyClassNames[i] = dirStrategyClassNames[i];
        }
      }
      dirStrategyClassNames = finalDirStrategyClassNames;
      config.setMultiDirStrategyClassNames(finalDirStrategyClassNames);
    }
    for (int i = 0; i < dataDirs.length; ++i) {
      tiers.add(new DirectoryManager(i, dataDirs[i], dirStrategyClassNames[i]));
    }
    // init migration strategies
    migrationStrategies = new ArrayList<>();
    String[] migrationStrategyClassNames = config.getTierMigrationStrategyClassNames();
    if (migrationStrategyClassNames.length != tiers.size()) {
      logger.error(
          "Tier migration strategy amount {} doesn't match tier amount {}.",
          migrationStrategyClassNames.length,
          tiers.size());
    }
    for (int i = 0; i < dataDirs.length; ++i) {
      if (i < migrationStrategyClassNames.length) {
        migrationStrategies.add(IMigrationStrategy.parse(migrationStrategyClassNames[i]));
      } else {
        migrationStrategies.add(IMigrationStrategy.parse(DEFAULT_TIER_MIGRATION_STRATEGY));
      }
    }
    // init relationship between data directory and tier level
    updateDirectoryTierLevel(dataDirs);
  }

  private void updateDirectoryTierLevel(FSPath[][] dataDirs) {
    directoryTierLevel.clear();
    for (int tierLevel = 0; tierLevel < dataDirs.length; ++tierLevel) {
      for (FSPath directory : dataDirs[tierLevel]) {
        directoryTierLevel.put(directory.getAbsoluteFSPath().getRawFSPath(), tierLevel);
      }
    }
  }

  /**
   * Updates folders of each tier.
   *
   * @throws LoadConfigurationException if the amount of tiers is changed
   */
  public synchronized void updateFileFolders() throws LoadConfigurationException {
    FSPath[][] dataDirs = config.getDataDirs();
    String[] dirStrategyClassNames = config.getMultiDirStrategyClassNames();
    if (dataDirs.length != tiers.size()) {
      logger.error("Change tier amount from {} to {}.", tiers.size(), dataDirs.length);
      // reset multiDirStrategyClassNames
      String[] finalDirStrategyClassNames = new String[dataDirs.length];
      for (int i = 0; i < dataDirs.length; ++i) {
        finalDirStrategyClassNames[i] = DEFAULT_MULTI_DIR_STRATEGY;
        if (i < dirStrategyClassNames.length) {
          finalDirStrategyClassNames[i] = dirStrategyClassNames[i];
        }
      }
      dirStrategyClassNames = finalDirStrategyClassNames;
      config.setMultiDirStrategyClassNames(finalDirStrategyClassNames);
    }
    // update file folders in DirectoryManager or create new DirectoryManager
    List<DirectoryManager> updatedTiers = new ArrayList<>();
    for (int i = 0; i < dataDirs.length; ++i) {
      if (i < tiers.size()) {
        DirectoryManager tier = tiers.get(i);
        tier.updateFileFolders(dataDirs[i]);
        updatedTiers.add(tier);
      } else {
        updatedTiers.add(new DirectoryManager(i, dataDirs[i], dirStrategyClassNames[i]));
      }
    }
    this.tiers = updatedTiers;
    // update file folders' tier level
    updateDirectoryTierLevel(dataDirs);
  }

  /**
   * Updates multi-directory selecting strategy of each tier.
   *
   * @throws LoadConfigurationException if the strategy amount doesn't correspond to the tier amount
   */
  public synchronized void updateDirectoryStrategies() throws LoadConfigurationException {
    String[] dirStrategyClassNames = config.getMultiDirStrategyClassNames();
    if (dirStrategyClassNames.length != tiers.size()) {
      logger.error(
          "Multi-dir strategy amount {} doesn't match tier amount {}.",
          dirStrategyClassNames.length,
          tiers.size());
      throw new LoadConfigurationException(
          String.format(
              "Multi-dir strategy amount %d doesn't match tier amount %d.",
              dirStrategyClassNames.length, tiers.size()));
    }
    for (int i = 0; i < tiers.size(); ++i) {
      String directoryStrategyName = DEFAULT_MULTI_DIR_STRATEGY;
      if (i < dirStrategyClassNames.length) {
        directoryStrategyName = dirStrategyClassNames[i];
      }
      tiers.get(i).updateDirectoryStrategy(directoryStrategyName);
    }
  }

  /**
   * Updates default migration strategy of each tier.
   *
   * @throws LoadConfigurationException if the strategy amount doesn't correspond to the tier amount
   */
  public synchronized void updateMigrationStrategies() throws LoadConfigurationException {
    String[] migrationStrategyClassNames = config.getTierMigrationStrategyClassNames();
    if (migrationStrategyClassNames.length != tiers.size()) {
      logger.error(
          "Tier migration strategy amount {} doesn't match tier amount {}.",
          migrationStrategyClassNames.length,
          tiers.size());
      throw new LoadConfigurationException(
          String.format(
              "Tier migration strategy amount %d doesn't match tier amount %d.",
              migrationStrategyClassNames.length, tiers.size()));
    }
    List<IMigrationStrategy> migrationStrategies = new ArrayList<>();
    for (String migrationStrategyName : migrationStrategyClassNames) {
      migrationStrategies.add(IMigrationStrategy.parse(migrationStrategyName));
    }
    this.migrationStrategies = migrationStrategies;
  }

  /**
   * Returns all folders for sequence file, ascending ordered by tier level.
   *
   * @return all sequence file folders
   */
  public List<FSPath> getAllSequenceFileFolders() {
    List<FSPath> allSeqFileFolders = new ArrayList<>();
    for (DirectoryManager tier : tiers) {
      allSeqFileFolders.addAll(tier.getAllSequenceFileFolders());
    }
    return allSeqFileFolders;
  }

  /**
   * Returns all folders for unsequence file, ascending ordered by tier level.
   *
   * @return all unsequence file folders
   */
  public List<FSPath> getAllUnSequenceFileFolders() {
    List<FSPath> allUnSeqFileFolders = new ArrayList<>();
    for (DirectoryManager tier : tiers) {
      allUnSeqFileFolders.addAll(tier.getAllUnSequenceFileFolders());
    }
    return allUnSeqFileFolders;
  }

  /**
   * Gets next sequence folder for TsFile. If all folders in one tier is full, it will choose folder
   * from next tier.
   *
   * @return next sequence folder
   * @throws DiskSpaceInsufficientException if all sequence file folders are full
   */
  public FSPath getNextFolderForSequenceFile() throws DiskSpaceInsufficientException {
    for (DirectoryManager tier : tiers) {
      try {
        return tier.getNextFolderForSequenceFile();
      } catch (DiskSpaceInsufficientException e) {
        logger.error(e.getMessage());
      }
    }
    throw new DiskSpaceInsufficientException(getAllSequenceFileFolders());
  }

  /**
   * Gets next unsequence folder for TsFile. If all folders in one tier is full, it will choose
   * folder from next tier.
   *
   * @return next unsequence folder
   * @throws DiskSpaceInsufficientException if all unsequence file folders are full
   */
  public FSPath getNextFolderForUnSequenceFile() throws DiskSpaceInsufficientException {
    for (DirectoryManager tier : tiers) {
      try {
        return tier.getNextFolderForUnSequenceFile();
      } catch (DiskSpaceInsufficientException e) {
        logger.error(e.getMessage());
      }
    }
    throw new DiskSpaceInsufficientException(getAllUnSequenceFileFolders());
  }

  /**
   * Get the tier level of the internal TsFile. An internal file means that the file is in the
   * engine's data directories.
   *
   * @param tsFile an internal TsFile
   * @return tier level
   */
  public int getTierLevel(File tsFile) {
    // locate the data directory
    File dataDir =
        tsFile.getParentFile().getParentFile().getParentFile().getParentFile().getParentFile();
    Integer tierLevel =
        directoryTierLevel.get(FSPath.parse(dataDir).getAbsoluteFSPath().getRawFSPath());
    if (tierLevel == null) {
      String msg =
          String.format(
              "Cannot find the tier level of TsFile %s, maybe it isn't an internal file",
              tsFile.getAbsolutePath());
      logger.info(msg);
      throw new RuntimeException(msg);
    }
    return tierLevel;
  }

  /**
   * Gets {@code DirectoryManager} of a certain tier.
   *
   * @param tierLevel tier level
   * @return directory manager of a certain tier
   */
  public DirectoryManager getTierDirectoryManager(int tierLevel) {
    return tiers.get(tierLevel);
  }

  public List<IMigrationStrategy> getMigrationStrategies() {
    return migrationStrategies;
  }

  public int getTiersNum() {
    return tiers.size();
  }

  private static class TierManagerHolder {

    private static final TierManager INSTANCE = new TierManager();
  }
}
