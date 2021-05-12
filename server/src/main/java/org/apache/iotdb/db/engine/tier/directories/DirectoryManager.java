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

package org.apache.iotdb.db.engine.tier.directories;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.tier.directories.strategy.DirectoryStrategy;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.LoadConfigurationException;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/** The main class of multiple directories. Used to allocate folders to data files. */
public class DirectoryManager {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryManager.class);

  private int tierLevel;
  private List<FSPath> sequenceFileFolders;
  private List<FSPath> unsequenceFileFolders;
  private DirectoryStrategy sequenceStrategy;
  private DirectoryStrategy unsequenceStrategy;

  public DirectoryManager(int tierLevel, FSPath[] dataDirs, String directoryStrategyName) {
    this.tierLevel = tierLevel;
    sequenceFileFolders = new ArrayList<>();
    unsequenceFileFolders = new ArrayList<>();
    for (FSPath dataDir : dataDirs) {
      sequenceFileFolders.add(
          dataDir.postConcat(File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME));
      unsequenceFileFolders.add(
          dataDir.postConcat(File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME));
    }
    mkDataDirs(sequenceFileFolders);
    mkDataDirs(unsequenceFileFolders);

    try {
      Class<?> clazz = Class.forName(directoryStrategyName);
      sequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      sequenceStrategy.setFolders(sequenceFileFolders);
      unsequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      unsequenceStrategy.setFolders(unsequenceFileFolders);
    } catch (DiskSpaceInsufficientException e) {
      logger.error("All disks of folders in tier {} are full.", tierLevel, e);
    } catch (Exception e) {
      logger.error(
          "Can't find strategy {} for mult-directories of tier {}.",
          directoryStrategyName,
          tierLevel,
          e);
    }
  }

  public void updateFileFolders(FSPath[] dataDirs) throws LoadConfigurationException {
    try {
      List<FSPath> sequenceFileFolders = new ArrayList<>();
      List<FSPath> unsequenceFileFolders = new ArrayList<>();
      for (FSPath dataDir : dataDirs) {
        sequenceFileFolders.add(
            dataDir.postConcat(File.separator + IoTDBConstant.SEQUENCE_FLODER_NAME));
        unsequenceFileFolders.add(
            dataDir.postConcat(File.separator + IoTDBConstant.UNSEQUENCE_FLODER_NAME));
      }
      mkDataDirs(sequenceFileFolders);
      mkDataDirs(unsequenceFileFolders);
      sequenceStrategy.setFolders(sequenceFileFolders);
      unsequenceStrategy.setFolders(unsequenceFileFolders);
      this.sequenceFileFolders = sequenceFileFolders;
      this.unsequenceFileFolders = unsequenceFileFolders;
      logger.info("Success to update file folders.");
    } catch (DiskSpaceInsufficientException e) {
      logger.error("Fail to update file folders of tier {}, use previous folders.", tierLevel, e);
      throw new LoadConfigurationException(
          String.format(
              "Fail to update file folders of tier %d because all disks of folders are full, use previous folders.",
              tierLevel));
    }
  }

  public void updateDirectoryStrategy(String directoryStrategyName)
      throws LoadConfigurationException {
    try {
      Class<?> clazz = Class.forName(directoryStrategyName);
      sequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      sequenceStrategy.setFolders(sequenceFileFolders);
      unsequenceStrategy = (DirectoryStrategy) clazz.newInstance();
      unsequenceStrategy.setFolders(unsequenceFileFolders);
      logger.info("Success to update directory strategy.");
    } catch (Exception e) {
      logger.error(
          "Fail to update directory strategy {} of tier {}, use previous strategy",
          directoryStrategyName,
          tierLevel,
          e);
      throw new LoadConfigurationException(
          String.format(
              "Fail to update directory strategy of %d because can't find strategy %s for mult-directories, use previous strategy",
              tierLevel, directoryStrategyName));
    }
  }

  private void mkDataDirs(List<FSPath> folders) {
    for (FSPath folder : folders) {
      File file = folder.toFile();
      if (file.mkdirs()) {
        logger.info("folder {} doesn't exist, create it", file.getPath());
      } else {
        logger.info(
            "create folder {} failed. Is the folder existed: {}", file.getPath(), file.exists());
      }
    }
  }

  public FSPath getNextFolderForSequenceFile() throws DiskSpaceInsufficientException {
    return getSequenceFileFolder(getNextFolderIndexForSequenceFile());
  }

  /**
   * get next folder index for TsFile.
   *
   * @return next folder index
   */
  public int getNextFolderIndexForSequenceFile() throws DiskSpaceInsufficientException {
    return sequenceStrategy.nextFolderIndex();
  }

  public FSPath getSequenceFileFolder(int index) {
    return sequenceFileFolders.get(index);
  }

  public List<FSPath> getAllSequenceFileFolders() {
    return new ArrayList<>(sequenceFileFolders);
  }

  public FSPath getNextFolderForUnSequenceFile() throws DiskSpaceInsufficientException {
    return getUnSequenceFileFolder(getNextFolderIndexForUnSequenceFile());
  }

  /**
   * get next folder index for OverflowFile.
   *
   * @return next folder index
   */
  public int getNextFolderIndexForUnSequenceFile() throws DiskSpaceInsufficientException {
    return unsequenceStrategy.nextFolderIndex();
  }

  public FSPath getUnSequenceFileFolder(int index) {
    return unsequenceFileFolders.get(index);
  }

  public int getUnSequenceFileFolderIndex(String folder) {
    return unsequenceFileFolders.indexOf(folder);
  }

  public List<FSPath> getAllUnSequenceFileFolders() {
    return new ArrayList<>(unsequenceFileFolders);
  }

  public int getTierLevel() {
    return tierLevel;
  }
}
