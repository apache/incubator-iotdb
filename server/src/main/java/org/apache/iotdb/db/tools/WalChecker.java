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
package org.apache.iotdb.db.tools;

import static org.apache.iotdb.db.writelog.node.DifferentialWriteLogNode.WINDOW_LENGTH;
import static org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode.WAL_FILE_NAME;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.SystemCheckException;
import org.apache.iotdb.db.writelog.io.SingleFileLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WalChecker verifies that whether all insert ahead logs in the WAL folder are recognizable.
 */
public class WalChecker {

  private static final Logger logger = LoggerFactory.getLogger(WalChecker.class);

  /**
   * the root dir of wals, which should have wal directories of storage groups as its children.
   */
  private String walFolder;

  public WalChecker(String walFolder) {
    this.walFolder = walFolder;
  }

  /**
   * check the root wal dir and find the damaged files
   * @return a list of damaged files.
   * @throws SystemCheckException if the root wal dir does not exist.
   */
  public List<File> doCheck() throws SystemCheckException {
    File walFolderFile = SystemFileFactory.INSTANCE.getFile(walFolder);
    logger.info("Checking folder: {}", walFolderFile.getAbsolutePath());
    if(!walFolderFile.exists() || !walFolderFile.isDirectory()) {
      throw new SystemCheckException(walFolder);
    }

    File[] storageWalFolders = walFolderFile.listFiles();
    if (storageWalFolders == null || storageWalFolders.length == 0) {
      logger.info("No sub-directories under the given directory, check ends");
      return Collections.emptyList();
    }

    List<File> failedFiles = new ArrayList<>();
    for (int dirIndex = 0; dirIndex < storageWalFolders.length; dirIndex++) {
      File storageWalFolder = storageWalFolders[dirIndex];
      logger.info("Checking the No.{} directory {}", dirIndex, storageWalFolder.getName());
      File walFile = SystemFileFactory.INSTANCE.getFile(storageWalFolder, WAL_FILE_NAME);
      if (!checkFile(walFile)) {
        failedFiles.add(walFile);
      }
    }
    return failedFiles;
  }

  private boolean checkFile(File walFile) {
    if (!walFile.exists()) {
      logger.debug("No wal file in this dir, skipping");
      return true;
    }

    if (walFile.length() > 0 && walFile.length() < SingleFileLogReader.LEAST_LOG_SIZE) {
      // contains only one damaged log
      logger.error("{} fails the check because it is non-empty but does not contain enough bytes "
          + "even for one log.", walFile.getAbsoluteFile());
      return false;
    }

    SingleFileLogReader logReader = null;
    try {
      logReader = new SingleFileLogReader(walFile);
      while (logReader.hasNext()) {
        logReader.next();
      }
      if (logReader.isFileCorrupted()) {
        return false;
      }
    } catch (IOException e) {
      logger.error("{} fails the check because", walFile.getAbsoluteFile(), e);
      return false;
    } finally {
      if( logReader != null) {
        logReader.close();
      }
    }
    return true;
  }


  // a temporary method which should be in the integrated self-check module in the future
  public static void report(List<File> failedFiles) {
    if (failedFiles.isEmpty()) {
      logger.info("Check finished. There is no damaged file");
    } else {
      logger.error("There are {} failed files. They are {}", failedFiles.size(), failedFiles);
    }
  }

  /**
   *
   * @param args walRootDirectory
   */
  public static void main(String[] args) throws SystemCheckException {
    if (args.length < 1) {
      logger.error("No enough args: require the walRootDirectory");
      return;
    }

    WalChecker checker = new WalChecker(args[0]);
    List<File> files = checker.doCheck();
    report(files);
  }
}
