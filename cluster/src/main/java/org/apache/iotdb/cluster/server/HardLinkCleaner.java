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

package org.apache.iotdb.cluster.server;

import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class HardLinkCleaner implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(HardLinkCleaner.class);
  // hardlinks are kept for 3 days
  private static final long HARDLINK_LIFE_MS = 3 * 24 * 3600 * 1000L;

  @Override
  public void run() {
    scanFolders(TierManager.getInstance().getAllSequenceFileFolders());
    if (Thread.interrupted()) {
      return;
    }
    scanFolders(TierManager.getInstance().getAllUnSequenceFileFolders());
  }

  private void scanFolders(List<FSPath> folders) {
    for (FSPath folder : folders) {
      scanFolder(folder);
    }
  }

  private void scanFolder(FSPath folder) {
    File folderFile = folder.toFile();
    scanFile(folderFile);
  }

  private void scanFile(File file) {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File file1 : files) {
          scanFile(file1);
          if (Thread.interrupted()) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
      return;
    }
    long hardLinkCreateTime = getHardLinkCreateTime(file);
    long currentTime = System.currentTimeMillis();
    if (hardLinkCreateTime != -1 && currentTime - hardLinkCreateTime >= HARDLINK_LIFE_MS) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.debug(
            "Hardlink {} cannot be removed, leave it to the next try: {}", file, e.getMessage());
      }
    }
  }

  /**
   * @param file
   * @return -1 if the file is not a hardlink or its created time
   */
  private long getHardLinkCreateTime(File file) {
    String fileName = file.getName();
    // hardlinks have a suffix like ".[createTime]_[randomNumber]"
    int suffixIndex = fileName.lastIndexOf('.');
    if (suffixIndex > 0 && suffixIndex < fileName.length()) {
      String suffix = fileName.substring(suffixIndex + 1);
      String[] split = suffix.split("_");
      if (split.length != 2) {
        return -1;
      }
      try {
        return Long.parseLong(split[0]);
      } catch (NumberFormatException e) {
        return -1;
      }
    } else {
      return -1;
    }
  }
}
