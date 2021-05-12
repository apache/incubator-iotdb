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

package org.apache.iotdb.db.sync.sender.utils;

import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

public class FilesBlacklistWriter {
  private static final Logger logger = LoggerFactory.getLogger(FilesBlacklistWriter.class);

  private static final FilesBlacklistWriter deletedFilesBlacklistWriter =
      new FilesBlacklistWriter(SyncConstant.DELETED_BLACKLIST_FILE_NAME);
  private static final FilesBlacklistWriter toBeSyncedFilesBlacklistWriter =
      new FilesBlacklistWriter(SyncConstant.TO_BE_SYNCED_BLACKLIST_FILE_NAME);

  /** blacklist's file name */
  private final String blacklistFilename;

  private FilesBlacklistWriter(String blacklistFilename) {
    this.blacklistFilename = blacklistFilename;
  }

  public static FilesBlacklistWriter getWriter(BlacklistType type) {
    switch (type) {
      case DELETED_FILES:
        return deletedFilesBlacklistWriter;
      case TO_BE_SYNCED_FILES:
        return toBeSyncedFilesBlacklistWriter;
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Adds one tsFile to blacklist file.
   *
   * @param tsFile tsFile needs adding to blacklist
   */
  public void addToBlacklist(File tsFile, File receiverDir) {
    FSFactory fsFactory = FSFactoryProducer.getFSFactory(FSUtils.getFSType(receiverDir));
    File blacklistFile = fsFactory.getFile(receiverDir, blacklistFilename);
    try (BufferedWriter bw = fsFactory.getBufferedWriter(blacklistFile.getAbsolutePath(), true)) {
      bw.write(FSPath.parse(tsFile).getAbsoluteFSPath().getRawFSPath());
      bw.newLine();
      bw.flush();
    } catch (IOException e) {
      logger.error("Fail adding {} to blacklist {}.", tsFile, receiverDir);
    }
  }

  public enum BlacklistType {
    DELETED_FILES,
    TO_BE_SYNCED_FILES
  }
}
