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
package org.apache.iotdb.db.sync.sender.recover;

import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SyncSenderLogAnalyzer implements ISyncSenderLogAnalyzer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncSenderLogAnalyzer.class);
  private final FSFactory fsFactory;
  private FSPath senderPath;
  private File currentLocalFile;
  private File lastLocalFile;
  private File syncLogFile;
  private File tmpDeletedBlacklistFile;
  private File tmpToBeSyncedBlacklistFile;

  public SyncSenderLogAnalyzer(FSPath senderPath) {
    this.fsFactory = FSFactoryProducer.getFSFactory(senderPath.getFsType());
    this.senderPath = senderPath;
    this.currentLocalFile = senderPath.getChildFile(SyncConstant.CURRENT_LOCAL_FILE_NAME);
    this.lastLocalFile = senderPath.getChildFile(SyncConstant.LAST_LOCAL_FILE_NAME);
    this.syncLogFile = senderPath.getChildFile(SyncConstant.SYNC_LOG_NAME);
    this.tmpDeletedBlacklistFile =
        senderPath.getChildFile(
            SyncConstant.DELETED_BLACKLIST_FILE_NAME + SyncConstant.TMP_FILE_SUFFIX);
    this.tmpToBeSyncedBlacklistFile =
        senderPath.getChildFile(
            SyncConstant.TO_BE_SYNCED_BLACKLIST_FILE_NAME + SyncConstant.TMP_FILE_SUFFIX);
  }

  @Override
  public void recover() throws IOException {
    if (currentLocalFile.exists() && !lastLocalFile.exists()) {
      fsFactory.moveFile(currentLocalFile, lastLocalFile);
    } else {
      Set<String> lastLocalFiles = new HashSet<>();
      Set<String> deletedFiles = new HashSet<>();
      Set<String> newFiles = new HashSet<>();
      loadLastLocalFiles(lastLocalFiles);
      loadLogger(deletedFiles, newFiles);
      lastLocalFiles.removeAll(deletedFiles);
      lastLocalFiles.addAll(newFiles);
      updateLastLocalFile(lastLocalFiles);
    }
    FileUtils.deleteDirectory(senderPath.getChildFile(SyncConstant.DATA_SNAPSHOT_NAME));
    syncLogFile.delete();
  }

  @Override
  public void loadLastLocalFiles(Set<String> lastLocalFiles) {
    if (!lastLocalFile.exists()) {
      LOGGER.info("last local file {} doesn't exist.", lastLocalFile.getAbsolutePath());
      return;
    }
    try (BufferedReader br = fsFactory.getBufferedReader(lastLocalFile.getAbsolutePath())) {
      String line;
      while ((line = br.readLine()) != null) {
        lastLocalFiles.add(line);
      }
    } catch (IOException e) {
      LOGGER.error(
          "Can not load last local file list from file {}", lastLocalFile.getAbsoluteFile(), e);
    }
    filterToBeSyncedFiles(lastLocalFiles);
    filterDeletedFiles(lastLocalFiles);
  }

  private void filterToBeSyncedFiles(Set<String> lastLocalFiles) {
    if (!tmpToBeSyncedBlacklistFile.exists()) {
      LOGGER.debug(
          "tmp to-be-synced files blacklist {} doesn't exist.",
          tmpToBeSyncedBlacklistFile.getAbsolutePath());
      return;
    }
    try (BufferedReader br =
        fsFactory.getBufferedReader(tmpToBeSyncedBlacklistFile.getAbsolutePath())) {
      String line;
      while ((line = br.readLine()) != null) {
        File file = FSPath.parse(line).toFile();
        if (file.exists()) {
          lastLocalFiles.add(line);
        }
      }
    } catch (IOException e) {
      LOGGER.error(
          "Can not load tmp to-be-synced files blacklist from file {}",
          tmpToBeSyncedBlacklistFile.getAbsoluteFile(),
          e);
    }
  }

  private void filterDeletedFiles(Set<String> lastLocalFiles) {
    if (!tmpDeletedBlacklistFile.exists()) {
      LOGGER.debug(
          "tmp deleted files blacklist {} doesn't exist.",
          tmpDeletedBlacklistFile.getAbsolutePath());
      return;
    }
    try (BufferedReader br =
        fsFactory.getBufferedReader(tmpDeletedBlacklistFile.getAbsolutePath())) {
      String line;
      while ((line = br.readLine()) != null) {
        File file = FSPath.parse(line).toFile();
        if (!file.exists()) {
          lastLocalFiles.remove(line);
        }
      }
    } catch (IOException e) {
      LOGGER.error(
          "Can not load tmp deleted files blacklist from file {}",
          tmpDeletedBlacklistFile.getAbsoluteFile(),
          e);
    }
  }

  @Override
  public void loadLogger(Set<String> deletedFiles, Set<String> newFiles) {
    if (!syncLogFile.exists()) {
      LOGGER.info("log file {} doesn't exist.", syncLogFile.getAbsolutePath());
      return;
    }
    try (BufferedReader br = fsFactory.getBufferedReader(syncLogFile.getAbsolutePath())) {
      String line;
      int mode = 0;
      while ((line = br.readLine()) != null) {
        if (line.equals(SyncSenderLogger.SYNC_DELETED_FILE_NAME_START)) {
          mode = -1;
        } else if (line.equals(SyncSenderLogger.SYNC_TSFILE_START)) {
          mode = 1;
        } else {
          if (mode == -1) {
            deletedFiles.add(line);
          } else if (mode == 1) {
            newFiles.add(line);
          }
        }
      }
    } catch (IOException e) {
      LOGGER.error(
          "Can not load last local file list from file {}", lastLocalFile.getAbsoluteFile(), e);
    }
  }

  @Override
  public void updateLastLocalFile(Set<String> currentLocalFiles) throws IOException {
    try (BufferedWriter bw =
        fsFactory.getBufferedWriter(currentLocalFile.getAbsolutePath(), false)) {
      for (String line : currentLocalFiles) {
        bw.write(line);
        bw.newLine();
      }
      bw.flush();
    } catch (IOException e) {
      LOGGER.error("Can not clear sync log {}", syncLogFile.getAbsoluteFile(), e);
    }
    lastLocalFile.delete();
    fsFactory.moveFile(currentLocalFile, lastLocalFile);
  }
}
