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
package org.apache.iotdb.db.sync.receiver.recover;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.receiver.load.FileLoader;
import org.apache.iotdb.db.sync.receiver.load.FileLoaderManager;
import org.apache.iotdb.db.sync.receiver.load.IFileLoader;
import org.apache.iotdb.db.sync.receiver.load.LoadLogger;
import org.apache.iotdb.db.sync.receiver.load.LoadType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

public class SyncReceiverLogAnalyzer implements ISyncReceiverLogAnalyzer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncReceiverLogAnalyzer.class);

  private SyncReceiverLogAnalyzer() {}

  public static SyncReceiverLogAnalyzer getInstance() {
    return SyncReceiverLogAnalyzerHolder.INSTANCE;
  }

  @Override
  public void recoverAll() throws IOException {
    FSPath[][] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    LOGGER.info("Start to recover all sync state for sync receiver.");
    for (FSPath[] tierDataDirs : dataDirs) {
      for (FSPath dataDir : tierDataDirs) {
        String path = FilePathUtils.regularizePath(dataDir.getPath()) + SyncConstant.SYNC_RECEIVER;
        File file = new FSPath(dataDir.getFsType(), path).toFile();
        if (!file.exists()) {
          continue;
        }
        for (File syncFolder : file.listFiles()) {
          recover(syncFolder);
        }
      }
    }
    LOGGER.info("Finish to recover all sync states for sync receiver.");
  }

  private boolean recover(File senderFolder) throws IOException {
    FSPath senderFolderPath = FSPath.parse(senderFolder);
    // check the state
    if (!senderFolderPath.getChildFile(SyncConstant.SYNC_LOG_NAME).exists()) {
      senderFolderPath.getChildFile(SyncConstant.LOAD_LOG_NAME).delete();
      FileUtils.deleteDirectory(
          senderFolderPath.getChildFile(SyncConstant.RECEIVER_DATA_FOLDER_NAME));
      return true;
    }
    if (FileLoaderManager.getInstance().containsFileLoader(senderFolder.getName())) {
      FileLoaderManager.getInstance().getFileLoader(senderFolder.getName()).endSync();
      try {
        Thread.sleep(FileLoader.WAIT_TIME << 1);
      } catch (InterruptedException e) {
        LOGGER.error("Thread is interrupted from waiting for ending sync in recovery.");
        Thread.currentThread().interrupt();
      }
    } else {
      scanLogger(
          FileLoader.createFileLoader(senderFolder),
          senderFolderPath.getChildFile(SyncConstant.SYNC_LOG_NAME),
          senderFolderPath.getChildFile(SyncConstant.LOAD_LOG_NAME));
    }
    return !FileLoaderManager.getInstance().containsFileLoader(senderFolder.getName());
  }

  @Override
  public boolean recover(String senderName) throws IOException {
    FSPath[][] dataDirs = IoTDBDescriptor.getInstance().getConfig().getDataDirs();
    boolean recoverComplete = true;
    for (FSPath[] tierDataDirs : dataDirs) {
      for (FSPath dataDir : tierDataDirs) {
        String path = FilePathUtils.regularizePath(dataDir.getPath()) + SyncConstant.SYNC_RECEIVER;
        File file = new FSPath(dataDir.getFsType(), path).toFile();
        if (!file.exists()) {
          continue;
        }
        for (File syncFolder : file.listFiles()) {
          if (syncFolder.getName().equals(senderName)) {
            recoverComplete &= recover(syncFolder);
          }
        }
      }
    }
    return recoverComplete;
  }

  @Override
  public void scanLogger(IFileLoader loader, File syncLog, File loadLog) {
    LoadType loadType = LoadType.NONE;
    try (BufferedReader syncReader =
        FSFactoryProducer.getFSFactory(FSUtils.getFSType(syncLog))
            .getBufferedReader(syncLog.getAbsolutePath())) {
      String line;
      try (BufferedReader loadReader =
          FSFactoryProducer.getFSFactory(FSUtils.getFSType(loadLog))
              .getBufferedReader(loadLog.getAbsolutePath())) {
        while ((line = loadReader.readLine()) != null) {
          if (line.equals(LoadLogger.LOAD_DELETED_FILE_NAME_START)) {
            loadType = LoadType.DELETE;
          } else if (line.equals(LoadLogger.LOAD_TSFILE_START)) {
            loadType = LoadType.ADD;
          } else {
            while (!syncReader.readLine().equals(line)) {}
          }
        }
      }
      loader.setCurType(loadType);
      while ((line = syncReader.readLine()) != null) {
        if (line.equals(SyncReceiverLogger.SYNC_DELETED_FILE_NAME_START)) {
          loadType = LoadType.DELETE;
        } else if (line.equals(SyncReceiverLogger.SYNC_TSFILE_START)) {
          loadType = LoadType.ADD;
        } else {
          switch (loadType) {
            case ADD:
              loader.addTsfile(FSPath.parse(line).toFile());
              break;
            case DELETE:
              loader.addDeletedFileName(FSPath.parse(line).toFile());
              break;
            default:
              LOGGER.error("Wrong load type {}", loadType);
          }
        }
      }
      loader.endSync();
    } catch (IOException e) {
      LOGGER.error("Can not scan log for recovery", e);
    }
  }

  private static class SyncReceiverLogAnalyzerHolder {

    private static final SyncReceiverLogAnalyzer INSTANCE = new SyncReceiverLogAnalyzer();
  }
}
