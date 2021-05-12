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

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.SyncDeviceOwnerConflictException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class FileLoader implements IFileLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileLoader.class);

  public static final int WAIT_TIME = 100;

  private FSPath syncFolder;

  private String senderName;

  private BlockingQueue<LoadTask> queue = new LinkedBlockingQueue<>();

  private ILoadLogger loadLog;

  private LoadType curType = LoadType.NONE;

  private volatile boolean endSync = false;

  private FileLoader(String senderName, FSPath syncFolder) throws IOException {
    this.senderName = senderName;
    this.syncFolder = syncFolder;
    this.loadLog = new LoadLogger(syncFolder.getChildFile(SyncConstant.LOAD_LOG_NAME));
  }

  public static FileLoader createFileLoader(String senderName, FSPath syncFolder)
      throws IOException {
    FileLoader fileLoader = new FileLoader(senderName, syncFolder);
    FileLoaderManager.getInstance().addFileLoader(senderName, fileLoader);
    FileLoaderManager.getInstance().addLoadTaskRunner(fileLoader.loadTaskRunner);
    return fileLoader;
  }

  public static FileLoader createFileLoader(File syncFolder) throws IOException {
    return createFileLoader(syncFolder.getName(), FSPath.parse(syncFolder));
  }

  private Runnable loadTaskRunner =
      () -> {
        try {
          while (true) {
            if (queue.isEmpty() && endSync) {
              cleanUp();
              break;
            }
            LoadTask loadTask = queue.poll(WAIT_TIME, TimeUnit.MILLISECONDS);
            if (loadTask != null) {
              try {
                handleLoadTask(loadTask);
              } catch (Exception e) {
                LOGGER.error("Can not load task {}", loadTask, e);
              }
            }
          }
        } catch (InterruptedException e) {
          LOGGER.error("Can not handle load task", e);
          Thread.currentThread().interrupt();
        }
      };

  @Override
  public void addDeletedFileName(File deletedFile) {
    queue.add(new LoadTask(deletedFile, LoadType.DELETE));
  }

  @Override
  public void addTsfile(File tsfile) {
    queue.add(new LoadTask(tsfile, LoadType.ADD));
  }

  @Override
  public void endSync() {
    if (!endSync && FileLoaderManager.getInstance().containsFileLoader(senderName)) {
      this.endSync = true;
    }
  }

  @Override
  public void handleLoadTask(LoadTask task) throws IOException {
    switch (task.type) {
      case ADD:
        loadNewTsfile(task.file);
        break;
      case DELETE:
        loadDeletedFile(task.file);
        break;
      default:
        LOGGER.error("Wrong load task type {}", task.type);
    }
  }

  private void loadNewTsfile(File newTsFile) throws IOException {
    if (curType != LoadType.ADD) {
      loadLog.startLoadTsFiles();
      curType = LoadType.ADD;
    }
    if (!newTsFile.exists()) {
      LOGGER.info("Tsfile {} doesn't exist.", newTsFile.getAbsolutePath());
      return;
    }
    TsFileResource tsFileResource = new TsFileResource(newTsFile);
    FileLoaderUtils.checkTsFileResource(tsFileResource);
    try {
      FileLoaderManager.getInstance().checkAndUpdateDeviceOwner(tsFileResource);
      StorageEngine.getInstance().loadNewTsFileForSync(tsFileResource);
    } catch (SyncDeviceOwnerConflictException e) {
      LOGGER.error("Device owner has conflicts, so skip the loading file", e);
    } catch (LoadFileException | StorageEngineException | IllegalPathException e) {
      throw new IOException(
          String.format("Can not load new tsfile %s", newTsFile.getAbsolutePath()), e);
    }
    loadLog.finishLoadTsfile(newTsFile);
  }

  private void loadDeletedFile(File deletedTsFile) throws IOException {
    if (curType != LoadType.DELETE) {
      loadLog.startLoadDeletedFiles();
      curType = LoadType.DELETE;
    }
    try {
      if (!StorageEngine.getInstance().deleteTsfileForSync(deletedTsFile)) {
        LOGGER.info("The file {} to be deleted doesn't exist.", deletedTsFile.getAbsolutePath());
      }
    } catch (StorageEngineException | IllegalPathException e) {
      throw new IOException(
          String.format("Can not load deleted tsfile %s", deletedTsFile.getAbsolutePath()), e);
    }
    loadLog.finishLoadDeletedFile(deletedTsFile);
  }

  @Override
  public void cleanUp() {
    try {
      loadLog.close();
      syncFolder.getChildFile(SyncConstant.SYNC_LOG_NAME).delete();
      syncFolder.getChildFile(SyncConstant.LOAD_LOG_NAME).delete();
      FileUtils.deleteDirectory(syncFolder.getChildFile(SyncConstant.RECEIVER_DATA_FOLDER_NAME));
      FileLoaderManager.getInstance().removeFileLoader(senderName);
      LOGGER.info("Sync loading process for {} has finished.", senderName);
    } catch (IOException e) {
      LOGGER.error("Can not clean up sync resource.", e);
    }
  }

  @Override
  public void setCurType(LoadType curType) {
    this.curType = curType;
  }

  class LoadTask {

    private File file;
    private LoadType type;

    LoadTask(File file, LoadType type) {
      this.file = file;
      this.type = type;
    }

    @Override
    public String toString() {
      return "LoadTask{" + "file=" + file.getAbsolutePath() + ", type=" + type + '}';
    }
  }
}
