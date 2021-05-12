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
package org.apache.iotdb.db.sync.receiver.transfer;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.exception.SyncDeviceOwnerConflictException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.sync.conf.SyncConstant;
import org.apache.iotdb.db.sync.receiver.load.FileLoader;
import org.apache.iotdb.db.sync.receiver.load.FileLoaderManager;
import org.apache.iotdb.db.sync.receiver.load.IFileLoader;
import org.apache.iotdb.db.sync.receiver.recover.SyncReceiverLogAnalyzer;
import org.apache.iotdb.db.sync.receiver.recover.SyncReceiverLogger;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SyncUtils;
import org.apache.iotdb.service.sync.thrift.ConfirmInfo;
import org.apache.iotdb.service.sync.thrift.SyncService;
import org.apache.iotdb.service.sync.thrift.SyncStatus;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SyncServiceImpl implements SyncService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(SyncServiceImpl.class);

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ThreadLocal<FSPath> syncFolder = new ThreadLocal<>();

  private ThreadLocal<String> currentSG = new ThreadLocal<>();

  private ThreadLocal<SyncReceiverLogger> syncLog = new ThreadLocal<>();

  private ThreadLocal<String> senderName = new ThreadLocal<>();

  private ThreadLocal<File> currentFile = new ThreadLocal<>();

  private ThreadLocal<FileOutputStream> currentFileWriter = new ThreadLocal<>();

  private ThreadLocal<MessageDigest> messageDigest = new ThreadLocal<>();

  /** Verify IP address of sender */
  @Override
  public SyncStatus check(ConfirmInfo info) {
    String ipAddress = info.address, uuid = info.uuid;
    Thread.currentThread().setName(ThreadName.SYNC_SERVER.getName());
    if (!info.version.equals(IoTDBConstant.VERSION)) {
      return getErrorResult(
          String.format(
              "Version mismatch: the sender <%s>, the receiver <%s>",
              info.version, IoTDBConstant.VERSION));
    }
    if (info.partitionInterval
        != IoTDBDescriptor.getInstance().getConfig().getPartitionInterval()) {
      return getErrorResult(
          String.format(
              "Partition interval mismatch: the sender <%d>, the receiver <%d>",
              info.partitionInterval,
              IoTDBDescriptor.getInstance().getConfig().getPartitionInterval()));
    }
    if (SyncUtils.verifyIPSegment(config.getIpWhiteList(), ipAddress)) {
      senderName.set(ipAddress + SyncConstant.SYNC_DIR_NAME_SEPARATOR + uuid);
      if (checkRecovery()) {
        logger.info("Start to sync with sender {}", senderName.get());
        return getSuccessResult();
      } else {
        return getErrorResult("Receiver is processing data from previous sync tasks");
      }
    } else {
      return getErrorResult(
          "Sender IP is not in the white list of receiver IP and synchronization tasks are not allowed.");
    }
  }

  private boolean checkRecovery() {
    try {
      if (currentFileWriter.get() != null) {
        currentFileWriter.get().close();
      }
      if (syncLog.get() != null) {
        syncLog.get().close();
      }
      return SyncReceiverLogAnalyzer.getInstance().recover(senderName.get());
    } catch (IOException e) {
      logger.error("Check recovery state fail", e);
      return false;
    }
  }

  @Override
  public SyncStatus startSync() {
    try {
      initPath();
      currentSG.remove();
      FileLoader.createFileLoader(senderName.get(), syncFolder.get());
      syncLog.set(
          new SyncReceiverLogger(syncFolder.get().getChildFile(SyncConstant.SYNC_LOG_NAME)));
      return getSuccessResult();
    } catch (DiskSpaceInsufficientException | IOException e) {
      logger.error("Can not receiver data from sender", e);
      return getErrorResult(e.getMessage());
    }
  }

  /** Init file path. */
  private void initPath() throws DiskSpaceInsufficientException {
    FSPath seqFolder = TierManager.getInstance().getNextFolderForSequenceFile();
    String syncFolderPath =
        FilePathUtils.regularizePath(seqFolder.toFile().getParentFile().getAbsolutePath())
            + SyncConstant.SYNC_RECEIVER
            + File.separatorChar
            + senderName.get();
    syncFolder.set(new FSPath(seqFolder.getFsType(), syncFolderPath));
  }

  /** Init threadLocal variable. */
  @Override
  public SyncStatus init(String storageGroup) {
    logger.info("Sync process started to receive data of storage group {}", storageGroup);
    currentSG.set(storageGroup);
    try {
      syncLog.get().startSyncDeletedFilesName();
    } catch (IOException e) {
      logger.error("Can not init sync process", e);
      return getErrorResult(e.getMessage());
    }
    return getSuccessResult();
  }

  @Override
  public SyncStatus syncDeletedFileName(String fileName) {
    try {
      syncLog
          .get()
          .finishSyncDeletedFileName(
              getSyncDataPath().getChildFile(currentSG.get() + File.separatorChar + fileName));
      FileLoaderManager.getInstance()
          .getFileLoader(senderName.get())
          .addDeletedFileName(
              getSyncDataPath().getChildFile(currentSG.get() + File.separatorChar + fileName));
    } catch (IOException e) {
      logger.error("Can not sync deleted file", e);
      return getErrorResult(
          String.format("Can not sync deleted file %s because %s", fileName, e.getMessage()));
    }
    return getSuccessResult();
  }

  @SuppressWarnings("squid:S2095") // Suppress unclosed resource warning
  @Override
  public SyncStatus initSyncData(String filename) {
    try {
      File file;
      if (currentSG.get() == null) { // schema mlog.txt file
        file = getSyncDataPath().getChildFile(filename);
      } else {
        file = getSyncDataPath().getChildFile(currentSG.get() + File.separatorChar + filename);
      }
      file.delete();
      currentFile.set(file);
      if (!file.getParentFile().exists()) {
        file.getParentFile().mkdirs();
      }
      if (currentFileWriter.get() != null) {
        currentFileWriter.get().close();
      }
      currentFileWriter.set(new FileOutputStream(file));
      syncLog.get().startSyncTsFiles();
      messageDigest.set(MessageDigest.getInstance(SyncConstant.MESSAGE_DIGIT_NAME));
    } catch (IOException | NoSuchAlgorithmException e) {
      logger.error("Can not init sync resource for file {}", filename, e);
      return getErrorResult(
          String.format(
              "Can not init sync resource for file %s because %s", filename, e.getMessage()));
    }
    return getSuccessResult();
  }

  @Override
  public SyncStatus syncData(ByteBuffer buff) {
    try {
      int pos = buff.position();
      currentFileWriter.get().getChannel().write(buff);
      buff.position(pos);
      messageDigest.get().update(buff);
    } catch (IOException e) {
      logger.error("Can not sync data for file {}", currentFile.get().getAbsoluteFile(), e);
      return getErrorResult(
          String.format(
              "Can not sync data for file %s because %s",
              currentFile.get().getName(), e.getMessage()));
    }
    return getSuccessResult();
  }

  @SuppressWarnings("squid:S2095") // Suppress unclosed resource warning
  @Override
  public SyncStatus checkDataDigest(String digestOfSender) {
    String digestOfReceiver = (new BigInteger(1, messageDigest.get().digest())).toString(16);
    try {
      if (currentFileWriter.get() != null) {
        currentFileWriter.get().close();
      }
      if (!digestOfSender.equals(digestOfReceiver)) {
        currentFile.get().delete();
        currentFileWriter.set(new FileOutputStream(currentFile.get()));
        return getErrorResult(
            String.format(
                "Digest of the sender is differ from digest of the receiver of the file %s.",
                currentFile.get().getAbsolutePath()));
      } else {
        if (currentFile.get().getName().endsWith(MetadataConstant.METADATA_LOG)) {
          loadMetadata();
        } else {
          if (!currentFile.get().getName().endsWith(TsFileResource.RESOURCE_SUFFIX)) {
            logger.info("Receiver has received {} successfully.", currentFile.get());
            FileLoaderManager.getInstance()
                .checkAndUpdateDeviceOwner(
                    new TsFileResource(
                        FSPath.parse(currentFile.get())
                            .postConcat(TsFileResource.RESOURCE_SUFFIX)
                            .toFile()));
            syncLog.get().finishSyncTsfile(currentFile.get());
            FileLoaderManager.getInstance()
                .getFileLoader(senderName.get())
                .addTsfile(currentFile.get());
          }
        }
      }
    } catch (IOException e) {
      logger.error("Can not check data digest for file {}", currentFile.get().getAbsoluteFile(), e);
      return getErrorResult(
          String.format(
              "Can not check data digest for file %s because %s",
              currentFile.get().getName(), e.getMessage()));
    } catch (SyncDeviceOwnerConflictException e) {
      logger.error(
          "Device owner has conflicts, skip all other tsfiles in the sg {}.", currentSG.get());
      return new SyncStatus(
          SyncConstant.CONFLICT_CODE,
          String.format(
              "Device owner has conflicts, skip all other tsfiles in the same sg %s because %s",
              currentSG.get(), e.getMessage()));
    }
    return new SyncStatus(SyncConstant.SUCCESS_CODE, digestOfReceiver);
  }

  private void loadMetadata() {
    logger.info("Start to load metadata in sync process.");
    if (currentFile.get().exists()) {
      try (MLogReader mLogReader = new MLogReader(currentFile.get())) {
        while (mLogReader.hasNext()) {
          PhysicalPlan plan = null;
          try {
            plan = mLogReader.next();
            if (plan == null) {
              continue;
            }
            IoTDB.metaManager.operation(plan);
          } catch (Exception e) {
            logger.error(
                "Can not operate metadata operation {} for err:{}",
                plan == null ? "" : plan.getOperatorType(),
                e);
          }
        }
      } catch (IOException e) {
        logger.error("Cannot read the file {}.", currentFile.get().getAbsoluteFile(), e);
      }
    }
  }

  @Override
  public SyncStatus endSync() {
    try {
      if (syncLog.get() != null) {
        syncLog.get().close();
      }
      IFileLoader loader = FileLoaderManager.getInstance().getFileLoader(senderName.get());
      if (loader != null) {
        loader.endSync();
      } else {
        return getErrorResult(
            String.format("File Loader of the storage group %s is null", currentSG.get()));
      }
      if (currentFileWriter.get() != null) {
        currentFileWriter.get().close();
      }
      logger.info("Sync process with sender {} finished.", senderName.get());
    } catch (IOException e) {
      logger.error("Can not end sync", e);
      return getErrorResult(String.format("Can not end sync because %s", e.getMessage()));
    } finally {
      syncFolder.remove();
      currentSG.remove();
      syncLog.remove();
      senderName.remove();
      currentFile.remove();
      currentFileWriter.remove();
      messageDigest.remove();
    }
    return getSuccessResult();
  }

  private FSPath getSyncDataPath() {
    return syncFolder.get().postConcat(File.separatorChar + SyncConstant.RECEIVER_DATA_FOLDER_NAME);
  }

  private SyncStatus getSuccessResult() {
    return new SyncStatus(SyncConstant.SUCCESS_CODE, "");
  }

  private SyncStatus getErrorResult(String errorMsg) {
    return new SyncStatus(SyncConstant.ERROR_CODE, errorMsg);
  }

  /**
   * release resources or cleanup when a client (a sender) is disconnected (normally or abnormally).
   */
  public void handleClientExit() {
    // do nothing now
  }
}
