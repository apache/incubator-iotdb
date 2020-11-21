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
package org.apache.iotdb.cluster.log.manage.serializable;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.HardState;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.StableEntryManager;
import org.apache.iotdb.cluster.utils.DoublyBuffer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.version.SimpleFileVersionController;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncLogDequeSerializer implements StableEntryManager {

  private static final Logger logger = LoggerFactory.getLogger(SyncLogDequeSerializer.class);
  private static final String LOG_DATA_FILE_SUFFIX = "data";
  private static final String LOG_INDEX_FILE_SUFFIX = "idx";

  /**
   * the log data files
   */
  private List<File> logDataFileList;

  /**
   * the log index files
   */
  private List<File> logIndexFileList;

  private LogParser parser = LogParser.getINSTANCE();
  private File metaFile;
  private FileOutputStream currentLogDataOutputStream;
  private FileOutputStream currentLogIndexOutputStream;
  private LogManagerMeta meta;
  private HardState state;

  /**
   * min version of available log
   */
  private long minAvailableVersion = 0;

  /**
   * max version of available log
   */
  private long maxAvailableVersion = Long.MAX_VALUE;

  private String logDir;

  private VersionController versionController;

  private DoublyBuffer logDataBuffer = new DoublyBuffer(
      ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize() / 2);
  private DoublyBuffer logIndexBuffer = new DoublyBuffer(
      ClusterDescriptor.getInstance().getConfig().getRaftLogBufferSize() / 2);

  private long offsetOfTheCurrentLogDataOutputStream = 0;

  private static final int maxNumberOfLogsPerFetchOnDisk = ClusterDescriptor.getInstance()
      .getConfig().getMaxNumberOfLogsPerFetchOnDisk();

  private static final ExecutorService FLUSH_BUFFER_THREAD_POOL = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("Flush-Raft-Log-Thread-%d").setDaemon(true)
          .build());

  /**
   * file name pattern:
   * <p>
   * for log data file: ${startLogIndex}-${endLogIndex}-{version}-data
   * <p>
   * for log index file: ${startLogIndex}-${endLogIndex}-{version}-idx
   */
  private static final int FILE_NAME_PART_LENGTH = 4;

  private int maxRaftLogIndexSizeInMemory = ClusterDescriptor.getInstance().getConfig()
      .getMaxRaftLogIndexSizeInMemory();

  private int maxRaftLogPersistDataSizePerFile = ClusterDescriptor.getInstance().getConfig()
      .getMaxRaftLogPersistDataSizePerFile();

  private int maxNumberOfPersistRaftLogFiles = ClusterDescriptor.getInstance().getConfig()
      .getMaxNumberOfPersistRaftLogFiles();

  private int maxPersistRaftLogNumberOnDisk = ClusterDescriptor.getInstance().getConfig()
      .getMaxPersistRaftLogNumberOnDisk();

  private ScheduledExecutorService persistLogDeleteExecutorService;
  private ScheduledFuture<?> persistLogDeleteLogFuture;

  /**
   * indicate the first raft log's index of {@link SyncLogDequeSerializer#logIndexOffsetList}, for
   * example, if firstLogIndex=1000, then the offset of the log index 1000 equals
   * logIndexOffsetList[0], the offset of the log index 1001 equals logIndexOffsetList[1], and so
   * on.
   */
  private long firstLogIndex = 0;

  /**
   * the offset of the log's index, for example, the first value is the offset of index
   * ${firstLogIndex}, the second value is the offset of index ${firstLogIndex+1}
   */
  private List<Long> logIndexOffsetList;

  private static final int logDeleteCheckIntervalSecond = 5;

  /**
   * the lock uses when change the log data buffer or log index buffer
   */
  private final Lock bufferLock = new ReentrantLock();

  /**
   * the lock uses when close/flush/open the log data file or log index file
   */
  private final Lock fileLock = new ReentrantLock();


  private volatile boolean isClosed = false;

  private void initCommonProperties() {
    this.logDataFileList = new ArrayList<>();
    this.logIndexFileList = new ArrayList<>();
    this.logIndexOffsetList = new ArrayList<>(maxRaftLogIndexSizeInMemory);
    try {
      versionController = new SimpleFileVersionController(logDir);
    } catch (IOException e) {
      logger.error("log serializer build version controller failed", e);
    }
    this.persistLogDeleteExecutorService = new ScheduledThreadPoolExecutor(1,
        new BasicThreadFactory.Builder().namingPattern("persist-log-delete-" + logDir).daemon(true)
            .build());

    this.persistLogDeleteLogFuture = persistLogDeleteExecutorService
        .scheduleAtFixedRate(this::checkDeletePersistRaftLog, logDeleteCheckIntervalSecond,
            logDeleteCheckIntervalSecond,
            TimeUnit.SECONDS);
  }

  /**
   * for log tools
   *
   * @param logPath log dir path
   */
  public SyncLogDequeSerializer(String logPath) {
    logDir = logPath + File.separator;
    initCommonProperties();
    initMetaAndLogFiles();
  }

  /**
   * log in disk is [size of log1 | log1 buffer] [size of log2 | log2 buffer]
   * <p>
   * build serializer with node id
   */
  public SyncLogDequeSerializer(int nodeIdentifier) {
    logDir = getLogDir(nodeIdentifier);
    initCommonProperties();
    initMetaAndLogFiles();
  }

  public static String getLogDir(int nodeIdentifier) {
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    return systemDir + File.separator + "raftLog" + File.separator +
        nodeIdentifier + File.separator;
  }

  @TestOnly
  String getLogDir() {
    return logDir;
  }

  @TestOnly
  File getMetaFile() {
    return metaFile;
  }

  /**
   * for log tools
   */
  public LogManagerMeta getMeta() {
    return meta;
  }

  /**
   * Recover all the logs in disk. This function will be called once this instance is created.
   */
  @Override
  public List<Log> getAllEntriesAfterAppliedIndex() {
    logger.debug("getAllEntriesBeforeAppliedIndex, maxHaveAppliedCommitIndex={}, commitLogIndex={}",
        meta.getMaxHaveAppliedCommitIndex(), meta.getCommitLogIndex());
    if (meta.getMaxHaveAppliedCommitIndex() >= meta.getCommitLogIndex()) {
      return Collections.emptyList();
    }
    return getLogs(meta.getMaxHaveAppliedCommitIndex(), meta.getCommitLogIndex());
  }

  @Override
  public void append(List<Log> entries, long maxHaveAppliedCommitIndex) throws IOException {
    bufferLock.lock();
    try {
      putLogs(entries);
      Log entry = entries.get(entries.size() - 1);
      meta.setCommitLogIndex(entry.getCurrLogIndex());
      meta.setCommitLogTerm(entry.getCurrLogTerm());
      meta.setLastLogIndex(entry.getCurrLogIndex());
      meta.setLastLogTerm(entry.getCurrLogTerm());
      meta.setMaxHaveAppliedCommitIndex(maxHaveAppliedCommitIndex);
      logger.debug("maxHaveAppliedCommitIndex={}, commitLogIndex={}, lastLogIndex={}",
          maxHaveAppliedCommitIndex, meta.getCommitLogIndex(), meta.getLastLogIndex());
    } catch (BufferOverflowException e) {
      throw new IOException(
          "Log cannot fit into buffer, please increase raft_log_buffer_size;"
              + "otherwise, please increase the JVM memory", e
      );
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("append logs was interrupted, logs size={}", entries.size(), e);
    } finally {
      bufferLock.unlock();
    }
  }

  /**
   * Put each log in entries to local buffer. If the buffer overflows, flush the buffer to the disk,
   * and try to push the log again.
   *
   * @param entries logs to put to buffer
   */
  private void putLogs(List<Log> entries) throws InterruptedException {
    for (Log log : entries) {
      logDataBuffer.getWorkingBuffer().mark();
      logIndexBuffer.getWorkingBuffer().mark();
      ByteBuffer logData = log.serialize();
      int size = logData.capacity() + Integer.BYTES;
      try {
        logDataBuffer.getWorkingBuffer().putInt(logData.capacity());
        logDataBuffer.getWorkingBuffer().put(logData);
        logIndexBuffer.getWorkingBuffer().putLong(offsetOfTheCurrentLogDataOutputStream);
        logIndexOffsetList.add(offsetOfTheCurrentLogDataOutputStream);
        offsetOfTheCurrentLogDataOutputStream += size;
      } catch (BufferOverflowException e) {
        logger.debug("Raft log buffer overflow!");
        logDataBuffer.getWorkingBuffer().reset();
        logIndexBuffer.getWorkingBuffer().reset();

        logDataBuffer.switchWorkingBufferToFlushing();
        logIndexBuffer.switchWorkingBufferToFlushing();

        if (offsetOfTheCurrentLogDataOutputStream > maxRaftLogPersistDataSizePerFile) {
          FLUSH_BUFFER_THREAD_POOL
              .submit(() -> flushBufferAndCloseFile(log.getCurrLogIndex() - 1));
          offsetOfTheCurrentLogDataOutputStream = 0;
        } else {
          FLUSH_BUFFER_THREAD_POOL.submit(this::flushLogBuffer);
        }

        logDataBuffer.switchIdlingBufferToWorking();
        logIndexBuffer.switchIdlingBufferToWorking();

        logDataBuffer.getWorkingBuffer().putInt(logData.capacity());
        logDataBuffer.getWorkingBuffer().put(logData);
        logIndexBuffer.getWorkingBuffer().putLong(offsetOfTheCurrentLogDataOutputStream);
        logIndexOffsetList.add(offsetOfTheCurrentLogDataOutputStream);
        offsetOfTheCurrentLogDataOutputStream += size;
      }
    }
  }

  private void flushBufferAndCloseFile(long commitIndex) {
    fileLock.lock();
    try {
      flushLogBuffer();
      closeCurrentFile(commitIndex);
      serializeMeta(meta);
      createNewLogFile(logDir, commitIndex + 1);
    } catch (Exception e) {
      logger.error("flush buffer and check close current file exception", e);
    } finally {
      fileLock.unlock();
    }
  }

  private void closeCurrentFile(long commitIndex) throws IOException {
    if (currentLogDataOutputStream != null) {
      currentLogDataOutputStream.close();
      logger.info("{}: Closed a log data file {}", this, getCurrentLogDataFile());
      currentLogDataOutputStream = null;

      File currentLogDataFile = getCurrentLogDataFile();
      String newDataFileName = currentLogDataFile.getName()
          .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(commitIndex));
      File newCurrentLogDatFile = SystemFileFactory.INSTANCE
          .getFile(currentLogDataFile.getParent() + File.separator + newDataFileName);
      if (!currentLogDataFile.renameTo(newCurrentLogDatFile)) {
        logger.error("rename log data file={} to {} failed", currentLogDataFile.getAbsoluteFile(),
            newCurrentLogDatFile);
      }
      logDataFileList.set(logDataFileList.size() - 1, newCurrentLogDatFile);

      logger.debug("rename data file={} to file={}", currentLogDataFile.getAbsoluteFile(),
          newCurrentLogDatFile.getAbsoluteFile());
    }

    if (currentLogIndexOutputStream != null) {
      currentLogIndexOutputStream.close();
      logger.info("{}: Closed a log index file {}", this, getCurrentLogIndexFile());
      currentLogIndexOutputStream = null;

      File currentLogIndexFile = getCurrentLogIndexFile();
      String newIndexFileName = currentLogIndexFile.getName()
          .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(commitIndex));
      File newCurrentLogIndexFile = SystemFileFactory.INSTANCE
          .getFile(currentLogIndexFile.getParent() + File.separator + newIndexFileName);
      if (!currentLogIndexFile.renameTo(newCurrentLogIndexFile)) {
        logger.error("rename log index file={} failed", currentLogIndexFile.getAbsoluteFile());
      }
      logger.debug("rename index file={} to file={}", currentLogIndexFile.getAbsoluteFile(),
          newCurrentLogIndexFile.getAbsoluteFile());

      logIndexFileList.set(logIndexFileList.size() - 1, newCurrentLogIndexFile);
    }
  }

  @Override
  public void flushLogBuffer() {
    if (isClosed) {
      return;
    }

    if (logDataBuffer.getFlushingBuffer().position() == 0) {
      try {
        logDataBuffer.switchFlushingBufferToIdling();
        logIndexBuffer.switchFlushingBufferToIdling();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("flush log buffer interrupted", e);
      }
      return;
    }

    fileLock.lock();
    try {
      // write into disk
      try {
        checkStream();
        // 1. write to the log data file
        ReadWriteIOUtils
            .writeWithoutSize(logDataBuffer.getFlushingBuffer(), 0,
                logDataBuffer.getFlushingBuffer().position(),
                currentLogDataOutputStream);
        ReadWriteIOUtils
            .writeWithoutSize(logIndexBuffer.getFlushingBuffer(), 0,
                logIndexBuffer.getFlushingBuffer().position(),
                currentLogIndexOutputStream);
        if (ClusterDescriptor.getInstance().getConfig().getFlushRaftLogThreshold() == 0) {
          currentLogDataOutputStream.getChannel().force(true);
          currentLogIndexOutputStream.getChannel().force(true);
        }
      } catch (IOException e) {
        logger.error("Error in logs serialization: ", e);
      }
    } finally {
      try {
        logDataBuffer.switchFlushingBufferToIdling();
        logIndexBuffer.switchFlushingBufferToIdling();
        logger.debug("End flushing log buffer.");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("flush log buffer interrupted", e);
      }
      fileLock.unlock();
    }
  }

  private void forceFlushLogBufferWithoutCloseFile() {
    if (isClosed) {
      return;
    }

    flushLogBuffer();
    serializeMeta(meta);
    fileLock.lock();
    try {
      if (currentLogDataOutputStream != null) {
        currentLogDataOutputStream.getChannel().force(true);
      }
      if (currentLogIndexOutputStream != null) {
        currentLogIndexOutputStream.getChannel().force(true);
      }
    } catch (ClosedByInterruptException e) {
      // ignore
    } catch (IOException e) {
      logger.error("Error when force flushing logs serialization: ", e);
    } finally {
      fileLock.unlock();
    }
  }

  /**
   * flush the log buffer and check if the file needs to be closed
   */
  @Override
  public void forceFlushLogBuffer() {
    bufferLock.lock();
    try {
      logDataBuffer.switchWorkingBufferToFlushing();
      logIndexBuffer.switchWorkingBufferToFlushing();

      logDataBuffer.switchIdlingBufferToWorking();
      logIndexBuffer.switchIdlingBufferToWorking();

      if (offsetOfTheCurrentLogDataOutputStream > maxRaftLogPersistDataSizePerFile) {
        forceFlushLogBufferWithoutCloseFile();
        closeCurrentFile(meta.getCommitLogIndex());
        offsetOfTheCurrentLogDataOutputStream = 0;
      } else {
        forceFlushLogBufferWithoutCloseFile();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("force flush log buffer thread is interrupted", e);
    } catch (IOException e) {
      logger.error("force flush log buffer error", e);
    } finally {
      bufferLock.unlock();
    }
  }

  @Override
  public void setHardStateAndFlush(HardState state) {
    this.state = state;
    serializeMeta(meta);
  }

  @Override
  public HardState getHardState() {
    return state;
  }

  @Override
  public void removeCompactedEntries(long index) {
    // do nothing
  }

  private void initMetaAndLogFiles() {
    recoverMetaFile();
    recoverMeta();
    this.firstLogIndex = meta.getCommitLogIndex() + 1;
    try {
      recoverLogFiles();

      logDataFileList.sort(
          this::comparePersistLogFileName);

      logIndexFileList.sort(
          this::comparePersistLogFileName);

      // add init log file
      if (logDataFileList.isEmpty()) {
        createNewLogFile(metaFile.getParentFile().getPath(), meta.getCommitLogIndex() + 1);
      }

    } catch (IOException e) {
      logger.error("Error in init log file: ", e);
    }
  }

  /**
   * The file name rules are as follows: ${startLogIndex}-${endLogIndex}-${version}.data
   */
  private void recoverLogFiles() {
    // 1. first we should recover the log index file
    recoverLogFiles(LOG_INDEX_FILE_SUFFIX);

    // 2. recover the log data file
    recoverLogFiles(LOG_DATA_FILE_SUFFIX);

    // 3. recover the last log file in case of abnormal exit
    recoverTheLastLogFile();
  }

  private void recoverLogFiles(String logFileType) {
    FileFilter logFilter = pathname -> {
      String s = pathname.getName();
      return s.endsWith(logFileType);
    };

    List<File> logFiles = Arrays.asList(metaFile.getParentFile().listFiles(logFilter));
    logger.info("Find log type ={} log files {}", logFileType, logFiles);

    for (File file : logFiles) {
      if (checkLogFile(file, logFileType)) {
        switch (logFileType) {
          case LOG_DATA_FILE_SUFFIX:
            logDataFileList.add(file);
            break;
          case LOG_INDEX_FILE_SUFFIX:
            logIndexFileList.add(file);
            break;
          default:
            logger.error("unknown file type={}", logFileType);
        }
      }
    }
  }

  /**
   * Check that the file is legal or not
   *
   * @param file     file needs to be check
   * @param fileType {@link SyncLogDequeSerializer#LOG_DATA_FILE_SUFFIX} or  {@link
   *                 SyncLogDequeSerializer#LOG_INDEX_FILE_SUFFIX}
   * @return true if the file legal otherwise false
   */
  private boolean checkLogFile(File file, String fileType) {
    if (file.length() == 0 || !file.getName().endsWith(fileType)) {
      try {
        if (file.exists() && !file.isDirectory() && file.length() == 0) {
          Files.delete(file.toPath());
        }
      } catch (IOException e) {
        logger.warn("Cannot delete empty log file {}", file, e);
      }
      return false;
    }

    long fileVersion = getFileVersion(file);
    // this means system down between save meta and data
    if (fileVersion <= minAvailableVersion || fileVersion >= maxAvailableVersion) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete outdated log file {}", file);
      }
      return false;
    }

    String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
    // start index should be smaller than end index
    if (Long.parseLong(splits[0]) > Long.parseLong(splits[1])) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete incorrect log file {}", file);
      }
      return false;
    }
    return true;
  }

  private void recoverTheLastLogFile() {
    if (logIndexFileList.isEmpty()) {
      logger.info("no log index file to recover");
      return;
    }

    File lastIndexFile = logIndexFileList.get(logIndexFileList.size() - 1);
    long endIndex = Long.parseLong(lastIndexFile.getName().split(FILE_NAME_SEPARATOR)[1]);
    boolean success = true;
    if (endIndex != Long.MAX_VALUE) {
      logger.info("last log index file={} no need to recover", lastIndexFile.getAbsoluteFile());
    } else {
      success = recoverTheLastLogIndexFile(lastIndexFile);
    }

    if (!success) {
      logger.error("recover log index file failed, clear all logs in disk, {}",
          lastIndexFile.getAbsoluteFile());
      while (!logDataFileList.isEmpty()) {
        success = deleteTheFirstLogDataAndIndexFile();
        if (!success) {
          forceDeleteAllLogFiles();
        }
      }
      clearFirstLogIndex();
      return;
    }

    File lastDataFile = logDataFileList.get(logDataFileList.size() - 1);
    endIndex = Long.parseLong(lastDataFile.getName().split(FILE_NAME_SEPARATOR)[1]);
    if (endIndex != Long.MAX_VALUE) {
      logger.info("last log data file={} no need to recover", lastDataFile.getAbsoluteFile());
      return;
    }

    success = recoverTheLastLogDataFile(logDataFileList.get(logDataFileList.size() - 1));
    if (!success) {
      logger.error("recover log data file failed, clear all logs in disk,{}",
          lastDataFile.getAbsoluteFile());
      while (!logDataFileList.isEmpty()) {
        success = deleteTheFirstLogDataAndIndexFile();
        if (!success) {
          forceDeleteAllLogFiles();
        }
      }
      clearFirstLogIndex();
    }
  }

  private boolean recoverTheLastLogDataFile(File file) {
    String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
    long startIndex = Long.parseLong(splits[0]);
    Pair<File, Pair<Long, Long>> fileStartAndEndIndex = getLogIndexFile(startIndex);
    if (fileStartAndEndIndex.right.left == startIndex) {
      long endIndex = fileStartAndEndIndex.right.right;
      String newDataFileName = file.getName()
          .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(endIndex));
      File newLogDataFile = SystemFileFactory.INSTANCE
          .getFile(file.getParent() + File.separator + newDataFileName);
      if (!file.renameTo(newLogDataFile)) {
        logger.error("rename log data file={} failed when recover", file.getAbsoluteFile());
      }
      logDataFileList.remove(logDataFileList.size() - 1);
      logDataFileList.add(newLogDataFile);
      return true;
    }
    return false;
  }

  private boolean recoverTheLastLogIndexFile(File file) {
    logger.debug("start to recover the last log index file={}", file.getAbsoluteFile());
    String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
    long startIndex = Long.parseLong(splits[0]);
    int longLength = 8;
    byte[] bytes = new byte[longLength];

    int totalCount = 0;
    long offset = 0;
    try (FileInputStream fileInputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      firstLogIndex = startIndex;
      while (bufferedInputStream.read(bytes) != -1) {
        offset = BytesUtils.bytesToLong(bytes);
        logIndexOffsetList.add(offset);
        totalCount++;
      }
    } catch (IOException e) {
      logger.error("recover log index file failed,", e);
    }
    long endIndex = startIndex + totalCount - 1;
    logger.debug("recover log index file={}, startIndex={}, endIndex={}", file.getAbsoluteFile(),
        startIndex, endIndex);

    if (endIndex < meta.getCommitLogIndex()) {
      logger.error(
          "due to the last abnormal exit, part of the raft logs are lost. "
              + "The commit index saved by the meta shall prevail, and all logs will be deleted"
              + "meta commitLogIndex={}, endIndex={}", meta.getCommitLogIndex(), endIndex);
      return false;
    }
    if (endIndex >= startIndex) {
      String newIndexFileName = file.getName()
          .replaceAll(String.valueOf(Long.MAX_VALUE), String.valueOf(endIndex));
      File newLogIndexFile = SystemFileFactory.INSTANCE
          .getFile(file.getParent() + File.separator + newIndexFileName);
      if (!file.renameTo(newLogIndexFile)) {
        logger.error("rename log index file={} failed when recover", file.getAbsoluteFile());
      }
      logIndexFileList.set(logIndexFileList.size() - 1, newLogIndexFile);
    } else {
      logger.error("recover log index file failed,{}", file.getAbsoluteFile());
      return false;
    }
    return true;
  }

  private void clearFirstLogIndex() {
    firstLogIndex = meta.getCommitLogIndex() + 1;
    logIndexOffsetList.clear();
  }

  private void recoverMetaFile() {
    metaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta");

    // build dir
    if (!metaFile.getParentFile().exists()) {
      metaFile.getParentFile().mkdirs();
    }

    File tempMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta.tmp");
    // if we have temp file
    if (tempMetaFile.exists()) {
      recoverMetaFileFromTemp(tempMetaFile);
    } else if (!metaFile.exists()) {
      createNewMetaFile();
    }
  }

  private void recoverMetaFileFromTemp(File tempMetaFile) {
    // if temp file is empty, just return
    if (tempMetaFile.length() == 0) {
      try {
        Files.delete(tempMetaFile.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete file {}", tempMetaFile);
      }
    }
    // else use temp file rather than meta file
    else {
      try {
        Files.deleteIfExists(metaFile.toPath());
      } catch (IOException e) {
        logger.warn("Cannot delete file {}", metaFile);
      }
      if (!tempMetaFile.renameTo(metaFile)) {
        logger.warn("Failed to rename log meta file");
      }
    }
  }

  private void createNewMetaFile() {
    try {
      if (!metaFile.createNewFile()) {
        logger.warn("Cannot create log meta file");
      }
    } catch (IOException e) {
      logger.error("Cannot create new log meta file ", e);
    }
  }

  private void checkStream() throws FileNotFoundException {
    if (currentLogDataOutputStream == null) {
      currentLogDataOutputStream = new FileOutputStream(getCurrentLogDataFile(), true);
      logger.info("{}: Opened a new log data file: {}", this, getCurrentLogDataFile());
    }

    if (currentLogIndexOutputStream == null) {
      currentLogIndexOutputStream = new FileOutputStream(getCurrentLogIndexFile(), true);
      logger.info("{}: Opened a new index data file: {}", this, getCurrentLogIndexFile());
    }
  }

  /**
   * for unclosed file, the file name is ${startIndex}-${Long.MAX_VALUE}-{version}
   */
  private void createNewLogFile(String dirName, long startLogIndex) throws IOException {
    fileLock.lock();
    try {
      long nextVersion = versionController.nextVersion();
      long endLogIndex = Long.MAX_VALUE;

      String fileNamePrefix =
          dirName + File.separator + startLogIndex + FILE_NAME_SEPARATOR + endLogIndex
              + FILE_NAME_SEPARATOR + nextVersion + FILE_NAME_SEPARATOR;
      File logDataFile = SystemFileFactory.INSTANCE
          .getFile(fileNamePrefix + LOG_DATA_FILE_SUFFIX);
      File logIndexFile = SystemFileFactory.INSTANCE
          .getFile(fileNamePrefix + LOG_INDEX_FILE_SUFFIX);

      if (!logDataFile.createNewFile()) {
        logger.warn("Cannot create new log data file {}", logDataFile);
      }

      if (!logIndexFile.createNewFile()) {
        logger.warn("Cannot create new log index file {}", logDataFile);
      }
      logDataFileList.add(logDataFile);
      logIndexFileList.add(logIndexFile);
    } finally {
      fileLock.unlock();
    }
  }

  private File getCurrentLogDataFile() {
    return logDataFileList.get(logDataFileList.size() - 1);
  }

  private File getCurrentLogIndexFile() {
    return logIndexFileList.get(logIndexFileList.size() - 1);
  }

  private void recoverMeta() {
    if (meta != null) {
      return;
    }

    if (metaFile.exists() && metaFile.length() > 0) {
      if (logger.isInfoEnabled()) {
        SimpleDateFormat format = new SimpleDateFormat();
        logger.info("MetaFile {} exists, last modified: {}", metaFile.getPath(),
            format.format(new Date(metaFile.lastModified())));
      }
      try (FileInputStream fileInputStream = new FileInputStream(metaFile);
          BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
        minAvailableVersion = ReadWriteIOUtils.readLong(bufferedInputStream);
        maxAvailableVersion = ReadWriteIOUtils.readLong(bufferedInputStream);
        meta = LogManagerMeta.deserialize(
            ByteBuffer
                .wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(bufferedInputStream)));
        state = HardState.deserialize(
            ByteBuffer
                .wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(bufferedInputStream)));
      } catch (IOException e) {
        logger.error("Cannot recover log meta: ", e);
        meta = new LogManagerMeta();
        state = new HardState();
      }
    } else {
      meta = new LogManagerMeta();
      state = new HardState();
    }
    logger
        .info("Recovered log meta: {}, availableVersion: [{},{}], state: {}",
            meta, minAvailableVersion, maxAvailableVersion, state);
  }

  private void serializeMeta(LogManagerMeta meta) {
    File tempMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta.tmp");
    tempMetaFile.getParentFile().mkdirs();
    logger.debug("Serializing log meta into {}", tempMetaFile.getPath());
    try (FileOutputStream tempMetaFileOutputStream = new FileOutputStream(tempMetaFile)) {
      ReadWriteIOUtils.write(minAvailableVersion, tempMetaFileOutputStream);
      ReadWriteIOUtils.write(maxAvailableVersion, tempMetaFileOutputStream);
      ReadWriteIOUtils.write(meta.serialize(), tempMetaFileOutputStream);
      ReadWriteIOUtils.write(state.serialize(), tempMetaFileOutputStream);

    } catch (IOException e) {
      logger.error("Error in serializing log meta: ", e);
    }
    // rename
    try {
      Files.deleteIfExists(metaFile.toPath());
    } catch (IOException e) {
      logger.warn("Cannot delete old log meta file {}", metaFile, e);
    }
    if (!tempMetaFile.renameTo(metaFile)) {
      logger.warn("Cannot rename new log meta file {}", tempMetaFile);
    }

    // rebuild meta stream
    this.meta = meta;
    logger.debug("Serialized log meta into {}", tempMetaFile.getPath());
  }

  @Override
  public void close() {
    if (isClosed) {
      return;
    }
    logger.info("{} is closing", this);
    forceFlushLogBuffer();
    fileLock.lock();
    try {
      closeCurrentFile(meta.getCommitLogIndex());
      if (persistLogDeleteExecutorService != null) {
        persistLogDeleteExecutorService.shutdownNow();
        persistLogDeleteLogFuture.cancel(true);
        try {
          persistLogDeleteExecutorService.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.warn("Close persist log delete thread interrupted");
        }
        persistLogDeleteExecutorService = null;
      }
    } catch (IOException e) {
      logger.error("Error in log serialization: ", e);
    } finally {
      fileLock.unlock();
      logger.info("{} is closed", this);
      isClosed = true;
    }
  }

  @Override
  public void clearAllLogs(long commitIndex) {
    fileLock.lock();
    try {
      // 1. delete
      forceFlushLogBuffer();
      closeCurrentFile(meta.getCommitLogIndex());
      while (!logDataFileList.isEmpty()) {
        boolean success = deleteTheFirstLogDataAndIndexFile();
        if (!success) {
          forceDeleteAllLogFiles();
        }
      }
      deleteMetaFile();

      logDataFileList.clear();
      logIndexFileList.clear();

      // 2. init
      if (!logIndexOffsetList.isEmpty()) {
        this.firstLogIndex = Math
            .max(commitIndex + 1, firstLogIndex + logIndexOffsetList.size());
      } else {
        this.firstLogIndex = commitIndex + 1;
      }
      this.logIndexOffsetList.clear();
      recoverMetaFile();
      meta = new LogManagerMeta();
      createNewLogFile(logDir, firstLogIndex);
      logger.info("{}, clean all logs success, the new firstLogIndex={}", this, firstLogIndex);
    } catch (IOException e) {
      logger.error("clear all logs failed,", e);
    } finally {
      fileLock.unlock();
    }
  }

  private void deleteMetaFile() {
    fileLock.lock();
    try {
      File tmpMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta.tmp");
      Files.deleteIfExists(tmpMetaFile.toPath());
      File localMetaFile = SystemFileFactory.INSTANCE.getFile(logDir + "logMeta");
      Files.deleteIfExists(localMetaFile.toPath());
    } catch (IOException e) {
      logger.error("{}: delete meta log files failed", this, e);
    } finally {
      fileLock.unlock();
    }
  }

  /**
   * get file version from file The file name structure is as follows：
   * {startLogIndex}-{endLogIndex}-{version}-data)
   *
   * @param file file
   * @return version from file
   */
  private long getFileVersion(File file) {
    return Long.parseLong(file.getName().split(FILE_NAME_SEPARATOR)[2]);
  }

  public void checkDeletePersistRaftLog() {
    // 1. check the log index offset list size
    bufferLock.lock();
    try {
      if (logIndexOffsetList.size() > maxRaftLogIndexSizeInMemory) {
        int compactIndex = logIndexOffsetList.size() - maxRaftLogIndexSizeInMemory;
        logIndexOffsetList.subList(0, compactIndex).clear();
        firstLogIndex += compactIndex;
      }
    } finally {
      bufferLock.unlock();
    }

    // 2. check the persist log file number
    fileLock.lock();
    try {
      while (logDataFileList.size() > maxNumberOfPersistRaftLogFiles) {
        deleteTheFirstLogDataAndIndexFile();
      }
    } finally {
      fileLock.unlock();
    }

    // 3. check the persist log index number
    fileLock.lock();
    try {
      while (logDataFileList.size() > 1) {
        File firstFile = logDataFileList.get(0);
        String[] splits = firstFile.getName().split(FILE_NAME_SEPARATOR);
        if (meta.getCommitLogIndex() - Long.parseLong(splits[1]) > maxPersistRaftLogNumberOnDisk) {
          deleteTheFirstLogDataAndIndexFile();
        } else {
          return;
        }
      }
    } finally {
      fileLock.unlock();
    }
  }

  private void forceDeleteAllLogDataFiles() {
    FileFilter logFilter = pathname -> {
      String s = pathname.getName();
      return s.endsWith(LOG_DATA_FILE_SUFFIX);
    };
    List<File> logFiles = Arrays.asList(metaFile.getParentFile().listFiles(logFilter));
    logger.info("get log data files {} when forcing delete all logs", logFiles);
    for (File logFile : logFiles) {
      try {
        FileUtils.forceDelete(logFile);
      } catch (IOException e) {
        logger.error("forcing delete log data file={} failed", logFile.getAbsoluteFile(), e);
      }
    }
    logDataFileList.clear();
  }

  private void forceDeleteAllLogIndexFiles() {
    FileFilter logIndexFilter = pathname -> {
      String s = pathname.getName();
      return s.endsWith(LOG_INDEX_FILE_SUFFIX);
    };

    List<File> logIndexFiles = Arrays.asList(metaFile.getParentFile().listFiles(logIndexFilter));
    logger.info("get log index files {} when forcing delete all logs", logIndexFiles);
    for (File logFile : logIndexFiles) {
      try {
        FileUtils.forceDelete(logFile);
      } catch (IOException e) {
        logger.error("forcing delete log index file={} failed", logFile.getAbsoluteFile(), e);
      }
    }
    logIndexFileList.clear();
  }

  private void forceDeleteAllLogFiles() {
    forceDeleteAllLogDataFiles();
    forceDeleteAllLogIndexFiles();
  }

  @SuppressWarnings("ConstantConditions")
  private boolean deleteTheFirstLogDataAndIndexFile() {
    if (logDataFileList.isEmpty()) {
      return true;
    }

    File logDataFile = null;
    File logIndexFile = null;

    fileLock.lock();
    try {
      logDataFile = logDataFileList.get(0);
      logIndexFile = logIndexFileList.get(0);
      if (logDataFile == null || logIndexFile == null) {
        logger.error("the log data or index file is null, some error occurred");
        return false;
      }
      Files.delete(logDataFile.toPath());
      Files.delete(logIndexFile.toPath());
      logDataFileList.remove(0);
      logIndexFileList.remove(0);
      logger.debug("delete date file={}, index file={}", logDataFile.getAbsoluteFile(),
          logIndexFile.getAbsoluteFile());
    } catch (IOException e) {
      logger.error("delete file failed, data file={}, index file={}",
          logDataFile.getAbsoluteFile(),
          logIndexFile.getAbsoluteFile());
      return false;
    } finally {
      fileLock.unlock();
    }
    return true;
  }

  /**
   * The file name structure is as follows： {startLogIndex}-{endLogIndex}-{version}-data)
   *
   * @param file1 File to compare
   * @param file2 File to compare
   */
  private int comparePersistLogFileName(File file1, File file2) {
    String[] items1 = file1.getName().split(FILE_NAME_SEPARATOR);
    String[] items2 = file2.getName().split(FILE_NAME_SEPARATOR);
    if (items1.length != FILE_NAME_PART_LENGTH || items2.length != FILE_NAME_PART_LENGTH) {
      logger.error(
          "file1={}, file2={} name should be in the following format: startLogIndex-endLogIndex-version-data",
          file1.getAbsoluteFile(), file2.getAbsoluteFile());
    }
    long startLogIndex1 = Long.parseLong(items1[0]);
    long startLogIndex2 = Long.parseLong(items2[0]);
    int res = Long.compare(startLogIndex1, startLogIndex2);
    if (res == 0) {
      return Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
    }
    return res;
  }

  /**
   * @param startIndex the log start index
   * @param endIndex   the log end index
   * @return the raft log which index between [startIndex, endIndex] or empty if not found
   */
  @Override
  public List<Log> getLogs(long startIndex, long endIndex) {
    if (startIndex > endIndex) {
      logger
          .error("startIndex={} should be less than or equal to endIndex={}", startIndex,
              endIndex);
      return Collections.emptyList();
    }
    if (startIndex < 0 || endIndex < 0) {
      logger
          .error("startIndex={} and endIndex={} should be larger than zero", startIndex,
              endIndex);
      return Collections.emptyList();
    }

    long newEndIndex = endIndex;
    if (endIndex - startIndex > maxNumberOfLogsPerFetchOnDisk) {
      newEndIndex = startIndex + maxNumberOfLogsPerFetchOnDisk;
    }
    logger
        .debug("intend to get logs between[{}, {}], actually get logs between[{},{}]", startIndex,
            endIndex, startIndex, newEndIndex);

    // maybe the logs will be deleted during checkDeletePersistRaftLog or clearAllLogs,
    // use lock for two reasons:
    // 1.if the log file to read is the last log file, we need to get write lock to flush logBuffer,
    // 2.prevent these log files from being deleted
    fileLock.lock();
    try {
      List<Pair<File, Pair<Long, Long>>> logDataFileAndOffsetList = getLogDataFileAndOffset(
          startIndex, newEndIndex);
      if (logDataFileAndOffsetList.isEmpty()) {
        return Collections.emptyList();
      }

      List<Log> result = new ArrayList<>();
      for (Pair<File, Pair<Long, Long>> pair : logDataFileAndOffsetList) {
        result.addAll(getLogsFromOneLogDataFile(pair.left, pair.right));
      }

      return result;
    } finally {
      fileLock.unlock();
    }
  }


  /**
   * @param logIndex the log's index
   * @return The offset of the data file corresponding to the log index, -1 if not found
   */
  public long getOffsetAccordingToLogIndex(long logIndex) {
    long offset = -1;

    long maxLogIndex = firstLogIndex + logIndexOffsetList.size();
    if (logIndex >= maxLogIndex) {
      logger.error("given log index={} exceed the max log index={}, firstLogIndex={}", logIndex,
          maxLogIndex, firstLogIndex);
      return -1;
    }
    // 1. first find in memory
    if (logIndex >= firstLogIndex) {
      int arrayIndex = (int) (logIndex - firstLogIndex);
      if (arrayIndex < logIndexOffsetList.size()) {
        offset = logIndexOffsetList.get(arrayIndex);
        logger.debug(
            "found the offset in memory, logIndex={}, firstLogIndex={}, logIndexOffsetList size={}, offset={}",
            logIndex, firstLogIndex, logIndexOffsetList.size(), offset);
        return offset;
      }
    }

    logger.debug(
        "can not found the offset in memory, logIndex={}, firstLogIndex={}, logIndexOffsetList size={}",
        logIndex, firstLogIndex, logIndexOffsetList.size());

    // 2. second read the log index file
    Pair<File, Pair<Long, Long>> fileWithStartAndEndIndex = getLogIndexFile(logIndex);
    if (fileWithStartAndEndIndex == null) {
      return -1;
    }
    File file = fileWithStartAndEndIndex.left;
    Pair<Long, Long> startAndEndIndex = fileWithStartAndEndIndex.right;
    logger.debug(
        "start to read the log index file={} for log index={}, file size={}",
        file.getAbsoluteFile(), logIndex, file.length());
    try (FileInputStream fileInputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      long bytesNeedToSkip = (logIndex - startAndEndIndex.left) * (Long.BYTES);
      long bytesActuallySkip = bufferedInputStream.skip(bytesNeedToSkip);
      logger.debug("skip {} bytes when read file={}", bytesActuallySkip,
          file.getAbsoluteFile());
      if (bytesNeedToSkip != bytesActuallySkip) {
        logger.error("read file={} failed, should skip={}, actually skip={}",
            file.getAbsoluteFile(), bytesNeedToSkip, bytesActuallySkip);
        return -1;
      }
      offset = ReadWriteIOUtils.readLong(bufferedInputStream);
      return offset;
    } catch (IOException e) {
      logger.error("can not read the log index file={}", file.getAbsoluteFile(), e);
      return -1;
    }
  }

  /**
   * @param startIndex the log start index
   * @param endIndex   the log end index
   * @return first value-> the log data file, second value-> the left value is the start offset of
   * the file, the right is the end offset of the file
   */
  private List<Pair<File, Pair<Long, Long>>> getLogDataFileAndOffset(long startIndex,
      long endIndex) {
    long startIndexInOneFile = startIndex;
    long endIndexInOneFile = 0;
    List<Pair<File, Pair<Long, Long>>> fileNameWithStartAndEndOffset = new ArrayList();
    // 1. get the start offset with the startIndex
    long startOffset = getOffsetAccordingToLogIndex(startIndexInOneFile);
    if (startOffset == -1) {
      return Collections.emptyList();
    }
    Pair<File, Pair<Long, Long>> logDataFileWithStartAndEndLogIndex = getLogDataFile(
        startIndexInOneFile);
    if (logDataFileWithStartAndEndLogIndex == null) {
      return Collections.emptyList();
    }
    endIndexInOneFile = logDataFileWithStartAndEndLogIndex.right.right;
    // 2. judge whether the fileEndLogIndex>=endIndex
    while (endIndex > endIndexInOneFile) {
      //  this means the endIndex's offset can not be found in the file
      //  logDataFileWithStartAndEndLogIndex.left; and should be find in the next log data file.
      //3. get the file's end offset
      long endOffset = getOffsetAccordingToLogIndex(endIndexInOneFile);
      fileNameWithStartAndEndOffset.add(
          new Pair<>(logDataFileWithStartAndEndLogIndex.left,
              new Pair<>(startOffset, endOffset)));

      logger
          .debug("get log data offset=[{},{}] according to log index=[{},{}], file={}",
              startOffset,
              endOffset, startIndexInOneFile, endIndexInOneFile,
              logDataFileWithStartAndEndLogIndex.left);
      //4. search the next file to get the log index of fileEndLogIndex + 1
      startIndexInOneFile = endIndexInOneFile + 1;
      startOffset = getOffsetAccordingToLogIndex(startIndexInOneFile);
      if (startOffset == -1) {
        return Collections.emptyList();
      }
      logDataFileWithStartAndEndLogIndex = getLogDataFile(startIndexInOneFile);
      if (logDataFileWithStartAndEndLogIndex == null) {
        return Collections.emptyList();
      }
      endIndexInOneFile = logDataFileWithStartAndEndLogIndex.right.right;
    }
    // this means the endIndex's offset can not be found in the file logDataFileWithStartAndEndLogIndex.left
    long endOffset = getOffsetAccordingToLogIndex(endIndex);
    fileNameWithStartAndEndOffset.add(
        new Pair<>(logDataFileWithStartAndEndLogIndex.left, new Pair<>(startOffset, endOffset)));
    logger
        .debug("get log data offset=[{},{}] according to log index=[{},{}], file={}", startOffset,
            endOffset, startIndexInOneFile, endIndex, logDataFileWithStartAndEndLogIndex.left);
    return fileNameWithStartAndEndOffset;
  }

  /**
   * @param startIndex the start log index
   * @return the first value of the pair is the log index file which contains the start index; the
   * second pair's first value is the file's start log index. the second pair's second value is the
   * file's end log index. null if not found
   */
  public Pair<File, Pair<Long, Long>> getLogIndexFile(long startIndex) {
    for (File file : logIndexFileList) {
      String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
      if (splits.length != FILE_NAME_PART_LENGTH) {
        logger.error(
            "file={} name should be in the following format: startLogIndex-endLogIndex-version-idx",
            file.getAbsoluteFile());
      }
      if (Long.parseLong(splits[0]) <= startIndex && startIndex <= Long.parseLong(splits[1])) {
        return new Pair<>(file,
            new Pair<>(Long.parseLong(splits[0]), Long.parseLong(splits[1])));
      }
    }
    logger.debug("can not found the log index file for startIndex={}", startIndex);
    return null;
  }

  /**
   * @param startIndex the start log index
   * @return the first value of the pair is the log data file which contains the start index; the
   * second pair's first value is the file's start log index. the second pair's second value is the
   * file's end log index. null if not found
   */
  public Pair<File, Pair<Long, Long>> getLogDataFile(long startIndex) {
    for (File file : logDataFileList) {
      String[] splits = file.getName().split(FILE_NAME_SEPARATOR);
      if (splits.length != FILE_NAME_PART_LENGTH) {
        logger.error(
            "file={} name should be in the following format: startLogIndex-endLogIndex-version-data",
            file.getAbsoluteFile());
      }
      if (Long.parseLong(splits[0]) <= startIndex && startIndex <= Long.parseLong(splits[1])) {
        return new Pair<>(file,
            new Pair<>(Long.parseLong(splits[0]), Long.parseLong(splits[1])));
      }
    }
    logger.debug("can not found the log data file for startIndex={}", startIndex);
    return null;
  }

  /**
   * @param file              the log data file
   * @param startAndEndOffset the left value is the start offset of the file,  the right is the end
   *                          offset of the file
   * @return the logs between start offset and end offset
   */

  private List<Log> getLogsFromOneLogDataFile(File file, Pair<Long, Long> startAndEndOffset) {
    List<Log> result = new ArrayList<>();
    if (file.getName().equals(getCurrentLogDataFile().getName())) {
      try {
        logDataBuffer.switchWorkingBufferToFlushing();
        logIndexBuffer.switchWorkingBufferToFlushing();

        logDataBuffer.switchIdlingBufferToWorking();
        logIndexBuffer.switchIdlingBufferToWorking();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("append logs from one log data file was interrupted, file={}",
            file.getAbsoluteFile(), e);
      }
      forceFlushLogBufferWithoutCloseFile();
    }
    try (FileInputStream fileInputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream)) {
      long bytesSkip = bufferedInputStream.skip(startAndEndOffset.left);
      if (bytesSkip != startAndEndOffset.left) {
        logger.error("read file={} failed when skip {} bytes, actual skip bytes={}",
            file.getAbsoluteFile(), startAndEndOffset.left, bytesSkip);
        return result;
      }

      logger.debug(
          "start to read file={} and skip {} bytes, startOffset={}, endOffset={}, fileLength={}",
          file.getAbsoluteFile(), bytesSkip, startAndEndOffset.left, startAndEndOffset.right,
          file.length());

      long currentReadOffset = bytesSkip;
      // because we want to get all the logs whose offset between [startAndEndOffset.left, startAndEndOffset.right]
      // which means, the last offset's value should be still read, in other words,
      // the first log index of the offset starting with startAndEndOffset.right also needs to be read.
      while (currentReadOffset <= startAndEndOffset.right) {
        logger.debug("read file={}, currentReadOffset={}, end offset={}, file length={}",
            file.getAbsoluteFile(), currentReadOffset, startAndEndOffset.right, file.length());
        int logSize = ReadWriteIOUtils.readInt(bufferedInputStream);
        Log log = null;
        try {
          log = parser
              .parse(ByteBuffer.wrap(ReadWriteIOUtils.readBytes(bufferedInputStream, logSize)));
          result.add(log);
        } catch (UnknownLogTypeException e) {
          logger.error("Unknown log detected ", e);
        }
        currentReadOffset = currentReadOffset + Integer.BYTES + logSize;
      }
    } catch (IOException e) {
      logger.error("Cannot read log from file={} ", file.getAbsoluteFile(), e);
    }
    return result;
  }

  @TestOnly
  public void setLogDataBuffer(DoublyBuffer logDataBuffer) {
    this.logDataBuffer = logDataBuffer;
  }

  @TestOnly
  public void setMaxRaftLogIndexSizeInMemory(int maxRaftLogIndexSizeInMemory) {
    this.maxRaftLogIndexSizeInMemory = maxRaftLogIndexSizeInMemory;
  }

  @TestOnly
  public void setMaxRaftLogPersistDataSizePerFile(int maxRaftLogPersistDataSizePerFile) {
    this.maxRaftLogPersistDataSizePerFile = maxRaftLogPersistDataSizePerFile;
  }

  @TestOnly
  public void setMaxNumberOfPersistRaftLogFiles(int maxNumberOfPersistRaftLogFiles) {
    this.maxNumberOfPersistRaftLogFiles = maxNumberOfPersistRaftLogFiles;
  }

  @TestOnly
  public void setMaxPersistRaftLogNumberOnDisk(int maxPersistRaftLogNumberOnDisk) {
    this.maxPersistRaftLogNumberOnDisk = maxPersistRaftLogNumberOnDisk;
  }

  @TestOnly
  public List<File> getLogDataFileList() {
    return logDataFileList;
  }

  @TestOnly
  public List<File> getLogIndexFileList() {
    return logIndexFileList;
  }

  @TestOnly
  public long getFirstLogIndex() {
    return firstLogIndex;
  }

  @TestOnly
  public List<Long> getLogIndexOffsetList() {
    return logIndexOffsetList;
  }
}
