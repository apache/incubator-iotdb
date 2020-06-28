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
package org.apache.iotdb.db.writelog.node;

import static org.apache.iotdb.db.service.metrics.MicroMetricName.GROUP_TAG;
import static org.apache.iotdb.db.service.metrics.MicroMetricName.WAL_SYNC_COUNT;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This WriteLogNode is used to manage insert ahead logs of a TsFile.
 */
public class ExclusiveWriteLogNode implements WriteLogNode, Comparable<ExclusiveWriteLogNode> {

  public static final String WAL_FILE_NAME = "wal";
  private static final Logger logger = LoggerFactory.getLogger(ExclusiveWriteLogNode.class);

  private String identifier;

  private String logDirectory;

  private ILogWriter currentFileWriter;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ByteBuffer logBuffer = ByteBuffer
      .allocate(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize());

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  private long fileId = 0;
  private long lastFlushedId = 0;

  private int bufferedLogNum = 0;
  private Counter syncCounter = null;

  /**
   * constructor of ExclusiveWriteLogNode.
   *
   * @param identifier ExclusiveWriteLogNode identifier
   */
  public ExclusiveWriteLogNode(String identifier) {
    this.identifier = identifier;
    this.logDirectory =
        DirectoryManager.getInstance().getWALFolder() + File.separator + this.identifier;
    if (SystemFileFactory.INSTANCE.getFile(logDirectory).mkdirs()) {
      logger.info("create the WAL folder {}.", logDirectory);
    }
    if (StorageEngine.enableMetricService) {
      syncCounter = Metrics.counter(WAL_SYNC_COUNT, GROUP_TAG,
          identifier.contains("-") ? identifier.substring(0, identifier.indexOf("-")) : identifier);
    }
  }

  @Override
  public void write(PhysicalPlan plan) throws IOException {
    lock.writeLock().lock();
    try {
      putLog(plan);
      if (bufferedLogNum >= config.getFlushWalThreshold()) {
        sync();
      }
    } catch (BufferOverflowException e) {
      throw new IOException(
          "Log cannot fit into buffer, if you don't enable Dynamic Parameter Adapter, please increase wal_buffer_size;"
              + "otherwise, please increase the JVM memory", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void putLog(PhysicalPlan plan) {
    logBuffer.mark();
    try {
      plan.serialize(logBuffer);
    } catch (BufferOverflowException e) {
      logger.info("WAL BufferOverflow !");
      logBuffer.reset();
      sync();
      plan.serialize(logBuffer);
    }
    bufferedLogNum ++;
  }

  @Override
  public void close() {
    sync();
    forceWal();
    lock.writeLock().lock();
    try {
      if (this.currentFileWriter != null) {
        this.currentFileWriter.close();
        this.currentFileWriter = null;
      }
      logger.debug("Log node {} closed successfully", identifier);
    } catch (IOException e) {
      logger.error("Cannot close log node {} because:", identifier, e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void forceSync() {
    sync();
    forceWal();
  }


  @Override
  public void notifyStartFlush() {
    lock.writeLock().lock();
    try {
      close();
      nextFileWriter();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void notifyEndFlush() {
    lock.writeLock().lock();
    try {
      File logFile = SystemFileFactory.INSTANCE.getFile(logDirectory, WAL_FILE_NAME + ++lastFlushedId);
      discard(logFile);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public String getLogDirectory() {
    return logDirectory;
  }

  @Override
  public void delete() throws IOException {
    lock.writeLock().lock();
    try {
      logBuffer.clear();
      close();
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(logDirectory));
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ILogReader getLogReader() {
    File[] logFiles = SystemFileFactory.INSTANCE.getFile(logDirectory).listFiles();
    Arrays.sort(logFiles,
        Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace(WAL_FILE_NAME, ""))));
    return new MultiFileLogReader(logFiles);
  }

  private void discard(File logFile) {
    if (!logFile.exists()) {
      logger.info("Log file does not exist");
    } else {
      try {
        FileUtils.forceDelete(logFile);
        logger.info("Log node {} cleaned old file", identifier);
      } catch (IOException e) {
        logger.error("Old log file {} of {} cannot be deleted", logFile.getName(), identifier, e);
      }
    }
  }

  private void forceWal() {
    lock.writeLock().lock();
    try {
      try {
        if (currentFileWriter != null) {
          currentFileWriter.force();
        }
      } catch (IOException e) {
        logger.error("Log node {} force failed.", identifier, e);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void sync() {
    lock.writeLock().lock();
    try {
      if (bufferedLogNum == 0) {
        return;
      }
      if (StorageEngine.enableMetricService) {
        syncCounter.increment();
      }
      try {
        getCurrentFileWriter().write(logBuffer);
      } catch (IOException e) {
        logger.error("Log node {} sync failed, change system mode to read-only", identifier, e);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        return;
      }
      logBuffer.clear();
      bufferedLogNum = 0;
      logger.debug("Log node {} ends sync.", identifier);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private ILogWriter getCurrentFileWriter() {
    if (currentFileWriter == null) {
      nextFileWriter();
    }
    return currentFileWriter;
  }

  private void nextFileWriter() {
    fileId++;
    File newFile = SystemFileFactory.INSTANCE.getFile(logDirectory, WAL_FILE_NAME + fileId);
    if (newFile.getParentFile().mkdirs()) {
      logger.info("create WAL parent folder {}.", newFile.getParent());
    }
    currentFileWriter = new LogWriter(newFile);
  }

  @Override
  public int hashCode() {
    return identifier.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    return compareTo((ExclusiveWriteLogNode) obj) == 0;
  }

  @Override
  public String toString() {
    return "Log node " + identifier;
  }

  @Override
  public int compareTo(ExclusiveWriteLogNode o) {
    return this.identifier.compareTo(o.identifier);
  }
}
