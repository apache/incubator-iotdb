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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
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

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private WalWriteProcessor flushingWalWriteProcessor;
  private WalWriteProcessor workingWalWriteProcessor = new WalWriteProcessor(false);

  private ReentrantLock lock = new ReentrantLock();
  private static final ExecutorService FLUSH_BUFFER_THREAD_POOL =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("Flush-WAL-Thread-%d").setDaemon(true).build());

  private long fileId = 0;
  private long lastFlushedId = 0;

  private boolean deleted;

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
  }

  @Override
  public void write(PhysicalPlan plan, boolean toPrevious) throws IOException {
    if (deleted) {
      throw new IOException("WAL node deleted");
    }
    lock.lock();
    try {
      putLog(plan, toPrevious);
      if (getWalWriteProcessor(toPrevious).getBufferedLogNum() >= config.getFlushWalThreshold()) {
        sync(toPrevious);
      }
    } catch (BufferOverflowException e) {
      throw new IOException(
          "Log cannot fit into the buffer, please increase wal_buffer_size", e);
    } finally {
      lock.unlock();
    }
  }

  private void putLog(PhysicalPlan plan, boolean toPrevious) {
    WalWriteProcessor walWriteProcessor = getWalWriteProcessor(toPrevious);
    walWriteProcessor.getLogBufferWorking().mark();
    try {
      plan.serialize(walWriteProcessor.getLogBufferWorking());
    } catch (BufferOverflowException e) {
      logger.info("WAL BufferOverflow !");
      walWriteProcessor.getLogBufferWorking().reset();
      sync(toPrevious);
      plan.serialize(walWriteProcessor.getLogBufferWorking());
    }
    getWalWriteProcessor(toPrevious).incrementLogCnt();
  }

  @Override
  public void close() {
    close(false);
  }

  public void close(boolean toPrevious){
    sync(toPrevious);
    forceWal(toPrevious);
    lock.lock();
    try {
      WalWriteProcessor walWriteProcessor = getWalWriteProcessor(toPrevious);
      synchronized (walWriteProcessor.getSwitchBufferCondition()) {
        while (walWriteProcessor.getLogBufferFlushing() != null && !deleted) {
          walWriteProcessor.getSwitchBufferCondition().wait();
        }
        walWriteProcessor.getSwitchBufferCondition().notifyAll();
      }

      if (walWriteProcessor.getFileWriter() != null) {
        walWriteProcessor.getFileWriter().close();
        logger.debug("WAL file {} is closed", walWriteProcessor.getFileWriter());
        walWriteProcessor.setFileWriter(null);
      }
      logger.debug("Log node {} closed successfully", identifier);
    } catch (IOException e) {
      logger.error("Cannot close log node {} because:", identifier, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Waiting for current buffer being flushed interrupted");
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void forceSync() {
    if (deleted) {
      return;
    }
    sync(false);
    forceWal(false);
  }

  @Override
  public void flushWindowChange() throws IOException {
    lock.lock();
    try {
      if (flushingWalWriteProcessor != null){
        close(true);
      }
      getFileWriter(false);
      flushingWalWriteProcessor = workingWalWriteProcessor;
      flushingWalWriteProcessor.setPrevious(true);
      workingWalWriteProcessor = new WalWriteProcessor(false);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void flushWindowEnd() {
    lock.lock();
    try {
      notifyEndFlush();
    } finally {
      lock.unlock();
    }
  }


  @Override
  public void notifyStartFlush() throws FileNotFoundException {
    lock.lock();
    try {
      close();
      nextFileWriter(false);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void notifyEndFlush() {
    lock.lock();
    try {
      File logFile = SystemFileFactory.INSTANCE
          .getFile(logDirectory, WAL_FILE_NAME + ++lastFlushedId);
      discard(logFile);
    } finally {
      lock.unlock();
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
    lock.lock();
    try {
      close();
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(logDirectory));
      deleted = true;
    } finally {
      lock.unlock();
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

  private void forceWal(boolean toPrevious) {
    lock.lock();
    try {
      try {
        if (getWalWriteProcessor(toPrevious).getFileWriter() != null) {
          getWalWriteProcessor(toPrevious).getFileWriter().force();
        }
      } catch (IOException e) {
        logger.error("Log node {} force failed.", identifier, e);
      }
    } finally {
      lock.unlock();
    }
  }

  private void sync(boolean toPrevious) {
    lock.lock();
    try {
      if (getWalWriteProcessor(toPrevious).getBufferedLogNum() == 0) {
        return;
      }
      switchBufferWorkingToFlushing(toPrevious);
      ILogWriter currWriter = getFileWriter(toPrevious);
      FLUSH_BUFFER_THREAD_POOL.submit(() -> flushBuffer(currWriter, toPrevious));
      switchBufferIdleToWorking(toPrevious);

      getWalWriteProcessor(toPrevious).setBufferedLogNum(0);
      logger.debug("Log node {} ends sync.", identifier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Waiting for available buffer interrupted");
    } catch (FileNotFoundException e) {
      logger.warn("can not found file {}", identifier, e);
    } finally {
      lock.unlock();
    }
  }

  private void flushBuffer(ILogWriter writer, boolean toPrevious) {
    try {
      writer.write(getWalWriteProcessor(toPrevious).getLogBufferFlushing());
    } catch (ClosedChannelException e) {
      // ignore
    } catch (IOException e) {
      logger.error("Log node {} sync failed, change system mode to read-only", identifier, e);
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      return;
    }
    getWalWriteProcessor(toPrevious).getLogBufferFlushing().clear();

    try {
      switchBufferFlushingToIdle(toPrevious);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void switchBufferWorkingToFlushing(boolean toPrevious) throws InterruptedException {
    WalWriteProcessor walWriteProcessor = getWalWriteProcessor(toPrevious);
    synchronized (walWriteProcessor.getSwitchBufferCondition()) {
      while (walWriteProcessor.getLogBufferFlushing() != null && !deleted) {
        walWriteProcessor.getSwitchBufferCondition().wait();
      }
      walWriteProcessor.setLogBufferFlushing(walWriteProcessor.getLogBufferWorking());
      walWriteProcessor.setLogBufferWorking(null);
      walWriteProcessor.getSwitchBufferCondition().notifyAll();
    }
  }

  private void switchBufferIdleToWorking(boolean toPrevious) throws InterruptedException {
    WalWriteProcessor walWriteProcessor = getWalWriteProcessor(toPrevious);
    synchronized (walWriteProcessor.getSwitchBufferCondition()) {
      while (walWriteProcessor.getLogBufferIdle() == null && !deleted) {
        walWriteProcessor.getSwitchBufferCondition().wait();
      }
      walWriteProcessor.setLogBufferWorking(walWriteProcessor.getLogBufferIdle());
      walWriteProcessor.setLogBufferIdle(null);
      walWriteProcessor.getSwitchBufferCondition().notifyAll();
    }
  }

  private void switchBufferFlushingToIdle(boolean toPrevious) throws InterruptedException {
    WalWriteProcessor walWriteProcessor = getWalWriteProcessor(toPrevious);
    synchronized (walWriteProcessor.getSwitchBufferCondition()) {
      while (walWriteProcessor.getLogBufferIdle() != null && !deleted) {
        walWriteProcessor.getSwitchBufferCondition().wait();
      }
      walWriteProcessor.setLogBufferIdle(walWriteProcessor.getLogBufferFlushing());
      walWriteProcessor.getLogBufferIdle().clear();
      walWriteProcessor.setLogBufferFlushing(null);
      walWriteProcessor.getSwitchBufferCondition().notifyAll();
    }
  }

  private ILogWriter getFileWriter(boolean toPrevious) throws FileNotFoundException {
    if (getWalWriteProcessor(toPrevious).getFileWriter() == null) {
      nextFileWriter(toPrevious);
    }
    return getWalWriteProcessor(toPrevious).getFileWriter();
  }

  private void nextFileWriter(boolean toPrevious) throws FileNotFoundException{
    fileId++;
    File newFile = SystemFileFactory.INSTANCE.getFile(logDirectory, WAL_FILE_NAME + fileId);
    if (newFile.getParentFile().mkdirs()) {
      logger.info("create WAL parent folder {}.", newFile.getParent());
    }
    logger.debug("WAL file {} is opened", newFile);
    getWalWriteProcessor(toPrevious).setFileWriter(new LogWriter(newFile, config.getForceWalPeriodInMs() == 0));
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

  private WalWriteProcessor getWalWriteProcessor(boolean isPrevious){
    return isPrevious? flushingWalWriteProcessor : workingWalWriteProcessor;
  }
}
