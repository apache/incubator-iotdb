/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.flush;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.flush.pool.FlushTaskPoolManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlushManager implements FlushManagerMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(FlushManager.class);
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ConcurrentLinkedDeque<TsFileProcessor> tsFileProcessorQueue = new ConcurrentLinkedDeque<>();

  private FlushTaskPoolManager flushPool = FlushTaskPoolManager.getInstance();

  @Override
  public void start() throws StartupException {
    FlushSubTaskPoolManager.getInstance().start();
    FlushTaskPoolManager.getInstance().start();
    try {
      JMXService.registerMBean(this, ServiceType.FLUSH_SERVICE.getJmxName());
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    FlushSubTaskPoolManager.getInstance().stop();
    FlushTaskPoolManager.getInstance().stop();
    JMXService.deregisterMBean(ServiceType.FLUSH_SERVICE.getJmxName());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FLUSH_SERVICE;
  }

  @Override
  public int getNumberOfWorkingTasks() {
    return flushPool.getWorkingTasksNumber();
  }

  @Override
  public int getNumberOfPendingTasks() {
    return flushPool.getWaitingTasksNumber();
  }

  @Override
  public int getNumberOfWorkingSubTasks() {
    return FlushSubTaskPoolManager.getInstance().getWorkingTasksNumber();
  }

  @Override
  public int getNumberOfPendingSubTasks() {
    return FlushSubTaskPoolManager.getInstance().getWaitingTasksNumber();
  }

  class FlushThread extends WrappedRunnable {

    @Override
    public void runMayThrow() {
      TsFileProcessor tsFileProcessor = tsFileProcessorQueue.poll();
      if (config.isEnableSlidingMemTable() && tsFileProcessor.isSequence()) {
        long startTime = System.currentTimeMillis();
        while (tsFileProcessor.getFlushingMemTableSize() == 1 && !tsFileProcessor.isShouldClose()
            && !SystemInfo.getInstance().shouldForceFlush()
            && System.currentTimeMillis() - startTime < tsFileProcessor.getFlushWaitTime()) {
          // wait
          try {
            TimeUnit.MILLISECONDS.sleep(config.getCheckPeriodWhenInsertBlocked());
          } catch (InterruptedException e) {
            logger.error("flush mem table wait error", e);
          }
        }
      }
      if (tsFileProcessor.isSequence() && config.isEnableSlidingMemTable() &&
          tsFileProcessor.isFlushMemTableAlive()) {
        tsFileProcessor.getUpdateLatestFlushTimeCallback().call(tsFileProcessor, true);
      }
      tsFileProcessor.flushOneMemTable();
      tsFileProcessor.setManagedByFlushManager(false);
      if (logger.isDebugEnabled()) {
        logger.debug("Flush Thread re-register TSProcessor {} to the queue.",
            tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());
      }
      registerTsFileProcessor(tsFileProcessor);
      // update stat monitor cache to system during each flush()
      if (config.isEnableStatMonitor() && config.isEnableMonitorSeriesWrite()) {
        try {
          StatMonitor.getInstance().saveStatValue(tsFileProcessor.getStorageGroupName());
        } catch (StorageEngineException | MetadataException e) {
          logger.error("Inserting monitor series data error.", e);
        }
      }
    }
  }

  /**
   * Add TsFileProcessor to asyncTryToFlush manager
   */
  @SuppressWarnings("squid:S2445")
  public void registerTsFileProcessor(TsFileProcessor tsFileProcessor) {
    synchronized (tsFileProcessor) {
      if (!tsFileProcessor.isManagedByFlushManager()
          && tsFileProcessor.getFlushingMemTableSize() > 0) {
        tsFileProcessorQueue.add(tsFileProcessor);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{} begin to submit a flush thread, flushing memtable size: {}, queue size: {}",
              tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath(),
              tsFileProcessor.getFlushingMemTableSize(), tsFileProcessorQueue.size());
        }
        tsFileProcessor.setManagedByFlushManager(true);
        flushPool.submit(new FlushThread());
      } else if (logger.isDebugEnabled()) {
        if (tsFileProcessor.isManagedByFlushManager()) {
          logger.debug(
              "{} is already in the flushPool, the first one: {}, the given processor flushMemtable number = {}",
              tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath(),
              tsFileProcessorQueue.isEmpty() ? "empty now"
                  : tsFileProcessorQueue.getFirst().getStorageGroupName(),
              tsFileProcessor.getFlushingMemTableSize());
        } else {
          logger.debug("No flushing memetable to do, register TsProcessor {} failed.",
              tsFileProcessor.getTsFileResource().getTsFile().getAbsolutePath());
        }
      }
    }
  }

  private FlushManager() {
  }

  public static FlushManager getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static FlushManager instance = new FlushManager();
  }

  public String toString() {
    return String.format("TSProcessors in the queue: %d, TaskPool size %d + %d,",
        tsFileProcessorQueue.size(), flushPool.getWorkingTasksNumber(),
        flushPool.getWaitingTasksNumber());
  }
}
