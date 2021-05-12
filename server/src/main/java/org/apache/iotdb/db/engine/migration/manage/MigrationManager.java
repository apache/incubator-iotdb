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

package org.apache.iotdb.db.engine.migration.manage;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MigrationManager implements IService, MigrationManagerMBean {

  private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);
  private static final int WAIT_TIMEOUT = 2000;

  /** executes migration task */
  private ExecutorService pool;

  private MigrationManager() {}

  public void submitMigrationTask(Runnable migrationTask) {
    if (pool != null && !pool.isTerminated()) {
      pool.submit(migrationTask);
    }
  }

  @Override
  public void start() throws StartupException {
    if (pool == null) {
      int threadCnt = IoTDBDescriptor.getInstance().getConfig().getMigrationThreadNum();
      pool =
          IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, ThreadName.FLUSH_SERVICE.getName());
    }
    logger.info("MigrationManager started.");
  }

  @Override
  public void stop() {
    if (pool != null) {
      close();
    }
  }

  @Override
  public void waitAndStop(long milliseconds) {
    if (pool != null) {
      try {
        pool.shutdown();
        pool.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.warn("MigrationManager cannot be closed in {} ms", milliseconds);
        Thread.currentThread().interrupt();
      }
      close();
    }
  }

  private void close() {
    pool.shutdownNow();
    long totalWaitTime = WAIT_TIMEOUT;
    logger.info("Waiting for migration thread pool to shut down");
    while (!pool.isTerminated()) {
      try {
        if (!pool.awaitTermination(WAIT_TIMEOUT, TimeUnit.MILLISECONDS)) {
          logger.info("Migration thread pool doesn't exit after {} ms.", +totalWaitTime);
        }
        totalWaitTime += WAIT_TIMEOUT;
      } catch (InterruptedException e) {
        logger.error(
            "MigrationManager cannot be closed because it is interrupted while waiting migration thread pool to exit.",
            e);
        Thread.currentThread().interrupt();
      }
    }
    pool = null;
    logger.info("MigrationManager stopped");
  }

  @Override
  public int getNumberOfWorkingTasks() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

  @Override
  public int getNumberOfPendingTasks() {
    return ((ThreadPoolExecutor) pool).getQueue().size();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MIGRATION_SERVICE;
  }

  public static MigrationManager getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final MigrationManager instance = new MigrationManager();
  }
}
