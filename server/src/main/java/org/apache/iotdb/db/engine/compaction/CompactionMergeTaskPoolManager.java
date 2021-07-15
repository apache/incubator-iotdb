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

package org.apache.iotdb.db.engine.compaction;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** CompactionMergeTaskPoolManager provides a ThreadPool to queue and run all compaction tasks. */
public class CompactionMergeTaskPoolManager implements IService {

  private static final Logger logger =
      LoggerFactory.getLogger(CompactionMergeTaskPoolManager.class);
  private static final CompactionMergeTaskPoolManager INSTANCE =
      new CompactionMergeTaskPoolManager();
  private ScheduledExecutorService scheduledPool;
  private ExecutorService pool;

  private static ConcurrentHashMap<String, Boolean> sgCompactionStatus = new ConcurrentHashMap<>();

  public static CompactionMergeTaskPoolManager getInstance() {
    return INSTANCE;
  }

  @Override
  public void start() {
    if (pool == null) {
      this.pool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              IoTDBDescriptor.getInstance().getConfig().getCompactionThreadNum(),
              ThreadName.COMPACTION_SERVICE.getName());
      this.scheduledPool =
          IoTDBThreadPoolFactory.newScheduledThreadPool(
              IoTDBDescriptor.getInstance().getConfig().getCompactionThreadNum(),
              ThreadName.COMPACTION_SERVICE.getName());
    }
    logger.info("Compaction task manager started.");
  }

  @Override
  public void stop() {
    if (pool != null) {
      scheduledPool.shutdownNow();
      pool.shutdownNow();
      logger.info("Waiting for task pool to shut down");
      waitTermination();
    }
  }

  @Override
  public void waitAndStop(long millseconds) {
    if (pool != null) {
      awaitTermination(scheduledPool, millseconds);
      awaitTermination(pool, millseconds);
      logger.info("Waiting for task pool to shut down");
      waitTermination();
    }
  }

  @TestOnly
  public void waitAllCompactionFinish() {
    if (pool != null) {
      File sgDir =
          FSFactoryProducer.getFSFactory()
              .getFile(
                  FilePathUtils.regularizePath(
                          IoTDBDescriptor.getInstance().getConfig().getSystemDir())
                      + "storage_groups");
      File[] subDirList = sgDir.listFiles();
      if (subDirList != null) {
        for (File subDir : subDirList) {
          while (FSFactoryProducer.getFSFactory()
              .getFile(
                  subDir.getAbsoluteFile()
                      + File.separator
                      + subDir.getName()
                      + COMPACTION_LOG_NAME)
              .exists()) {
            // wait
          }
        }
      }
      logger.info("All compaction task finish");
    }
  }

  private void waitTermination() {
    long startTime = System.currentTimeMillis();
    while (!pool.isTerminated()) {
      int timeMillis = 0;
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.error(
            "CompactionMergeTaskPoolManager {} shutdown",
            ThreadName.COMPACTION_SERVICE.getName(),
            e);
        Thread.currentThread().interrupt();
      }
      timeMillis += 200;
      long time = System.currentTimeMillis() - startTime;
      if (timeMillis % 60_000 == 0) {
        logger.warn("CompactionManager has wait for {} seconds to stop", time / 1000);
      }
    }
    scheduledPool = null;
    pool = null;
    logger.info("CompactionManager stopped");
  }

  private void awaitTermination(ExecutorService service, long millseconds) {
    try {
      service.shutdown();
      service.awaitTermination(millseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("CompactionThreadPool can not be closed in {} ms", millseconds);
      Thread.currentThread().interrupt();
    }
    service.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.COMPACTION_SERVICE;
  }

  public synchronized void clearCompactionStatus(String storageGroupName) {
    // for test
    if (sgCompactionStatus == null) {
      sgCompactionStatus = new ConcurrentHashMap<>();
    }
    sgCompactionStatus.put(storageGroupName, false);
  }

  public void init(Runnable function) {
    scheduledPool.scheduleWithFixedDelay(
        function, 1000, 30000, TimeUnit.MILLISECONDS);
  }

  public synchronized void submitTask(StorageGroupCompactionTask storageGroupCompactionTask)
      throws RejectedExecutionException {
    if (pool != null && !pool.isTerminated()) {
      String storageGroup = storageGroupCompactionTask.getStorageGroupName();
      boolean isCompacting = sgCompactionStatus.computeIfAbsent(storageGroup, k -> false);
      if (isCompacting) {
        return;
      }
      sgCompactionStatus.put(storageGroup, true);
      storageGroupCompactionTask.setSgCompactionStatus(sgCompactionStatus);
      pool.submit(storageGroupCompactionTask);
    }
  }

  public boolean isTerminated() {
    return pool == null || pool.isTerminated();
  }
}
