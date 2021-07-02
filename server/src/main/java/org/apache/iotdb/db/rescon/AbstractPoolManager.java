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

package org.apache.iotdb.db.rescon;

import org.slf4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractPoolManager {

  private static final int WAIT_TIMEOUT = 2000;

  protected ExecutorService pool;

  /** Block new flush submits and exit when all RUNNING THREADS AND TASKS IN THE QUEUE end. */
  public void close() {
    Logger logger = getLogger();
    pool.shutdownNow();
    long totalWaitTime = WAIT_TIMEOUT;
    logger.info("Waiting for {} thread pool to shut down.", getName());
    while (!pool.isTerminated()) {
      try {
        if (!pool.awaitTermination(WAIT_TIMEOUT, TimeUnit.MILLISECONDS)) {
          logger.info("{} thread pool doesn't exit after {}ms.", getName(), +totalWaitTime);
        }
        totalWaitTime += WAIT_TIMEOUT;
      } catch (InterruptedException e) {
        logger.error("Interrupted while waiting {} thread pool to exit. ", getName(), e);
        Thread.currentThread().interrupt();
      }
    }
  }

  public synchronized Future<?> submit(Runnable task) {
    return pool.submit(task);
  }

  public synchronized <T> Future<T> submit(Callable<T> task) {
    return pool.submit(task);
  }

  public int getWorkingTasksNumber() {
    return ((ThreadPoolExecutor) pool).getActiveCount();
  }

  public int getWaitingTasksNumber() {
    return ((ThreadPoolExecutor) pool).getQueue().size();
  }

  public int getTotalTasks() {
    return getWorkingTasksNumber() + getWaitingTasksNumber();
  }

  public int getCorePoolSize() {
    return ((ThreadPoolExecutor) pool).getCorePoolSize();
  }

  public abstract Logger getLogger();

  public abstract void start();

  public void stop() {
    if (pool != null) {
      close();
      pool = null;
    }
  }

  public abstract String getName();
}
