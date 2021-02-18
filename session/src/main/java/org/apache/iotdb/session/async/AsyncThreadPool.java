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
package org.apache.iotdb.session.async;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.session.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncThreadPool {

  private static final Logger logger = LoggerFactory.getLogger(AsyncThreadPool.class);
  private ExecutorService pool;
  private BlockingQueue<Runnable> threadQueue;

  public AsyncThreadPool() {
    threadQueue = new LinkedBlockingQueue<>(Config.DEFAULT_BLOCKING_QUEUE_SIZE);
    pool = new ThreadPoolExecutor(Config.DEFAULT_THREAD_POOL_SIZE, Config.DEFAULT_THREAD_POOL_SIZE,
        0L, TimeUnit.MILLISECONDS, threadQueue, new CustomPolicy());
  }

  public AsyncThreadPool(int poolSize, int blockingQueueSize) {
    threadQueue = new LinkedBlockingQueue<>(blockingQueueSize);
    pool = new ThreadPoolExecutor(poolSize, poolSize,
        0L, TimeUnit.MILLISECONDS, threadQueue, new CustomPolicy());
  }

  public synchronized Future<?> submit(Runnable task) {
    return pool.submit(task);
  }

  public synchronized <T> Future<T> submit(Callable<T> task) {
    return pool.submit(task);
  }

  public ExecutorService getThreadPool() {
    return pool;
  }

  private static class CustomPolicy implements RejectedExecutionHandler {
    public CustomPolicy() {}

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      try {
        synchronized (r) {
          r.wait(Config.DEFAULT_THREAD_WAIT_TIME_MS);
        }
      } catch (InterruptedException e) {
        logger.error("Interrupted while insertion thread is waiting:", e);
      }
    }
  }
}
