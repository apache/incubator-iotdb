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

package org.apache.iotdb.db.utils.windowing.runtime;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.IoTThreadFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.rescon.AbstractPoolManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WindowEvaluationTaskPoolManager extends AbstractPoolManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(WindowEvaluationTaskPoolManager.class);

  private WindowEvaluationTaskPoolManager() {
    final int nThreads =
        IoTDBDescriptor.getInstance().getConfig().getConcurrentWindowEvaluationThread();
    LOGGER.info("WindowEvaluationTaskPoolManager is initializing, thread number: {}", nThreads);
    pool =
        new ThreadPoolExecutor(
            nThreads,
            nThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(
                IoTDBDescriptor.getInstance().getConfig().getMaxPendingWindowEvaluationTasks()),
            new IoTThreadFactory(ThreadName.WINDOW_EVALUATION_SERVICE.getName()));
  }

  public void submit(WindowEvaluationTask task) {
    try {
      super.submit(task);
    } catch (RejectedExecutionException e) {
      task.onRejection();
    }
  }

  @Override
  public Logger getLogger() {
    return LOGGER;
  }

  @Override
  public String getName() {
    return "window evaluation task";
  }

  @Override
  public void start() {
    if (pool != null) {
      return;
    }

    pool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            IoTDBDescriptor.getInstance().getConfig().getConcurrentWindowEvaluationThread(),
            ThreadName.WINDOW_EVALUATION_SERVICE.getName());
  }

  public static WindowEvaluationTaskPoolManager getInstance() {
    return WindowEvaluationTaskPoolManager.InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
      // nothing to do
    }

    private static final WindowEvaluationTaskPoolManager INSTANCE =
        new WindowEvaluationTaskPoolManager();
  }
}
