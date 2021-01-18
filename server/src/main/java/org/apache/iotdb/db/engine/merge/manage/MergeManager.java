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

package org.apache.iotdb.db.engine.merge.manage;

import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.merge.task.MergeMultiChunkTask.MergeChunkHeapTask;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeManager provides a ThreadPool to queue and run all mergeUnseq tasks to restrain the total
 * resources occupied by mergeUnseq and manages a Timer to periodically issue a global mergeUnseq.
 */
public class MergeManager implements IService, MergeManagerMBean {

  private static final Logger logger = LoggerFactory.getLogger(MergeManager.class);
  private static final MergeManager INSTANCE = new MergeManager();
  private final String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());
  private final RateLimiter mergeWriteRateLimiter = RateLimiter.create(Double.MAX_VALUE);

  private AtomicInteger threadCnt = new AtomicInteger();
  private ThreadPoolExecutor mergeTaskPool;
  private ThreadPoolExecutor mergeChunkSubTaskPool;
  private ScheduledExecutorService timedMergeThreadPool;
  private ScheduledExecutorService taskCleanerThreadPool;

  private Map<String, Set<MergeFuture>> storageGroupMainTasks = new ConcurrentHashMap<>();
  private Map<String, Set<MergeFuture>> storageGroupSubTasks = new ConcurrentHashMap<>();

  private MergeManager() {
  }

  public RateLimiter getMergeWriteRateLimiter() {
    setWriteMergeRate(IoTDBDescriptor.getInstance().getConfig().getMergeWriteThroughputMbPerSec());
    return mergeWriteRateLimiter;
  }

  /**
   * wait by throughoutMbPerSec limit to avoid continuous Write Or Read
   */
  public static void mergeRateLimiterAcquire(RateLimiter limiter, long bytesLength) {
    while (bytesLength >= Integer.MAX_VALUE) {
      limiter.acquire(Integer.MAX_VALUE);
      bytesLength -= Integer.MAX_VALUE;
    }
    if (bytesLength > 0) {
      limiter.acquire((int) bytesLength);
    }
  }

  private void setWriteMergeRate(final double throughoutMbPerSec) {
    double throughout = throughoutMbPerSec * 1024.0 * 1024.0;
    // if throughout = 0, disable rate limiting
    if (throughout == 0) {
      throughout = Double.MAX_VALUE;
    }
    if (mergeWriteRateLimiter.getRate() != throughout) {
      mergeWriteRateLimiter.setRate(throughout);
    }
  }

  public static MergeManager getINSTANCE() {
    return INSTANCE;
  }

  public void submitMainTask(MergeTask mergeTask) {
    MergeFuture future = (MergeFuture) mergeTaskPool.submit(mergeTask);
    storageGroupMainTasks.computeIfAbsent(mergeTask.getStorageGroupName(),
        k -> new ConcurrentSkipListSet<>()).add(future);
  }

  public Future<Void> submitChunkSubTask(MergeChunkHeapTask task) {
    MergeFuture future = (MergeFuture) mergeChunkSubTaskPool.submit(task);
    storageGroupSubTasks
        .computeIfAbsent(task.getStorageGroupName(), k -> new ConcurrentSkipListSet<>())
        .add(future);
    return future;
  }

  @Override
  public void start() {
    JMXService.registerMBean(this, mbeanName);
    if (mergeTaskPool == null) {
      int threadNum = IoTDBDescriptor.getInstance().getConfig().getMergeThreadNum();
      if (threadNum <= 0) {
        threadNum = 1;
      }

      int chunkSubThreadNum = IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
      if (chunkSubThreadNum <= 0) {
        chunkSubThreadNum = 1;
      }

      mergeTaskPool = new MergeThreadPool(threadNum,
          r -> new Thread(r, "MergeThread-" + threadCnt.getAndIncrement()));
      mergeChunkSubTaskPool = new MergeThreadPool(threadNum * chunkSubThreadNum,
          r -> new Thread(r, "MergeChunkSubThread-" + threadCnt.getAndIncrement()));
      long mergeInterval = IoTDBDescriptor.getInstance().getConfig().getMergeIntervalSec();
      if (mergeInterval > 0) {
        timedMergeThreadPool = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,
            "TimedMergeThread"));
        timedMergeThreadPool.scheduleAtFixedRate(this::mergeAll, mergeInterval,
            mergeInterval, TimeUnit.SECONDS);
      }

      taskCleanerThreadPool = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r,
          "MergeTaskCleaner"));
      taskCleanerThreadPool.scheduleAtFixedRate(this::cleanFinishedTask, 30, 30, TimeUnit.MINUTES);
      logger.info("MergeManager started");
    }
  }

  @Override
  public void stop() {
    if (mergeTaskPool != null) {
      if (timedMergeThreadPool != null) {
        timedMergeThreadPool.shutdownNow();
        timedMergeThreadPool = null;
      }
      taskCleanerThreadPool.shutdownNow();
      taskCleanerThreadPool = null;
      mergeTaskPool.shutdownNow();
      mergeChunkSubTaskPool.shutdownNow();
      logger.info("Waiting for task pool to shut down");
      long startTime = System.currentTimeMillis();
      while (!mergeTaskPool.isTerminated() || !mergeChunkSubTaskPool.isTerminated()) {
        int timeMillis = 0;
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          logger.error("CompactionMergeTaskPoolManager {} shutdown",
              ThreadName.COMPACTION_SERVICE.getName(), e);
          Thread.currentThread().interrupt();
        }
        timeMillis += 200;
        long time = System.currentTimeMillis() - startTime;
        if (timeMillis % 60_000 == 0) {
          logger.warn("MergeManager has wait for {} seconds to stop", time / 1000);
        }
      }
      mergeTaskPool = null;
      storageGroupMainTasks.clear();
      storageGroupSubTasks.clear();
      logger.info("MergeManager stopped");
    }
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public void waitAndStop(long milliseconds) {
    if (mergeTaskPool != null) {
      if (timedMergeThreadPool != null) {
        awaitTermination(timedMergeThreadPool, milliseconds);
        timedMergeThreadPool = null;
      }
      awaitTermination(taskCleanerThreadPool, milliseconds);
      taskCleanerThreadPool = null;

      awaitTermination(mergeTaskPool, milliseconds);
      awaitTermination(mergeChunkSubTaskPool, milliseconds);
      logger.info("Waiting for task pool to shut down");
      long startTime = System.currentTimeMillis();
      while (!mergeTaskPool.isTerminated() || !mergeChunkSubTaskPool.isTerminated()) {
        int timeMillis = 0;
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          logger.error("CompactionMergeTaskPoolManager {} shutdown",
              ThreadName.COMPACTION_SERVICE.getName(), e);
          Thread.currentThread().interrupt();
        }
        timeMillis += 200;
        long time = System.currentTimeMillis() - startTime;
        if (timeMillis % 60_000 == 0) {
          logger.warn("MergeManager has wait for {} seconds to stop", time / 1000);
        }
      }
      mergeTaskPool = null;
      storageGroupMainTasks.clear();
      storageGroupSubTasks.clear();
      logger.info("MergeManager stopped");
    }
  }

  private void awaitTermination(ExecutorService service, long milliseconds) {
    try {
      service.shutdown();
      service.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("MergeThreadPool can not be closed in {} ms", milliseconds);
      Thread.currentThread().interrupt();
    }
    service.shutdownNow();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MERGE_SERVICE;
  }

  private void mergeAll() {
    try {
      StorageEngine.getInstance()
          .mergeAll(IoTDBDescriptor.getInstance().getConfig().isForceFullMerge());
    } catch (StorageEngineException e) {
      logger.error("Cannot perform a global mergeUnseq because", e);
    }
  }

  /**
   * Abort all merges of a storage group. The caller must acquire the write lock of the
   * corresponding storage group.
   */
  @Override
  public void abortMerge(String storageGroup) {
    // abort sub-tasks first
    Set<MergeFuture> subTasks = storageGroupSubTasks
        .getOrDefault(storageGroup, Collections.emptySet());
    Iterator<MergeFuture> subIterator = subTasks.iterator();
    while (subIterator.hasNext()) {
      Future<Void> next = subIterator.next();
      if (!next.isDone() && !next.isCancelled()) {
        next.cancel(true);
      }
      subIterator.remove();
    }
    // abort main tasks
    Set<MergeFuture> mainTasks = storageGroupMainTasks
        .getOrDefault(storageGroup, Collections.emptySet());
    Iterator<MergeFuture> mainIterator = mainTasks.iterator();
    while (mainIterator.hasNext()) {
      Future<Void> next = mainIterator.next();
      if (!next.isDone() && !next.isCancelled()) {
        next.cancel(true);
      }
      mainIterator.remove();
    }
  }

  private void cleanFinishedTask() {
    for (Set<MergeFuture> subTasks : storageGroupSubTasks.values()) {
      subTasks.removeIf(next -> next.isDone() || next.isCancelled());
    }
    for (Set<MergeFuture> mainTasks : storageGroupMainTasks.values()) {
      mainTasks.removeIf(next -> next.isDone() || next.isCancelled());
    }
  }

  /**
   * @return 2 maps, the first map contains status of main mergeUnseq tasks and the second map contains
   * status of mergeUnseq chunk heap tasks, both map use storage groups as keys and list of mergeUnseq status
   * as values.
   */
  public Map<String, List<TaskStatus>>[] collectTaskStatus() {
    Map<String, List<TaskStatus>>[] result = new Map[2];
    result[0] = new HashMap<>();
    result[1] = new HashMap<>();
    for (Entry<String, Set<MergeFuture>> stringSetEntry : storageGroupMainTasks.entrySet()) {
      String storageGroup = stringSetEntry.getKey();
      Set<MergeFuture> tasks = stringSetEntry.getValue();
      for (MergeFuture task : tasks) {
        result[0].computeIfAbsent(storageGroup, s -> new ArrayList<>()).add(new TaskStatus(task));
      }
    }

    for (Entry<String, Set<MergeFuture>> stringSetEntry : storageGroupSubTasks.entrySet()) {
      String storageGroup = stringSetEntry.getKey();
      Set<MergeFuture> tasks = stringSetEntry.getValue();
      for (MergeFuture task : tasks) {
        result[1].computeIfAbsent(storageGroup, s -> new ArrayList<>()).add(new TaskStatus(task));
      }
    }
    return result;
  }

  public String genMergeTaskReport() {
    Map<String, List<TaskStatus>>[] statusMaps = collectTaskStatus();
    StringBuilder builder = new StringBuilder("Main tasks:").append(System.lineSeparator());
    for (Entry<String, List<TaskStatus>> stringListEntry : statusMaps[0].entrySet()) {
      String storageGroup = stringListEntry.getKey();
      List<TaskStatus> statusList = stringListEntry.getValue();
      builder.append("\t").append("Storage group: ").append(storageGroup)
          .append(System.lineSeparator());
      for (TaskStatus status : statusList) {
        builder.append("\t\t").append(status.toString()).append(System.lineSeparator());
      }
    }

    builder.append("Sub tasks:").append(System.lineSeparator());
    for (Entry<String, List<TaskStatus>> stringListEntry : statusMaps[1].entrySet()) {
      String storageGroup = stringListEntry.getKey();
      List<TaskStatus> statusList = stringListEntry.getValue();
      builder.append("\t").append("Storage group: ").append(storageGroup)
          .append(System.lineSeparator());
      for (TaskStatus status : statusList) {
        builder.append("\t\t").append(status.toString()).append(System.lineSeparator());
      }
    }
    return builder.toString();
  }

  @Override
  public void printMergeStatus() {
    if (logger.isInfoEnabled()) {
      logger.info("Running tasks:\n {}", genMergeTaskReport());
    }
  }

  public static class TaskStatus {

    private String taskName;
    private String createdTime;
    private String progress;
    private boolean isDone;
    private boolean isCancelled;

    public TaskStatus(MergeFuture future) {
      this.taskName = future.getTaskName();
      this.createdTime = future.getCreatedTime();
      this.progress = future.getProgress();
      this.isCancelled = future.isCancelled();
      this.isDone = future.isDone();
    }

    @Override
    public String toString() {
      return String.format("%s, "
              + "%s, %s, done:%s, cancelled:%s", taskName,
          createdTime, progress, isDone, isCancelled);
    }

    public String getTaskName() {
      return taskName;
    }

    public String getCreatedTime() {
      return createdTime;
    }

    public String getProgress() {
      return progress;
    }

    public boolean isDone() {
      return isDone;
    }

    public boolean isCancelled() {
      return isCancelled;
    }
  }
}
