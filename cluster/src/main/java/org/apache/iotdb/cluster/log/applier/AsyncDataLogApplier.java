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

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.server.monitor.Timer;
import org.apache.iotdb.cluster.server.monitor.Timer.Statistic;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.service.IoTDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class AsyncDataLogApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(AsyncDataLogApplier.class);
  private static final int CONCURRENT_CONSUMER_NUM = Runtime.getRuntime().availableProcessors();
  private LogApplier embeddedApplier;
  private Map<PartialPath, DataLogConsumer> consumerMap;
  private ExecutorService consumerPool;
  private String name;

  // a plan that affects multiple sgs should wait until all consumers become empty to assure all
  // previous logs are applied, such a plan will wait on this condition if it finds any
  // consumers nonempty, and each time a consumer becomes empty, this will be notified so the
  // waiting log can start another round of check
  private final Object consumerEmptyCondition = new Object();

  public AsyncDataLogApplier(LogApplier embeddedApplier, String name) {
    this.embeddedApplier = embeddedApplier;
    consumerMap = new HashMap<>();
    consumerPool =
        new ThreadPoolExecutor(
            CONCURRENT_CONSUMER_NUM,
            Integer.MAX_VALUE,
            0,
            TimeUnit.SECONDS,
            new SynchronousQueue<>());
    this.name = name;
  }

  @Override
  public void close() {
    consumerPool.shutdownNow();
  }

  @Override
  // synchronized: when a log is draining consumers, avoid other threads adding more logs so that
  // the consumers will never be drained
  public synchronized void apply(Log log) {

    PartialPath logKey;
    try {
      logKey = getLogKey(log);
    } catch (StorageGroupNotSetException e) {
      logger.debug("Exception occurred when applying {}", log, e);
      log.setException(e);
      log.setApplied(true);
      return;
    }

    if (logKey != null) {
      // this plan only affects one sg, so we can run it with other plans in parallel
      long startTime = Statistic.RAFT_SENDER_COMMIT_TO_CONSUMER_LOGS.getOperationStartTime();
      provideLogToConsumers(logKey, log);
      Statistic.RAFT_SENDER_COMMIT_TO_CONSUMER_LOGS.calOperationCostTimeFromStart(startTime);
      return;
    }

    logger.debug("{}: {} is waiting for consumers to drain", name, log);
    long startTime = Statistic.RAFT_SENDER_COMMIT_EXCLUSIVE_LOGS.getOperationStartTime();
    drainConsumers();
    applyInternal(log);
    Statistic.RAFT_SENDER_COMMIT_EXCLUSIVE_LOGS.calOperationCostTimeFromStart(startTime);
  }

  private PartialPath getLogKey(Log log) throws StorageGroupNotSetException {
    // we can only apply some kinds of plans in parallel, for other logs, we must wait until all
    // previous logs are applied, or the order of deletions and insertions may get wrong
    if (log instanceof PhysicalPlanLog) {
      PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
      PhysicalPlan plan = physicalPlanLog.getPlan();
      // this plan only affects one sg, so we can run it with other plans in parallel
      return getPlanKey(plan);
    } else if (log instanceof CloseFileLog) {
      CloseFileLog closeFileLog = (CloseFileLog) log;
      PartialPath partialPath = null;
      try {
        partialPath = new PartialPath(closeFileLog.getStorageGroupName());
      } catch (IllegalPathException e) {
        // unreachable
      }
      return partialPath;
    }
    return null;
  }

  private PartialPath getPlanKey(PhysicalPlan plan) throws StorageGroupNotSetException {
    return getPlanSG(plan);
  }

  /**
   * We can sure that the storage group of all InsertTabletPlans in InsertMultiTabletPlan are the
   * same. this is done in {@link
   * org.apache.iotdb.cluster.query.ClusterPlanRouter#splitAndRoutePlan(InsertMultiTabletPlan)}
   *
   * <p>We can also sure that the storage group of all InsertRowPlans in InsertRowsPlan are the
   * same. this is done in {@link
   * org.apache.iotdb.cluster.query.ClusterPlanRouter#splitAndRoutePlan(InsertRowsPlan)}
   *
   * @return the sg that the plan belongs to
   * @throws StorageGroupNotSetException if no sg found
   */
  private PartialPath getPlanSG(PhysicalPlan plan) throws StorageGroupNotSetException {
    PartialPath sgPath = null;
    if (plan instanceof InsertMultiTabletPlan) {
      PartialPath deviceId = ((InsertMultiTabletPlan) plan).getFirstDeviceId();
      sgPath = IoTDB.metaManager.getStorageGroupPath(deviceId);
    } else if (plan instanceof InsertRowsPlan) {
      PartialPath path = ((InsertRowsPlan) plan).getFirstDeviceId();
      sgPath = IoTDB.metaManager.getStorageGroupPath(path);
    } else if (plan instanceof InsertPlan) {
      PartialPath deviceId = ((InsertPlan) plan).getPrefixPath();
      sgPath = IoTDB.metaManager.getStorageGroupPath(deviceId);
    } else if (plan instanceof CreateTimeSeriesPlan) {
      PartialPath path = ((CreateTimeSeriesPlan) plan).getPath();
      sgPath = IoTDB.metaManager.getStorageGroupPath(path);
    }
    return sgPath;
  }

  private void provideLogToConsumers(PartialPath planKey, Log log) {
    if (Timer.ENABLE_INSTRUMENTING) {
      log.setEnqueueTime(System.nanoTime());
    }
    consumerMap
        .computeIfAbsent(planKey, d -> new DataLogConsumer(name + "-" + d.getFullPath()))
        .accept(log);
  }

  private void drainConsumers() {
    synchronized (consumerEmptyCondition) {
      while (!allConsumersEmpty()) {
        // wait until all consumers empty
        try {
          consumerEmptyCondition.wait(5);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  private boolean allConsumersEmpty() {
    for (DataLogConsumer consumer : consumerMap.values()) {
      if (!consumer.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Consumer not empty: {}", consumer);
        }
        return false;
      }
    }
    return true;
  }

  private void applyInternal(Log log) {
    long startTime = Statistic.RAFT_SENDER_DATA_LOG_APPLY.getOperationStartTime();
    embeddedApplier.apply(log);
    if (Timer.ENABLE_INSTRUMENTING) {
      Statistic.RAFT_SENDER_DATA_LOG_APPLY.calOperationCostTimeFromStart(startTime);
    }
  }

  private class DataLogConsumer implements Runnable, Consumer<Log> {

    private BlockingQueue<Log> logQueue = new ArrayBlockingQueue<>(4096);
    private volatile long lastLogIndex;
    private volatile long lastAppliedLogIndex;
    private String name;
    private Future<?> future;

    public DataLogConsumer(String name) {
      this.name = name;
    }

    public boolean isEmpty() {
      return lastLogIndex == lastAppliedLogIndex;
    }

    @Override
    public void run() {
      // appliers have a higher priority than normal threads (like client threads and low
      // priority background threads), to assure fast ingestion, but a lower priority than
      // heartbeat threads
      Thread.currentThread().setPriority(8);
      Thread.currentThread().setName(name);
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Log log = logQueue.take();
          Statistic.RAFT_SENDER_IN_APPLY_QUEUE.calOperationCostTimeFromStart(log.getEnqueueTime());
          try {
            applyInternal(log);
          } finally {
            lastAppliedLogIndex = log.getCurrLogIndex();
            if (isEmpty()) {
              synchronized (consumerEmptyCondition) {
                consumerEmptyCondition.notifyAll();
              }
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          logger.error("DataLogConsumer exits", e);
          return;
        }
      }
      logger.info("DataLogConsumer exits");
    }

    @Override
    public void accept(Log log) {
      if (future == null || future.isCancelled() || future.isDone()) {
        if (future != null) {
          try {
            future.get();
          } catch (InterruptedException e) {
            logger.error("Last applier thread exits unexpectedly", e);
            Thread.currentThread().interrupt();
          } catch (ExecutionException e) {
            logger.error("Last applier thread exits unexpectedly", e);
          }
        }
        future = consumerPool.submit(this);
      }

      try {
        lastLogIndex = log.getCurrLogIndex();
        logQueue.put(log);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.setException(e);
        log.setApplied(true);
        lastAppliedLogIndex = log.getCurrLogIndex();
      }
    }

    @Override
    public String toString() {
      return "DataLogConsumer{"
          + "logQueue="
          + logQueue.size()
          + ", lastLogIndex="
          + lastLogIndex
          + ", lastAppliedLogIndex="
          + lastAppliedLogIndex
          + ", name='"
          + name
          + '\''
          + '}';
    }
  }
}
