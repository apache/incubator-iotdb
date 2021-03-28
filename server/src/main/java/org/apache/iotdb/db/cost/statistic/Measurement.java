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
package org.apache.iotdb.db.cost.statistic;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Measurement is used to record execution time of operations defined in enum class Operation. It
 * can display average time of each operation, and proportion of operation whose execution time fall
 * into time range defined in BUCKET_IN_MS. If you want to change abscissa of histogram, just change
 * the BUCKET_IN_MS array. For recording a operation, you should: 1) add a item in enum class
 * Operation. 2) call <code>startTimeInNano = System.nanoTime()</code> to recode startTime of that
 * operation. 3) call <code>
 * Measurement.INSTANCE.addOperationLatency(operation, startTimeInNano)
 * </code> at the end of that operation;
 *
 * @see Operation
 */
public class Measurement implements MeasurementMBean, IService {
  private static Logger logger = LoggerFactory.getLogger(Measurement.class);

  /** queue for async store time latencies. */
  private ConcurrentCircularArray[] operationLatenciesQueue;

  /** size of each queue, this is calculated by memory. */
  private final int queueSize;

  /** latencies sum of each operation. */
  private long[] operationLatencies;

  /** the num of each operation. */
  private long[] operationCnt;

  /** abscissa of histogram. */
  private static final int[] BUCKET_IN_MS = {1, 4, 16, 64, 256, 1024, Integer.MAX_VALUE};

  /** length of BUCKET_IN_MS. */
  private static final int BUCKET_SIZE = BUCKET_IN_MS.length;

  /**
   * the num of operation that execution time falls into time range of BUCKET_IN_MS. The outer array
   * is each operation, the inner array is each time range in BUCKET_IN_MS.
   */
  private long[][] operationHistogram;

  /** display thread and queue consumer thread. */
  private ScheduledExecutorService service;

  /** future task of display thread and queue consumer thread. */
  private ScheduledFuture<?> displayFuture;

  private ScheduledFuture<?> consumeFuture;

  /** lock for modifying isEnableStat, displayFuture and consumeFuture. */
  private ReentrantLock stateChangeLock = new ReentrantLock();

  public static final Measurement INSTANCE = AsyncMeasurementHolder.MEASUREMENT;

  private boolean isEnableStat;
  private long displayIntervalInMs;
  private Map<String, Boolean> operationSwitch;

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          "org.apache.iotdb.db.cost.statistic", IoTDBConstant.JMX_TYPE, getID().getJmxName());

  private Measurement() {
    IoTDBConfig tdbConfig = IoTDBDescriptor.getInstance().getConfig();
    isEnableStat = tdbConfig.isEnablePerformanceStat();
    displayIntervalInMs = tdbConfig.getPerformanceStatDisplayInterval();
    int memoryInKb = tdbConfig.getPerformanceStatMemoryInKB();

    queueSize = memoryInKb * 1000 / Operation.values().length / 8;
    operationLatenciesQueue = new ConcurrentCircularArray[Operation.values().length];
    operationLatencies = new long[Operation.values().length];
    operationCnt = new long[Operation.values().length];
    operationSwitch = new HashMap<>(Operation.values().length);
    for (Operation op : Operation.values()) {
      operationLatenciesQueue[op.ordinal()] = new ConcurrentCircularArray(queueSize);
      operationCnt[op.ordinal()] = 0;
      operationLatencies[op.ordinal()] = 0;
      operationSwitch.put(op.getName(), true);
    }
    operationHistogram = new long[Operation.values().length][BUCKET_SIZE];
    for (Operation operation : Operation.values()) {
      for (int i = 0; i < BUCKET_SIZE; i++) {
        operationHistogram[operation.ordinal()][i] = 0;
      }
    }
    logger.info("start measurement stats module...");
    service =
        IoTDBThreadPoolFactory.newScheduledThreadPool(2, ThreadName.TIME_COST_STATISTIC.getName());
  }

  public boolean addOperationLatency(Operation op, long startTime) {
    if (isEnableStat && operationSwitch.get(op.getName())) {
      return operationLatenciesQueue[op.ordinal()].put((System.currentTimeMillis() - startTime));
    }
    return false;
  }

  @Override
  public void startStatistics() {
    stateChangeLock.lock();
    try {
      isEnableStat = true;
      if (consumeFuture != null && !consumeFuture.isCancelled()) {
        logger.info("The consuming task in measurement stat module is already running...");
      } else {
        consumeFuture =
            service.scheduleWithFixedDelay(
                new Measurement.DisplayRunnable(), 20, displayIntervalInMs, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      logger.error("Find error when start performance statistic thread, ", e);
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public void startContinuousPrintStatistics() {
    stateChangeLock.lock();
    try {
      isEnableStat = true;
      if (displayFuture != null && !displayFuture.isCancelled()) {
        logger.info("The display task in measurement stat module is already running...");
      } else {
        displayFuture =
            service.scheduleWithFixedDelay(
                new Measurement.DisplayRunnable(), 20, displayIntervalInMs, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      logger.error("Find error when start performance statistic thread, ", e);
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public void startPrintStatisticsOnce() {
    showMeasurements();
  }

  @Override
  public void stopPrintStatistic() {
    stateChangeLock.lock();
    try {
      displayFuture = cancelFuture(displayFuture);
    } catch (Exception e) {
      logger.error("Find error when stop display thread, ", e);
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public void stopStatistic() {
    stateChangeLock.lock();
    try {
      isEnableStat = false;
      displayFuture = cancelFuture(displayFuture);
      consumeFuture = cancelFuture(consumeFuture);
    } catch (Exception e) {
      logger.error("Find error when stop display and consuming threads, ", e);
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public void clearStatisticalState() {
    for (Operation op : Operation.values()) {
      operationLatenciesQueue[op.ordinal()].clear();
      operationCnt[op.ordinal()] = 0;
      operationLatencies[op.ordinal()] = 0;
      for (int i = 0; i < BUCKET_SIZE; i++) {
        operationHistogram[op.ordinal()][i] = 0;
      }
    }
  }

  @Override
  public boolean changeOperationSwitch(String operationName, Boolean operationState) {
    if (operationSwitch.containsKey(operationName)) {
      operationSwitch.put(operationName, operationState);
      return true;
    } else {
      return false;
    }
  }

  /** start service. */
  @Override
  public void start() throws StartupException {
    // start display thread and consumer threads.
    logger.info("start the consuming task in the measurement stats module...");
    this.clearStatisticalState();
    if (service.isShutdown()) {
      service =
          IoTDBThreadPoolFactory.newScheduledThreadPool(
              2, ThreadName.TIME_COST_STATISTIC.getName());
    }
    // we have to check again because someone may change the value.
    isEnableStat = IoTDBDescriptor.getInstance().getConfig().isEnablePerformanceStat();
    if (isEnableStat) {
      consumeFuture = service.schedule(new QueueConsumerThread(), 0, TimeUnit.MILLISECONDS);
    }
    try {
      JMXService.registerMBean(INSTANCE, mbeanName);
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  /** stop service. */
  @Override
  public void stop() {
    logger.info("stop measurement stats module...");
    JMXService.deregisterMBean(mbeanName);
    if (service == null || service.isShutdown()) {
      return;
    }
    service.shutdownNow();
    try {
      consumeFuture = cancelFuture(consumeFuture);
      displayFuture = cancelFuture(displayFuture);
      service.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("Performance statistic service could not be shutdown, {}", e.getMessage());
      // Restore interrupted state...
      Thread.currentThread().interrupt();
    }
  }

  /**
   * @param future
   * @return always return null;
   */
  private ScheduledFuture<?> cancelFuture(ScheduledFuture<?> future) {
    if (future != null) {
      future.cancel(true);
    }
    return null;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PERFORMANCE_STATISTIC_SERVICE;
  }

  @Override
  public boolean isEnableStat() {
    return isEnableStat;
  }

  @Override
  public long getDisplayIntervalInMs() {
    return displayIntervalInMs;
  }

  @Override
  public void setDisplayIntervalInMs(long displayIntervalInMs) {
    this.displayIntervalInMs = displayIntervalInMs;
  }

  @Override
  public Map<String, Boolean> getOperationSwitch() {
    return operationSwitch;
  }

  private static class AsyncMeasurementHolder {

    private static final Measurement MEASUREMENT = new Measurement();

    private AsyncMeasurementHolder() {}
  }

  private void showMeasurements() {
    Date date = new Date();
    logger.info(
        "====================================={} Measurement (ms)======================================",
        date);
    String head =
        String.format("%-45s%-25s%-25s%-25s", "OPERATION", "COUNT", "TOTAL_TIME", "AVG_TIME");
    logger.info(head);
    for (Operation operation : Operation.values()) {
      if (!operationSwitch.get(operation.getName())) {
        continue;
      }
      long cnt = operationCnt[operation.ordinal()];
      long totalInMs = operationLatencies[operation.ordinal()];
      String avg = String.format("%.4f", (totalInMs / (cnt + 1e-9)));
      String item =
          String.format("%-45s%-25s%-25s%-25s", operation.name, cnt + "", totalInMs + "", avg);
      logger.info(item);
    }
    logger.info(
        "==========================================OPERATION HISTOGRAM====================================================");
    StringBuilder histogramHead = new StringBuilder(String.format("%-45s", "OPERATION"));
    for (int i = 0; i < BUCKET_SIZE; i++) {
      histogramHead.append(String.format("%-8s", BUCKET_IN_MS[i] + "ms"));
    }
    if (logger.isInfoEnabled()) {
      logger.info(histogramHead.toString());
    }
    for (Operation operation : Operation.values()) {
      if (!operationSwitch.get(operation.getName())) {
        continue;
      }
      StringBuilder item = new StringBuilder(String.format("%-45s", operation.getName()));
      long cnt = operationCnt[operation.ordinal()];
      for (int i = 0; i < BUCKET_SIZE; i++) {
        String avg =
            String.format(
                "%.2f", (operationHistogram[operation.ordinal()][i] / (cnt + 1e-9) * 100));
        item.append(String.format("%-8s", avg + "%"));
      }
      if (logger.isInfoEnabled()) {
        logger.info(item.toString());
      }
    }

    logger.info(
        "=================================================================================================================");
  }

  class DisplayRunnable extends WrappedRunnable {

    @Override
    public void runMayThrow() {
      showMeasurements();
    }
  }

  class QueueConsumerThread extends WrappedRunnable {

    @Override
    public void runMayThrow() {
      consumer();
    }

    private void consumer() {
      boolean allEmpty;
      while (isEnableStat) {
        allEmpty = true;
        for (Operation op : Operation.values()) {
          if (!operationSwitch.get(op.getName())) {
            continue;
          }
          int idx = op.ordinal();
          ConcurrentCircularArray queue = operationLatenciesQueue[idx];
          if (queue.hasData()) {
            long time = queue.take();
            operationLatencies[idx] += time;
            operationCnt[idx]++;
            operationHistogram[idx][calIndex(time)]++;
            allEmpty = false;
          }
        }
        if (allEmpty) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }

    private int calIndex(long x) {
      for (int i = 0; i < BUCKET_SIZE; i++) {
        if (BUCKET_IN_MS[i] >= x) {
          return i;
        }
      }
      return BUCKET_SIZE - 1;
    }
  }

  public long[] getOperationLatencies() {
    return operationLatencies;
  }

  public long[] getOperationCnt() {
    return operationCnt;
  }
}
