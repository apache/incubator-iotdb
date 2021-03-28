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

package org.apache.iotdb.metrics;

import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.impl.DoNothingMetricReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

/**
 * MetricService is the entry to manage all Metric system, include MetricManager and MetricReporter.
 */
public class MetricService {

  private static final Logger logger = LoggerFactory.getLogger(MetricService.class);
  private static final MetricConfig metricConfig =
      MetricConfigDescriptor.getInstance().getMetricConfig();

  static {
    init();
  }

  private static final MetricService INSTANCE = new MetricService();

  private static MetricManager metricManager;
  private static MetricReporter metricReporter;

  public static MetricService getInstance() {
    return INSTANCE;
  }

  private MetricService() {}

  private static void init() {
    logger.info("init metric service");
    ServiceLoader<MetricManager> metricManagers = ServiceLoader.load(MetricManager.class);
    int size = 0;
    MetricManager nothingManager = new DoNothingMetricManager();

    for (MetricManager mf : metricManagers) {
      size++;
      if (metricConfig.getMetricManagerType().equals(mf.getName())) {
        metricManager = mf;
        break;
      }
    }

    // if no more implementations, we use nothingManager.
    if (size == 0 || metricManager == null) {
      metricManager = nothingManager;
    } else if (size > 1) {
      logger.warn(
          "detect more than one MetricManager, will use {}", metricManager.getClass().getName());
    }
    // do some init work
    metricManager.init();

    ServiceLoader<MetricReporter> reporter = ServiceLoader.load(MetricReporter.class);
    size = 0;
    for (MetricReporter r : reporter) {
      size++;
      if (metricConfig.getMetricReporterType().equals(r.getName())) {
        metricReporter = r;
        logger.info("detect MetricReporter {}", r.getClass().getName());
      }
    }

    // if no more implementations, we use nothingReporter.
    if (size == 0 || metricReporter == null) {
      metricReporter = new DoNothingMetricReporter();
    } else if (size > 1) {
      logger.warn(
          "detect more than one MetricReporter, will use {}", metricReporter.getClass().getName());
    }
    // do some init work
    metricReporter.setMetricManager(metricManager);
    if (isEnable()) {
      metricReporter.start();
    }
  }

  /** Stop metric service. If disable, do nothing. */
  public static void stop() {
    if (!isEnable()) {
      return;
    }
    metricReporter.stop();
  }

  public static MetricManager getMetricManager() {
    return metricManager;
  }

  public static void enableKnownMetric(KnownMetric metric) {
    metricManager.enableKnownMetric(metric);
  }

  public static boolean isEnable() {
    return metricConfig.getEnableMetric();
  }
}
