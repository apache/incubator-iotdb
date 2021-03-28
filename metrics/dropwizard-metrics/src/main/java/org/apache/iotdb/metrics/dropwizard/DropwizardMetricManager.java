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

package org.apache.iotdb.metrics.dropwizard;

import org.apache.iotdb.metrics.KnownMetric;
import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardCounter;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardGauge;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardHistogram;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardRate;
import org.apache.iotdb.metrics.dropwizard.type.DropwizardTimer;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.IMetric;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.JvmAttributeGaugeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Metric manager based on dropwizard metrics. More details in https://metrics.dropwizard.io/4.1.2/.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class DropwizardMetricManager implements MetricManager {

  private static final Logger logger = LoggerFactory.getLogger(DropwizardMetricManager.class);

  Map<MetricName, IMetric> currentMeters;
  boolean isEnable;

  com.codahale.metrics.MetricRegistry metricRegistry;
  MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

  /** init the field with dropwizard library. */
  public DropwizardMetricManager() {
    metricRegistry = new MetricRegistry();
    isEnable = metricConfig.getEnableMetric();
    currentMeters = new ConcurrentHashMap<>();
  }

  @Override
  public Counter getOrCreateCounter(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingCounter;
    }
    MetricName name = new MetricName(metric, tags);
    return (Counter)
        currentMeters.computeIfAbsent(
            name, key -> new DropwizardCounter(metricRegistry.counter(name.toFlatString())));
  }

  @Override
  public Gauge getOrCreatGauge(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingGauge;
    }
    MetricName name = new MetricName(metric, tags);
    return (Gauge)
        currentMeters.computeIfAbsent(
            name,
            key -> {
              DropwizardGauge dropwizardGauge = new DropwizardGauge();
              metricRegistry.register(
                  name.toFlatString(), dropwizardGauge.getDropwizardCachedGauge());
              return dropwizardGauge;
            });
  }

  @Override
  public Rate getOrCreatRate(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingRate;
    }
    MetricName name = new MetricName(metric, tags);
    return (Rate)
        currentMeters.computeIfAbsent(
            name, key -> new DropwizardRate(metricRegistry.meter(name.toFlatString())));
  }

  @Override
  public Histogram getOrCreateHistogram(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingHistogram;
    }
    MetricName name = new MetricName(metric, tags);
    return (Histogram)
        currentMeters.computeIfAbsent(
            name, key -> new DropwizardHistogram(metricRegistry.histogram(name.toFlatString())));
  }

  @Override
  public Timer getOrCreateTimer(String metric, String... tags) {
    if (!isEnable) {
      return DoNothingMetricManager.doNothingTimer;
    }
    MetricName name = new MetricName(metric, tags);
    return (Timer)
        currentMeters.computeIfAbsent(
            name, key -> new DropwizardTimer(metricRegistry.timer(name.toFlatString())));
  }

  @Override
  public void count(int delta, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Counter)
            currentMeters.computeIfAbsent(
                name, key -> new DropwizardCounter(metricRegistry.counter(name.toFlatString()))))
        .inc(delta);
  }

  @Override
  public void count(long delta, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Counter)
            currentMeters.computeIfAbsent(
                name, key -> new DropwizardCounter(metricRegistry.counter(name.toFlatString()))))
        .inc(delta);
  }

  @Override
  public void gauge(int value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Gauge)
            currentMeters.computeIfAbsent(
                name,
                key -> {
                  DropwizardGauge dropwizardGauge = new DropwizardGauge();
                  metricRegistry.register(
                      name.toFlatString(), dropwizardGauge.getDropwizardCachedGauge());
                  return dropwizardGauge;
                }))
        .set(value);
  }

  @Override
  public void gauge(long value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Gauge)
            currentMeters.computeIfAbsent(
                name,
                key -> {
                  DropwizardGauge dropwizardGauge = new DropwizardGauge();
                  metricRegistry.register(
                      name.toFlatString(), dropwizardGauge.getDropwizardCachedGauge());
                  return dropwizardGauge;
                }))
        .set(value);
  }

  @Override
  public void rate(int value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Rate)
            currentMeters.computeIfAbsent(
                name, key -> new DropwizardRate(metricRegistry.meter(name.toFlatString()))))
        .mark(value);
  }

  @Override
  public void rate(long value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Rate)
            currentMeters.computeIfAbsent(
                name, key -> new DropwizardRate(metricRegistry.meter(name.toFlatString()))))
        .mark(value);
  }

  @Override
  public void histogram(int value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Histogram)
            currentMeters.computeIfAbsent(
                name,
                key -> new DropwizardHistogram(metricRegistry.histogram(name.toFlatString()))))
        .update(value);
  }

  @Override
  public void histogram(long value, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Histogram)
            currentMeters.computeIfAbsent(
                name,
                key -> new DropwizardHistogram(metricRegistry.histogram(name.toFlatString()))))
        .update(value);
  }

  @Override
  public void timer(long delta, TimeUnit timeUnit, String metric, String... tags) {
    if (!isEnable) {
      return;
    }
    MetricName name = new MetricName(metric, tags);
    ((Timer)
            currentMeters.computeIfAbsent(
                name, key -> new DropwizardTimer(metricRegistry.timer(name.toFlatString()))))
        .update(delta, timeUnit);
  }

  @Override
  public List<String[]> getAllMetricKeys() {
    if (!isEnable) {
      return Collections.emptyList();
    }
    List<String[]> keys = new ArrayList<>(currentMeters.size());
    currentMeters.keySet().forEach(k -> keys.add(k.toStringArray()));
    return keys;
  }

  @Override
  public Map<String[], Counter> getAllCounters() {
    Map<String[], Counter> counterMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Counter) {
        counterMap.put(entry.getKey().toStringArray(), (Counter) entry.getValue());
      }
    }
    return counterMap;
  }

  @Override
  public Map<String[], Gauge> getAllGauges() {
    Map<String[], Gauge> gaugeMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Gauge) {
        gaugeMap.put(entry.getKey().toStringArray(), (Gauge) entry.getValue());
      }
    }
    return gaugeMap;
  }

  @Override
  public Map<String[], Rate> getAllRates() {
    Map<String[], Rate> rateMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Rate) {
        rateMap.put(entry.getKey().toStringArray(), (Rate) entry.getValue());
      }
    }
    return rateMap;
  }

  @Override
  public Map<String[], Histogram> getAllHistograms() {
    Map<String[], Histogram> histogramMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Histogram) {
        histogramMap.put(entry.getKey().toStringArray(), (Histogram) entry.getValue());
      }
    }
    return histogramMap;
  }

  @Override
  public Map<String[], Timer> getAllTimers() {
    Map<String[], Timer> timerMap = new HashMap<>();
    for (Map.Entry<MetricName, IMetric> entry : currentMeters.entrySet()) {
      if (entry.getValue() instanceof Timer) {
        timerMap.put(entry.getKey().toStringArray(), (Timer) entry.getValue());
      }
    }
    return timerMap;
  }

  @Override
  public boolean isEnable() {
    return isEnable;
  }

  @Override
  public void enableKnownMetric(KnownMetric metric) {
    if (!isEnable) {
      return;
    }
    switch (metric) {
      case JVM:
        enableJvmMetrics();
        break;
      case SYSTEM:
        break;
      case THREAD:
        break;
      default:
        logger.warn("Unsupported metric type {}", metric);
    }
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  private void enableJvmMetrics() {
    if (!isEnable) {
      return;
    }
    metricRegistry.registerAll(new JvmAttributeGaugeSet());
    metricRegistry.registerAll(new GarbageCollectorMetricSet());
    metricRegistry.registerAll(new ClassLoadingGaugeSet());
    metricRegistry.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    metricRegistry.registerAll(new CachedThreadStatesGaugeSet(5, TimeUnit.MILLISECONDS));
  }

  @Override
  public boolean init() {
    return true;
  }

  @Override
  public String getName() {
    return "DropwizardMetricManager";
  }
}
