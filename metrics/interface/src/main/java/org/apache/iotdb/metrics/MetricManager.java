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

import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface MetricManager {
  /*
   * The following functions will create or get a exist Metric
   * @param metric: the metric name
   * @param tags:
   *    string appear in pairs, like sg="ln",user="user1" will be "sg", "ln", "user", "user1"
   * @return Metric Instance
   */
  Counter getOrCreateCounter(String metric, String... tags);

  Gauge getOrCreatGauge(String metric, String... tags);

  Rate getOrCreatRate(String metric, String... tags);

  Histogram getOrCreateHistogram(String metric, String... tags);

  Timer getOrCreateTimer(String metric, String... tags);

  /*
   * The following functions just update the current record value
   * @param the delta value will be recorded
   * @param metric the metric name
   * @param tags
   *    string appear in pairs, like sg="ln",user="user1" will be "sg", "ln", "user", "user1"
   */

  void count(int delta, String metric, String... tags);

  void count(long delta, String metric, String... tags);

  void gauge(int value, String metric, String... tags);

  void gauge(long value, String metric, String... tags);

  void rate(int value, String metric, String... tags);

  void rate(long value, String metric, String... tags);

  void histogram(int value, String metric, String... tags);

  void histogram(long value, String metric, String... tags);

  void timer(long delta, TimeUnit timeUnit, String metric, String... tags);

  /**
   * get all metric keys.
   *
   * @return all MetricKeys, key is metric name, value is tags, which is a string array.
   */
  List<String[]> getAllMetricKeys();

  // key is name + tags, value
  Map<String[], Counter> getAllCounters();

  Map<String[], Gauge> getAllGauges();

  Map<String[], Rate> getAllRates();

  Map<String[], Histogram> getAllHistograms();

  Map<String[], Timer> getAllTimers();

  boolean isEnable();

  /**
   * enable pre-defined metric set.
   *
   * @param metric which metric set we want to collect
   */
  void enableKnownMetric(KnownMetric metric);

  /**
   * init something.
   *
   * @return whether success
   */
  boolean init();

  String getName();
}
