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

package org.apache.iotdb.metrics.micrometer.type;

import org.apache.iotdb.metrics.type.Gauge;

import io.micrometer.core.instrument.Tags;

import java.util.concurrent.atomic.AtomicLong;

public class MicrometerGauge implements Gauge {
  private final AtomicLong atomicLong;

  public MicrometerGauge(
      io.micrometer.core.instrument.MeterRegistry meterRegistry,
      String metricName,
      String... tags) {
    atomicLong = meterRegistry.gauge(metricName, Tags.of(tags), new AtomicLong(0));
  }

  @Override
  public long value() {
    return atomicLong.get();
  }

  @Override
  public void set(long value) {
    atomicLong.set(value);
  }
}
