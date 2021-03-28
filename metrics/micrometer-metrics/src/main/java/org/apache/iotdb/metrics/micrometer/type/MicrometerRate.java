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

import org.apache.iotdb.metrics.type.Rate;

import com.codahale.metrics.Meter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * could not publish to other metrics system exclude jmx and csv, because micrometer assumes that
 * other metrics system have the ability to calculate rate. Details is at
 * https://github.com/micrometer-metrics/micrometer/issues/1935.
 *
 * <p>Now, we only record a gauge for the rate record in micrometer, and we use dropwizard meter to
 * calculate the meter.
 */
public class MicrometerRate implements Rate {
  AtomicLong atomicLong;
  Meter meter;

  public MicrometerRate(AtomicLong atomicLong) {
    this.atomicLong = atomicLong;
    meter = new Meter();
  }

  @Override
  public long getCount() {
    return meter.getCount();
  }

  @Override
  public double getOneMinuteRate() {
    return meter.getOneMinuteRate();
  }

  @Override
  public double getMeanRate() {
    return meter.getMeanRate();
  }

  @Override
  public double getFiveMinuteRate() {
    return meter.getFiveMinuteRate();
  }

  @Override
  public double getFifteenMinuteRate() {
    return meter.getFifteenMinuteRate();
  }

  @Override
  public void mark() {
    atomicLong.set(1);
    meter.mark();
  }

  @Override
  public void mark(long n) {
    atomicLong.set(n);
    meter.mark(n);
  }
}
