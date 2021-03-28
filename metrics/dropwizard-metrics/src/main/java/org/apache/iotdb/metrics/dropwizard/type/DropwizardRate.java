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

package org.apache.iotdb.metrics.dropwizard.type;

import org.apache.iotdb.metrics.type.Rate;

import com.codahale.metrics.Meter;

public class DropwizardRate implements Rate {

  Meter meter;
  // read-only meter
  com.codahale.metrics.Timer timer;

  public DropwizardRate(Meter meter) {
    this.meter = meter;
    this.timer = null;
  }

  public DropwizardRate(com.codahale.metrics.Timer timer) {
    this.timer = timer;
    this.meter = null;
  }

  @Override
  public long getCount() {
    if (meter != null) {
      return meter.getCount();
    }
    return timer.getCount();
  }

  @Override
  public double getOneMinuteRate() {
    if (meter != null) {
      return meter.getOneMinuteRate();
    }
    return timer.getOneMinuteRate();
  }

  @Override
  public double getMeanRate() {
    if (meter != null) {
      return meter.getMeanRate();
    }
    return timer.getMeanRate();
  }

  @Override
  public double getFiveMinuteRate() {
    if (meter != null) {
      return meter.getFiveMinuteRate();
    }
    return timer.getFiveMinuteRate();
  }

  @Override
  public double getFifteenMinuteRate() {
    if (meter != null) {
      return meter.getFifteenMinuteRate();
    }
    return timer.getFifteenMinuteRate();
  }

  @Override
  public void mark() {
    if (meter != null) {
      meter.mark();
    }
  }

  @Override
  public void mark(long n) {
    if (meter != null) {
      meter.mark(n);
    }
  }
}
