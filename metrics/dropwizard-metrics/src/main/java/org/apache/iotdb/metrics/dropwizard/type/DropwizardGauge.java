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

import org.apache.iotdb.metrics.type.Gauge;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DropwizardGauge implements Gauge {

  AtomicLong atomicLong;
  DropwizardCachedGauge dropwizardCachedGauge;

  public DropwizardGauge() {
    atomicLong = new AtomicLong(0);
    dropwizardCachedGauge = new DropwizardCachedGauge(5, TimeUnit.MILLISECONDS);
  }

  public class DropwizardCachedGauge extends com.codahale.metrics.CachedGauge<Long> {

    protected DropwizardCachedGauge(long timeout, TimeUnit timeoutUnit) {
      super(timeout, timeoutUnit);
    }

    @Override
    protected Long loadValue() {
      return atomicLong.get();
    }
  }

  @Override
  public long value() {
    return dropwizardCachedGauge.getValue();
  }

  @Override
  public void set(long value) {
    atomicLong.set(value);
  }

  public com.codahale.metrics.Gauge<Long> getDropwizardCachedGauge() {
    return dropwizardCachedGauge;
  }
}
