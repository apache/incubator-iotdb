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

import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;

import java.util.concurrent.TimeUnit;

public class DropwizardTimer implements Timer {
  com.codahale.metrics.Timer timer;

  public DropwizardTimer(com.codahale.metrics.Timer timer) {
    this.timer = timer;
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    timer.update(duration, unit);
  }

  @Override
  public HistogramSnapshot takeSnapshot() {
    return new DropwizardHistogramSnapshot(timer.getSnapshot());
  }

  @Override
  public Rate getImmutableRate() {
    return new DropwizardRate(timer);
  }
}
