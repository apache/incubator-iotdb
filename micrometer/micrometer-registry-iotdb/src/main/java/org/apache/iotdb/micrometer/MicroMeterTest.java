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
package org.apache.iotdb.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;

public class MicroMeterTest {

  public static void main(String[] args) {
    Metrics.addRegistry(new SimpleMeterRegistry());
    Counter counter = Metrics.counter("iotdb.storage.insert.count", "_group", "test1");

    counter.increment();

    counter = Metrics.counter("iotdb.storage.insert.count", "_group", "test1");

    counter.increment();

    System.out.println(counter.count());

    Timer timer = Metrics.timer("iotdb.storage.insert.latency", "_group", "test1");
    timer.record(10, TimeUnit.NANOSECONDS);
    System.out.println(timer.mean(timer.baseTimeUnit()));
  }
}
