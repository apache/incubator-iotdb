/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.micrometer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionCounter;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionTimer;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.internal.DefaultGauge;
import io.micrometer.core.instrument.internal.DefaultLongTaskTimer;
import io.micrometer.core.instrument.internal.DefaultMeter;
import io.micrometer.core.instrument.push.PushMeterRegistry;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

/**
 * Registry which stores all values as IoTDB Time Series.
 * All time-related values should be milliseconds.
 * <p>
 * With default settings, each second all values are written.
 */
public class IoTDBRegistry extends PushMeterRegistry {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBRegistry.class);
    private final SessionPool sessionPool;

    public IoTDBRegistry(IoTDBRegistryConfig config, Clock clock) {
        super(config, clock);

        sessionPool = new SessionPool(config.getIp(), config.getPort(), config.getUser(), config.getPassword(), 1);

        // Prepare a metric for here...
        start(new NamedThreadFactory("iotdb-metrics-publisher"));
    }

    @Override
    protected <T> Gauge newGauge(Meter.Id id, T obj, ToDoubleFunction<T> valueFunction) {
        return new DefaultGauge<>(id, obj, valueFunction);
    }

    @Override
    protected Counter newCounter(Meter.Id id) {
        return new CumulativeCounter(id);
    }

    @Override
    protected LongTaskTimer newLongTaskTimer(Meter.Id id) {
        return new DefaultLongTaskTimer(id, clock);
    }

    @Override
    protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, PauseDetector pauseDetector) {
        return new CumulativeTimer(id, clock, distributionStatisticConfig, pauseDetector, TimeUnit.MILLISECONDS);
    }

    @Override
    protected DistributionSummary newDistributionSummary(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, double scale) {
        return new CumulativeDistributionSummary(id, clock, distributionStatisticConfig, scale, true);
    }

    @Override
    protected Meter newMeter(Meter.Id id, Meter.Type type, Iterable<Measurement> measurements) {
        return new DefaultMeter(id, type, measurements);
    }

    @Override
    protected <T> FunctionTimer newFunctionTimer(Meter.Id id, T obj, ToLongFunction<T> countFunction, ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnit) {
        return new CumulativeFunctionTimer<T>(id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnit, TimeUnit.MILLISECONDS);
    }

    @Override
    protected <T> FunctionCounter newFunctionCounter(Meter.Id id, T obj, ToDoubleFunction<T> countFunction) {
        return new CumulativeFunctionCounter<T>(id, obj, countFunction);
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Override
    protected DistributionStatisticConfig defaultHistogramConfig() {
        return DistributionStatisticConfig.DEFAULT;
    }


    @Override
    protected void publish() {
        Metrics.timer("iotdb.metrics.write.timer").record(this::writeMetrics);
    }

    @Override
    public void stop() {
        super.stop();
        sessionPool.close();
    }

    private void writeMetrics() {
        for (Meter meter : getMeters()) {

            // Add this to an IoTDB Timeseries now
            final String conventionName = meter.getId().getConventionName(NamingConvention.dot);
            final List<Tag> conventionTags = meter.getId().getConventionTags(NamingConvention.dot);

            // Now we add this as a timeseries
            Map<String, String> metrics = meter.match(
                    g -> createMetrics(g.value()),
                    c -> createMetrics(c.count()),
                    this::createMetrics,
                    a -> {
                        throw new NotImplementedException("");
                    },
                    this::createMetrics,
                    tg -> createMetrics(tg.value(TimeUnit.MILLISECONDS)),
                    fc -> createMetrics(fc.count()),
                    a -> {
                        throw new NotImplementedException("");
                    },
                    a -> {
                        throw new NotImplementedException("");
                    }
            );

            ArrayList<String> measurements = new ArrayList<>();
            ArrayList<String> values = new ArrayList<>();

            // Add Metrics
            for (Map.Entry<String, String> entry : metrics.entrySet()) {
                measurements.add(entry.getKey());
                values.add(entry.getValue());
            }

            // Add Tags as other measurements
            for (Tag tag : conventionTags) {
                measurements.add(tag.getKey());
                values.add(tag.getValue());
            }

            try {
                sessionPool.insertRecord(String.format("root._metrics.%s", escapeName(conventionName)), Instant.now().toEpochMilli(), measurements, values);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.debug("Unable to store metric {}", conventionName, e);
            }
        }
    }

    private Map<String, String> createMetrics(double value) {
        Map<String, String> metrics = new HashMap<>();

        metrics.put("value", Double.toString(value));

        return metrics;
    }

    private Map<String, String> createMetrics(Timer timer) {
        Map<String, String> metrics = new HashMap<>();

        metrics.put("_count", String.valueOf(timer.count()));
        metrics.put("_mean", String.valueOf(timer.mean(TimeUnit.MILLISECONDS)));
        metrics.put("_max", String.valueOf(timer.max(TimeUnit.MILLISECONDS)));
        metrics.put("_total", String.valueOf(timer.totalTime(TimeUnit.MILLISECONDS)));

        return metrics;
    }

    private Map<String, String> createMetrics(LongTaskTimer timer) {
        Map<String, String> metrics = new HashMap<>();

        metrics.put("_active", String.valueOf(timer.activeTasks()));
        metrics.put("_duration", String.valueOf(timer.duration(TimeUnit.MILLISECONDS)));

        return metrics;
    }

    private String escapeName(String conventionName) {
        return conventionName
                .replace("load", "_load")
                .replace("count", "_count")
                .replace("time", "_time")
                .replace("storage", "_storage")
                .replace("insert", "_insert");
    }

}
