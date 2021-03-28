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

package org.apache.iotdb.metrics.micrometer;

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricReporter;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.ReporterType;

import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.List;

public class MicrometerMetricReporter implements MetricReporter {
  private static final Logger logger = LoggerFactory.getLogger(MicrometerMetricReporter.class);
  private MetricManager micrometerMetricManager;
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
  private Thread runThread;

  @Override
  public boolean start() {

    List<String> reporters = metricConfig.getMetricReporterList();
    for (String reporter : reporters) {
      switch (ReporterType.get(reporter)) {
        case JMX:
          startJmxReporter(
              ((MicrometerMetricManager) micrometerMetricManager).getJmxMeterRegistry());
          break;
        case IOTDB:
          break;
        case PROMETHEUS:
          startPrometheusReporter(
              ((MicrometerMetricManager) micrometerMetricManager).getPrometheusMeterRegistry());
          break;
        default:
          logger.warn("Dropwizard don't support reporter type {}", reporter);
      }
    }

    return true;
  }

  private void startPrometheusReporter(PrometheusMeterRegistry prometheusMeterRegistry) {
    DisposableServer server =
        HttpServer.create()
            .port(
                Integer.parseInt(
                    metricConfig.getPrometheusReporterConfig().getPrometheusExporterPort()))
            .route(
                routes ->
                    routes.get(
                        "/prometheus",
                        (request, response) ->
                            response.sendString(Mono.just(prometheusMeterRegistry.scrape()))))
            .bindNow();

    runThread = new Thread(server::onDispose);
    runThread.start();
  }

  private void startJmxReporter(JmxMeterRegistry jmxMeterRegistry) {
    logger.info("start jmx reporter from micrometer {}", jmxMeterRegistry);
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    micrometerMetricManager = metricManager;
  }

  @Override
  public boolean stop() {
    List<String> reporters = metricConfig.getMetricReporterList();
    for (String reporter : reporters) {
      switch (ReporterType.get(reporter)) {
        case JMX:
          stopJmxReporter(
              ((MicrometerMetricManager) micrometerMetricManager).getJmxMeterRegistry());
          break;
        case IOTDB:
          break;
        case PROMETHEUS:
          stopPrometheusReporter();
          break;
        default:
          logger.warn("Dropwizard don't support reporter type {}", reporter);
      }
    }
    return true;
  }

  private void stopPrometheusReporter() {
    try {
      // stop prometheus reporter
      if (runThread != null) {
        runThread.join();
      }
    } catch (InterruptedException e) {
      logger.warn("Failed to stop prometheus reporter", e);
      Thread.currentThread().interrupt();
    }
  }

  private void stopJmxReporter(JmxMeterRegistry jmxMeterRegistry) {
    if (jmxMeterRegistry != null) {
      jmxMeterRegistry.stop();
    }
  }

  @Override
  public String getName() {
    return "MicrometerMetricReporter";
  }
}
