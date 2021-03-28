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

import org.apache.iotdb.metrics.MetricManager;
import org.apache.iotdb.metrics.MetricReporter;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.ReporterType;

import com.codahale.metrics.jmx.JmxReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DropwizardMetricReporter implements MetricReporter {
  private static final Logger logger = LoggerFactory.getLogger(DropwizardMetricReporter.class);
  private MetricManager dropwizardMetricManager;
  private final MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();

  private JmxReporter jmxReporter;

  @Override
  public boolean start() {
    List<String> reporters = metricConfig.getMetricReporterList();
    for (String reporter : reporters) {
      switch (ReporterType.get(reporter)) {
        case JMX:
          startJmxReporter();
          break;
        case IOTDB:
          break;
        case PROMETHEUS:
          break;
        default:
          logger.warn("Dropwizard don't support reporter type {}", reporter);
      }
    }
    return false;
  }

  private void startJmxReporter() {
    jmxReporter =
        JmxReporter.forRegistry(
                ((DropwizardMetricManager) dropwizardMetricManager).getMetricRegistry())
            .build();
    jmxReporter.start();
  }

  @Override
  public void setMetricManager(MetricManager metricManager) {
    dropwizardMetricManager = metricManager;
  }

  @Override
  public boolean stop() {
    List<String> reporters = metricConfig.getMetricReporterList();
    for (String reporter : reporters) {
      switch (ReporterType.get(reporter)) {
        case JMX:
          stopJmxReporter(jmxReporter);
          break;
        case IOTDB:
          break;
        case PROMETHEUS:
          break;
        default:
          logger.warn("Dropwizard don't support reporter type {}", reporter);
      }
    }
    return true;
  }

  private void stopJmxReporter(JmxReporter jmxReporter) {
    if (jmxReporter != null) {
      jmxReporter.stop();
    }
  }

  @Override
  public String getName() {
    return "DropwizardMetricReporter";
  }
}
