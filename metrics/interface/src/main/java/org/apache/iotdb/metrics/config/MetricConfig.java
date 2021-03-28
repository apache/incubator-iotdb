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

package org.apache.iotdb.metrics.config;

import java.util.Arrays;
import java.util.List;

public class MetricConfig {
  static final String CONFIG_NAME = "iotdb-metric.yml";

  /** enable publishing data. */
  private Boolean enableMetric = true;

  /** The period of data pushed by the reporter to the remote monitoring system. */
  private Integer pushPeriodInSecond = 5;

  private String metricManagerType = "MicrometerMetricManager";
  private String metricReporterType = "MicrometerMetricReporter";

  /** provide or push metric data to remote system, could be jmx, prometheus, iotdb, etc. */
  private List<String> metricReporterList = Arrays.asList("jmx");

  private PrometheusReporterConfig prometheusReporterConfig;
  private IotdbReporterConfig iotdbReporterConfig;

  public Boolean getEnableMetric() {
    return enableMetric;
  }

  public void setEnableMetric(Boolean enableMetric) {
    this.enableMetric = enableMetric;
  }

  public Integer getPushPeriodInSecond() {
    return pushPeriodInSecond;
  }

  public void setPushPeriodInSecond(Integer pushPeriodInSecond) {
    this.pushPeriodInSecond = pushPeriodInSecond;
  }

  public String getMetricManagerType() {
    return metricManagerType;
  }

  public void setMetricManagerType(String metricManagerType) {
    this.metricManagerType = metricManagerType;
  }

  public String getMetricReporterType() {
    return metricReporterType;
  }

  public void setMetricReporterType(String metricReporterType) {
    this.metricReporterType = metricReporterType;
  }

  public List<String> getMetricReporterList() {
    return metricReporterList;
  }

  public void setMetricReporterList(List<String> metricReporterList) {
    this.metricReporterList = metricReporterList;
  }

  public PrometheusReporterConfig getPrometheusReporterConfig() {
    return prometheusReporterConfig;
  }

  public void setPrometheusReporterConfig(PrometheusReporterConfig prometheusReporterConfig) {
    this.prometheusReporterConfig = prometheusReporterConfig;
  }

  public IotdbReporterConfig getIotdbReporterConfig() {
    return iotdbReporterConfig;
  }

  public void setIotdbReporterConfig(IotdbReporterConfig iotdbReporterConfig) {
    this.iotdbReporterConfig = iotdbReporterConfig;
  }

  /** the following is prometheus related config. */
  public static class PrometheusReporterConfig {
    /** the http server's port for prometheus exporter to get metric data. */
    private String prometheusExporterPort = "8090";

    public String getPrometheusExporterPort() {
      return prometheusExporterPort;
    }

    public void setPrometheusExporterPort(String prometheusExporterPort) {
      this.prometheusExporterPort = prometheusExporterPort;
    }
  }

  /** the following is iotdb related config. */
  public static class IotdbReporterConfig {
    private String iotdbSg = "_sysmetric";
    private String iotdbUser = "root";
    private String iotdbPw = "root";
    private String iotdbIp = "127.0.0.1";
    private String iotdbPort = "6667";

    public String getIotdbSg() {
      return iotdbSg;
    }

    public void setIotdbSg(String iotdbSg) {
      this.iotdbSg = iotdbSg;
    }

    public String getIotdbUser() {
      return iotdbUser;
    }

    public void setIotdbUser(String iotdbUser) {
      this.iotdbUser = iotdbUser;
    }

    public String getIotdbPw() {
      return iotdbPw;
    }

    public void setIotdbPw(String iotdbPw) {
      this.iotdbPw = iotdbPw;
    }

    public String getIotdbIp() {
      return iotdbIp;
    }

    public void setIotdbIp(String iotdbIp) {
      this.iotdbIp = iotdbIp;
    }

    public String getIotdbPort() {
      return iotdbPort;
    }

    public void setIotdbPort(String iotdbPort) {
      this.iotdbPort = iotdbPort;
    }
  }
}
