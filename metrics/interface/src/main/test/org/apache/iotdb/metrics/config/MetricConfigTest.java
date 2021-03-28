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

import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class MetricConfigTest {

  @Test
  public void yamlConfigTest() {
    String url = this.getClass().getClassLoader().getResource("iotdb-metric.yml").getPath();
    System.out.println(url);

    MetricConfig metricConfig = MetricConfigDescriptor.getInstance().getMetricConfig();
    Constructor constructor = new Constructor(MetricConfig.class);
    Yaml yaml = new Yaml(constructor);
    if (url != null) {
      try (InputStream inputStream = new FileInputStream(new File(url))) {
        metricConfig = (MetricConfig) yaml.load(inputStream);
      } catch (IOException e) {
        Assert.fail();
      }
    }

    List<String> lists = metricConfig.getMetricReporterList();

    Assert.assertEquals(lists.size(), 2);
    Assert.assertEquals(metricConfig.getPrometheusReporterConfig().getPrometheusExporterPort(), "80990");
    Assert.assertEquals(metricConfig.getIoTDBReporterConfig().getIotdbIp(), "0.0.0.0");
  }
}
