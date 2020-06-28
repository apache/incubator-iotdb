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

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.IMetricRegistry;

public class PrometheusMeterRegistryWrapper implements IMetricRegistry {
  private HttpServer server;
  PrometheusMeterRegistry prometheusMeterRegistry;
  @Override
  public MeterRegistry registry() {
    prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    //registries.add(prometheusMeterRegistry);
    // Serve an Endpoint for prometheus
    int port = IoTDBDescriptor.getInstance().getConfig().getMetricsPrometheusPort();
    String endpoint = IoTDBDescriptor.getInstance().getConfig().getMetricsPrometheusEndpoint();
    try {
      server = HttpServer.create(new InetSocketAddress(port), 0);
      server.createContext(endpoint, httpExchange -> {
        String response = prometheusMeterRegistry.scrape();
        httpExchange.sendResponseHeaders(200, response.getBytes().length);
        try (OutputStream os = httpExchange.getResponseBody()) {
          os.write(response.getBytes());
        }
      });
    } catch (IOException e) {
      throw new IllegalStateException("Unable to start server", e);
    }
    return prometheusMeterRegistry;
  }

  @Override
  public void start() {
    new Thread(server::start, "PrometheusMeterRegisteryServer").start();
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop(0);
    }
  }
}
