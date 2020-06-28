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

package org.apache.iotdb.db.service;

import io.github.mweirauch.micrometer.jvm.extras.ProcessMemoryMetrics;
import io.github.mweirauch.micrometer.jvm.extras.ProcessThreadMetrics;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.service.metrics.IMetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics Service that is based on micrometer.
 * It does two things.
 * First, exposes a Prometheus Endpoint on :8080/metrics.
 * Second, it logs all collected metrics into IoTDB in the storage group root._metrics.
 */
// The warning about com.sun.* package can be ignored
// See command here https://stackoverflow.com/questions/3732109/simple-http-server-in-java-using-only-java-se-api
@SuppressWarnings({"squid:S1191", "java:S1191"})
public class MetricsService implements IService {
    private static Logger logger = LoggerFactory.getLogger(MetricsService.class);

    private static final MetricsService INSTANCE = new MetricsService();

    private List<IMetricRegistry> registryWrappers = new ArrayList<>();
    private JvmGcMetrics jvmGcMetrics;
    private JmxMeterRegistry jmxMeterRegistry;

    public MetricsService() {
        // Do nothing
    }

    public static IService getInstance() {
        return INSTANCE;
    }

    @Override
    public void start() throws StartupException {
        // Define Meter Registry
        List<MeterRegistry> registries = new ArrayList<>();
        if (IoTDBDescriptor.getInstance().getConfig().isEnableIotDBMetricsStorage()) {
            addRegister("org.apache.iotdb.micrometer.IoTDBMeterRegistryWrapper", registries);
        }
        if (IoTDBDescriptor.getInstance().getConfig().isEnablePrometheusMetricsEndpoint()) {
            addRegister("org.apache.iotdb.micrometer.PrometheusMeterRegistryWrapper", registries);
        }
        //by default, we allow JMXRegistry
        jmxMeterRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
        registries.add(jmxMeterRegistry);
        MeterRegistry registry = new CompositeMeterRegistry(Clock.SYSTEM,
                registries);
        // Set this as default, then users can simply write Metrics.xxx
        Metrics.addRegistry(registry);
        // Wire up JVM and Other Default Bindings
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        jvmGcMetrics = new JvmGcMetrics();
        jvmGcMetrics.bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessMemoryMetrics().bindTo(registry);
        new ProcessThreadMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
        new FileDescriptorMetrics().bindTo(registry);
    }

    /**
     * initialize the registerClass and put it into registers
     * @param registerClassName
     * @param registries
     */
    private void addRegister(String registerClassName, List<MeterRegistry> registries) {
        Class clazz = null;
        try {
            clazz = Class.forName(registerClassName);
            IMetricRegistry wrapper = (IMetricRegistry) clazz.newInstance();
            registries.add(wrapper.registry());
            registryWrappers.add(wrapper);
            wrapper.start();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            logger.error("load micrometer registry {} failed", registerClassName, e);
        }
    }

    @Override
    public void stop() {
        jvmGcMetrics.close();
        for (IMetricRegistry wrapper: registryWrappers) {
            wrapper.stop();
        }
        //jmxMeterRegistry.stop();
        Metrics.globalRegistry.close();
        Metrics.globalRegistry.getRegistries().forEach(Metrics::removeRegistry);

    }

    @Override
    public ServiceType getID() {
        return ServiceType.METRICS2_SERVICE;
    }
}
