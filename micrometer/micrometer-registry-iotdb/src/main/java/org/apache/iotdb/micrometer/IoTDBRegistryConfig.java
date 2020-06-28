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

import io.micrometer.core.instrument.push.PushRegistryConfig;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@SuppressWarnings({"java:S1214", "squid:S1214"}) // Structure is defined by Micrometer
public interface IoTDBRegistryConfig extends PushRegistryConfig {

    IoTDBRegistryConfig DEFAULT = new DefaultIoTDBRegistryConfig();

    @Override
    default String prefix() {
        return "iotdb";
    }

    @Override
    default Duration step() {
        return Duration.of(1, ChronoUnit.SECONDS);
    }

    String getIp();

    int getPort();

    String getUser();

    String getPassword();

    class DefaultIoTDBRegistryConfig implements IoTDBRegistryConfig {
        @Override
        public String getIp() {
            return "localhost";
        }

        @Override
        public int getPort() {
            return 6667;
        }

        @Override
        public String getUser() {
            return "root";
        }

        @Override
        public String getPassword() {
            return "root";
        }

        @Override
        public String get(String s) {
            return null;
        }
    }
}
