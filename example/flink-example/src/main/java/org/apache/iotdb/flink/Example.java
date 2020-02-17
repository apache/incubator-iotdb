/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.flink;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Example {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);

        IoTDBOptions options = new IoTDBOptions();
        options.setHost("127.0.0.1");
        options.setPort(6667);
        options.setUser("root");
        options.setPassword("root");
        options.setStorageGroup("root.sg");
        options.setTimeseries(Lists.newArrayList("root.sg.d1.s1"));

        IoTSerializationSchema serializationSchema = new DefaultIoTSerializationSchema();
        IoTDBSink ioTDBSink = new IoTDBSink(options, serializationSchema)
                // enable batching
                .withBatchFlushOnCheckpoint(true)
                .withBatchSize(10)
                ;

        env.addSource(new SensorSource())
                .name("sensor-source")
                .setParallelism(1)

                .addSink(ioTDBSink)
                .name("iotdb-sink")
                .setParallelism(1)
        ;

        try {
            env.execute("iotdb-flink-example");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class SensorSource implements SourceFunction<Map<String,String>> {
        boolean running = true;

        @Override
        public void run(SourceContext context) throws Exception {
            Random random = new Random();
            while (running) {
                Map tuple = new HashMap();
                tuple.put("device", "root.sg.d1");
                tuple.put("timestamp", String.valueOf(System.currentTimeMillis()));
                tuple.put("measurement", "s1");
                tuple.put("value", String.valueOf(random.nextDouble()));

                context.collect(tuple);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
