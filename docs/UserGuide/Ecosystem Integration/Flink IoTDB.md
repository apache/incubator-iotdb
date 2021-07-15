<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## Flink-IoTDB-Connector 

IoTDB integration for [Apache Flink](https://flink.apache.org/). This module includes the IoTDB sink that allows a flink job to write events into timeseries, and the IoTDB source allowing reading data from IoTDB.

### IoTDBSink

To use the `IoTDBSink`,  you need construct an instance of it by specifying `IoTDBSinkOptions` and `IoTSerializationSchema` instances.
The `IoTDBSink` send only one event after another by default, but you can change to batch by invoking `withBatchSize(int)`. 

#### Example

This example shows a case that sends data to a IoTDB server from a Flink job:

- A simulated Source `SensorSource` generates data points per 1 second.
- Flink uses `IoTDBSink` to consume the generated data points and write the data into IoTDB.

It is noteworthy that to use IoTDBSink, schema auto-creation in IoTDB should be enabled. 

```java
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class FlinkIoTDBSink {
  public static void main(String[] args) throws Exception {
    // run the flink job on local mini cluster
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    IoTDBSinkOptions options = new IoTDBSinkOptions();
    options.setHost("127.0.0.1");
    options.setPort(6667);
    options.setUser("root");
    options.setPassword("root");

    // If the server enables auto_create_schema, then we do not need to register all timeseries
    // here.
    options.setTimeseriesOptionList(
        Lists.newArrayList(
            new IoTDBSinkOptions.TimeseriesOption(
                "root.sg.d1.s1", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY)));

    IoTSerializationSchema serializationSchema = new DefaultIoTSerializationSchema();
    IoTDBSink ioTDBSink =
        new IoTDBSink(options, serializationSchema)
            // enable batching
            .withBatchSize(10)
            // how many connectons to the server will be created for each parallelism
            .withSessionPoolSize(3);

    env.addSource(new SensorSource())
        .name("sensor-source")
        .setParallelism(1)
        .addSink(ioTDBSink)
        .name("iotdb-sink");

    env.execute("iotdb-flink-example");
  }

  private static class SensorSource implements SourceFunction<Map<String, String>> {
    boolean running = true;
    Random random = new SecureRandom();

    @Override
    public void run(SourceContext context) throws Exception {
      while (running) {
        Map<String, String> tuple = new HashMap();
        tuple.put("device", "root.sg.d1");
        tuple.put("timestamp", String.valueOf(System.currentTimeMillis()));
        tuple.put("measurements", "s1");
        tuple.put("types", "DOUBLE");
        tuple.put("values", String.valueOf(random.nextDouble()));

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

```

#### Usage

* Launch the IoTDB server.
* Run `org.apache.iotdb.flink.FlinkIoTDBSink.java` to run the flink job on local mini cluster.

### IoTDBSource
To use the `IoTDBSource`, you need to construct an instance of `IoTDBSource` by specifying `IoTDBSourceOptions`
and implementing the abstract method `convert()` in `IoTDBSource`. The `convert` methods defines how 
you want the row data to be transformed.

#### Example
This example shows a case where data are read from IoTDB.
```java
import org.apache.iotdb.flink.options.IoTDBSourceOptions;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class FlinkIoTDBSource {

  static final String LOCAL_HOST = "127.0.0.1";
  static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  static final String ROOT_SG1_D1 = "root.sg1.d1";

  public static void main(String[] args) throws Exception {
    prepareData();

    // run the flink job on local mini cluster
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    IoTDBSourceOptions ioTDBSourceOptions =
        new IoTDBSourceOptions("127.0.0.1", 6667, "root", "root",
            "select s1 from " + ROOT_SG1_D1 + " align by device");

    env.addSource(
        new IoTDBSource<RowRecord>(ioTDBSourceOptions) {
          @Override
          public RowRecord convert(RowRecord rowRecord) {
            return rowRecord;
          }
        })
        .name("sensor-source")
        .print()
        .setParallelism(2);
    env.execute();
  }

  /**
   * Write some data to IoTDB
   */
  private static void prepareData() throws IoTDBConnectionException, StatementExecutionException {
    Session session = new Session(LOCAL_HOST, 6667, "root", "root");
    session.open(false);
    try {
      session.setStorageGroup("root.sg1");
      if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
        session.createTimeseries(
            ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        measurements.add("s1");
        measurements.add("s2");
        measurements.add("s3");
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);

        for (long time = 0; time < 100; time++) {
          List<Object> values = new ArrayList<>();
          values.add(1L);
          values.add(2L);
          values.add(3L);
          session.insertRecord(ROOT_SG1_D1, time, measurements, types, values);
        }
      }
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
        throw e;
      }
    }
  }
}
```

#### Usage
Launch the IoTDB server.
Run org.apache.iotdb.flink.FlinkIoTDBSource.java to run the flink job on local mini cluster.

