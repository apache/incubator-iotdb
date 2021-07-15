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

import org.apache.iotdb.flink.options.IoTDBSinkOptions;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.common.base.Preconditions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The `IoTDBSink` allows flink jobs to write events into IoTDB timeseries. By default send only one
 * event after another, but you can change to batch by invoking `withBatchSize(int)`.
 *
 * @param <IN> the input data type
 */
public class IoTDBSink<IN> extends RichSinkFunction<IN> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(IoTDBSink.class);

  private IoTDBSinkOptions options;
  private IoTSerializationSchema<IN> serializationSchema;
  private Map<String, IoTDBSinkOptions.TimeseriesOption> timeseriesOptionMap;
  private transient SessionPool pool;
  private transient ScheduledExecutorService scheduledExecutor;

  private int batchSize = 0;
  private int flushIntervalMs = 3000;
  private List<Event> batchList;
  private int sessionPoolSize = 2;

  public IoTDBSink(IoTDBSinkOptions options, IoTSerializationSchema<IN> schema) {
    this.options = options;
    this.serializationSchema = schema;
    this.batchList = new LinkedList<>();
    this.timeseriesOptionMap = new HashMap<>();
    for (IoTDBSinkOptions.TimeseriesOption timeseriesOption : options.getTimeseriesOptionList()) {
      timeseriesOptionMap.put(timeseriesOption.getPath(), timeseriesOption);
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    initSession();
    initScheduler();
  }

  void initSession() {
    pool =
        new SessionPool(
            options.getHost(),
            options.getPort(),
            options.getUser(),
            options.getPassword(),
            sessionPoolSize);
  }

  void initScheduler() {
    if (batchSize > 0) {
      scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
      scheduledExecutor.scheduleAtFixedRate(
          () -> {
            try {
              flush();
            } catch (Exception e) {
              LOG.error("flush error", e);
            }
          },
          flushIntervalMs,
          flushIntervalMs,
          TimeUnit.MILLISECONDS);
    }
  }

  //  for testing
  void setSessionPool(SessionPool pool) {
    this.pool = pool;
  }

  @Override
  public void invoke(IN input, Context context) throws Exception {
    Event event = serializationSchema.serialize(input);
    if (event == null) {
      return;
    }

    if (batchSize > 0) {
      synchronized (batchList) {
        batchList.add(event);
        if (batchList.size() >= batchSize) {
          flush();
        }
        return;
      }
    }

    convertText(event.getDevice(), event.getMeasurements(), event.getValues());
    pool.insertRecord(
        event.getDevice(),
        event.getTimestamp(),
        event.getMeasurements(),
        event.getTypes(),
        event.getValues());
    LOG.debug("send event successfully");
  }

  public IoTDBSink<IN> withBatchSize(int batchSize) {
    Preconditions.checkArgument(batchSize >= 0);
    this.batchSize = batchSize;
    return this;
  }

  public IoTDBSink<IN> withFlushIntervalMs(int flushIntervalMs) {
    Preconditions.checkArgument(flushIntervalMs > 0);
    this.flushIntervalMs = flushIntervalMs;
    return this;
  }

  public IoTDBSink<IN> withSessionPoolSize(int sessionPoolSize) {
    Preconditions.checkArgument(sessionPoolSize > 0);
    this.sessionPoolSize = sessionPoolSize;
    return this;
  }

  @Override
  public void close() {
    if (pool != null) {
      try {
        flush();
      } catch (Exception e) {
        LOG.error("flush error", e);
      }
      pool.close();
    }
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
    }
  }

  private void convertText(String device, List<String> measurements, List<Object> values) {
    if (device != null
        && measurements != null
        && values != null
        && measurements.size() == values.size()) {
      for (int i = 0; i < measurements.size(); i++) {
        String measurement = device + TsFileConstant.PATH_SEPARATOR + measurements.get(i);
        IoTDBSinkOptions.TimeseriesOption timeseriesOption = timeseriesOptionMap.get(measurement);
        if (timeseriesOption != null && TSDataType.TEXT.equals(timeseriesOption.getDataType())) {
          // The TEXT data type should be covered by " or '
          values.set(i, "'" + values.get(i) + "'");
        }
      }
    }
  }

  private void flush() throws Exception {
    if (batchSize > 0) {
      synchronized (batchList) {
        if (batchList.size() > 0) {
          List<String> deviceIds = new ArrayList<>();
          List<Long> timestamps = new ArrayList<>();
          List<List<String>> measurementsList = new ArrayList<>();
          List<List<TSDataType>> typesList = new ArrayList<>();
          List<List<Object>> valuesList = new ArrayList<>();

          for (Event event : batchList) {
            convertText(event.getDevice(), event.getMeasurements(), event.getValues());
            deviceIds.add(event.getDevice());
            timestamps.add(event.getTimestamp());
            measurementsList.add(event.getMeasurements());
            typesList.add(event.getTypes());
            valuesList.add(event.getValues());
          }
          pool.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
          LOG.debug("send event successfully");
          batchList.clear();
        }
      }
    }
  }
}
