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
package org.apache.iotdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class SessionExample {

  private static Session session;
  private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  private static final String ROOT_SG1_D1_S2 = "root.sg1.d1.s2";
  private static final String ROOT_SG1_D1_S3 = "root.sg1.d1.s3";
  private static final String ROOT_SG1_D1_S4 = "root.sg1.d1.s4";
  private static final String ROOT_SG1_D1_S5 = "root.sg1.d1.s5";
  private static final String ROOT_SG1_D1 = "root.sg1.d1";


  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, ExecutionException, InterruptedException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    //set session fetchSize
    session.setFetchSize(10000);

    try {
      session.setStorageGroup("root.sg1");
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
        throw e;
      }
    }

    createTimeseries();
    createMultiTimeseries();
    insertRecord();
    asyncInsertRecord();
    insertRecords();
    asyncInsertRecords();
    insertTablet();
    asyncInsertTablet();
    insertTablets();
    asyncInsertTablets();
    nonQuery();
    query();
    queryWithTimeout();
    rawDataQuery();
    queryByIterator();
    deleteData();
    deleteTimeseries();
    setTimeout();
    session.close();
  }

  private static void createTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
      session.createTimeseries(ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S2)) {
      session.createTimeseries(ROOT_SG1_D1_S2, TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S3)) {
      session.createTimeseries(ROOT_SG1_D1_S3, TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    }

    // create timeseries with tags and attributes
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S4)) {
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "v1");
      Map<String, String> attributes = new HashMap<>();
      tags.put("description", "v1");
      session.createTimeseries(ROOT_SG1_D1_S4, TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY, null, tags, attributes, "temperature");
    }

    // create timeseries with SDT property, SDT will take place when flushing
    if (!session.checkTimeseriesExists(ROOT_SG1_D1_S5)) {
      // COMPDEV is required
      // COMPMAXTIME and COMPMINTIME are optional and their unit is ms
      Map<String, String> props = new HashMap<>();
      props.put("LOSS", "sdt");
      props.put("COMPDEV", "0.01");
      props.put("COMPMINTIME", "2");
      props.put("COMPMAXTIME", "10");
      session.createTimeseries(ROOT_SG1_D1_S5, TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY, props, null, null, null);
    }
  }

  private static void createMultiTimeseries()
      throws IoTDBConnectionException, BatchExecutionException, StatementExecutionException {

    if (!session.checkTimeseriesExists("root.sg1.d2.s1") && !session
        .checkTimeseriesExists("root.sg1.d2.s2")) {
      List<String> paths = new ArrayList<>();
      paths.add("root.sg1.d2.s1");
      paths.add("root.sg1.d2.s2");
      List<TSDataType> tsDataTypes = new ArrayList<>();
      tsDataTypes.add(TSDataType.INT64);
      tsDataTypes.add(TSDataType.INT64);
      List<TSEncoding> tsEncodings = new ArrayList<>();
      tsEncodings.add(TSEncoding.RLE);
      tsEncodings.add(TSEncoding.RLE);
      List<CompressionType> compressionTypes = new ArrayList<>();
      compressionTypes.add(CompressionType.SNAPPY);
      compressionTypes.add(CompressionType.SNAPPY);

      List<Map<String, String>> tagsList = new ArrayList<>();
      Map<String, String> tags = new HashMap<>();
      tags.put("unit", "kg");
      tagsList.add(tags);
      tagsList.add(tags);

      List<Map<String, String>> attributesList = new ArrayList<>();
      Map<String, String> attributes = new HashMap<>();
      attributes.put("minValue", "1");
      attributes.put("maxValue", "100");
      attributesList.add(attributes);
      attributesList.add(attributes);

      List<String> alias = new ArrayList<>();
      alias.add("weight1");
      alias.add("weight2");

      session
          .createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList,
              attributesList, alias);
    }
  }

  private static void insertRecord() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
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
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  private static void asyncInsertRecord() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    for (long time = 100; time < 200; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      session.asyncInsertRecord(deviceId, time, measurements, types, values,
          1000, SessionExample::insertRecordAsyncCallback);
    }
  }

  private static void insertRecordAsyncCallback(
      String deviceId, long time, List<String> measurements, List<TSDataType> types,
      List<Object> values, Throwable e) {
    System.out.println("InsertRecord() failed, deviceId " + deviceId + " with " +
        measurements.size() + " measurements, time " + time + ":" + e.getMessage());
  }

  private static void insertStrRecord()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");

    for (long time = 0; time < 10; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insertRecord(deviceId, time, measurements, values);
    }
  }

  private static void insertRecordInObject()
      throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      session.insertRecord(deviceId, time, measurements, types, 1L, 1L, 1L);
    }
  }

  private static void insertRecords() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }
    session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
  }

  private static void asyncInsertRecords() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = ROOT_SG1_D1;
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    for (long time = 500; time < 1000; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.asyncInsertRecords(deviceIds, timestamps, measurementsList,
            typesList, valuesList,1000, null);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.asyncInsertRecords(deviceIds, timestamps, measurementsList, typesList,
        valuesList, 1000, SessionExample::insertRecordsAsyncCallback);
  }

  private static void insertRecordsAsyncCallback(
      List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,
      List<List<TSDataType>> typesList, List<List<Object>> valuesList, Throwable e) {
    System.out.println("InsertRecords() failed when inserting " + times.size() + " records:" + e.getMessage());
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * a Tablet example:
   *
   *      device1
   * time s1, s2, s3
   * 1,   1,  1,  1
   * 2,   2,  2,  2
   * 3,   3,  3,  3
   *
   * Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   */
  private static void insertTablet() throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 100);

    //Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    //Method 2 to add tablet data
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void asyncInsertTablet() {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 100);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    for (long time = 100; time < 200; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.asyncInsertTablet(tablet, true, 1000, null);
        tablet.reset();
      }
    }
  }

  private static void insertTablets() throws IoTDBConnectionException, StatementExecutionException,
      ExecutionException, InterruptedException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet1 = new Tablet(ROOT_SG1_D1, schemaList, 100);
    Tablet tablet2 = new Tablet("root.sg1.d2", schemaList, 100);
    Tablet tablet3 = new Tablet("root.sg1.d3", schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(ROOT_SG1_D1, tablet1);
    tabletMap.put("root.sg1.d2", tablet2);
    tabletMap.put("root.sg1.d3", tablet3);

    //Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);
      tablet3.addTimestamp(row3, timestamp);
      for (int i = 0; i < 3; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
        tablet3.addValue(schemaList.get(i).getMeasurementId(), row3, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
      timestamp++;
    }

    if (tablet1.rowSize != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }

    //Method 2 to add tablet data
    long[] timestamps1 = tablet1.timestamps;
    Object[] values1 = tablet1.values;
    long[] timestamps2 = tablet2.timestamps;
    Object[] values2 = tablet2.values;
    long[] timestamps3 = tablet3.timestamps;
    Object[] values3 = tablet3.values;

    for (long time = 0; time < 100; time++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      timestamps1[row1] = time;
      timestamps2[row2] = time;
      timestamps3[row3] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor1 = (long[]) values1[i];
        sensor1[row1] = i;
        long[] sensor2 = (long[]) values2[i];
        sensor2[row2] = i;
        long[] sensor3 = (long[]) values3[i];
        sensor3[row3] = i;
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        session.insertTablets(tabletMap, true);

        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
    }

    if (tablet1.rowSize != 0) {
      session.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
      tablet3.reset();
    }
  }

  private static void asyncInsertTablets() throws ExecutionException, InterruptedException {
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet1 = new Tablet(ROOT_SG1_D1, schemaList, 100);
    Tablet tablet2 = new Tablet("root.sg1.d2", schemaList, 100);
    Tablet tablet3 = new Tablet("root.sg1.d3", schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(ROOT_SG1_D1, tablet1);
    tabletMap.put("root.sg1.d2", tablet2);
    tabletMap.put("root.sg1.d3", tablet3);
    long[] timestamps1 = tablet1.timestamps;
    Object[] values1 = tablet1.values;
    long[] timestamps2 = tablet2.timestamps;
    Object[] values2 = tablet2.values;
    long[] timestamps3 = tablet3.timestamps;
    Object[] values3 = tablet3.values;

    for (long time = 100; time < 200; time++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      int row3 = tablet3.rowSize++;
      timestamps1[row1] = time;
      timestamps2[row2] = time;
      timestamps3[row3] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor1 = (long[]) values1[i];
        sensor1[row1] = i;
        long[] sensor2 = (long[]) values2[i];
        sensor2[row2] = i;
        long[] sensor3 = (long[]) values3[i];
        sensor3[row3] = i;
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        Future<Integer> future =
            session.asyncInsertTablets(tabletMap, true, 1000, null);
        future.get();

        tablet1.reset();
        tablet2.reset();
        tablet3.reset();
      }
    }
  }

  private static void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    String path = ROOT_SG1_D1_S1;
    long deleteTime = 99;
    session.deleteData(path, deleteTime);
  }

  private static void deleteTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);
    session.deleteTimeseries(paths);
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void queryWithTimeout()
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1", 2000);
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  private static void rawDataQuery() throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(ROOT_SG1_D1_S1);
    paths.add(ROOT_SG1_D1_S2);
    paths.add(ROOT_SG1_D1_S3);
    long startTime = 10L;
    long endTime = 200L;

    SessionDataSet dataSet = session.executeRawDataQuery(paths, startTime, endTime);
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024);
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    dataSet.closeOperationHandle();
  }

  private static void queryByIterator()
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    DataIterator iterator = dataSet.iterator();
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (iterator.next()) {
      StringBuilder builder = new StringBuilder();
      // get time
      builder.append(iterator.getLong(1)).append(",");
      // get second column
      if (!iterator.isNull(2)) {
        builder.append(iterator.getLong(2)).append(",");
      } else {
        builder.append("null").append(",");
      }

      // get third column
      if (!iterator.isNull(ROOT_SG1_D1_S2)) {
        builder.append(iterator.getLong(ROOT_SG1_D1_S2)).append(",");
      } else {
        builder.append("null").append(",");
      }

      // get forth column
      if (!iterator.isNull(4)) {
        builder.append(iterator.getLong(4)).append(",");
      } else {
        builder.append("null").append(",");
      }

      // get fifth column
      if (!iterator.isNull(ROOT_SG1_D1_S4)) {
        builder.append(iterator.getObject(ROOT_SG1_D1_S4));
      } else {
        builder.append("null");
      }

      System.out.println(builder.toString());
    }

    dataSet.closeOperationHandle();
  }

  private static void nonQuery() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement("insert into root.sg1.d1(timestamp,s1) values(200, 1)");
  }

  private static void setTimeout() throws StatementExecutionException {
    Session tempSession = new Session("127.0.0.1", 6667, "root", "root", 10000, 20000);
    tempSession.setTimeout(60000);
  }
}
