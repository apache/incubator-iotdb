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
package org.apache.iotdb.session;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSessionIT {

  private static Logger logger = LoggerFactory.getLogger(IoTDBSessionIT.class);

  private Session session;

  @Before
  public void setUp() {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSortTablet() {
        /*
        To test sortTablet in Class Session
        !!!
        Before testing, change the sortTablet from private method to public method
        !!!
         */
    session = new Session("127.0.0.1", 6667, "root", "root");
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1",TSDataType.INT64, TSEncoding.RLE));
    // insert three rows data
    Tablet tablet = new Tablet("root.sg1.d1", schemaList, 3);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

        /*
        inorder data before inserting
        timestamp   s1
        2           0
        0           1
        1           2
         */
    // inorder timestamps
    timestamps[0] = 2;
    timestamps[1] = 0;
    timestamps[2] = 1;
    // just one column INT64 data
    long[] sensor = (long[]) values[0];
    sensor[0] = 0;
    sensor[1] = 1;
    sensor[2] = 2;
    tablet.rowSize = 3;

    session.sortTablet(tablet);

        /*
        After sorting, if the tablet data is sorted according to the timestamps,
        data in tablet will be
        timestamp   s1
        0           1
        1           2
        2           0

        If the data equal to above tablet, test pass, otherwise test fialed
         */
    long[] resTimestamps = tablet.timestamps;
    long[] resValues = (long[])tablet.values[0];
    long[] expectedTimestamps = new long[]{0, 1, 2};
    long[] expectedValues = new long[]{1,2,0};
    try {
      assertArrayEquals(expectedTimestamps, resTimestamps);
      assertArrayEquals(expectedValues, resValues);
    }
    catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testInsertByStrAndSelectFailedData() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";

    session.createTimeseries(deviceId + "s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    session.createTimeseries(deviceId + "s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    session.createTimeseries(deviceId + "s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));

    Tablet tablet = new Tablet("root.sg1.d1", schemaList, 10);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 10; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      long[] sensor = (long[]) values[0];
      sensor[row] = time;
      double[] sensor2 = (double[]) values[1];
      sensor2[row] = 0.1 + time;
      Binary[] sensor3 = (Binary[]) values[2];
      sensor3[row] = Binary.valueOf("ha" + time);
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      System.out.println(record.toString());
      assertEquals(i, record.getFields().get(1).getLongV());
      assertTrue(record.getFields().get(2).isNull());
      assertTrue(record.getFields().get(3).isNull());
      i++;
    }
  }


  @Test
  public void testInsertByStr() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();
    insertByStr();

    // sql test
    insert_via_sql();
    query3();

    session.close();

  }

  private void insertByStr() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");

    for (long time = 0; time < 100; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.insertRecord(deviceId, time, measurements, values);
    }
  }

  @Test
  public void testInsertByBlankStrAndInferType() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1 ");

    List<String> values = new ArrayList<>();
    values.add("1.0");
    session.insertRecord(deviceId, 1L, measurements, values);

    String[] expected = new String[]{"root.sg1.d1.s1 "};

    assertFalse(session.checkTimeseriesExists("root.sg1.d1.s1 "));
    SessionDataSet dataSet = session.executeQueryStatement("show timeseries");
    int i = 0;
    while (dataSet.hasNext()) {
      assertEquals(expected[i], dataSet.next().getFields().get(0).toString());
      i++;
    }

    session.close();
  }

  @Test
  public void testInsertByStrAndInferType() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    measurements.add("s4");

    List<String> values = new ArrayList<>();
    values.add("1");
    values.add("1.2");
    values.add("true");
    values.add("dad");
    session.insertRecord(deviceId, 1L, measurements, values);

    String[] expected = new String[]{
        IoTDBDescriptor.getInstance().getConfig().getIntegerStringInferType().name(),
        IoTDBDescriptor.getInstance().getConfig().getFloatingStringInferType().name(),
        IoTDBDescriptor.getInstance().getConfig().getBooleanStringInferType().name(),
        TSDataType.TEXT.name()
    };

    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    int i = 0;
    while (dataSet.hasNext()) {
      assertEquals(expected[i], dataSet.next().getFields().get(3).getStringValue());
      i++;
    }

    session.close();
  }

  @Test
  public void testInsertByObjAndNotInferType() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    measurements.add("s4");

    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.DOUBLE);
    dataTypes.add(TSDataType.TEXT);
    dataTypes.add(TSDataType.TEXT);

    List<Object> values = new ArrayList<>();
    values.add(1L);
    values.add(1.2d);
    values.add("true");
    values.add("dad");
    session.insertRecord(deviceId, 1L, measurements, dataTypes, values);

    String[] expected = new String[]{
        TSDataType.INT64.name(),
        TSDataType.DOUBLE.name(),
        TSDataType.TEXT.name(),
        TSDataType.TEXT.name()
    };

    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root");
    int i = 0;
    while (dataSet.hasNext()) {
      assertEquals(expected[i], dataSet.next().getFields().get(3).getStringValue());
      i++;
    }

    session.close();
  }

  @Test
  public void testInsertByObject() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();
    insertInObject();

    // sql test
    insert_via_sql();
    query3();

    session.close();
  }


  @Test
  public void testAlignByDevice() throws IoTDBConnectionException,
      StatementExecutionException, BatchExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertTabletTest2("root.sg1.d1");

    queryForAlignByDevice();
    queryForAlignByDevice2();
  }

  // it's will output too much to travis, so ignore it
  public void testTime()
      throws IoTDBConnectionException, StatementExecutionException, BatchExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseriesForTime();

    insertTabletTestForTime("root.sg1.d1");
  }

  @Test
  public void testBatchInsertSeqAndUnseq() throws SQLException, ClassNotFoundException,
      IoTDBConnectionException, StatementExecutionException, BatchExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertTabletTest2("root.sg1.d1");
    // flush
    session.executeNonQueryStatement("FLUSH");
    session.executeNonQueryStatement("FLUSH root.sg1");
    session.executeNonQueryStatement("MERGE");
    session.executeNonQueryStatement("FULL MERGE");
    insertTabletTest3("root.sg1.d1");

    queryForBatchSeqAndUnseq();
  }

  @Test
  public void testBatchInsert() throws StatementExecutionException, SQLException,
      ClassNotFoundException, IoTDBConnectionException, BatchExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertTabletTest2("root.sg1.d1");

    queryForBatch();
  }

  @Test
  public void testCreateMultiTimeseries()
      throws IoTDBConnectionException, BatchExecutionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    List<String> paths = new ArrayList<>();
    paths.add("root.sg1.d1.s1");
    paths.add("root.sg1.d1.s2");
    List<TSDataType> tsDataTypes = new ArrayList<>();
    tsDataTypes.add(TSDataType.DOUBLE);
    tsDataTypes.add(TSDataType.DOUBLE);
    List<TSEncoding> tsEncodings = new ArrayList<>();
    tsEncodings.add(TSEncoding.RLE);
    tsEncodings.add(TSEncoding.RLE);
    List<CompressionType> compressionTypes = new ArrayList<>();
    compressionTypes.add(CompressionType.SNAPPY);
    compressionTypes.add(CompressionType.SNAPPY);

    List<Map<String, String>> tagsList = new ArrayList<>();
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "v1");
    tagsList.add(tags);
    tagsList.add(tags);

    session
        .createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList,
            null, null);

    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.s1"));
    Assert.assertTrue(session.checkTimeseriesExists("root.sg1.d1.s2"));

  }

  @Test
  public void testTestMethod()
      throws StatementExecutionException, IoTDBConnectionException, BatchExecutionException {

    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");
    String deviceId = "root.sg1.d1";

    createTimeseries();


    // test insert tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet("root.sg1.d1", schemaList, 100);

    session.testInsertTablet(tablet);

    // test insert record
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    for (long time = 0; time < 100; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");
      session.testInsertRecord(deviceId, time, measurements, values);
    }

    // test insert records
    measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<String>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.testInsertRecords(deviceIds, timestamps, measurementsList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.testInsertRecords(deviceIds, timestamps, measurementsList, valuesList);
  }

  @Test
  @Ignore
  // TODO is this test relevant? Then it should be fixed ASAP
  public void testChineseCharacter() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (!System.getProperty("sun.jnu.encoding").contains("UTF-8")) {
      logger.error("The system does not support UTF-8, so skip Chinese test...");
      session.close();
      return;
    }
    String storageGroup = "root.存储组1";
    String[] devices = new String[]{
        "设备1.指标1",
        "设备1.s2",
        "d2.s1",
        "d2.指标2"
    };
    session.setStorageGroup(storageGroup);

    createTimeseriesInChinese(storageGroup, devices);
    insertInChinese(storageGroup, devices);

    // Check if the Series exist
    for (String device : devices) {
      final boolean sessionExists = session.checkTimeseriesExists(storageGroup + "." + device);
      assertTrue(sessionExists);
    }

    // Check if they all have 10 enneintries
    final SessionDataSet sessionDataSet = session.executeQueryStatement("SELECT COUNT(*) FROM " + storageGroup);
    assertEquals(4, sessionDataSet.getColumnNames().size());
    final RowRecord record = sessionDataSet.next();

    for (Field field : record.getFields()) {
      assertEquals(10, field.getIntV());
    }

    session.deleteStorageGroup(storageGroup);
    session.close();
  }

  @Test
  public void test() throws ClassNotFoundException, SQLException,
      IoTDBConnectionException, StatementExecutionException, BatchExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insert();

    // sql test
    insert_via_sql();

    query3();

//    insertTabletTest1();
    deleteData();

    query();

    deleteTimeseries();

    query2();

    insertRecords();

    query4();

    // special characters
    session.createTimeseries("root.sg1.d1.1_2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.\"1.2.3\"", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.\'1.2.4\'", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);

    session.setStorageGroup("root.1");
    session.createTimeseries("root.1.2.3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);

    // Add another storage group to test the deletion of storage group
    session.setStorageGroup("root.sg2");
    session.createTimeseries("root.sg2.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);

    deleteStorageGroupTest();

    // set storage group but do not create timeseries
    session.setStorageGroup("root.sg3");
    insertTabletTest1("root.sg3.d1");

    // create timeseries but do not set storage group
    session.createTimeseries("root.sg4.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg4.d1.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg4.d1.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    insertTabletTest1("root.sg4.d1");

    // do not set storage group and create timeseries
    insertTabletTest1("root.sg5.d1");

    session.close();
  }

  @Test
  public void testByStr() throws ClassNotFoundException, SQLException,
      IoTDBConnectionException, StatementExecutionException, BatchExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }

    session.setStorageGroup("root.sg1");

    createTimeseries();

    insertByStr();

    // sql test
    insert_via_sql();

    query3();

//    insertTabletTest1();
    deleteData();

    query();

    deleteTimeseries();

    query2();

    insertRecordsByStr();

    query4();

    // special characters
    session.createTimeseries("root.sg1.d1.1_2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.\"1.2.3\"", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.\'1.2.4\'", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);

    session.setStorageGroup("root.1");
    session.createTimeseries("root.1.2.3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);

    // Add another storage group to test the deletion of storage group
    session.setStorageGroup("root.sg2");
    session.createTimeseries("root.sg2.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);

    deleteStorageGroupTest();

    // set storage group but do not create timeseries
    session.setStorageGroup("root.sg3");
    insertTabletTest1("root.sg3.d1");

    // create timeseries but do not set storage group
    session.createTimeseries("root.sg4.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg4.d1.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg4.d1.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    insertTabletTest1("root.sg4.d1");

    // do not set storage group and create timeseries
    insertTabletTest1("root.sg5.d1");

    session.close();
  }

  @Test
  @Ignore
  // TODO is this test relevant? Then it should be fixed ASAP
  public void TestSessionInterfacesWithDisabledWAL()
      throws StatementExecutionException, IoTDBConnectionException,
          BatchExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    try {
      session.open();
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }

    session.setStorageGroup("root.sg1");
    String deviceId = "root.sg1.d1";

    boolean isEnableWAL = IoTDBDescriptor.getInstance().getConfig().isEnableWal();
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(false);
    createTimeseries();

    // test insert record
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

    // test insert tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet(deviceId, schemaList, 100);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    for (int time = 1; time <= 100; time++) {
      timestamps[time - 1] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[time - 1] = i;
      }
      tablet.rowSize++;
    }

    session.insertTablet(tablet);

    // Ensure that overall 200 records where inserted
    final SessionDataSet result = session.executeQueryStatement("SELECT COUNT(*) FROM " + deviceId);

    for (Field field : result.next().getFields()) {
      assertEquals(200, field.getIntV());
    }

    IoTDBDescriptor.getInstance().getConfig().setEnableWal(isEnableWAL);
    session.close();
  }

  private void createTimeseriesForTime()
      throws StatementExecutionException, IoTDBConnectionException {
    session.createTimeseries("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s4", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s5", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s6", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
  }

  private void createTimeseries() throws StatementExecutionException, IoTDBConnectionException {
    session.createTimeseries("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s2", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s3", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
  }

  private void createTimeseriesInChinese(String storageGroup, String[] devices)
      throws StatementExecutionException, IoTDBConnectionException {
    for (String path : devices) {
      String fullPath = storageGroup + "." + path;
      session.createTimeseries(fullPath, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
  }

  private void insertInChinese(String storageGroup, String[] devices)
      throws StatementExecutionException, IoTDBConnectionException {
    for (String path : devices) {
      for (int i = 0; i < 10; i++) {
        String[] ss = path.split("\\.");
        String deviceId = storageGroup;
        for (int j = 0; j < ss.length - 1; j++) {
          deviceId += ("." + ss[j]);
        }
        String sensorId = ss[ss.length - 1];
        List<String> measurements = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();

        measurements.add(sensorId);
        types.add(TSDataType.INT64);
        values.add(100L);
        session.insertRecord(deviceId, i, measurements, types, values);
      }
    }
  }

  private void insertRecords() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d2";
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

  private void insertRecordsByStr() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d2";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<String>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<String> values = new ArrayList<>();
      values.add("1");
      values.add("2");
      values.add("3");

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecords(deviceIds, timestamps, measurementsList, valuesList);
  }

  private void insertInObject() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 100; time++) {
      session.insertRecord(deviceId, time, measurements, types,1L, 2L, 3L);
    }
  }

  private void insert() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d1";
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

  private void insertTabletTest1(String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet(deviceId, schemaList, 100);

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
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    String path1 = "root.sg1.d1.s1";
    String path2 = "root.sg1.d1.s2";
    String path3 = "root.sg1.d1.s3";
    long deleteTime = 100;

    List<String> paths = new ArrayList<>();
    paths.add(path1);
    paths.add(path2);
    paths.add(path3);
    session.deleteData(paths, deleteTime);
  }

  private void deleteTimeseries() throws IoTDBConnectionException, StatementExecutionException {
    session.deleteTimeseries("root.sg1.d1.s1");
  }

  private void query() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard =
        "Time\n" + "root.sg1.d1.s1\n" + "root.sg1.d1.s2\n" + "root.sg1.d1.s3\n"
            + "root.sg1.d2.s1\n" + "root.sg1.d2.s2\n" + "root.sg1.d2.s3\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1)).append("\n");
      }
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    }
  }

  private void queryForAlignByDevice()
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet sessionDataSet = session
        .executeQueryStatement("select '11', s1, '11' from root.sg1.d1 align by device");
    sessionDataSet.setFetchSize(1024);
    int count = 0;
    while (sessionDataSet.hasNext()) {
      count++;
      StringBuilder sb = new StringBuilder();
      List<Field> fields = sessionDataSet.next().getFields();
      for (Field f : fields) {
        sb.append(f.getStringValue()).append(",");
      }
      Assert.assertEquals("root.sg1.d1,11,0,11,", sb.toString());
    }
    Assert.assertEquals(1000, count);
    sessionDataSet.closeOperationHandle();
  }

  private void queryForAlignByDevice2()
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet sessionDataSet = session.executeQueryStatement(
        "select '11', s1, '11', s5, s1, s5 from root.sg1.d1 align by device");
    sessionDataSet.setFetchSize(1024);
    int count = 0;
    while (sessionDataSet.hasNext()) {
      count++;
      StringBuilder sb = new StringBuilder();
      List<Field> fields = sessionDataSet.next().getFields();
      for (Field f : fields) {
        sb.append(f.getStringValue()).append(",");
      }
      Assert.assertEquals("root.sg1.d1,11,0,11,null,0,null,", sb.toString());
    }
    Assert.assertEquals(1000, count);
    sessionDataSet.closeOperationHandle();
  }


  private void query2() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard =
        "Time\n" + "root.sg1.d1.s2\n" + "root.sg1.d1.s3\n"
            + "root.sg1.d2.s1\n" + "root.sg1.d2.s2\n" + "root.sg1.d2.s3\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1)).append("\n");
      }
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(resultStr.toString(), standard);
    }
  }

  public void deleteStorageGroupTest() throws ClassNotFoundException, SQLException,
      IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteStorageGroup("root.sg1.d1.s1");
    } catch (StatementExecutionException e) {
      assertTrue(e.getMessage().contains("Path [root.sg1.d1.s1] does not exist"));
    }
    session.deleteStorageGroup("root.sg1");
    File folder = new File("data/system/storage_groups/root.sg1/");
    assertFalse(folder.exists());
    session.setStorageGroup("root.sg1.d1");
    session.createTimeseries("root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE,
        CompressionType.SNAPPY);
    // using the query result as the QueryTest to verify the deletion and the new insertion
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard = "Time\n" + "root.1.2.3\n" + "root.sg2.d1.s1\n" + "root.sg1.d1.s1\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1)).append("\n");
      }
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          resultStr.append(resultSet.getString(i)).append(",");
        }
        resultStr.append("\n");
      }
      Assert.assertEquals(standard, resultStr.toString());
      List<String> storageGroups = new ArrayList<>();
      storageGroups.add("root.sg1.d1");
      storageGroups.add("root.sg2");
      session.deleteStorageGroups(storageGroups);
    }
  }

  private void query4() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet sessionDataSet = session.executeQueryStatement("select * from root.sg1.d2");
    sessionDataSet.setFetchSize(1024);
    int count = 0;
    while (sessionDataSet.hasNext()) {
      long index = 1;
      count++;
      for (Field f : sessionDataSet.next().getFields()) {
        Assert.assertEquals(f.getLongV(), index);
        index++;
      }
    }
    Assert.assertEquals(500, count);
    sessionDataSet.closeOperationHandle();
  }


  private void query3() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet sessionDataSet = session.executeQueryStatement("select * from root.sg1.d1");
    sessionDataSet.setFetchSize(1024);
    int count = 0;
    while (sessionDataSet.hasNext()) {
      long index = 1;
      count++;
      for (Field f : sessionDataSet.next().getFields()) {
        Assert.assertEquals(index, f.getLongV());
        index++;
      }
    }
    Assert.assertEquals(101, count);
    sessionDataSet.closeOperationHandle();
  }


  private void insert_via_sql() throws IoTDBConnectionException, StatementExecutionException {
    session.executeNonQueryStatement(
        "insert into root.sg1.d1(timestamp,s1, s2, s3) values(100, 1,2,3)");
  }

  @Test
  public void checkPathTest() throws IoTDBConnectionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    //test set sg
    checkSetSG(session, "root.vehicle", true);
    checkSetSG(session, "root.123456", true);
    checkSetSG(session, "root._1234", true);
    checkSetSG(session, "root._vehicle", true);
    checkSetSG(session, "root.\tvehicle", false);
    checkSetSG(session, "root.\nvehicle", false);
    checkSetSG(session, "root..vehicle", false);
    checkSetSG(session, "root.1234a4", true);
    checkSetSG(session, "root.1_2", true);
    checkSetSG(session, "root.%12345", true);
    checkSetSG(session, "root.+12345", true);
    checkSetSG(session, "root.-12345", true);
    checkSetSG(session, "root.a{12345}", true);

    //test create timeseries
    checkCreateTimeseries(session, "root.vehicle.d0.s0", true);
    checkCreateTimeseries(session, "root.vehicle.1110.s0", true);
    checkCreateTimeseries(session, "root.vehicle.d0.1220", true);
    checkCreateTimeseries(session, "root.vehicle._1234.s0", true);
    checkCreateTimeseries(session, "root.vehicle.1245.\"1.2.3\"", true);
    checkCreateTimeseries(session, "root.vehicle.1245.\'1.2.4\'", true);
    checkCreateTimeseries(session, "root.vehicle./d0.s0", true);
    checkCreateTimeseries(session, "root.vehicle.d\t0.s0", false);
    checkCreateTimeseries(session, "root.vehicle.!d\t0.s0", false);
    checkCreateTimeseries(session, "root.vehicle.d{dfewrew0}.s0", true);

    session.close();
  }

  private void checkSetSG(Session session, String sg, boolean correctStatus)
      throws IoTDBConnectionException {
    boolean status = true;
    try {
      session.setStorageGroup(sg);
    } catch (StatementExecutionException e) {
      status = false;
    }
    assertEquals(correctStatus, status);
  }

  private void checkCreateTimeseries(Session session, String timeseries, boolean correctStatus)
      throws IoTDBConnectionException {
    boolean status = true;
    try {
      session.createTimeseries(timeseries, TSDataType.INT64, TSEncoding.RLE,
          CompressionType.SNAPPY);
    } catch (StatementExecutionException e) {
      status = false;
    }
    assertEquals(correctStatus, status);
  }

  private void insertTabletTest2(String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet(deviceId, schemaList, 256);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 1000; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private void insertTabletTest3(String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));

    Tablet tablet = new Tablet(deviceId, schemaList, 200);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 500; time < 1500; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  private void insertTabletTestForTime(String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {

    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s4", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s5", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s6", TSDataType.INT64, TSEncoding.RLE));
    long count = 10000000;
    long begin = 0;

    Tablet tablet = new Tablet(deviceId, schemaList, 1000);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = begin; time < count + begin; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 6; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }

  }

  private void queryForBatch() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard =
        "Time\n" + "root.sg1.d1.s1\n" + "root.sg1.d1.s2\n" + "root.sg1.d1.s3\n" +
            "root.sg1.d2.s1\n" + "root.sg1.d2.s2\n" + "root.sg1.d2.s3\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1)).append("\n");
      }

      int count = 0;
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          count++;
        }
      }
      Assert.assertEquals(standard, resultStr.toString());
      // d1 and d2 will align
      Assert.assertEquals(7000, count);
    }
  }

  public void queryForBatchCheckOrder() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard =
        "Time\n" + "root.sg1.d1.s1\n" + "root.sg1.d1.s2\n" + "root.sg1.d1.s3\n" +
            "root.sg1.d2.s1\n" + "root.sg1.d2.s2\n" + "root.sg1.d2.s3\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "192.168.130.18:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT s_0 FROM root.group_0.d_0 LIMIT 10000");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1)).append("\n");
      }

      int count = 0;
      long beforeTime = 0;
      int errorCount = 0;
      while (resultSet.next()) {
        long curTime = resultSet.getLong(1);
        if (beforeTime < curTime) {
          beforeTime = curTime;
        } else {
          errorCount++;
          if (errorCount > 10) {
            System.exit(-1);
          }
        }

        for (int i = 1; i <= colCount; i++) {
          count++;
        }
      }
      Assert.assertEquals(standard, resultStr.toString());
      // d1 and d2 will align
      Assert.assertEquals(7000, count);
    }
  }

  private void queryForBatchSeqAndUnseq() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    String standard =
        "Time\n" + "root.sg1.d1.s1\n" + "root.sg1.d1.s2\n" + "root.sg1.d1.s3\n" +
            "root.sg1.d2.s1\n" + "root.sg1.d2.s2\n" + "root.sg1.d2.s3\n";
    try (Connection connection = DriverManager
        .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery("SELECT * FROM root");
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int colCount = metaData.getColumnCount();
      StringBuilder resultStr = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        resultStr.append(metaData.getColumnLabel(i + 1)).append("\n");
      }

      int count = 0;
      while (resultSet.next()) {
        for (int i = 1; i <= colCount; i++) {
          count++;
        }
      }
      Assert.assertEquals(standard, resultStr.toString());
      // d1 and d2 will align
      Assert.assertEquals(10500, count);
    }
  }

}
