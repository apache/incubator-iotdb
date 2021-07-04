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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SessionUT {

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
    session = new Session("127.0.0.1", 6667, "root", "root", null);
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
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
    long[] resValues = (long[]) tablet.values[0];
    long[] expectedTimestamps = new long[] {0, 1, 2};
    long[] expectedValues = new long[] {1, 2, 0};
    try {
      assertArrayEquals(expectedTimestamps, resTimestamps);
      assertArrayEquals(expectedValues, resValues);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void testInsertByStrAndSelectFailedData()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root", null);
    session.open();

    String deviceId = "root.sg1.d1";

    session.createTimeseries(
        deviceId + ".s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    session.createTimeseries(
        deviceId + ".s2", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    session.createTimeseries(
        deviceId + ".s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.UNCOMPRESSED);
    session.createTimeseries(
        deviceId + ".s4", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.UNCOMPRESSED);

    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));
    schemaList.add(new MeasurementSchema("s4", TSDataType.INT64, TSEncoding.PLAIN));

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
      long[] sensor4 = (long[]) values[3];
      sensor4[row] = time;
    }

    try {
      session.insertTablet(tablet);
      fail();
    } catch (StatementExecutionException e) {
      // ignore
    }

    SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      int nullCount = 0;
      for (int j = 0; j < 4; ++j) {
        if (record.getFields().get(j) == null || record.getFields().get(j).getDataType() == null) {
          ++nullCount;
        } else {
          assertEquals(i, record.getFields().get(j).getLongV());
        }
      }
      assertEquals(3, nullCount);
      i++;
    }
  }

  @Test
  public void testSetTimeZone() throws StatementExecutionException, IoTDBConnectionException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();
    assertEquals("+05:00", session.getTimeZone());
    session.setTimeZone("+09:00");
    assertEquals("+09:00", session.getTimeZone());
  }

  @Test
  public void setTimeout() throws StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root", 10000, 20000);
    Assert.assertEquals(20000, session.getTimeout());
    session.setTimeout(60000);
    Assert.assertEquals(60000, session.getTimeout());
  }

  @Test
  public void createDeviceTemplate() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    session.open();

    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    List<String> measurements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      measurements.add("s" + i);
    }

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      dataTypes.add(TSDataType.INT64);
    }

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      encodings.add(TSEncoding.RLE);
    }

    measurementList.add(measurements);
    dataTypeList.add(dataTypes);
    encodingList.add(encodings);

    List<CompressionType> compressionTypes = new ArrayList<>();
    for (int i = 0; i < 11; i++) {
      compressionTypes.add(CompressionType.SNAPPY);
    }

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");
    schemaNames.add("test_vector");

    session.createSchemaTemplate(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
    session.setSchemaTemplate("template1", "root.sg.1");

    MeasurementSchema testMeasurementSchema =
        new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE);
    session.createSchemaTemplate("template2", Collections.singletonList(testMeasurementSchema));

    String[] measurementArray = measurements.toArray(new String[measurements.size()]);
    TSDataType[] dataTypeArray = dataTypes.toArray(new TSDataType[dataTypes.size()]);
    TSEncoding[] encodingArray = encodings.toArray(new TSEncoding[encodings.size()]);
    VectorMeasurementSchema testVectorMeasurementSchema =
        new VectorMeasurementSchema(
            "vector_1", measurementArray, dataTypeArray, encodingArray, CompressionType.SNAPPY);
    session.createSchemaTemplate(
        "template3", Collections.singletonList(testVectorMeasurementSchema));
  }
}
