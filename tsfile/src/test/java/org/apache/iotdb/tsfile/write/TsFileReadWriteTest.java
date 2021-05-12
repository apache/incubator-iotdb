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
package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsFileReadWriteTest {

  private final double delta = 0.0000001;
  private String path = TestConstant.BASE_OUTPUT_PATH.concat("read_write_rle.tsfile");
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private File f;

  @Before
  public void setUp() {
    f = fsFactory.getFile(path);
    if (f.exists()) {
      assertTrue(f.delete());
    }
  }

  @After
  public void tearDown() {
    f = fsFactory.getFile(path);
    if (f.exists()) {
      assertTrue(f.delete());
    }
  }

  @Test
  public void intTest() throws IOException, WriteProcessException {
    List<TSEncoding> encodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.RLE,
            TSEncoding.TS_2DIFF,
            TSEncoding.REGULAR,
            TSEncoding.GORILLA);
    for (TSEncoding encoding : encodings) {
      intTest(encoding);
    }
  }

  private void intTest(TSEncoding encoding) throws IOException, WriteProcessException {
    writeDataByTSRecord(TSDataType.INT32, (i) -> new IntDataPoint("sensor_1", (int) i), encoding);
    readData((i, field, delta) -> assertEquals(i, field.getIntV()));
  }

  @Test
  public void longTest() throws IOException, WriteProcessException {
    List<TSEncoding> encodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.RLE,
            TSEncoding.TS_2DIFF,
            TSEncoding.REGULAR,
            TSEncoding.GORILLA);
    for (TSEncoding encoding : encodings) {
      longTest(encoding);
    }
  }

  public void longTest(TSEncoding encoding) throws IOException, WriteProcessException {
    writeDataByTSRecord(TSDataType.INT64, (i) -> new LongDataPoint("sensor_1", i), encoding);
    readData((i, field, delta) -> assertEquals(i, field.getLongV()));
  }

  @Test
  public void floatTest() throws IOException, WriteProcessException {
    List<TSEncoding> encodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.RLE,
            TSEncoding.TS_2DIFF,
            TSEncoding.GORILLA_V1,
            TSEncoding.GORILLA);
    for (TSEncoding encoding : encodings) {
      floatTest(encoding);
    }
  }

  public void floatTest(TSEncoding encoding) throws IOException, WriteProcessException {
    writeDataByTSRecord(
        TSDataType.FLOAT, (i) -> new FloatDataPoint("sensor_1", (float) i), encoding);
    readData((i, field, delta) -> assertEquals(i, field.getFloatV(), delta));
  }

  @Test
  public void doubleTest() throws IOException, WriteProcessException {
    List<TSEncoding> encodings =
        Arrays.asList(
            TSEncoding.PLAIN,
            TSEncoding.RLE,
            TSEncoding.TS_2DIFF,
            TSEncoding.GORILLA_V1,
            TSEncoding.GORILLA);
    for (TSEncoding encoding : encodings) {
      doubleTest(encoding);
    }
  }

  public void doubleTest(TSEncoding encoding) throws IOException, WriteProcessException {
    writeDataByTSRecord(
        TSDataType.DOUBLE, (i) -> new DoubleDataPoint("sensor_1", (double) i), encoding);
    readData((i, field, delta) -> assertEquals(i, field.getDoubleV(), delta));
  }

  // If no dataPoint in "device_1.sensor_2", it will throws a nomeasurement
  // exception,
  // cause no schema in tsfilemetadata anymore.
  @Test
  public void readEmptyMeasurementTest() throws IOException, WriteProcessException {
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      // add measurements into file schema
      tsFileWriter.registerTimeseries(
          new Path("device_1", "sensor_1"),
          new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
      tsFileWriter.registerTimeseries(
          new Path("device_1", "sensor_2"),
          new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
      // construct TSRecord
      TSRecord tsRecord = new TSRecord(1, "device_1");
      DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
      tsRecord.addTuple(dPoint1);
      // write a TSRecord to TsFile
      tsFileWriter.write(tsRecord);
    }

    // read example : no filter
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(path));
    ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("device_1", "sensor_2"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);
    try {
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    } catch (IOException e) {
      // Assert.fail();
    } finally {
      reader.close();
    }

    assertTrue(f.delete());
  }

  @Test
  public void readMeasurementWithRegularEncodingTest() throws IOException, WriteProcessException {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("REGULAR");
    writeDataByTSRecord(
        TSDataType.INT64, (i) -> new LongDataPoint("sensor_1", i), TSEncoding.REGULAR);
    readData((i, field, delta) -> assertEquals(i, field.getLongV()));
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
  }

  private void writeDataByTSRecord(
      TSDataType dataType, DataPointProxy proxy, TSEncoding encodingType)
      throws IOException, WriteProcessException {
    int floatCount = 1024 * 1024 * 13 + 1023;
    // add measurements into file schema
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      tsFileWriter.registerTimeseries(
          new Path("device_1", "sensor_1"),
          new MeasurementSchema("sensor_1", dataType, encodingType));
      for (long i = 1; i < floatCount; i++) {
        // construct TSRecord
        TSRecord tsRecord = new TSRecord(i, "device_1");
        DataPoint dPoint1 = proxy.generateOne(i);
        tsRecord.addTuple(dPoint1);
        // write a TSRecord to TsFile
        tsFileWriter.write(tsRecord);
      }
    }
  }

  private void readData(ReadDataPointProxy proxy) throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(path));
    ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("device_1", "sensor_1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    for (int j = 0; j < paths.size(); j++) {
      assertEquals(paths.get(j), queryDataSet.getPaths().get(j));
    }
    int i = 1;
    while (queryDataSet.hasNext()) {
      RowRecord r = queryDataSet.next();
      assertEquals(i, r.getTimestamp());
      proxy.assertEqualProxy(i, r.getFields().get(0), delta);
      i++;
    }
    reader.close();
  }

  private interface DataPointProxy {

    DataPoint generateOne(long value);
  }

  private interface ReadDataPointProxy {

    void assertEqualProxy(long i, Field field, double delta);
  }
}
