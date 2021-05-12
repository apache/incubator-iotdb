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

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.encoding.TsFileEncodingException;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.*;

public class TsFileWriterTest {
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  TsFileWriter writer = null;
  long fileName = System.nanoTime();
  boolean closed = false;

  @Before
  public void setUp() {
    try {
      writer = new TsFileWriter(fsFactory.getFile("target/tsfileWriter-" + fileName));
      addMeasurement();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() {
    if (!closed) {
      closeFile();
    }
    try {
      Files.deleteIfExists(fsFactory.getFile("target/tsfileWriter-" + fileName).toPath());
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void addMeasurement() {
    try {
      // String measurementId, TSDataType type, TSEncoding encoding,
      //      CompressionType compressionType
      writer.registerTimeseries(
          new Path("d1", "s1"),
          new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      // String measurementId, TSDataType type, TSEncoding encoding,
      //      CompressionType compressionType
      writer.registerTimeseries(
          new Path("d1", "s1"),
          new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      Assert.assertEquals("given timeseries has exists! d1.s1", e.getMessage());
    }
    try {
      // String measurementId, TSDataType type, TSEncoding encoding,
      //      CompressionType compressionType
      writer.registerTimeseries(
          new Path("d1", "s2"),
          new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    try {
      for (int i = 2; i < 3; i++) {
        writer.registerTimeseries(
            new Path("d" + i, "s1"),
            new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void addMeasurementTest() {
    closeFile();
    readNothing();
  }

  private void writeTSRecord() throws IOException, WriteProcessException {
    // normal
    TSRecord record = new TSRecord(10000, "d1");
    record.addTuple(new FloatDataPoint("s1", 5.0f));
    record.addTuple(new IntDataPoint("s2", 5));
    writer.write(record);

    // not existed time series
    record = new TSRecord(10001, "d1");
    record.addTuple(new FloatDataPoint("s3", 5.0f));
    try {
      writer.write(record);
    } catch (WriteProcessException e) {
      assertTrue(e instanceof NoMeasurementException);
    }
  }

  @Test
  public void writeTSRecordTest() throws IOException, WriteProcessException {
    writeTSRecord();
    closeFile();
    readOneRow();
  }

  @Test
  public void writeIncorrectTSRecord0() throws IOException, WriteProcessException {
    // incorrect data type
    TSRecord record = new TSRecord(10002, "d2");
    record.addTuple(new IntDataPoint("s1", 5));
    try {
      writer.write(record);
    } catch (TsFileEncodingException e) {
      // do nothing
    }
    closeFile();
    readNothing();
  }

  @Test
  public void writeIncorrectTSRecords() throws IOException, WriteProcessException {
    // incorrect data type
    for (int i = 2; i < 3; i++) {
      TSRecord record = new TSRecord(10000 + i, "d" + i);
      record.addTuple(new IntDataPoint("s1", 5));
      try {
        writer.write(record);
      } catch (TsFileEncodingException e) {
        // do nothing
      }
    }
    closeFile();
    readNothing();
  }

  @Test
  public void writeIncorrectTSRecord() throws IOException, WriteProcessException {
    writeTSRecord();
    // incorrect data type
    TSRecord record = new TSRecord(10002, "d2");
    record.addTuple(new IntDataPoint("s1", 5));
    try {
      writer.write(record);
    } catch (TsFileEncodingException e) {
      // do nothing
    }
    closeFile();
    readOneRow();
  }

  @Test
  public void writeTablet() throws IOException, WriteProcessException {
    Tablet tablet =
        new Tablet(
            "d1",
            Arrays.asList(
                new MeasurementSchema(
                    "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY),
                new MeasurementSchema(
                    "s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY)));
    tablet.timestamps[0] = 10000;
    ((float[]) tablet.values[0])[0] = 5.0f;
    ((int[]) tablet.values[1])[0] = 5;
    tablet.rowSize = 1;
    writer.write(tablet);
    closeFile();
    readOneRow();
  }

  @Test
  public void writeTablet2() throws IOException, WriteProcessException {
    Tablet tablet =
        new Tablet(
            "d1",
            Arrays.asList(
                new MeasurementSchema(
                    "s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY),
                new MeasurementSchema(
                    "s2", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY)));
    tablet.timestamps[0] = 10000;
    ((float[]) tablet.values[0])[0] = 5.0f;
    tablet.rowSize = 1;
    writer.write(tablet);
    closeFile();
    // in this case, the value of s2 = 0 at time 10000.
    readOneRow(0);
  }

  @Test
  public void getIOWriter() {
    // The interface is just for test
    writer.getIOWriter();
    closeFile();
    readNothing();
  }

  @Test
  public void flushForTest() throws IOException {
    // The interface is just for test
    writer.flushAllChunkGroups();
    closeFile();
    readNothing();
  }

  @Test
  public void flushForTestWithVersion() throws IOException {
    // The interface is just for test
    writer.flushAllChunkGroups();
    closeFile();
    readNothing();
  }

  private void closeFile() {
    try {
      closed = true;
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void readNothing() {
    // using TsFileReader for test
    try {
      ReadOnlyTsFile readOnlyTsFile =
          new ReadOnlyTsFile(
              new TsFileSequenceReader(fsFactory.getFile("target/tsfileWriter-" + fileName)));
      QueryDataSet dataSet =
          readOnlyTsFile.query(
              QueryExpression.create()
                  .addSelectedPath(new Path("d1", "s1"))
                  .addSelectedPath(new Path("d1", "s2")));
      assertFalse(dataSet.hasNext());
      readOnlyTsFile.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void readOneRow() {
    readOneRow(5);
  }

  private void readOneRow(int s2Value) {
    try {
      ReadOnlyTsFile readOnlyTsFile =
          new ReadOnlyTsFile(
              new TsFileSequenceReader(fsFactory.getFile("target/tsfileWriter-" + fileName)));
      QueryDataSet dataSet =
          readOnlyTsFile.query(
              QueryExpression.create()
                  .addSelectedPath(new Path("d1", "s1"))
                  .addSelectedPath(new Path("d1", "s2"))
                  .addSelectedPath(new Path("d1", "s3")));
      while (dataSet.hasNext()) {
        RowRecord result = dataSet.next();
        assertEquals(2, result.getFields().size());
        assertEquals(10000, result.getTimestamp());
        assertEquals(5.0f, result.getFields().get(0).getFloatV(), 0.00001);
        assertEquals(s2Value, result.getFields().get(1).getIntV());
      }
      readOnlyTsFile.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
