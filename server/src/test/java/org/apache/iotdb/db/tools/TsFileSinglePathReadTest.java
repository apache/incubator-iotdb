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

package org.apache.iotdb.db.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileSinglePathReadTest {

  String fileName = "target/tsfileWriter-" + System.nanoTime();
  TsFileWriter writer = null;
  String path = "d1.s1";

  @Before
  public void setUp() {
    try {
      writer = new TsFileWriter(new File(fileName));
      writeRecordAndClose();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @After
  public void tearDown() {
    try {
      Files.deleteIfExists(new File(fileName).toPath());
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void writeRecordAndClose() throws IOException, WriteProcessException {
    addMeasurement();
    writeTSRecord();
    writer.close();
  }

  private void addMeasurement() {
    try {
      //String measurementId, TSDataType type, TSEncoding encoding,
      //      CompressionType compressionType
      writer.registerTimeseries(new Path("d1", "s1"),
          new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY));
    } catch (WriteProcessException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void writeTSRecord() throws IOException, WriteProcessException {
    TSRecord record = new TSRecord(10000, "d1");
    record.addTuple(new FloatDataPoint("s1", 5.0f));
    writer.write(record);

    record = new TSRecord(10001, "d1");
    record.addTuple(new FloatDataPoint("s1", 5.0f));
    try {
      writer.write(record);
    } catch (WriteProcessException e) {
      assertTrue(e instanceof NoMeasurementException);
    }
  }

  @Test
  public void SinglePathReadTest() throws IOException {
    assertEquals(2, TsFileSinglePathRead.printTsFileSinglePath(new Pair<>(fileName, path)));
  }
}
