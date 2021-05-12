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
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
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
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SameMeasurementsWithDifferentDataTypesTest {

  private String TEMPLATE_1 = "template1";
  private String TEMPLATE_2 = "template2";
  private String tsfilePath = TestConstant.BASE_OUTPUT_PATH.concat("test.tsfile");
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  @Before
  public void before() throws IOException, WriteProcessException {
    writeFile(tsfilePath);
  }

  @After
  public void after() {
    File file = fsFactory.getFile(tsfilePath);
    try {
      Files.deleteIfExists(file.toPath());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSameMeasurementsWithDiffrentDataTypes() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s1"));
    pathList.add(new Path("d2", "s1"));
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    TsFileSequenceReader fileReader = new TsFileSequenceReader(fsFactory.getFile(tsfilePath));
    ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(fileReader);
    QueryDataSet dataSet = readOnlyTsFile.query(queryExpression);
    int i = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (i == 0) {
        assertEquals(1L, r.getTimestamp());
        assertEquals(2, r.getFields().size());
        assertEquals(TSDataType.FLOAT, r.getFields().get(0).getDataType());
        assertEquals(TSDataType.INT64, r.getFields().get(1).getDataType());
      }
      i++;
    }
    Assert.assertEquals(6, i);
  }

  private void writeFile(String tsfilePath) throws IOException, WriteProcessException {
    File f = fsFactory.getFile(tsfilePath);
    try {
      Files.deleteIfExists(f.toPath());
    } catch (IOException e) {
      fail(e.getMessage());
    }

    Schema schema = new Schema();
    schema.extendTemplate(
        TEMPLATE_1, new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        TEMPLATE_1, new MeasurementSchema("s2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        TEMPLATE_1, new MeasurementSchema("s3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    schema.extendTemplate(
        TEMPLATE_2, new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        TEMPLATE_2, new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));

    schema.registerDevice("d1", TEMPLATE_1);
    schema.registerDevice("d2", TEMPLATE_2);

    TsFileWriter tsFileWriter = new TsFileWriter(f, schema);

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1, "d1");
    DataPoint dPoint1 = new FloatDataPoint("s1", 1.2f);
    DataPoint dPoint2 = new IntDataPoint("s2", 20);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);

    // write a TSRecord to TsFile
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "d1");
    dPoint2 = new IntDataPoint("s2", 20);
    DataPoint dPoint3 = new IntDataPoint("s3", 50);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(3, "d1");
    dPoint1 = new FloatDataPoint("s1", 1.4f);
    dPoint2 = new IntDataPoint("s2", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(4, "d1");
    dPoint1 = new FloatDataPoint("s1", 1.2f);
    dPoint2 = new IntDataPoint("s2", 20);
    dPoint3 = new IntDataPoint("s3", 51);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(6, "d1");
    dPoint1 = new FloatDataPoint("s1", 7.2f);
    dPoint2 = new IntDataPoint("s2", 10);
    dPoint3 = new IntDataPoint("s3", 11);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(7, "d1");
    dPoint1 = new FloatDataPoint("s1", 6.2f);
    dPoint2 = new IntDataPoint("s2", 20);
    dPoint3 = new IntDataPoint("s3", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(8, "d1");
    dPoint1 = new FloatDataPoint("s1", 9.2f);
    dPoint2 = new IntDataPoint("s2", 30);
    dPoint3 = new IntDataPoint("s3", 31);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(1, "d2");
    dPoint1 = new LongDataPoint("s1", 2000L);
    dPoint2 = new LongDataPoint("s2", 210L);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "d2");
    dPoint2 = new LongDataPoint("s2", 2090L);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(3, "d2");
    dPoint1 = new LongDataPoint("s1", 1400L);
    dPoint2 = new LongDataPoint("s2", 21L);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(4, "d2");
    dPoint1 = new LongDataPoint("s1", 1200L);
    dPoint2 = new LongDataPoint("s2", 20L);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    // close TsFile
    tsFileWriter.close();
  }
}
