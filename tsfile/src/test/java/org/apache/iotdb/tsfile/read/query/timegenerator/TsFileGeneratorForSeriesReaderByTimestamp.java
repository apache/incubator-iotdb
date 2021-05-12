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
package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.FileUtils;
import org.apache.iotdb.tsfile.utils.FileUtils.Unit;
import org.apache.iotdb.tsfile.utils.RecordUtils;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.Assert;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

@Ignore
public class TsFileGeneratorForSeriesReaderByTimestamp {

  public static final long START_TIMESTAMP = 1480562618000L;
  private static final Logger LOG =
      LoggerFactory.getLogger(TsFileGeneratorForSeriesReaderByTimestamp.class);
  public static TsFileWriter innerWriter;
  public static String inputDataFile;
  public static String outputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("testTsFile.tsfile");
  public static String errorOutputDataFile;
  public static Schema schema;
  private static int rowCount;
  private static int chunkGroupSize;
  private static int pageSize;
  private static int preChunkGroupSize;
  private static int prePageSize;
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  public static void generateFile(int rc, int rs, int ps) throws IOException {
    rowCount = rc;
    chunkGroupSize = rs;
    pageSize = ps;
    prepare();
    write();
  }

  public static void prepare() throws IOException {
    inputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perTestInputData");
    errorOutputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perTestErrorOutputData.tsfile");
    generateTestData();
    generateSampleInputDataFile();
  }

  public static void after() {
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(preChunkGroupSize);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(prePageSize);
    File file = fsFactory.getFile(inputDataFile);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
    file = fsFactory.getFile(outputDataFile);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
    file = fsFactory.getFile(errorOutputDataFile);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
  }

  private static void generateSampleInputDataFile() throws IOException {
    File file = fsFactory.getFile(inputDataFile);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
    file.getParentFile().mkdirs();
    FileWriter fw = new FileWriter(file);

    long startTime = START_TIMESTAMP;
    for (int i = 0; i < rowCount; i += 2) {
      // write d1
      String d1 = "d1," + (startTime + i) + ",s1," + (i * 10 + 1) + ",s2," + (i * 10 + 2);
      if (i % 5 == 0) {
        d1 += ",s3," + (i * 10 + 3);
      }
      if (i % 8 == 0) {
        d1 += ",s4," + "dog" + i;
      }
      if (i % 9 == 0) {
        d1 += ",s5," + "false";
      }
      if (i % 10 == 0) {
        d1 += ",s6," + ((int) (i / 9.0) * 100) / 100.0;
      }
      if (i % 11 == 0) {
        d1 += ",s7," + ((int) (i / 10.0) * 100) / 100.0;
      }
      fw.write(d1 + "\r\n");

      // write d2
      String d2 = "d2," + (startTime + i) + ",s2," + (i * 10 + 2) + ",s3," + (i * 10 + 3);
      if (i % 20 < 5) {
        // LOG.info("write null to d2:" + (startTime + i));
        d2 = "d2," + (startTime + i) + ",s2,,s3," + (i * 10 + 3);
      }
      if (i % 5 == 0) {
        d2 += ",s1," + (i * 10 + 1);
      }
      if (i % 8 == 0) {
        d2 += ",s4," + "dog" + i % 4;
      }
      fw.write(d2 + "\r\n");
    }
    // write error
    String d =
        "d2,3,"
            + (startTime + rowCount)
            + ",s2,"
            + (rowCount * 10 + 2)
            + ",s3,"
            + (rowCount * 10 + 3);
    fw.write(d + "\r\n");
    d = "d2," + (startTime + rowCount + 1) + ",2,s-1," + (rowCount * 10 + 2);
    fw.write(d + "\r\n");
    fw.close();
  }

  public static void write() throws IOException {
    File file = fsFactory.getFile(outputDataFile);
    File errorFile = fsFactory.getFile(errorOutputDataFile);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
    if (errorFile.exists()) {
      Assert.assertTrue(errorFile.delete());
    }

    // LOG.info(jsonSchema.toString());
    preChunkGroupSize = TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();
    prePageSize = TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);
    innerWriter = new TsFileWriter(file, schema, TSFileDescriptor.getInstance().getConfig());

    // write
    try {
      writeToFile(schema);
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
    LOG.info("write to file successfully!!");
  }

  private static void generateTestData() {
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    schema = new Schema();
    schema.registerTimeseries(
        new Path("d1", "s1"),
        new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1", "s2"),
        new MeasurementSchema(
            "s2",
            TSDataType.INT64,
            TSEncoding.valueOf(conf.getValueEncoder()),
            CompressionType.UNCOMPRESSED));
    schema.registerTimeseries(
        new Path("d1", "s3"),
        new MeasurementSchema(
            "s3",
            TSDataType.INT64,
            TSEncoding.valueOf(conf.getValueEncoder()),
            CompressionType.SNAPPY));
    schema.registerTimeseries(
        new Path("d1", "s4"), new MeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));
    schema.registerTimeseries(
        new Path("d1", "s5"), new MeasurementSchema("s5", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    schema.registerTimeseries(
        new Path("d1", "s6"), new MeasurementSchema("s6", TSDataType.FLOAT, TSEncoding.RLE));
    schema.registerTimeseries(
        new Path("d1", "s7"), new MeasurementSchema("s7", TSDataType.DOUBLE, TSEncoding.RLE));
    schema.registerTimeseries(
        new Path("d2", "s1"),
        new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d2", "s2"),
        new MeasurementSchema(
            "s2",
            TSDataType.INT64,
            TSEncoding.valueOf(conf.getValueEncoder()),
            CompressionType.UNCOMPRESSED));
    schema.registerTimeseries(
        new Path("d2", "s3"),
        new MeasurementSchema(
            "s3",
            TSDataType.INT64,
            TSEncoding.valueOf(conf.getValueEncoder()),
            CompressionType.SNAPPY));
    schema.registerTimeseries(
        new Path("d2", "s4"), new MeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));
  }

  public static void writeToFile(Schema schema) throws IOException, WriteProcessException {
    Scanner in = getDataFile(inputDataFile);
    long lineCount = 0;
    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis();
    assert in != null;
    while (in.hasNextLine()) {
      if (lineCount % 1000000 == 0) {
        endTime = System.currentTimeMillis();
        // logger.info("write line:{},inner space consumer:{},use
        // time:{}",lineCount,innerWriter.calculateMemSizeForEachGroup(),endTime);
        LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
      }
      String str = in.nextLine();
      TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
      innerWriter.write(record);
      lineCount++;
    }
    endTime = System.currentTimeMillis();
    LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
    innerWriter.close();
    in.close();
    endTime = System.currentTimeMillis();
    LOG.info("write total:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
    LOG.info("src file size:{}GB", FileUtils.getLocalFileByte(inputDataFile, Unit.GB));
    LOG.info("src file size:{}MB", FileUtils.getLocalFileByte(outputDataFile, Unit.MB));
  }

  private static Scanner getDataFile(String path) {
    File file = fsFactory.getFile(path);
    try {
      Scanner in = new Scanner(file);
      return in;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }
}
