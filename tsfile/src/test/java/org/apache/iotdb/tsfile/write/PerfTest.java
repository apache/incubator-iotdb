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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.RecordUtils;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.gson.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

/**
 * This is used for performance test, no asserting. User could change {@code ROW_COUNT} for larger
 * data test.
 */
public class PerfTest {

  public static final int ROW_COUNT = 1000; // 0000;
  private static final Logger LOG = LoggerFactory.getLogger(PerfTest.class);
  public static TsFileWriter innerWriter;
  public static String inputDataFile;
  public static String outputDataFile;
  public static String errorOutputDataFile;
  public static Schema schema;
  public static Random rm = new Random();
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  private static void generateSampleInputDataFile() throws IOException {
    File file = fsFactory.getFile(inputDataFile);
    if (file.exists()) {
      file.delete();
    }
    FileWriter fw = new FileWriter(file);

    long startTime = System.currentTimeMillis();
    startTime = startTime - startTime % 1000;
    try {
      for (int i = 0; i < ROW_COUNT; i++) {
        String string4 = ",s4," + (char) (97 + i % 26);
        // write d1
        String d1 =
            "d1," + (startTime + i) + ",s1," + (i * 10 + 1) + ",s2," + (i * 10 + 2) + string4;
        if (rm.nextInt(1000) < 100) {
          // LOG.info("write null to d1:" + (startTime + i));
          d1 = "d1," + (startTime + i) + ",s1,,s2," + (i * 10 + 2) + string4;
        }
        if (i % 5 == 0) {
          d1 += ",s3," + (i * 10 + 3);
        }
        fw.write(d1 + "\r\n");

        // write d2
        String d2 =
            "d2," + (startTime + i) + ",s2," + (i * 10 + 2) + ",s3," + (i * 10 + 3) + string4;
        if (rm.nextInt(1000) < 100) {
          // LOG.info("write null to d2:" + (startTime + i));
          d2 = "d2," + (startTime + i) + ",s2,,s3," + (i * 10 + 3) + string4;
        }
        if (i % 5 == 0) {
          d2 += ",s1," + (i * 10 + 1);
        }
        fw.write(d2 + "\r\n");
      }
      // write error
      String d =
          "d2,3,"
              + (startTime + ROW_COUNT)
              + ",s2,"
              + (ROW_COUNT * 10 + 2)
              + ",s3,"
              + (ROW_COUNT * 10 + 3);
      fw.write(d + "\r\n");
      d = "d2," + (startTime + ROW_COUNT + 1) + ",2,s-1," + (ROW_COUNT * 10 + 2);
      fw.write(d + "\r\n");
    } finally {
      fw.close();
    }
  }

  private static void write() throws IOException, InterruptedException {
    File file = fsFactory.getFile(outputDataFile);
    File errorFile = fsFactory.getFile(errorOutputDataFile);
    if (file.exists()) {
      file.delete();
    }
    if (errorFile.exists()) {
      errorFile.delete();
    }

    // TSFileDescriptor.conf.chunkGroupSize = 2000;
    // TSFileDescriptor.conf.pageSizeInByte = 100;
    innerWriter = new TsFileWriter(file, schema, TSFileDescriptor.getInstance().getConfig());

    // write
    try {
      writeToFile(schema);
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
    LOG.info("write to file successfully!!");
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

  private static void writeToFile(Schema schema)
      throws InterruptedException, IOException, WriteProcessException {
    Scanner in = getDataFile(inputDataFile);
    assert in != null;
    while (in.hasNextLine()) {
      String str = in.nextLine();
      TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
      innerWriter.write(record);
    }
    innerWriter.close();
  }

  private static Schema generateTestData() {
    Schema schema = new Schema();
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    schema.registerTimeseries(
        new Path("d1", "s1"),
        new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1", "s2"),
        new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1", "s3"),
        new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d1", "s4"), new MeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));
    schema.registerTimeseries(
        new Path("d2", "s1"),
        new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d2", "s2"),
        new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d2", "s3"),
        new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.valueOf(conf.getValueEncoder())));
    schema.registerTimeseries(
        new Path("d2", "s4"), new MeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));

    JsonObject s4 = new JsonObject();
    s4.addProperty(JsonFormatConstant.MEASUREMENT_UID, "s4");
    s4.addProperty(JsonFormatConstant.DATA_TYPE, TSDataType.TEXT.toString());
    s4.addProperty(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.PLAIN.toString());
    return schema;
  }

  @Before
  public void prepare() throws IOException {
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    // set global log level
    ch.qos.logback.classic.Logger logger = loggerContext.getLogger("root");
    logger.setLevel(Level.toLevel("info"));

    inputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perTestInputData");
    outputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perTestOutputData.tsfile");
    errorOutputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perTestErrorOutputData.tsfile");
    schema = generateTestData();
    generateSampleInputDataFile();
  }

  @After
  public void after() {
    File file = fsFactory.getFile(inputDataFile);
    if (file.exists()) {
      file.delete();
    }
    file = fsFactory.getFile(outputDataFile);
    if (file.exists()) {
      file.delete();
    }
    file = fsFactory.getFile(errorOutputDataFile);
    if (file.exists()) {
      file.delete();
    }
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    // set global log level
    ch.qos.logback.classic.Logger logger = loggerContext.getLogger("root");
    logger.setLevel(Level.toLevel("info"));
  }

  @Test
  public void writeTest() throws IOException, InterruptedException, WriteProcessException {
    write();
  }
}
