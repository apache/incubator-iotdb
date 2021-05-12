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
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.RecordUtils;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** test writing processing correction combining writing process and reading process. */
public class WriteTest {

  private static final Logger LOG = LoggerFactory.getLogger(WriteTest.class);
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private final int ROW_COUNT = 2000000;
  private TsFileWriter tsFileWriter;
  private String inputDataFile;
  private String outputDataFile;
  private String errorOutputDataFile;
  private Random rm = new Random();
  private ArrayList<MeasurementSchema> measurementArray;
  private ArrayList<Path> pathArray;
  private Schema schema;
  private int stageSize = 4;
  private int stageState = -1;
  private int prePageSize;
  private int prePageCheckThres;
  private TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();

  private String[][] stageDeviceIds = {{"d1", "d2", "d3"}, {"d1"}, {"d2", "d3"}};
  private String[] measurementIds = {"s0", "s1", "s2", "s3", "s4", "s5"};
  private long longBase = System.currentTimeMillis() * 1000;
  private String[] enums = {"MAN", "WOMAN"};

  @Before
  public void prepare() throws IOException {
    inputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("writeTestInputData");
    outputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("writeTestOutputData.tsfile");
    errorOutputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("writeTestErrorOutputData.tsfile");
    // for each row, flush page forcely
    prePageSize = conf.getPageSizeInByte();
    conf.setPageSizeInByte(0);
    prePageCheckThres = conf.getPageCheckSizeThreshold();
    conf.setPageCheckSizeThreshold(0);

    try {
      generateSampleInputDataFile();
    } catch (IOException e) {
      fail();
    }
    File file = fsFactory.getFile(outputDataFile);
    File errorFile = fsFactory.getFile(errorOutputDataFile);
    if (file.exists()) {
      file.delete();
    }
    if (errorFile.exists()) {
      errorFile.delete();
    }
    measurementArray = new ArrayList<>();
    measurementArray.add(new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.RLE));
    measurementArray.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.TS_2DIFF));
    HashMap<String, String> props = new HashMap<>();
    props.put("max_point_number", "2");
    measurementArray.add(
        new MeasurementSchema(
            "s2",
            TSDataType.FLOAT,
            TSEncoding.RLE,
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            props));
    props = new HashMap<>();
    props.put("max_point_number", "3");
    measurementArray.add(
        new MeasurementSchema(
            "s3",
            TSDataType.DOUBLE,
            TSEncoding.TS_2DIFF,
            TSFileDescriptor.getInstance().getConfig().getCompressor(),
            props));
    measurementArray.add(new MeasurementSchema("s4", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    pathArray = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      pathArray.add(new Path("d1", "s" + i));
    }
    schema = new Schema();
    LOG.info(schema.toString());
    tsFileWriter = new TsFileWriter(file, schema, conf);
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
  }

  @After
  public void end() {
    conf.setPageSizeInByte(prePageSize);
    conf.setPageCheckSizeThreshold(prePageCheckThres);
  }

  private void generateSampleInputDataFile() throws IOException {
    File file = fsFactory.getFile(inputDataFile);
    if (file.exists()) {
      file.delete();
    }
    FileWriter fw = new FileWriter(file);

    long startTime = System.currentTimeMillis();
    startTime = startTime - startTime % 1000;

    // first stage:int, long, float, double, boolean, enums
    try {
      for (int i = 0; i < ROW_COUNT; i++) {
        // write d1
        String d1 = "d1," + (startTime + i) + ",s1," + (i * 10 + 1) + ",s2," + (i * 10 + 2);
        if (rm.nextInt(1000) < 100) {
          d1 = "d1," + (startTime + i) + ",s1,,s2," + (i * 10 + 2) + ",s4,HIGH";
        }
        if (i % 5 == 0) {
          d1 += ",s3," + (i * 10 + 3);
        }
        fw.write(d1 + "\r\n");

        // write d2
        String d2 = "d2," + (startTime + i) + ",s2," + (i * 10 + 2) + ",s3," + (i * 10 + 3);
        if (rm.nextInt(1000) < 100) {
          d2 = "d2," + (startTime + i) + ",s2,,s3," + (i * 10 + 3) + ",s5,MAN";
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

  @Test
  public void writeTest() throws IOException {
    try {
      write();
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
    LOG.info("write processing has finished");
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(outputDataFile));
    TsFileMetadata metaData = reader.readFileMetadata();
  }

  public void write() throws IOException, WriteProcessException {
    long lineCount = 0;
    long startTime = System.currentTimeMillis();
    String[] strings;
    // add all measurement except the last one at before writing
    for (int i = 0; i < measurementArray.size() - 1; i++) {
      tsFileWriter.registerTimeseries(pathArray.get(i), measurementArray.get(i));
    }
    while (true) {
      if (lineCount % stageSize == 0) {
        LOG.info(
            "write line:{},use time:{}s",
            lineCount,
            (System.currentTimeMillis() - startTime) / 1000);
        stageState++;
        LOG.info("stage:" + stageState);
        if (stageState == stageDeviceIds.length) {
          break;
        }
      }
      if (lineCount == ROW_COUNT / 2) {
        tsFileWriter.registerTimeseries(
            pathArray.get(measurementArray.size() - 1),
            measurementArray.get(measurementArray.size() - 1));
      }
      strings = getNextRecord(lineCount, stageState);
      for (String str : strings) {
        TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
        tsFileWriter.write(record);
      }
      lineCount++;
    }
    // test duplicate measurement adding
    Path path = pathArray.get(measurementArray.size() - 1);
    MeasurementSchema dupTimeseries = measurementArray.get(measurementArray.size() - 1);
    try {
      tsFileWriter.registerTimeseries(path, dupTimeseries);
    } catch (WriteProcessException e) {
      assertEquals("given timeseries has exists! " + path, e.getMessage());
    }
    try {
      tsFileWriter.close();
    } catch (IOException e) {
      fail("close writer failed");
    }
    LOG.info("stage size: {}, write {} group data", stageSize, lineCount);
  }

  private String[] getNextRecord(long lineCount, int stage) {

    String[] ret = new String[stageDeviceIds[stage].length];
    for (int i = 0; i < ret.length; i++) {
      StringContainer sc = new StringContainer(JsonFormatConstant.TSRECORD_SEPARATOR);
      sc.addTail(stageDeviceIds[stage][i], lineCount);
      sc.addTail(
          measurementIds[0],
          lineCount * 10 + i,
          measurementIds[1],
          longBase + lineCount * 20 + i,
          measurementIds[2],
          (lineCount * 30 + i) / 3.0,
          measurementIds[3],
          (longBase + lineCount * 40 + i) / 7.0);
      sc.addTail(measurementIds[4], ((lineCount + i) & 1) == 0);
      sc.addTail(measurementIds[5], enums[(int) (lineCount + i) % enums.length]);
      ret[i] = sc.toString();
    }
    return ret;
  }
}
