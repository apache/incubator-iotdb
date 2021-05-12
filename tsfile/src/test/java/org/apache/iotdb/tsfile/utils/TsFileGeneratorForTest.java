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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.junit.Assert;
import org.junit.Ignore;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Scanner;

@Ignore
public class TsFileGeneratorForTest {

  public static final long START_TIMESTAMP = 1480562618000L;
  private static String inputDataFile;
  public static String outputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("testTsFile.tsfile");
  private static String errorOutputDataFile;
  private static int rowCount;
  private static int chunkGroupSize;
  private static int pageSize;
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  public static void generateFile(int rowCount, int chunkGroupSize, int pageSize)
      throws IOException {
    generateFile(rowCount, rowCount, chunkGroupSize, pageSize);
  }

  public static void generateFile(
      int minRowCount, int maxRowCount, int chunkGroupSize, int pageSize) throws IOException {
    TsFileGeneratorForTest.rowCount = maxRowCount;
    TsFileGeneratorForTest.chunkGroupSize = chunkGroupSize;
    TsFileGeneratorForTest.pageSize = pageSize;
    prepare(minRowCount, maxRowCount);
    write();
  }

  public static void prepare(int minrowCount, int maxRowCount) throws IOException {
    inputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perTestInputData");
    errorOutputDataFile = TestConstant.BASE_OUTPUT_PATH.concat("perTestErrorOutputData.tsfile");
    generateSampleInputDataFile(minrowCount, maxRowCount);
  }

  public static void after() {
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

  private static void generateSampleInputDataFile(int minRowCount, int maxRowCount)
      throws IOException {
    File file = fsFactory.getFile(inputDataFile);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
    file.getParentFile().mkdirs();
    FileWriter fw = new FileWriter(file);

    long startTime = START_TIMESTAMP;
    try {
      for (int i = 0; i < maxRowCount; i++) {
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
        if (i % 10 == 0 && i < minRowCount) {
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
    } finally {
      fw.close();
    }
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

    Schema schema = generateTestSchema();

    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(chunkGroupSize);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(pageSize);

    // write
    try (TsFileWriter innerWriter =
            new TsFileWriter(file, schema, TSFileDescriptor.getInstance().getConfig());
        Scanner in = new Scanner(fsFactory.getFile(inputDataFile))) {
      while (in.hasNextLine()) {
        String str = in.nextLine();
        TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
        innerWriter.write(record);
      }
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
  }

  private static Schema generateTestSchema() {
    Schema schema = new Schema();
    schema.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
    schema.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
    schema.registerTimeseries(
        new Path("d1", "s3"), new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.TS_2DIFF));
    schema.registerTimeseries(
        new Path("d1", "s4"),
        new MeasurementSchema(
            "s4",
            TSDataType.TEXT,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            Collections.singletonMap(Encoder.MAX_STRING_LENGTH, "20")));
    schema.registerTimeseries(
        new Path("d1", "s5"), new MeasurementSchema("s5", TSDataType.BOOLEAN, TSEncoding.RLE));
    schema.registerTimeseries(
        new Path("d1", "s6"),
        new MeasurementSchema(
            "s6",
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            Collections.singletonMap(Encoder.MAX_POINT_NUMBER, "5")));
    schema.registerTimeseries(
        new Path("d1", "s7"),
        new MeasurementSchema("s7", TSDataType.DOUBLE, TSEncoding.GORILLA_V1));

    schema.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
    schema.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
    schema.registerTimeseries(
        new Path("d2", "s3"), new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.TS_2DIFF));
    schema.registerTimeseries(
        new Path("d2", "s4"),
        new MeasurementSchema(
            "s4",
            TSDataType.TEXT,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            Collections.singletonMap(Encoder.MAX_STRING_LENGTH, "20")));
    return schema;
  }

  /**
   * Writes a File with one incomplete chunk header
   *
   * @param file File to write
   * @throws IOException is thrown when encountering IO issues
   */
  public static void writeFileWithOneIncompleteChunkHeader(File file) throws IOException {
    TsFileWriter writer = new TsFileWriter(file);

    ChunkHeader header =
        new ChunkHeader("s1", 100, TSDataType.FLOAT, CompressionType.SNAPPY, TSEncoding.PLAIN, 5);
    ByteBuffer buffer = ByteBuffer.allocate(header.getSerializedSize());
    header.serializeTo(buffer);
    buffer.flip();
    byte[] data = new byte[3];
    buffer.get(data, 0, 3);
    writer.getIOWriter().getIOWriterOut().write(data);
    writer.getIOWriter().close();
  }
}
