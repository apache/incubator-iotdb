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
package org.apache.iotdb.hadoop.tsfile;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileHelper {

  private static final Logger logger = LoggerFactory.getLogger(TsFileHelper.class);

  public static boolean deleteTsFile(String filePath) {
    File file = new File(filePath);
    return file.delete();
  }

  public static void writeTsFile(String filePath) {

    try {
      File file = new File(filePath);

      if (file.exists()) {
        file.delete();
      }

      Schema schema = new Schema();

      List<IMeasurementSchema> schemaList = new ArrayList<>();

      // the number of rows to include in the tablet
      int rowNum = 1000000;
      // the number of values to include in the tablet
      int sensorNum = 10;

      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < 2; i++) {
        MeasurementSchema measurementSchema =
            new MeasurementSchema(
                Constant.SENSOR_PREFIX + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
        schema.registerTimeseries(
            new Path(Constant.DEVICE_1, Constant.SENSOR_PREFIX + (i + 1)), measurementSchema);
        schemaList.add(measurementSchema);
      }

      for (int i = 2; i < sensorNum; i++) {
        IMeasurementSchema measurementSchema =
            new MeasurementSchema(
                Constant.SENSOR_PREFIX + (i + 1), TSDataType.DOUBLE, TSEncoding.TS_2DIFF);
        schema.registerTimeseries(
            new Path(Constant.DEVICE_1, Constant.SENSOR_PREFIX + (i + 1)), measurementSchema);
        schemaList.add(measurementSchema);
      }

      // add measurements into TSFileWriter
      TsFileWriter tsFileWriter = new TsFileWriter(file, schema);

      // construct the tablet
      Tablet tablet = new Tablet(Constant.DEVICE_1, schemaList);

      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;

      long timestamp = 1;
      long value = 1000000L;
      double doubleValue = 1.1;

      try {
        for (int r = 0; r < rowNum; r++, value++, doubleValue = doubleValue + 0.1) {
          int row = tablet.rowSize++;
          timestamps[row] = timestamp++;
          for (int i = 0; i < 2; i++) {
            long[] sensor = (long[]) values[i];
            sensor[row] = value;
          }
          for (int i = 2; i < sensorNum; i++) {
            double[] sensor = (double[]) values[i];
            sensor[row] = doubleValue;
          }
          // write Tablet to TsFile
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            tsFileWriter.write(tablet);
            tablet.reset();
          }
        }
        // write Tablet to TsFile
        if (tablet.rowSize != 0) {
          tsFileWriter.write(tablet);
          tablet.reset();
        }
      } finally {
        tsFileWriter.close();
      }

    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }

  public static void main(String[] args) throws IOException {
    String filePath = "example_mr.tsfile";
    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
    writeTsFile(filePath);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file)) {
      logger.info("Get file meta data: {}", reader.readFileMetadata());
    }
  }
}
