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
package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.ForceAppendTsFileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TsFileForceAppendWrite {

  private static final Logger logger = LoggerFactory.getLogger(TsFileForceAppendWrite.class);
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory(Constant.DEFAULT_FS);

  public static void main(String[] args) throws IOException {
    String path = "test.tsfile";
    File f = fsFactory.getFile(path);
    if (f.exists()) {
      Files.delete(f.toPath());
    }
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {

      // add measurements into file schema
      for (int i = 0; i < 4; i++) {
        tsFileWriter.registerTimeseries(
            new Path(Constant.DEVICE_PREFIX + i, Constant.SENSOR_1),
            new MeasurementSchema(Constant.SENSOR_1, TSDataType.INT64, TSEncoding.RLE));
        tsFileWriter.registerTimeseries(
            new Path(Constant.DEVICE_PREFIX + i, Constant.SENSOR_2),
            new MeasurementSchema(Constant.SENSOR_2, TSDataType.INT64, TSEncoding.RLE));
        tsFileWriter.registerTimeseries(
            new Path(Constant.DEVICE_PREFIX + i, Constant.SENSOR_3),
            new MeasurementSchema(Constant.SENSOR_3, TSDataType.INT64, TSEncoding.RLE));
      }

      // construct TSRecord
      for (int i = 0; i < 100; i++) {
        TSRecord tsRecord = new TSRecord(i, Constant.DEVICE_PREFIX + (i % 4));
        DataPoint dPoint1 = new LongDataPoint(Constant.SENSOR_1, i);
        DataPoint dPoint2 = new LongDataPoint(Constant.SENSOR_2, i);
        DataPoint dPoint3 = new LongDataPoint(Constant.SENSOR_3, i);
        tsRecord.addTuple(dPoint1);
        tsRecord.addTuple(dPoint2);
        tsRecord.addTuple(dPoint3);

        // write TSRecord
        tsFileWriter.write(tsRecord);
      }
    } catch (Exception e) {
      logger.error("meet error in TsFileWrite ", e);
    }

    // open the closed file with ForceAppendTsFileWriter
    ForceAppendTsFileWriter fwriter = new ForceAppendTsFileWriter(f);
    fwriter.doTruncate();
    try (TsFileWriter tsFileWriter1 = new TsFileWriter(fwriter)) {
      // add measurements into file schema
      for (int i = 0; i < 4; i++) {
        tsFileWriter1.registerTimeseries(
            new Path(Constant.DEVICE_PREFIX + i, Constant.SENSOR_1),
            new MeasurementSchema(Constant.SENSOR_1, TSDataType.INT64, TSEncoding.RLE));
        tsFileWriter1.registerTimeseries(
            new Path(Constant.DEVICE_PREFIX + i, Constant.SENSOR_2),
            new MeasurementSchema(Constant.SENSOR_2, TSDataType.INT64, TSEncoding.RLE));
        tsFileWriter1.registerTimeseries(
            new Path(Constant.DEVICE_PREFIX + i, Constant.SENSOR_3),
            new MeasurementSchema(Constant.SENSOR_3, TSDataType.INT64, TSEncoding.RLE));
      }
      // construct TSRecord
      for (int i = 100; i < 120; i++) {
        TSRecord tsRecord = new TSRecord(i, Constant.DEVICE_PREFIX + (i % 4));
        DataPoint dPoint1 = new LongDataPoint(Constant.SENSOR_1, i);
        DataPoint dPoint2 = new LongDataPoint(Constant.SENSOR_2, i);
        DataPoint dPoint3 = new LongDataPoint(Constant.SENSOR_3, i);
        tsRecord.addTuple(dPoint1);
        tsRecord.addTuple(dPoint2);
        tsRecord.addTuple(dPoint3);

        // write TSRecord
        tsFileWriter1.write(tsRecord);
      }
    } catch (Exception e) {
      logger.error("meet error in TsFileWrite ", e);
    }
    fwriter.close();
  }
}
