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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TsFileWriteToHDFS {

  private static TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(TsFileWriteToHDFS.class);

  public static void main(String[] args) {
    config.setTSFileStorageFs(new FSType[] {FSType.HDFS});

    String path = "hdfs://localhost:9000/test.tsfile";
    File f = new FSPath(FSType.HDFS, path).toFile();
    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      tsFileWriter.registerTimeseries(
          new Path(Constant.DEVICE_1, Constant.SENSOR_1),
          new MeasurementSchema(Constant.SENSOR_1, TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter.registerTimeseries(
          new Path(Constant.DEVICE_1, Constant.SENSOR_2),
          new MeasurementSchema(Constant.SENSOR_2, TSDataType.INT64, TSEncoding.RLE));
      tsFileWriter.registerTimeseries(
          new Path(Constant.DEVICE_1, Constant.SENSOR_3),
          new MeasurementSchema(Constant.SENSOR_3, TSDataType.INT64, TSEncoding.RLE));

      // construct TSRecord
      for (int i = 0; i < 100; i++) {
        TSRecord tsRecord = new TSRecord(i, Constant.DEVICE_1);
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
      logger.error("Failed to write TsFile on HDFS. {}", e.getMessage());
    }
  }
}
