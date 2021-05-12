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
package org.apache.iotdb.spark.tool;

import java.io.File;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * Write an example TsFile as shown in README.
 */
public class TsFileExample {

  public static void create(String tsfilePath) throws Exception {
    File f = new File(tsfilePath);
    if (f.exists()) {
      f.delete();
    }
    TsFileWriter tsFileWriter = new TsFileWriter(f);

    // add measurements into file schema
    tsFileWriter
        .registerTimeseries(new Path("root.ln.wf01.wt01", "status"),
            new MeasurementSchema("status", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    tsFileWriter
        .registerTimeseries(new Path("root.ln.wf01.wt01", "temperature"),
            new MeasurementSchema("temperature", TSDataType.FLOAT, TSEncoding.RLE));
    tsFileWriter
        .registerTimeseries(new Path("root.ln.wf02.wt02", "temperature"),
            new MeasurementSchema("temperature", TSDataType.FLOAT, TSEncoding.RLE));
    tsFileWriter
        .registerTimeseries(new Path("root.ln.wf02.wt02", "hardware"),
            new MeasurementSchema("hardware", TSDataType.TEXT, TSEncoding.PLAIN));

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1, "root.ln.wf01.wt01");
    DataPoint dPoint1 = new FloatDataPoint("temperature", 2.2f);
    DataPoint dPoint2 = new BooleanDataPoint("status", true);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "root.ln.wf01.wt01");
    dPoint1 = new FloatDataPoint("temperature", 2.2f);
    tsRecord.addTuple(dPoint1);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(3, "root.ln.wf01.wt01");
    dPoint1 = new BooleanDataPoint("status", true);
    dPoint2 = new FloatDataPoint("temperature", 2.1f);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(5, "root.ln.wf01.wt01");
    dPoint1 = new BooleanDataPoint("status", false);
    tsRecord.addTuple(dPoint1);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(1, "root.ln.wf02.wt02");
    dPoint1 = new BooleanDataPoint("status", true);
    tsRecord.addTuple(dPoint1);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "root.ln.wf02.wt02");
    dPoint1 = new BooleanDataPoint("status", false);
    dPoint2 = new StringDataPoint("hardware", new Binary("aaa"));
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(4, "root.ln.wf02.wt02");
    dPoint1 = new BooleanDataPoint("status", true);
    dPoint2 = new StringDataPoint("hardware", new Binary("bbb"));
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(6, "root.ln.wf02.wt02");
    dPoint1 = new StringDataPoint("hardware", new Binary("ccc"));
    tsRecord.addTuple(dPoint1);
    tsFileWriter.write(tsRecord);

    // close TsFile
    tsFileWriter.close();
  }

}
