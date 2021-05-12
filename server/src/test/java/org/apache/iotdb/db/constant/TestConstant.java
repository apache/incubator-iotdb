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

package org.apache.iotdb.db.constant;

import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;

import java.io.File;

public class TestConstant {

  public static final String BASE_OUTPUT_PATH = "target".concat(File.separator);
  public static final String OUTPUT_DATA_DIR =
      BASE_OUTPUT_PATH.concat("data").concat(File.separator);
  public static final FSType DEFAULT_TEST_FS = FSType.LOCAL;

  public static final String d0 = "root.vehicle.d0";
  public static final String s0 = "s0";
  public static final String s1 = "s1";
  public static final String s2 = "s2";
  public static final String s3 = "s3";
  public static final String s4 = "s4";
  public static final String s5 = "s5";
  public static final String d1 = "root.vehicle.d1";
  public static final String TIMESTAMP_STR = "Time";
  public static boolean testFlag = true;
  public static String[] stringValue = new String[] {"A", "B", "C", "D", "E"};
  public static String[] booleanValue = new String[] {"true", "false"};

  public static String[] create_sql =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s5 WITH DATATYPE=DOUBLE, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      };

  public static String insertTemplate = "insert into %s(timestamp%s) values(%d%s)";

  public static String first_value(String path) {
    return String.format("first_value(%s)", path);
  }

  public static String last_value(String path) {
    return String.format("last_value(%s)", path);
  }

  public static String sum(String path) {
    return String.format("sum(%s)", path);
  }

  public static String avg(String path) {
    return String.format("avg(%s)", path);
  }

  public static String count(String path) {
    return String.format("count(%s)", path);
  }

  public static String max_time(String path) {
    return String.format("max_time(%s)", path);
  }

  public static String min_time(String path) {
    return String.format("min_time(%s)", path);
  }

  public static String max_value(String path) {
    return String.format("max_value(%s)", path);
  }

  public static String min_value(String path) {
    return String.format("min_value(%s)", path);
  }

  public static String recordToInsert(TSRecord record) {
    StringBuilder measurements = new StringBuilder();
    StringBuilder values = new StringBuilder();
    for (DataPoint dataPoint : record.dataPointList) {
      measurements.append(",").append(dataPoint.getMeasurementId());
      values.append(",").append(dataPoint.getValue());
    }
    return String.format(insertTemplate, record.deviceId, measurements, record.time, values);
  }
}
