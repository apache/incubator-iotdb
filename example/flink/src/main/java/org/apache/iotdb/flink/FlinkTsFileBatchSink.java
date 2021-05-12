/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.flink;

import org.apache.iotdb.flink.tsfile.RowTSRecordConverter;
import org.apache.iotdb.flink.tsfile.TSRecordOutputFormat;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** The example of writing to TsFile via Flink DataSet API. */
public class FlinkTsFileBatchSink {

  public static final String DEFAULT_TEMPLATE = "template";

  public static void main(String[] arg) throws Exception {
    String path = new File("test.tsfile").getAbsolutePath();
    new File(path).deleteOnExit();
    String[] filedNames = {
      QueryConstant.RESERVED_TIME,
      "device_1.sensor_1",
      "device_1.sensor_2",
      "device_1.sensor_3",
      "device_2.sensor_1",
      "device_2.sensor_2",
      "device_2.sensor_3"
    };
    TypeInformation[] typeInformations =
        new TypeInformation[] {
          Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG
        };
    RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, filedNames);
    Schema schema = new Schema();
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor_1", TSDataType.INT64, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor_2", TSDataType.INT64, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor_3", TSDataType.INT64, TSEncoding.TS_2DIFF));
    RowTSRecordConverter converter = new RowTSRecordConverter(rowTypeInfo);
    TSRecordOutputFormat<Row> outputFormat = new TSRecordOutputFormat<>(schema, converter);

    List<Tuple7> data = new ArrayList<>(7);
    data.add(new Tuple7(1L, 2L, 3L, 4L, 5L, 6L, 7L));
    data.add(new Tuple7(2L, 3L, 4L, 5L, 6L, 7L, 8L));
    data.add(new Tuple7(3L, 4L, 5L, 6L, 7L, 8L, 9L));
    data.add(new Tuple7(4L, 5L, 6L, 7L, 8L, 9L, 10L));
    data.add(new Tuple7(6L, 6L, 7L, 8L, 9L, 10L, 11L));
    data.add(new Tuple7(7L, 7L, 8L, 9L, 10L, 11L, 12L));
    data.add(new Tuple7(8L, 8L, 9L, 10L, 11L, 12L, 13L));

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    // If the parallelism > 1, flink create a directory and each subtask will create a tsfile under
    // the directory.
    env.setParallelism(1);
    DataSet<Tuple7> source =
        env.fromCollection(
            data,
            Types.TUPLE(
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.LONG,
                Types.LONG));
    source
        .map(
            t -> {
              Row row = new Row(7);
              for (int i = 0; i < 7; i++) {
                row.setField(i, t.getField(i));
              }
              return row;
            })
        .returns(rowTypeInfo)
        .write(outputFormat, path);

    env.execute();

    List<Path> paths =
        Arrays.stream(filedNames)
            .filter(s -> !s.equals(QueryConstant.RESERVED_TIME))
            .map(s -> new Path(s, true))
            .collect(Collectors.toList());
    String[] result = TsFileUtils.readTsFile(new File(path), paths);
    for (String row : result) {
      System.out.println(row);
    }
  }
}
