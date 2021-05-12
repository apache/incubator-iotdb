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

import org.apache.iotdb.flink.tsfile.RowRowRecordParser;
import org.apache.iotdb.flink.tsfile.TsFileInputFormat;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/** The example of reading TsFile via Flink DataStream API. */
public class FlinkTsFileStreamSource {

  public static void main(String[] args) {
    String path = "test.tsfile";
    TsFileUtils.writeTsFile(new File(path));
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
    List<Path> paths =
        Arrays.stream(filedNames)
            .filter(s -> !s.equals(QueryConstant.RESERVED_TIME))
            .map(s -> new Path(s, true))
            .collect(Collectors.toList());
    RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, filedNames);
    QueryExpression queryExpression = QueryExpression.create(paths, null);
    RowRowRecordParser parser =
        RowRowRecordParser.create(rowTypeInfo, queryExpression.getSelectedSeries());
    TsFileInputFormat<Row> inputFormat = new TsFileInputFormat<>(queryExpression, parser);
    StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    inputFormat.setFilePath("source.tsfile");
    DataStream<Row> source = senv.createInput(inputFormat);
    DataStream<String> rowString = source.map(Row::toString);
    Iterator<String> result = DataStreamUtils.collect(rowString);
    while (result.hasNext()) {
      System.out.println(result.next());
    }
  }
}
