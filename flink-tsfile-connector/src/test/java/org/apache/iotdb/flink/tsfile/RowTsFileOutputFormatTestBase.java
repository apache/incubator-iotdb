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

package org.apache.iotdb.flink.tsfile;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.types.Row;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.flink.util.TsFileWriteUtil.DEFAULT_TEMPLATE;

/** Base class for TsFileOutputFormat tests. */
public abstract class RowTsFileOutputFormatTestBase extends RowTsFileConnectorTestBase {

  protected ExecutionEnvironment env;
  protected RowTSRecordConverter rowTSRecordConverter;
  protected Schema schema;

  @Before
  public void prepareEnv() {
    env = ExecutionEnvironment.getExecutionEnvironment();
  }

  protected TSRecordOutputFormat<Row> prepareTSRecordOutputFormat(String path) {
    schema = new Schema();
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new MeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));
    rowTSRecordConverter = new RowTSRecordConverter(rowTypeInfo);
    return new TSRecordOutputFormat<>(path, schema, rowTSRecordConverter, config);
  }

  protected List<Row> prepareData() {
    List<Tuple7> tuples = new ArrayList<>(7);
    tuples.add(new Tuple7(1L, 1.2f, 20, null, 2.3f, 11, 19));
    tuples.add(new Tuple7(2L, null, 20, 50, 25.4f, 10, 21));
    tuples.add(new Tuple7(3L, 1.4f, 21, null, null, null, null));
    tuples.add(new Tuple7(4L, 1.2f, 20, 51, null, null, null));
    tuples.add(new Tuple7(6L, 7.2f, 10, 11, null, null, null));
    tuples.add(new Tuple7(7L, 6.2f, 20, 21, null, null, null));
    tuples.add(new Tuple7(8L, 9.2f, 30, 31, null, null, null));

    return tuples.stream()
        .map(
            t -> {
              Row row = new Row(7);
              for (int i = 0; i < 7; i++) {
                row.setField(i, t.getField(i));
              }
              return row;
            })
        .collect(Collectors.toList());
  }

  protected DataSet<Row> prepareDataSource() {
    List<Tuple7> input = new ArrayList<>(7);
    input.add(new Tuple7(1L, 1.2f, 20, null, 2.3f, 11, 19));
    input.add(new Tuple7(2L, null, 20, 50, 25.4f, 10, 21));
    input.add(new Tuple7(3L, 1.4f, 21, null, null, null, null));
    input.add(new Tuple7(4L, 1.2f, 20, 51, null, null, null));
    input.add(new Tuple7(6L, 7.2f, 10, 11, null, null, null));
    input.add(new Tuple7(7L, 6.2f, 20, 21, null, null, null));
    input.add(new Tuple7(8L, 9.2f, 30, 31, null, null, null));
    return env.fromCollection(prepareData(), rowTypeInfo);
  }

  protected String[] readTsFile(File tsFile, List<Path> paths) throws IOException {
    QueryExpression expression = QueryExpression.create(paths, null);
    TsFileSequenceReader reader = new TsFileSequenceReader(tsFile);
    ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
    QueryDataSet queryDataSet = readTsFile.query(expression);
    List<String> result = new ArrayList<>();
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      String row =
          rowRecord.getFields().stream()
              .map(f -> f == null ? "null" : f.getStringValue())
              .collect(Collectors.joining(","));
      result.add(rowRecord.getTimestamp() + "," + row);
    }
    return result.toArray(new String[0]);
  }
}
