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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertArrayEquals;

/** ITCases for TSRecordOutputFormat. */
public class RowTSRecordOutputFormatITCase extends RowTsFileOutputFormatTestBase {

  @Test
  public void testOutputFormat() throws Exception {
    DataSet<Row> source = prepareDataSource();
    String outputFilePath = tmpDir + File.separator + "test.tsfile";
    TSRecordOutputFormat<Row> outputFormat = prepareTSRecordOutputFormat(outputFilePath);

    source.output(outputFormat).setParallelism(1);
    env.execute();

    String[] actual = readTsFile(new File(outputFilePath), paths);
    String[] expected = {
      "1,1.2,20,null,2.3,11,19",
      "2,null,20,50,25.4,10,21",
      "3,1.4,21,null,null,null,null",
      "4,1.2,20,51,null,null,null",
      "6,7.2,10,11,null,null,null",
      "7,6.2,20,21,null,null,null",
      "8,9.2,30,31,null,null,null"
    };
    assertArrayEquals(actual, expected);
  }
}
