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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Tests for TSRecordOutputFormat */
public class RowTSRecordOutputFormatTest extends RowTsFileOutputFormatTestBase {

  @Test
  public void testWriteData() throws IOException {
    String outputDirPath = tmpDir + File.separator + "testOutput";
    new File(outputDirPath).mkdirs();
    TSRecordOutputFormat<Row> outputFormat = prepareTSRecordOutputFormat(outputDirPath);

    try {
      outputFormat.configure(new Configuration());
      outputFormat.open(0, 2);
      List<Row> data = prepareData();
      for (Row row : data) {
        outputFormat.writeRecord(row);
      }
    } finally {
      outputFormat.close();
    }

    String[] actual = readTsFile(new File(outputDirPath + File.separator + "1.tsfile"), paths);
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

  @Test
  public void testGetter() {
    String outputFilePath = tmpDir + File.separator + "test.tsfile";
    TSRecordOutputFormat<Row> outputFormat = prepareTSRecordOutputFormat(outputFilePath);

    assertEquals(rowTSRecordConverter, outputFormat.getConverter());
    assertEquals(schema, outputFormat.getSchema());
    assertEquals(config, outputFormat.getConfig().get());
  }
}
