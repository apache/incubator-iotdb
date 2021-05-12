/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultDeviceTemplateTest {
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  @Test
  public void testUsingDefaultDeviceTemplate() throws IOException, WriteProcessException {
    File file = fsFactory.getFile("target/defaultDeviceTemplate.tsfile");
    try (TsFileWriter writer = new TsFileWriter(file)) {
      MeasurementSchema s1 = new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN);
      MeasurementSchema s2 = new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN);

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(s1);
      schemaList.add(s2);

      Map<String, IMeasurementSchema> schema = new HashMap<>();
      schema.put("s1", s1);
      schema.put("s2", s2);

      writer.registerDeviceTemplate("defaultTemplate", schema);

      Tablet tablet = new Tablet("d1", schemaList);
      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;

      long timestamp = 1;
      long value = 1L;

      for (int r = 0; r < 10; r++, value++) {
        int row = tablet.rowSize++;
        timestamps[row] = timestamp++;
        for (int i = 0; i < 2; i++) {
          long[] sensor = (long[]) values[i];
          sensor[row] = value;
        }
        // write Tablet to TsFile
        if (tablet.rowSize == tablet.getMaxRowNumber()) {
          writer.write(tablet);
          tablet.reset();
        }
      }
      // write Tablet to TsFile
      if (tablet.rowSize != 0) {
        writer.write(tablet);
        tablet.reset();
      }
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(file);
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)) {

      // use these paths(all measurements) for all the queries
      ArrayList<Path> paths = new ArrayList<>();
      paths.add(new Path("d1", "s1"));

      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      int count = 0;
      while (queryDataSet.hasNext()) {
        queryDataSet.next();
        count++;
      }

      Assert.assertEquals(10, count);
    }

    Files.deleteIfExists(file.toPath());
  }
}
