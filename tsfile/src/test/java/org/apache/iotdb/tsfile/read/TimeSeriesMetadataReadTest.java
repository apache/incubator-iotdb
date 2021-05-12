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
package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.FileGenerator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TimeSeriesMetadataReadTest {

  private static final String FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TimeSeriesMetadataReadTest.tsfile");
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private final TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
  private int maxDegreeOfIndexNode;

  @Before
  public void before() throws IOException {
    int rowCount = 100;
    maxDegreeOfIndexNode = conf.getMaxDegreeOfIndexNode();
    conf.setMaxDegreeOfIndexNode(3);
    FileGenerator.generateFile(rowCount, 10000, FILE_PATH);
  }

  @After
  public void after() {
    FileGenerator.after();
    conf.setMaxDegreeOfIndexNode(maxDegreeOfIndexNode);
  }

  @Test
  public void testReadTimeseriesMetadata() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH));
    Path path = new Path("d1", "s1");
    Set<String> set = new HashSet<>();
    set.add("s1");
    set.add("s2");
    set.add("s3");
    // the Max Degree Of Index Node is set to be 3, so the leaf node should only contains 3 sensors
    // s4 should not be returned as result
    set.add("s4");
    List<TimeseriesMetadata> timeseriesMetadataList = reader.readTimeseriesMetadata(path, set);
    Assert.assertEquals(3, timeseriesMetadataList.size());
    for (int i = 1; i <= timeseriesMetadataList.size(); i++) {
      Assert.assertEquals("s" + i, timeseriesMetadataList.get(i - 1).getMeasurementId());
    }

    path = new Path("d1", "s5");
    set.clear();
    set.add("s5");
    set.add("s6");
    // this is a fake one, this file doesn't contain this measurement
    // so the result is not supposed to contain this measurement's timeseries metadata
    set.add("s8");
    timeseriesMetadataList = reader.readTimeseriesMetadata(path, set);
    Assert.assertEquals(2, timeseriesMetadataList.size());
    for (int i = 5; i < 7; i++) {
      Assert.assertEquals("s" + i, timeseriesMetadataList.get(i - 5).getMeasurementId());
    }
  }
}
