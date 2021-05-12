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

package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.FileGenerator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class GetAllDevicesTest {

  private final TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
  private int maxDegreeOfIndexNode;
  private static final String FILE_PATH = FileGenerator.outputDataFile;
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  @Before
  public void before() {
    maxDegreeOfIndexNode = conf.getMaxDegreeOfIndexNode();
    conf.setMaxDegreeOfIndexNode(3);
  }

  @After
  public void after() {
    FileGenerator.after();
    conf.setMaxDegreeOfIndexNode(maxDegreeOfIndexNode);
  }

  @Test
  public void testGetAllDevices1() throws IOException {
    testGetAllDevices(2, 2);
  }

  @Test
  public void testGetAllDevices2() throws IOException {
    testGetAllDevices(2, 50);
  }

  @Test
  public void testGetAllDevices3() throws IOException {
    testGetAllDevices(50, 2);
  }

  @Test
  public void testGetAllDevices4() throws IOException {
    testGetAllDevices(50, 50);
  }

  public void testGetAllDevices(int deviceNum, int measurementNum) throws IOException {
    FileGenerator.generateFile(10000, deviceNum, measurementNum);
    try (TsFileSequenceReader fileReader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH))) {

      List<String> devices = fileReader.getAllDevices();
      Assert.assertEquals(deviceNum, devices.size());
      for (int i = 0; i < deviceNum; i++) {
        Assert.assertTrue(devices.contains("d" + i));
      }

      FileGenerator.after();
    }
  }
}
