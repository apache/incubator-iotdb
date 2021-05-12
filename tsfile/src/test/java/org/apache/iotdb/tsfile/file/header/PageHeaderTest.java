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

package org.apache.iotdb.tsfile.file.header;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.utils.TestHelper;
import org.apache.iotdb.tsfile.file.metadata.utils.Utils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class PageHeaderTest {
  public static final int UNCOMPRESSED_SIZE = 123456;
  public static final int COMPRESSED_SIZE = 100000;
  public static final int NUM_OF_VALUES = 10000;
  public static final long MAX_TIMESTAMO = 523372036854775806L;
  public static final long MIN_TIMESTAMO = 423372036854775806L;
  public static final TSDataType DATA_TYPE = TSDataType.INT64;
  private final String PATH = TestConstant.BASE_OUTPUT_PATH.concat("outputPageHeader.tsfile");
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  @Before
  public void setUp() {}

  @After
  public void tearDown() {
    File file = fsFactory.getFile(PATH);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
  }

  @Test
  public void testWriteIntoFile() {
    PageHeader header = TestHelper.createTestPageHeader();
    serialized(header);
    PageHeader readHeader = deSerialized();
    Utils.isPageHeaderEqual(header, readHeader);
    serialized(readHeader);
  }

  private PageHeader deSerialized() {
    FileInputStream fis = null;
    PageHeader header = null;
    try {
      fis = new FileInputStream(fsFactory.getFile(PATH));
      header = PageHeader.deserializeFrom(fis, DATA_TYPE, true);
      return header;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return header;
  }

  private void serialized(PageHeader header) {
    File file = fsFactory.getFile(PATH);
    if (file.exists()) {
      Assert.assertTrue(file.delete());
    }
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      header.serializeTo(fos);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
