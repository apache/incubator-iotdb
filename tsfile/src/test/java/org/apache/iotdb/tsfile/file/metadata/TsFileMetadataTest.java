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
package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.constant.TestConstant;
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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class TsFileMetadataTest {

  final String PATH = TestConstant.BASE_OUTPUT_PATH.concat("output1.tsfile");
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  @Before
  public void setUp() {}

  @After
  public void tearDown() {
    File file = fsFactory.getFile(PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testWriteFileMetaData() {
    TsFileMetadata tsfMetaData = TestHelper.createSimpleFileMetaData();
    serialized(tsfMetaData);
    TsFileMetadata readMetaData = deSerialized();
    Assert.assertTrue(Utils.isFileMetaDataEqual(tsfMetaData, readMetaData));
  }

  private TsFileMetadata deSerialized() {
    FileInputStream fileInputStream = null;
    TsFileMetadata metaData = null;
    try {
      fileInputStream = new FileInputStream(fsFactory.getFile(PATH));
      FileChannel channel = fileInputStream.getChannel();
      ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
      channel.read(buffer);
      buffer.rewind();
      metaData = TsFileMetadata.deserializeFrom(buffer);
      return metaData;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fileInputStream != null) {
        try {
          fileInputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return metaData;
  }

  private void serialized(TsFileMetadata metaData) {
    File file = fsFactory.getFile(PATH);
    if (file.exists()) {
      file.delete();
    }
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      metaData.serializeTo(fos);
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
