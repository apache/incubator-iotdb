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
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.fail;

public class FileReaderManagerTest {

  private static final int MAX_FILE_SIZE = 10;

  private IoTDBConfig dbConfig = IoTDBDescriptor.getInstance().getConfig();
  private long cacheFileReaderClearPeriod;

  @Before
  public void setUp() {
    cacheFileReaderClearPeriod = dbConfig.getCacheFileReaderClearPeriod();
    dbConfig.setCacheFileReaderClearPeriod(3000);
  }

  @After
  public void tearDown() {
    dbConfig.setCacheFileReaderClearPeriod(cacheFileReaderClearPeriod);
  }

  @Test
  public void test() throws IOException, InterruptedException {

    String filePath = TestConstant.BASE_OUTPUT_PATH.concat("test.file");

    FileReaderManager manager = FileReaderManager.getInstance();
    QueryFileManager testManager = new QueryFileManager();

    TsFileResource[] tsFileResources = new TsFileResource[MAX_FILE_SIZE + 1];

    for (int i = 1; i <= MAX_FILE_SIZE; i++) {
      File file = SystemFileFactory.INSTANCE.getFile(filePath + i);
      file.createNewFile();
      tsFileResources[i] = new TsFileResource(file);
    }

    Thread t1 =
        new Thread(
            () -> {
              try {
                testManager.addQueryId(1L);

                for (int i = 1; i <= 6; i++) {
                  TsFileResource tsFile = tsFileResources[i];
                  testManager.addFilePathToMap(1L, tsFile, false);
                  manager.get(tsFile.getTsFile(), false);
                  Assert.assertTrue(manager.contains(tsFile, false));
                }
                for (int i = 1; i <= 6; i++) {
                  TsFileResource tsFile = tsFileResources[i];
                  manager.decreaseFileReaderReference(tsFile, false);
                }

              } catch (IOException e) {
                e.printStackTrace();
              }
            });
    t1.start();

    Thread t2 =
        new Thread(
            () -> {
              try {
                testManager.addQueryId(2L);

                for (int i = 4; i <= MAX_FILE_SIZE; i++) {
                  TsFileResource tsFile = tsFileResources[i];
                  testManager.addFilePathToMap(2L, tsFile, false);
                  manager.get(tsFile.getTsFile(), false);
                  Assert.assertTrue(manager.contains(tsFile, false));
                }
                for (int i = 4; i <= MAX_FILE_SIZE; i++) {
                  TsFileResource tsFile = tsFileResources[i];
                  manager.decreaseFileReaderReference(tsFile, false);
                }

              } catch (IOException e) {
                e.printStackTrace();
              }
            });
    t2.start();

    t1.join();
    t2.join();

    for (int i = 1; i <= MAX_FILE_SIZE; i++) {
      TsFileResource tsFile = new TsFileResource(SystemFileFactory.INSTANCE.getFile(filePath + i));
      Assert.assertTrue(manager.contains(tsFile, false));
    }

    // the code below is not valid because the cacheFileReaderClearPeriod config in this class is
    // not valid

    // TimeUnit.SECONDS.sleep(5);
    //
    // for (int i = 1; i <= MAX_FILE_SIZE; i++) {
    //
    // if (i == 4 || i == 5 || i == 6) {
    // Assert.assertTrue(manager.contains(filePath + i));
    // } else {
    // Assert.assertFalse(manager.contains(filePath + i));
    // }
    // }

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    for (int i = 1; i < MAX_FILE_SIZE; i++) {
      File file = SystemFileFactory.INSTANCE.getFile(filePath + i);
      boolean result = !file.exists() || file.delete();
      if (!result) {
        fail();
      }
    }
  }
}
