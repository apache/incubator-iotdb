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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.OpenFileNumUtil.OpenFileNumStatistics;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class OpenFileNumUtilTest {

  private static final Logger logger = LoggerFactory.getLogger(OpenFileNumUtilTest.class);
  private static final String TEST_FILE_PREFIX = "testFileForOpenFileNumUtil";
  private static final String MAC_OS_NAME = "mac";
  private static final String LINUX_OS_NAME = "linux";
  private static final int UNSUPPORTED_OS_ERROR_CODE = -2;
  private OpenFileNumUtil openFileNumUtil = OpenFileNumUtil.getInstance();
  private ArrayList<File> fileList = new ArrayList<>();
  private ArrayList<FileWriter> fileWriterList = new ArrayList<>();
  private String testFileName;
  private int totalOpenFileNumBefore;
  private int totalOpenFileNumAfter;
  private int totalOpenFileNumChange;
  private int testFileNum = 66;
  private String currDir;
  private File testDataDirRoot;
  private String os = System.getProperty("os.name").toLowerCase();

  private static int getProcessID() {
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    return Integer.parseInt(runtimeMXBean.getName().split("@")[0]);
  }

  @Before
  public void setUp() {
    int testProcessID = getProcessID();
    openFileNumUtil.setPid(testProcessID);
    FSPath dataFilePath = OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM.getPath().get(0);
    String userDir = System.getProperty("user.dir");
    currDir = userDir + File.separator + TestConstant.BASE_OUTPUT_PATH + testProcessID;
    testDataDirRoot = new File(currDir);
    currDir = currDir + File.separator + dataFilePath.getPath();
    File testDataDir = new File(currDir);
    if (!testDataDir.isDirectory() && !testDataDir.mkdirs()) {
      logger.error("Create test file dir {} failed.", testDataDir.getPath());
    }
    testFileName = TEST_FILE_PREFIX + testProcessID;
  }

  @After
  public void tearDown() {
    // close FileWriter
    for (FileWriter fw : fileWriterList) {
      try {
        fw.close();
      } catch (IOException e) {
        logger.error(e.getMessage());
      }
    }

    // delete test files
    for (File file : fileList) {
      if (file.exists()) {
        try {
          Files.delete(file.toPath());
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
      }
    }
    fileWriterList.clear();
    fileList.clear();
    try {
      FileUtils.deleteDirectory(testDataDirRoot);
    } catch (IOException e) {
      logger.error("Delete test data dir {} failed.", testDataDirRoot);
    }
  }

  @Test
  public void testDataOpenFileNumWhenCreateFile() {
    if (os.startsWith(MAC_OS_NAME) || os.startsWith(LINUX_OS_NAME)) {
      // get total open file number under /data/data before create new files
      totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
      for (int i = 0; i < testFileNum; i++) {
        fileList.add(new File(currDir + testFileName + i));
      }
      // create testFileNum File, then get total open file number statistics
      totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
      totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
      // create test file shall not affect total open file number statistics
      // if 'lsof' command is valid
      if (openFileNumUtil.isCommandValid()) {
        assertEquals(0, totalOpenFileNumChange);
      } else {
        assertEquals(UNSUPPORTED_OS_ERROR_CODE, totalOpenFileNumBefore);
        assertEquals(UNSUPPORTED_OS_ERROR_CODE, totalOpenFileNumAfter);
      }
    } else {
      assertEquals(
          UNSUPPORTED_OS_ERROR_CODE,
          openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM));
    }
  }

  @Test
  public void testDataOpenFileNumWhenCreateFileWriter() {
    if (os.startsWith(MAC_OS_NAME) || os.startsWith(LINUX_OS_NAME)) {
      for (int i = 0; i < testFileNum; i++) {
        fileList.add(new File(currDir + testFileName + i));
      }
      totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
      for (File file : fileList) {
        if (file.exists()) {
          try {
            fileWriterList.add(new FileWriter(file));
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        } else {
          try {
            boolean flag = file.createNewFile();
            if (flag) {
              logger.debug("Create a file {} successfully", file);
              fileWriterList.add(new FileWriter(file));
            } else {
              logger.error("create test file {} failed when creating file writer.", file.getPath());
            }
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        }
      }
      totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
      totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
      // create FileWriter shall cause total open file number increase by testFileNum
      if (openFileNumUtil.isCommandValid()) {
        assertEquals(testFileNum, totalOpenFileNumChange);
      } else {
        assertEquals(UNSUPPORTED_OS_ERROR_CODE, totalOpenFileNumBefore);
        assertEquals(UNSUPPORTED_OS_ERROR_CODE, totalOpenFileNumAfter);
      }
    } else {
      assertEquals(
          UNSUPPORTED_OS_ERROR_CODE,
          openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM));
    }
  }

  @Test
  public void testDataOpenFileNumWhenFileWriterWriting() {
    logger.debug("testDataOpenFileNumWhenFileWriterWriting...");
    if (os.startsWith(MAC_OS_NAME) || os.startsWith(LINUX_OS_NAME)) {
      for (int i = 0; i < testFileNum; i++) {
        fileList.add(new File(currDir + testFileName + i));
      }
      for (File file : fileList) {
        if (file.exists()) {
          try {
            fileWriterList.add(new FileWriter(file));
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        } else {
          try {
            if (!file.createNewFile()) {
              logger.error("create test file {} failed.", file.getPath());
            }
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
          try {
            fileWriterList.add(new FileWriter(file));
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        }
      }
      totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
      for (FileWriter fw : fileWriterList) {
        try {
          fw.write("this is a test file for open file number counting.");
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
      }
      totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
      totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
      // writing test file shall not affect total open file number statistics
      if (openFileNumUtil.isCommandValid()) {
        assertEquals(0, totalOpenFileNumChange);
      } else {
        assertEquals(UNSUPPORTED_OS_ERROR_CODE, totalOpenFileNumBefore);
        assertEquals(UNSUPPORTED_OS_ERROR_CODE, totalOpenFileNumAfter);
      }
    } else {
      assertEquals(
          UNSUPPORTED_OS_ERROR_CODE,
          openFileNumUtil.get(OpenFileNumStatistics.SEQUENCE_FILE_OPEN_NUM));
    }
  }

  @Test
  public void testDataOpenFileNumWhenFileWriterClose() {
    logger.debug("testDataOpenFileNumWhenFileWriterClose...");
    if (os.startsWith(MAC_OS_NAME) || os.startsWith(LINUX_OS_NAME)) {
      for (int i = 0; i < testFileNum; i++) {
        fileList.add(new File(currDir + testFileName + i));
      }
      for (File file : fileList) {
        if (file.exists()) {
          try {
            fileWriterList.add(new FileWriter(file));
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        } else {
          try {
            if (!file.createNewFile()) {
              logger.error("create test file {} failed.", file.getPath());
            }
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
          try {
            fileWriterList.add(new FileWriter(file));
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        }
      }
      for (FileWriter fw : fileWriterList) {
        try {
          fw.write("this is a test file for open file number counting.");
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
      }
      totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
      for (FileWriter fw : fileWriterList) {
        try {
          fw.close();
        } catch (IOException e) {
          logger.error(e.getMessage());
        }
      }
      totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM);
      totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
      // close FileWriter shall cause total open file number decrease by testFileNum
      if (openFileNumUtil.isCommandValid()) {
        assertEquals(-testFileNum, totalOpenFileNumChange);
      } else {
        assertEquals(UNSUPPORTED_OS_ERROR_CODE, totalOpenFileNumBefore);
        assertEquals(UNSUPPORTED_OS_ERROR_CODE, totalOpenFileNumAfter);
      }
    } else {
      assertEquals(
          UNSUPPORTED_OS_ERROR_CODE,
          openFileNumUtil.get(OpenFileNumStatistics.DIGEST_OPEN_FILE_NUM));
    }
  }
}
