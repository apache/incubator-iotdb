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

package org.apache.iotdb.tsfile.fileSystem;

import org.apache.iotdb.tsfile.constant.TestConstant;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

public class FSPathTest {

  @Test
  public void testParseUsingString() {
    FSPath fsPath;

    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    fsPath = FSPath.parse(pathWithFS);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(TestConstant.BASE_OUTPUT_PATH, fsPath.getPath());
    Assert.assertEquals(pathWithFS, fsPath.getRawFSPath());

    String pathWithOutFS = TestConstant.BASE_OUTPUT_PATH;
    fsPath = FSPath.parse(pathWithOutFS);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(TestConstant.BASE_OUTPUT_PATH, fsPath.getPath());
    Assert.assertEquals(pathWithFS, fsPath.getRawFSPath());
  }

  @Test
  public void testParseUsingFile() {
    File file = new File(TestConstant.BASE_OUTPUT_PATH);
    FSPath fsPath = FSPath.parse(file);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(file.getPath(), fsPath.getPath());
    Assert.assertEquals(
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + file.getPath(), fsPath.getRawFSPath());
  }

  @Test
  public void testToPath() {
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSPath fsPath = FSPath.parse(pathWithFS);
    Path expectedPath = new File(TestConstant.BASE_OUTPUT_PATH).toPath();
    Assert.assertEquals(expectedPath, fsPath.toPath());
  }

  @Test
  public void testToFile() {
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSPath fsPath = FSPath.parse(pathWithFS);
    File expectedFile = new File(TestConstant.BASE_OUTPUT_PATH);
    Assert.assertEquals(expectedFile, fsPath.toFile());
  }

  @Test
  public void testGetChildFile() {
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSPath fsPath = FSPath.parse(pathWithFS);
    String childFileName = "test.tsfile";
    File childFile = fsPath.getChildFile(childFileName);
    File expectedFile = new File(TestConstant.BASE_OUTPUT_PATH, childFileName);
    Assert.assertEquals(expectedFile, childFile);
  }

  @Test
  public void testPreConcat() {
    String fileName = "test.tsfile";
    String[] prefix = {TestConstant.BASE_OUTPUT_PATH};
    String pathWithFS = FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + fileName;
    FSPath fsPath = FSPath.parse(pathWithFS).preConcat(prefix);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(TestConstant.BASE_OUTPUT_PATH + fileName, fsPath.getPath());
    Assert.assertEquals(
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH + fileName,
        fsPath.getRawFSPath());
  }

  @Test
  public void testPostConcat() {
    String fileName = "test.tsfile";
    String[] suffix = {fileName};
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSPath fsPath = FSPath.parse(pathWithFS).postConcat(suffix);
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(TestConstant.BASE_OUTPUT_PATH + fileName, fsPath.getPath());
    Assert.assertEquals(
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH + fileName,
        fsPath.getRawFSPath());
  }

  @Test
  public void testGetAbsoluteFSPath() {
    File file = new File(TestConstant.BASE_OUTPUT_PATH);
    FSPath fsPath = FSPath.parse(file).getAbsoluteFSPath();
    Assert.assertEquals(FSType.LOCAL, fsPath.getFsType());
    Assert.assertEquals(file.getAbsolutePath(), fsPath.getPath());
    Assert.assertEquals(
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + file.getAbsolutePath(),
        fsPath.getRawFSPath());
  }
}
