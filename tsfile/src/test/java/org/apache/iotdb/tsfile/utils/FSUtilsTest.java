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

package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

public class FSUtilsTest {

  @Test
  public void testGetFSTypeUsingFile() {
    File file = new File(TestConstant.BASE_OUTPUT_PATH);
    FSType fsType = FSUtils.getFSType(file);
    Assert.assertEquals(FSType.LOCAL, fsType);
  }

  @Test
  public void testGetFSTypeUsingPath() {
    Path path = new File(TestConstant.BASE_OUTPUT_PATH).toPath();
    FSType fsType = FSUtils.getFSType(path);
    Assert.assertEquals(FSType.LOCAL, fsType);
  }

  @Test
  public void testGetFSTypeUsingRawFSPath() {
    String pathWithFS =
        FSType.LOCAL.name() + FSPath.FS_PATH_SEPARATOR + TestConstant.BASE_OUTPUT_PATH;
    FSType fsType = FSUtils.getFSType(pathWithFS);
    Assert.assertEquals(FSType.LOCAL, fsType);
  }
}
