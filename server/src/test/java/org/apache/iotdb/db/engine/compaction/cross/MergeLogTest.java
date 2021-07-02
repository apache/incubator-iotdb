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

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MergeLogTest extends MergeTest {

  File tempSGDir;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testMergeLog() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(Integer.MAX_VALUE);
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources.subList(0, 1), unseqResources.subList(0, 1)),
            tempSGDir.getPath(),
            this::testCallBack,
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();
  }

  private void testCallBack(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, File mergeLog) {
    int lineCnt = 0;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(mergeLog))) {
      while (bufferedReader.readLine() != null) {
        lineCnt++;
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertEquals(309, lineCnt);
  }
}
