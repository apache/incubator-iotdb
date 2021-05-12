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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.TsFileManagement.CompactionMergeTask;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.COMPACTION_LOG_NAME;
import static org.junit.Assert.assertFalse;

public class LevelCompactionLogTest extends LevelCompactionTest {

  File tempSGDir;
  boolean compactionMergeWorking = false;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = fsFactory.getFile(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testCompactionLog() {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    levelCompactionTsFileManagement.addAll(seqResources, true);
    levelCompactionTsFileManagement.addAll(unseqResources, false);
    levelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        levelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.call();
    while (compactionMergeWorking) {
      // wait
    }
    File logFile = fsFactory.getFile(tempSGDir.getPath(), COMPACTION_TEST_SG + COMPACTION_LOG_NAME);
    assertFalse(logFile.exists());
  }

  /** close compaction merge callback, to release some locks */
  private void closeCompactionMergeCallBack(
      boolean isMergeExecutedInCurrentTask, long timePartitionId) {
    this.compactionMergeWorking = false;
  }
}
