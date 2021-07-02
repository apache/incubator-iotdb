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

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.MergeMultiChunkTask;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.Test;

import java.util.PriorityQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MergeManagerTest extends MergeTest {

  @Test
  public void testRateLimiter() {
    RateLimiter compactionRateLimiter = MergeManager.getINSTANCE().getMergeWriteRateLimiter();
    long startTime = System.currentTimeMillis();
    MergeManager.mergeRateLimiterAcquire(compactionRateLimiter, 160 * 1024 * 1024L);
    assertTrue((System.currentTimeMillis() - startTime) <= 1000);
    MergeManager.mergeRateLimiterAcquire(compactionRateLimiter, 16 * 1024 * 1024L);
    assertTrue((System.currentTimeMillis() - startTime) >= 9000);
  }

  @Test
  public void testGenMergeReport() {
    FakedMergeMultiChunkTask chunkTask = new FakedMergeMultiChunkTask();
    for (int i = 0; i < 5; i++) {
      MergeManager.getINSTANCE().submitMainTask(new FakedMainMergeTask(i));
      MergeManager.getINSTANCE().submitChunkSubTask(chunkTask.createSubTask(i));
    }

    String report = MergeManager.getINSTANCE().genMergeTaskReport();
    checkReport(report);
  }

  @Test
  public void testAbortMerge() {
    FakedMergeMultiChunkTask chunkTask = new FakedMergeMultiChunkTask();
    for (int i = 0; i < 5; i++) {
      MergeManager.getINSTANCE().submitMainTask(new FakedMainMergeTask(i));
      MergeManager.getINSTANCE().submitChunkSubTask(chunkTask.createSubTask(i));
    }

    MergeManager.getINSTANCE().abortMerge("non-exist");
    String report = MergeManager.getINSTANCE().genMergeTaskReport();

    checkReport(report);

    MergeManager.getINSTANCE().abortMerge("test");
    report = MergeManager.getINSTANCE().genMergeTaskReport();
    assertEquals(String.format("Main tasks:%n" + "Sub tasks:%n"), report);
  }

  private void checkReport(String report) {
    String[] split = report.split(System.lineSeparator());
    assertEquals("Main tasks:", split[0]);
    assertEquals("\tStorage group: test", split[1]);
    for (int i = 0; i < 5; i++) {
      assertTrue(split[2 + i].contains("task" + i));
      assertTrue(split[2 + i].contains("0,"));
      assertTrue(split[2 + i].contains("done:false"));
      assertTrue(split[2 + i].contains("cancelled:false"));
    }
    assertEquals("Sub tasks:", split[7]);
    assertEquals("\tStorage group: test", split[8]);
    for (int i = 0; i < 5; i++) {
      assertTrue(split[9 + i].contains("task" + i));
      assertTrue(split[9 + i].contains("0,"));
      assertTrue(split[9 + i].contains("done:false"));
      assertTrue(split[9 + i].contains("cancelled:false"));
    }
  }

  static class FakedMainMergeTask extends CrossSpaceMergeTask {

    private int serialNum;
    private String progress = "0";

    public FakedMainMergeTask(int serialNum) {
      super(null, null, null, null, false, 0, null);
      this.serialNum = serialNum;
    }

    @Override
    public Void call() {
      while (!Thread.currentThread().isInterrupted()) {
        // wait until interrupt
      }
      progress = "1";
      return null;
    }

    @Override
    public String getStorageGroupName() {
      return "test";
    }

    @Override
    public String getProgress() {
      return progress;
    }

    @Override
    public String getTaskName() {
      return "task" + serialNum;
    }
  }

  static class FakedMergeMultiChunkTask extends MergeMultiChunkTask {

    public FakedMergeMultiChunkTask() {
      super(null, null, null, null, false, null, 0, null);
    }

    public MergeChunkHeapTask createSubTask(int serialNum) {
      return new FakedSubMergeTask(serialNum);
    }

    class FakedSubMergeTask extends MergeChunkHeapTask {

      private int serialNum;
      private String progress = "0";

      public FakedSubMergeTask(int serialNum) {
        super(new PriorityQueue<>(), null, null, null, null, null, null, false, serialNum);
        this.serialNum = serialNum;
      }

      @Override
      public Void call() {
        while (!Thread.currentThread().isInterrupted()) {
          // wait until interrupt
        }
        progress = "1";
        return null;
      }

      @Override
      public String getStorageGroupName() {
        return "test";
      }

      @Override
      public String getProgress() {
        return progress;
      }

      @Override
      public String getTaskName() {
        return "task" + serialNum;
      }
    }
  }
}
