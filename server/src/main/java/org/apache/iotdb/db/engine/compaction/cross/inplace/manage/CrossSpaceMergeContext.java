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

package org.apache.iotdb.db.engine.compaction.cross.inplace.manage;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** CrossSpaceMergeContext records the shared information between merge sub-tasks. */
public class CrossSpaceMergeContext {

  private Map<TsFileResource, Integer> mergedChunkCnt = new HashMap<>();
  private Map<TsFileResource, Integer> unmergedChunkCnt = new HashMap<>();
  private Map<TsFileResource, Map<PartialPath, List<Long>>> unmergedChunkStartTimes =
      new HashMap<>();

  private AtomicInteger totalChunkWritten = new AtomicInteger();
  private AtomicLong totalPointWritten = new AtomicLong();

  public void clear() {
    mergedChunkCnt.clear();
    unmergedChunkCnt.clear();
    unmergedChunkStartTimes.clear();
  }

  public Map<TsFileResource, Integer> getMergedChunkCnt() {
    return mergedChunkCnt;
  }

  public void setMergedChunkCnt(Map<TsFileResource, Integer> mergedChunkCnt) {
    this.mergedChunkCnt = mergedChunkCnt;
  }

  public Map<TsFileResource, Integer> getUnmergedChunkCnt() {
    return unmergedChunkCnt;
  }

  public void setUnmergedChunkCnt(Map<TsFileResource, Integer> unmergedChunkCnt) {
    this.unmergedChunkCnt = unmergedChunkCnt;
  }

  public Map<TsFileResource, Map<PartialPath, List<Long>>> getUnmergedChunkStartTimes() {
    return unmergedChunkStartTimes;
  }

  public void setUnmergedChunkStartTimes(
      Map<TsFileResource, Map<PartialPath, List<Long>>> unmergedChunkStartTimes) {
    this.unmergedChunkStartTimes = unmergedChunkStartTimes;
  }

  public int getTotalChunkWritten() {
    return totalChunkWritten.get();
  }

  public void incTotalChunkWritten() {
    this.totalChunkWritten.incrementAndGet();
  }

  public void incTotalPointWritten(long increment) {
    totalPointWritten.addAndGet(increment);
  }

  public long getTotalPointWritten() {
    return totalPointWritten.get();
  }
}
