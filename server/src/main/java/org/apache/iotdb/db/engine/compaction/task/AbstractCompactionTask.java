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

package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.engine.compaction.CompactionScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

import static org.apache.iotdb.db.engine.compaction.CompactionScheduler.currentTaskNum;

public abstract class AbstractCompactionTask implements Callable<Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCompactionTask.class);
  protected String fullStorageGroupName;
  protected long timePartition;

  public AbstractCompactionTask(String fullStorageGroupName, long timePartition) {
    this.fullStorageGroupName = fullStorageGroupName;
    this.timePartition = timePartition;
  }

  protected abstract void doCompaction() throws Exception;

  @Override
  public Void call() throws Exception {
    try {
      doCompaction();
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
    } finally {
      CompactionScheduler.currentTaskNum.decrementAndGet();
      LOGGER.warn("a compaction task is finished, currentTaskNum={}", currentTaskNum.get());
      CompactionScheduler.decPartitionCompaction(fullStorageGroupName, timePartition);
    }
    return null;
  }
}
