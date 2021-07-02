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

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeFuture.MainMergeFuture;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeFuture.SubMergeFuture;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.MergeMultiChunkTask.MergeChunkHeapTask;

import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MergeThreadPool extends ThreadPoolExecutor {

  public MergeThreadPool(int corePoolSize, ThreadFactory threadFactory) {
    super(
        corePoolSize,
        corePoolSize,
        0,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        threadFactory);
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
    if (callable instanceof CrossSpaceMergeTask) {
      return (RunnableFuture<T>) new MainMergeFuture((CrossSpaceMergeTask) callable);
    } else {
      return (RunnableFuture<T>) new SubMergeFuture((MergeChunkHeapTask) callable);
    }
  }
}
