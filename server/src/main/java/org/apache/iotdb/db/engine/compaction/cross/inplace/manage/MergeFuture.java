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

import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.MergeMultiChunkTask.MergeChunkHeapTask;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public abstract class MergeFuture extends FutureTask<Void> implements Comparable<MergeFuture> {

  private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss" + ".SSS'Z'");
  private Date createdTime;

  public MergeFuture(Callable callable) {
    super(callable);
    createdTime = new Date(System.currentTimeMillis());
  }

  public String getCreatedTime() {
    return dateFormat.format(createdTime);
  }

  public abstract String getTaskName();

  public abstract String getProgress();

  @Override
  public int compareTo(MergeFuture future) {
    return this.getTaskName().compareTo(future.getTaskName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MergeFuture future = (MergeFuture) o;
    return Objects.equals(getTaskName(), future.getTaskName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(createdTime);
  }

  public static class MainMergeFuture extends MergeFuture {

    private CrossSpaceMergeTask bindingTask;

    public MainMergeFuture(CrossSpaceMergeTask task) {
      super(task);
      bindingTask = task;
    }

    @Override
    public String getTaskName() {
      return bindingTask.getTaskName();
    }

    @Override
    public String getProgress() {
      return bindingTask.getProgress();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      MainMergeFuture that = (MainMergeFuture) o;
      return Objects.equals(bindingTask, that.bindingTask);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), bindingTask);
    }
  }

  public static class SubMergeFuture extends MergeFuture {

    private MergeChunkHeapTask bindingTask;

    public SubMergeFuture(MergeChunkHeapTask task) {
      super(task);
      bindingTask = task;
    }

    @Override
    public String getTaskName() {
      return bindingTask.getTaskName();
    }

    @Override
    public String getProgress() {
      return bindingTask.getProgress();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      SubMergeFuture that = (SubMergeFuture) o;
      return Objects.equals(bindingTask, that.bindingTask);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), bindingTask);
    }
  }
}
