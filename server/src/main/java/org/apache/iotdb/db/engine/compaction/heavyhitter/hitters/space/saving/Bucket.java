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

package org.apache.iotdb.db.engine.compaction.heavyhitter.hitters.space.saving;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.LinkedList;

/**
 * A bucket is a holding object for a collection of topk entries in the Space Saving algorithm and
 * is used by {@link StreamSummary}. It is used in a double linked list and is maintained sorted
 * through the algorithm.
 *
 * @param <T> The type of the item
 */
class Bucket<T> {

  private LinkedList<Counter<T>> children = Lists.newLinkedList();

  private long value;

  /**
   * Constructor for a bucket.
   *
   * @param value The current value of the bucket
   */
  Bucket(long value) {
    this.value = value;
  }

  public LinkedList<Counter<T>> getChildren() {
    return children;
  }

  public Bucket setChildren(LinkedList<Counter<T>> children) {
    this.children = children;
    return this;
  }

  public long getValue() {
    return value;
  }

  public Bucket setValue(long value) {
    this.value = value;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Bucket<?> bucket = (Bucket<?>) o;
    return value == bucket.value && Objects.equal(children, bucket.children);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(children, value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("children", children)
        .add("value", value)
        .toString();
  }
}
