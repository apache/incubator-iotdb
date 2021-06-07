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
import com.google.common.base.Preconditions;

/**
 * A counter is a holding object for a specific topk entry in the Space Saving algorithm and is used
 * by {@link StreamSummary}
 *
 * @param <T> The type of the item
 */
public class Counter<T> {

  /** The current value of the counter */
  private long value;

  /** The current error of the value. Minus this from the counter to get the "true counter" */
  private long error;

  /**
   * Owning bucket parent. This is used so that we can detach ourselves easily given the algorithm
   * in StreamSummary
   */
  private DoubleLinkedList.Node<Bucket<T>> bucket;

  /** The item we are counting */
  private T item;

  /** Constructor that initializes value & error to 0 */
  Counter(DoubleLinkedList.Node<Bucket<T>> bucket) {
    this.bucket = Preconditions.checkNotNull(bucket);
    this.value = 0;
    this.error = 0;
    item = null;
  }

  public long getValue() {
    return value;
  }

  public Counter setValue(long value) {
    this.value = value;
    return this;
  }

  public long getError() {
    return error;
  }

  public Counter setError(long error) {
    this.error = error;
    return this;
  }

  public DoubleLinkedList.Node<Bucket<T>> getBucket() {
    return bucket;
  }

  public Counter setBucket(DoubleLinkedList.Node<Bucket<T>> bucket) {
    this.bucket = bucket;
    return this;
  }

  public T getItem() {
    return item;
  }

  public Counter setItem(T item) {
    this.item = item;
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
    Counter<?> counter = (Counter<?>) o;
    return value == counter.value
        && error == counter.error
        && Objects.equal(bucket, counter.bucket)
        && Objects.equal(item, counter.item);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value, error, bucket, item);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("value", value)
        .add("error", error)
        .add("bucket", bucket)
        .add("item", item)
        .toString();
  }
}
