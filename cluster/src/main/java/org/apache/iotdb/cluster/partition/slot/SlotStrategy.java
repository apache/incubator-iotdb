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

package org.apache.iotdb.cluster.partition.slot;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.tsfile.utils.Murmur128Hash;

import static org.apache.iotdb.cluster.config.ClusterConstant.HASH_SALT;

/** SlotStrategy determines how a {fullStorageGroupName, value} pair is distributed to a slot. */
public interface SlotStrategy {
  int calculateSlotByTime(String storageGroupName, long timestamp, int maxSlotNum);

  int calculateSlotByPartitionNum(String storageGroupName, long partitionId, int maxSlotNum);

  class DefaultStrategy implements SlotStrategy {

    @Override
    public int calculateSlotByTime(String storageGroupName, long timestamp, int maxSlotNum) {
      long partitionNum = StorageEngine.getTimePartition(timestamp);
      return calculateSlotByPartitionNum(storageGroupName, partitionNum, maxSlotNum);
    }

    @Override
    public int calculateSlotByPartitionNum(
        String storageGroupName, long partitionId, int maxSlotNum) {
      int hash = Murmur128Hash.hash(storageGroupName, partitionId, HASH_SALT);
      return Math.abs(hash % maxSlotNum);
    }
  }
}
