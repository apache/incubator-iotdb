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

import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;

import java.util.Objects;
import java.util.Set;

public class SlotTimePartitionFilter implements TimePartitionFilter {

  private Set<Integer> slotSet;

  public SlotTimePartitionFilter(Set<Integer> slotSet) {
    this.slotSet = slotSet;
  }

  @Override
  public boolean satisfy(String storageGroupName, long timePartitionId) {
    int slot =
        SlotPartitionTable.getSlotStrategy()
            .calculateSlotByPartitionNum(
                storageGroupName, timePartitionId, ClusterConstant.SLOT_NUM);
    return slotSet.contains(slot);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof SlotTimePartitionFilter
        && Objects.equals(((SlotTimePartitionFilter) obj).slotSet, slotSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(slotSet);
  }
}
