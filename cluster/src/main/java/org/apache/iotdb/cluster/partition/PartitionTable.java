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

package org.apache.iotdb.cluster.partition;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;

import org.apache.commons.collections4.map.MultiKeyMap;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * PartitionTable manages the map whose key is the StorageGroupName with a time interval and the
 * value is a PartitionGroup witch contains all nodes that manage the corresponding data.
 */
public interface PartitionTable {

  /**
   * Given the fullStorageGroupName and the timestamp, return the list of nodes on which the storage
   * group and the corresponding time interval is managed.
   *
   * @param storageGroupName
   * @param timestamp
   * @return
   */
  PartitionGroup route(String storageGroupName, long timestamp);

  /**
   * Given the storageGroupName and the timestamp, return the header RaftNode of the partitionGroup
   * by which the storage group and the corresponding time interval is managed.
   *
   * @param storageGroupName
   * @param timestamp
   * @return
   */
  RaftNode routeToHeaderByTime(String storageGroupName, long timestamp);

  /**
   * Add a new node to update the partition table.
   *
   * @param node
   */
  void addNode(Node node);

  NodeAdditionResult getNodeAdditionResult(Node node);

  /**
   * Remove a node and update the partition table.
   *
   * @param node
   */
  void removeNode(Node node);

  /**
   * Get the result after remove node, include removedGroupList and newSlotOwners.
   *
   * @return result after remove node.
   */
  NodeRemovalResult getNodeRemovalResult();

  /**
   * @return All data groups where all VNodes of this node is the header. The first index indicates
   *     the VNode and the second index indicates the data group of one VNode.
   */
  List<PartitionGroup> getLocalGroups();

  /**
   * @param raftNode
   * @return the partition group starting from the header.
   */
  PartitionGroup getHeaderGroup(RaftNode raftNode);

  ByteBuffer serialize();

  /**
   * Deserialize partition table and check whether the partition table in byte buffer is valid
   *
   * @param buffer
   * @return true if the partition table is valid
   */
  boolean deserialize(ByteBuffer buffer);

  List<Node> getAllNodes();

  List<PartitionGroup> getGlobalGroups();

  List<PartitionGroup> calculateGlobalGroups(List<Node> nodeRing);

  /** get the last meta log index that modifies the partition table */
  long getLastMetaLogIndex();

  /** set the last meta log index that modifies the partition table */
  void setLastMetaLogIndex(long index);

  /**
   * @param path can be an incomplete path (but should contain a storage group name) e.g., if
   *     "root.sg" is a storage group, then path can not be "root".
   * @param timestamp
   * @return
   * @throws StorageGroupNotSetException
   */
  default PartitionGroup partitionByPathTime(PartialPath path, long timestamp)
      throws MetadataException {
    PartialPath storageGroup = IoTDB.metaManager.getStorageGroupPath(path);
    return this.route(storageGroup.getFullPath(), timestamp);
  }

  /**
   * Get partition info by path and range time
   *
   * @return (startTime, endTime) - partitionGroup pair @UsedBy NodeTool
   */
  default MultiKeyMap<Long, PartitionGroup> partitionByPathRangeTime(
      PartialPath path, long startTime, long endTime) throws MetadataException {
    long partitionInterval = StorageEngine.getTimePartitionInterval();

    MultiKeyMap<Long, PartitionGroup> timeRangeMapRaftGroup = new MultiKeyMap<>();
    PartialPath storageGroup = IoTDB.metaManager.getStorageGroupPath(path);
    startTime = StorageEngine.convertMilliWithPrecision(startTime);
    endTime = StorageEngine.convertMilliWithPrecision(endTime);
    while (startTime <= endTime) {
      long nextTime = (startTime / partitionInterval + 1) * partitionInterval;
      timeRangeMapRaftGroup.put(
          startTime,
          Math.min(nextTime - 1, endTime),
          this.route(storageGroup.getFullPath(), startTime));
      startTime = nextTime;
    }
    return timeRangeMapRaftGroup;
  }
}
