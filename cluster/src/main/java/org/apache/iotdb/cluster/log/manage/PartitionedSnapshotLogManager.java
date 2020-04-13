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

package org.apache.iotdb.cluster.log.manage;

import org.apache.iotdb.cluster.log.*;
import org.apache.iotdb.cluster.log.snapshot.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.snapshot.SnapshotFactory;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

/**
 * PartitionedSnapshotLogManager provides a PartitionedSnapshot as snapshot, dividing each log to
 * a sub-snapshot according to its slot and stores timeseries schemas of each slot.
 */
public abstract class PartitionedSnapshotLogManager<T extends Snapshot> extends RaftLogManager {

  private static final Logger logger = LoggerFactory.getLogger(PartitionedSnapshotLogManager.class);

  //TODO
  Map<Integer, T> slotSnapshots = new HashMap<>();
  private SnapshotFactory factory;
  Map<Integer, Collection<MeasurementSchema>> slotTimeseries = new HashMap<>();
  long snapshotLastLogId;
  long snapshotLastLogTerm;
  PartitionTable partitionTable;
  Node header;
  Node thisNode;


  public PartitionedSnapshotLogManager(LogApplier logApplier, PartitionTable partitionTable,
      Node header, Node thisNode, SnapshotFactory<T> factory) {
    super(new CommittedEntryManager(), new StableEntryManager(), logApplier);
    this.partitionTable = partitionTable;
    this.header = header;
    this.factory = factory;
    this.thisNode = thisNode;
  }

  @Override
  public Snapshot getSnapshot() {
    // copy snapshots
    synchronized (slotSnapshots) {
      PartitionedSnapshot partitionedSnapshot = new PartitionedSnapshot(factory);
      for (Entry<Integer, T> entry : slotSnapshots.entrySet()) {
        partitionedSnapshot.putSnapshot(entry.getKey(), entry.getValue());
      }
      partitionedSnapshot.setLastLogIndex(snapshotLastLogId);
      partitionedSnapshot.setLastLogTerm(snapshotLastLogTerm);
      return partitionedSnapshot;
    }
  }

  void collectTimeseriesSchemas() {
    slotTimeseries.clear();
    // TODO-Cluster#349: the collection is re-collected each time to prevent inconsistency when some of
    //  them are removed during two snapshots. Incremental addition or removal may be used to
    //  optimize
    List<StorageGroupMNode> allSgNodes = MManager.getInstance().getAllStorageGroupNodes();
    for (MNode sgNode : allSgNodes) {
      String storageGroupName = sgNode.getFullPath();
      int slot = PartitionUtils.calculateStorageGroupSlotByTime(storageGroupName, 0,
              partitionTable.getTotalSlotNumbers());

      Collection<MeasurementSchema> schemas = slotTimeseries.computeIfAbsent(slot,
              s -> new HashSet<>());
      MManager.getInstance().collectSeries(sgNode, schemas);
      logger.debug("{} timeseries are snapshot in slot {}", schemas.size(), slot);
    }
  }

  public void setSnapshot(T snapshot, int slot) {
    synchronized (slotSnapshots) {
      // TODO-Cluster#451: persist the remote snapshot so the pulling can be recovered in restart
      slotSnapshots.put(slot, snapshot);
    }
  }
}