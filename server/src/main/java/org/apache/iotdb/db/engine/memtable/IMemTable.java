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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertSinglePointPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * IMemTable is designed to store data points which are not flushed into TsFile yet. An instance of
 * IMemTable maintains all series belonging to one StorageGroup, corresponding to one
 * StorageGroupProcessor.<br>
 * The concurrent control of IMemTable is based on the concurrent control of StorageGroupProcessor,
 * i.e., Writing and querying operations must already have gotten writeLock and readLock
 * respectively.<br>
 */
public interface IMemTable {

  Map<String, Map<String, IWritableMemChunk>> getMemTableMap();

  void write(String deviceId, IMeasurementSchema schema, long insertTime, Object objectValue);

  void write(InsertTabletPlan insertTabletPlan, int start, int end);

  /** @return the number of points */
  long size();

  /** @return memory usage */
  long memSize();

  /** only used when mem control enabled */
  void addTVListRamCost(long cost);

  /** only used when mem control enabled */
  long getTVListsRamCost();

  /**
   * only used when mem control enabled
   *
   * @return whether the average number of points in each WritableChunk reaches the threshold
   */
  boolean reachTotalPointNumThreshold();

  int getSeriesNumber();

  long getTotalPointsNum();

  void insert(InsertRowPlan insertRowPlan);

  void InsertSinglePoint(InsertSinglePointPlan insertSinglePointPlan);

  /** [start, end) */
  void insertTablet(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException;

  ReadOnlyMemChunk query(
      String deviceId,
      String measurement,
      IMeasurementSchema schema,
      long timeLowerBound,
      List<TimeRange> deletionList)
      throws IOException, QueryProcessException, MetadataException;

  /** putBack all the memory resources. */
  void clear();

  boolean isEmpty();

  /**
   * Delete data in it whose timestamp <= 'timestamp' and belonging to timeseries path. Only called
   * for non-flushing MemTable.
   *
   * @param path the PartialPath the timeseries to be deleted.
   * @param devicePath the device path of the timeseries to be deleted.
   * @param startTimestamp the lower-bound of deletion time.
   * @param endTimestamp the upper-bound of deletion time
   */
  void delete(PartialPath path, PartialPath devicePath, long startTimestamp, long endTimestamp);

  /**
   * Make a copy of this MemTable.
   *
   * @return a MemTable with the same data as this one.
   */
  IMemTable copy();

  boolean isSignalMemTable();

  void setShouldFlush();

  boolean shouldFlush();

  void release();

  /** must guarantee the device exists in the work memtable only used when mem control enabled */
  boolean checkIfChunkDoesNotExist(String deviceId, String measurement);

  /** only used when mem control enabled */
  int getCurrentChunkPointNum(String deviceId, String measurement);

  /** only used when mem control enabled */
  void addTextDataSize(long textDataIncrement);

  long getMaxPlanIndex();

  long getMinPlanIndex();
}
