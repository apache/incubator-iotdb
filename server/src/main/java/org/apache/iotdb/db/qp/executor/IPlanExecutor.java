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
package org.apache.iotdb.db.qp.executor;

import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;

import java.io.IOException;
import java.sql.SQLException;

public interface IPlanExecutor {

  /**
   * process query plan of qp layer, construct queryDataSet.
   *
   * @param queryPlan QueryPlan
   * @return QueryDataSet
   */
  QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException, QueryFilterOptimizationException,
          QueryProcessException, MetadataException, SQLException, TException, InterruptedException;

  /**
   * Process Non-Query Physical plan, including insert/update/delete operation of
   * data/metadata/Privilege
   *
   * @param plan Physical Non-Query Plan
   */
  boolean processNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException;

  /**
   * execute update command and return whether the operator is successful.
   *
   * @param path : update series seriesPath
   * @param startTime start time in update command
   * @param endTime end time in update command
   * @param value - in type of string
   */
  void update(PartialPath path, long startTime, long endTime, String value)
      throws QueryProcessException;

  /**
   * execute delete command and return whether the operator is successful.
   *
   * @param deletePlan physical delete plan
   */
  void delete(DeletePlan deletePlan) throws QueryProcessException;

  /**
   * execute delete command and return whether the operator is successful.
   *
   * @param path delete series seriesPath
   * @param startTime start time in delete command
   * @param endTime end time in delete command
   * @param planIndex index of the deletion plan
   * @param partitionFilter specify involving time partitions, if null, all partitions are involved
   */
  void delete(
      PartialPath path,
      long startTime,
      long endTime,
      long planIndex,
      TimePartitionFilter partitionFilter)
      throws QueryProcessException;

  /**
   * execute insert command and return whether the operator is successful.
   *
   * @param insertRowPlan physical insert plan
   */
  void insert(InsertRowPlan insertRowPlan) throws QueryProcessException;

  /**
   * execute insert command and return whether the operator is successful.
   *
   * @param insertRowsPlan physical insert rows plan, which contains multi insertRowPlans
   */
  void insert(InsertRowsPlan insertRowsPlan) throws QueryProcessException;

  /**
   * execute insert command and return whether the operator is successful.
   *
   * @param insertRowsOfOneDevicePlan physical insert plan
   */
  void insert(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan) throws QueryProcessException;

  /**
   * execute batch insert plan
   *
   * @throws BatchProcessException when some of the rows failed
   */
  void insertTablet(InsertTabletPlan insertTabletPlan) throws QueryProcessException;

  /**
   * execute multi batch insert plan
   *
   * @throws QueryProcessException when some of the rows failed
   */
  void insertTablet(InsertMultiTabletPlan insertMultiTabletPlan) throws QueryProcessException;
}
