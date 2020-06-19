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

import java.io.IOException;
import java.sql.SQLException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.thrift.TException;

public interface IPlanExecutor {

  /**
   * process query plan of qp layer, construct queryDataSet.
   *
   * @param queryPlan QueryPlan
   * @return QueryDataSet
   */
  QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException,
      QueryFilterOptimizationException, QueryProcessException, MetadataException, SQLException, TException, InterruptedException;

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
   * @param path      : update series seriesPath
   * @param startTime start time in update command
   * @param endTime   end time in update command
   * @param value     - in type of string
   */
  void update(Path path, long startTime, long endTime, String value)
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
   * @param path       : delete series seriesPath
   * @param startTime start time in delete command
   * @param endTime end time in delete command
   */
  void delete(Path path, long startTime, long endTime) throws QueryProcessException;

  /**
   * execute insert command and return whether the operator is successful.
   *
   * @param insertPlan physical insert plan
   */
  void insert(InsertPlan insertPlan) throws QueryProcessException;

  /**
   * execute batch insert plan
   *
   * @return result of each row
   */
  TSStatus[] insertTablet(InsertTabletPlan insertTabletPlan) throws QueryProcessException;
}
