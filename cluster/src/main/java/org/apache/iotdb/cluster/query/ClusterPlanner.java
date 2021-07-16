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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.LogicalChecker;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.service.rpc.thrift.TSLastDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;

import java.time.ZoneId;

public class ClusterPlanner extends Planner {

  @Override
  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, ZoneId zoneId)
      throws QueryProcessException {
    // from SQL to logical operator
    Operator operator = LogicalGenerator.generate(sqlStr, zoneId);
    // check if there are logical errors
    LogicalChecker.check(operator);
    // optimize the logical operator
    operator = logicalOptimize(operator);
    // from logical operator to physical plan
    return new ClusterPhysicalGenerator().transformToPhysicalPlan(operator);
  }

  @Override
  public PhysicalPlan rawDataQueryReqToPhysicalPlan(
      TSRawDataQueryReq rawDataQueryReq, ZoneId zoneId)
      throws IllegalPathException, QueryProcessException {
    // from TSRawDataQueryReq to logical operator
    Operator operator = LogicalGenerator.generate(rawDataQueryReq, zoneId);
    // check if there are logical errors
    LogicalChecker.check(operator);
    // optimize the logical operator
    operator = logicalOptimize(operator);
    // from logical operator to physical plan
    return new ClusterPhysicalGenerator().transformToPhysicalPlan(operator);
  }

  @Override
  public PhysicalPlan lastDataQueryReqToPhysicalPlan(
      TSLastDataQueryReq lastDataQueryReq, ZoneId zoneId)
      throws QueryProcessException, IllegalPathException {
    // from TSLastDataQueryReq to logical operator
    Operator operator = LogicalGenerator.generate(lastDataQueryReq, zoneId);
    // check if there are logical errors
    LogicalChecker.check(operator);
    // optimize the logical operator
    operator = logicalOptimize(operator);
    // from logical operator to physical plan
    return new ClusterPhysicalGenerator().transformToPhysicalPlan(operator);
  }
}
