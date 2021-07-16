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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;

import java.time.ZoneId;

public class ClusterPlanner extends Planner {

  /** @param fetchSize this parameter only take effect when it is a query plan */
  @Override
  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, ZoneId zoneId, int fetchSize)
      throws QueryProcessException {
    Operator operator = logicalGenerator.generate(sqlStr, zoneId);
    operator = logicalOptimize(operator);
    PhysicalGenerator physicalGenerator = new ClusterPhysicalGenerator();
    return physicalGenerator.transformToPhysicalPlan(operator, fetchSize);
  }

  @Override
  protected ConcatPathOptimizer getConcatPathOptimizer() {
    return new ClusterConcatPathOptimizer();
  }
}
