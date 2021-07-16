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
package org.apache.iotdb.db.qp;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.MergeSingleFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.RemoveNotOptimizer;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.conf.IoTDBConstant.TIME;

/** provide a integration method for other user. */
public class Planner {

  protected LogicalGenerator logicalGenerator;

  public Planner() {
    this.logicalGenerator = new LogicalGenerator();
  }

  @TestOnly
  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr) throws QueryProcessException {
    return parseSQLToPhysicalPlan(sqlStr, ZoneId.systemDefault(), 1024);
  }

  /** @param fetchSize this parameter only take effect when it is a query plan */
  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, ZoneId zoneId, int fetchSize)
      throws QueryProcessException {
    Operator operator = logicalGenerator.generate(sqlStr, zoneId);
    operator = logicalOptimize(operator);
    PhysicalGenerator physicalGenerator = new PhysicalGenerator();
    return physicalGenerator.transformToPhysicalPlan(operator, fetchSize);
  }

  /** convert raw data query to physical plan directly */
  public PhysicalPlan rawDataQueryReqToPhysicalPlan(
      TSRawDataQueryReq rawDataQueryReq, ZoneId zoneId)
      throws QueryProcessException, IllegalPathException {
    List<String> paths = rawDataQueryReq.getPaths();
    long startTime = rawDataQueryReq.getStartTime();
    long endTime = rawDataQueryReq.getEndTime();

    // construct query operator and set its global time filter
    QueryOperator queryOp = new QueryOperator(SQLConstant.TOK_QUERY);
    FromOperator fromOp = new FromOperator(SQLConstant.TOK_FROM);
    SelectOperator selectOp = new SelectOperator(SQLConstant.TOK_SELECT, zoneId);

    // iterate the path list and add it to from operator
    for (String p : paths) {
      PartialPath path = new PartialPath(p);
      fromOp.addPrefixTablePath(path);
    }
    selectOp.addSelectPath(new PartialPath(""));

    queryOp.setSelectOperator(selectOp);
    queryOp.setFromOperator(fromOp);

    // set time filter operator
    FilterOperator filterOp = new FilterOperator(SQLConstant.KW_AND);
    PartialPath timePath = new PartialPath(TIME);
    filterOp.setSinglePath(timePath);
    Set<PartialPath> pathSet = new HashSet<>();
    pathSet.add(timePath);
    filterOp.setIsSingle(true);
    filterOp.setPathSet(pathSet);

    BasicFunctionOperator left =
        new BasicFunctionOperator(
            SQLConstant.GREATERTHANOREQUALTO, timePath, Long.toString(startTime));
    BasicFunctionOperator right =
        new BasicFunctionOperator(SQLConstant.LESSTHAN, timePath, Long.toString(endTime));
    filterOp.addChildOperator(left);
    filterOp.addChildOperator(right);

    queryOp.setFilterOperator(filterOp);

    SFWOperator op = (SFWOperator) logicalOptimize(queryOp);

    PhysicalGenerator physicalGenerator = new PhysicalGenerator();
    return physicalGenerator.transformToPhysicalPlan(op, rawDataQueryReq.fetchSize);
  }

  /**
   * given an unoptimized logical operator tree and return a optimized result.
   *
   * @param operator unoptimized logical operator
   * @return optimized logical operator
   * @throws LogicalOptimizeException exception in logical optimizing
   */
  protected Operator logicalOptimize(Operator operator)
      throws LogicalOperatorException, PathNumOverLimitException {
    switch (operator.getType()) {
      case AUTHOR:
      case METADATA:
      case SET_STORAGE_GROUP:
      case DELETE_STORAGE_GROUP:
      case CREATE_TIMESERIES:
      case DELETE_TIMESERIES:
      case ALTER_TIMESERIES:
      case LOADDATA:
      case INSERT:
      case GRANT_WATERMARK_EMBEDDING:
      case REVOKE_WATERMARK_EMBEDDING:
      case TTL:
      case LOAD_CONFIGURATION:
      case SHOW:
      case LOAD_FILES:
      case REMOVE_FILE:
      case MOVE_FILE:
      case FLUSH:
      case MERGE:
      case TRACING:
      case CLEAR_CACHE:
      case NULL:
      case SHOW_MERGE_STATUS:
      case DELETE_PARTITION:
      case CREATE_SCHEMA_SNAPSHOT:
      case KILL:
      case CREATE_FUNCTION:
      case DROP_FUNCTION:
      case CREATE_TRIGGER:
      case DROP_TRIGGER:
      case START_TRIGGER:
      case STOP_TRIGGER:
        return operator;
      case QUERY:
      case DELETE:
      case CREATE_INDEX:
      case DROP_INDEX:
      case QUERY_INDEX:
        SFWOperator root = (SFWOperator) operator;
        return optimizeSFWOperator(root);
      default:
        throw new LogicalOperatorException(operator.getType().toString(), "");
    }
  }

  /**
   * given an unoptimized select-from-where operator and return an optimized result.
   *
   * @param root unoptimized select-from-where operator
   * @return optimized select-from-where operator
   * @throws LogicalOptimizeException exception in SFW optimizing
   */
  private SFWOperator optimizeSFWOperator(SFWOperator root)
      throws LogicalOperatorException, PathNumOverLimitException {
    ConcatPathOptimizer concatPathOptimizer = getConcatPathOptimizer();
    root = (SFWOperator) concatPathOptimizer.transform(root);
    FilterOperator filter = root.getFilterOperator();
    if (filter == null) {
      return root;
    }
    Set<PartialPath> pathSet = filter.getPathSet();
    RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
    filter = removeNot.optimize(filter);
    DnfFilterOptimizer dnf = new DnfFilterOptimizer();
    filter = dnf.optimize(filter);
    MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
    filter = merge.optimize(filter);
    root.setFilterOperator(filter);
    filter.setPathSet(pathSet);
    return root;
  }

  protected ConcatPathOptimizer getConcatPathOptimizer() {
    return new ConcatPathOptimizer();
  }
}
