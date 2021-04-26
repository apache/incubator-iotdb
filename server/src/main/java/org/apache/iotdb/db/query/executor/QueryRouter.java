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

package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.read.QueryIndexExecutor;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByFillDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByTimeDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithoutValueFilterDataSet;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.GroupByMonthFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Query entrance class of IoTDB query process. All query clause will be transformed to physical
 * plan, physical plan will be executed by EngineQueryRouter.
 */
public class QueryRouter implements IQueryRouter {

  private static Logger logger = LoggerFactory.getLogger(QueryRouter.class);

  @Override
  public QueryDataSet rawDataQuery(RawDataQueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException {
    IExpression expression = queryPlan.getExpression();
    List<PartialPath> deduplicatedPaths = queryPlan.getDeduplicatedPaths();

    IExpression optimizedExpression;
    try {
      optimizedExpression =
          expression == null
              ? null
              : ExpressionOptimizer.getInstance()
                  .optimize(expression, new ArrayList<>(deduplicatedPaths));
    } catch (QueryFilterOptimizationException e) {
      throw new StorageEngineException(e.getMessage());
    }
    queryPlan.setExpression(optimizedExpression);

    RawDataQueryExecutor rawDataQueryExecutor = getRawDataQueryExecutor(queryPlan);

    if (!queryPlan.isAlignByTime()) {
      return rawDataQueryExecutor.executeNonAlign(context);
    }

    if (optimizedExpression != null
        && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME) {
      try {
        queryPlan.transformPaths(IoTDB.metaManager);
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
      return rawDataQueryExecutor.executeWithValueFilter(context);
    }

    // Currently, we only group the vector partial paths for raw query without value filter
    queryPlan.transformToVector();
    return rawDataQueryExecutor.executeWithoutValueFilter(context);
  }

  protected RawDataQueryExecutor getRawDataQueryExecutor(RawDataQueryPlan queryPlan) {
    return new RawDataQueryExecutor(queryPlan);
  }

  @Override
  public QueryDataSet aggregate(AggregationPlan aggregationPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
          IOException {

    if (logger.isDebugEnabled()) {
      logger.debug(
          "paths:"
              + aggregationPlan.getPaths()
              + " level:"
              + aggregationPlan.getLevel()
              + " duplicatePaths:"
              + aggregationPlan.getDeduplicatedPaths()
              + " deduplicatePaths:"
              + aggregationPlan.getDeduplicatedAggregations());
    }

    IExpression expression = aggregationPlan.getExpression();
    List<PartialPath> deduplicatedPaths = aggregationPlan.getDeduplicatedPaths();

    // optimize expression to an executable one
    IExpression optimizedExpression =
        expression == null
            ? null
            : ExpressionOptimizer.getInstance()
                .optimize(expression, new ArrayList<>(deduplicatedPaths));

    aggregationPlan.setExpression(optimizedExpression);

    AggregationExecutor engineExecutor = getAggregationExecutor(aggregationPlan);

    QueryDataSet dataSet;

    if (optimizedExpression != null
        && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME) {
      dataSet = engineExecutor.executeWithValueFilter(context, aggregationPlan);
    } else {
      dataSet = engineExecutor.executeWithoutValueFilter(context, aggregationPlan);
    }

    return dataSet;
  }

  protected AggregationExecutor getAggregationExecutor(AggregationPlan aggregationPlan) {
    return new AggregationExecutor(aggregationPlan);
  }

  @Override
  public QueryDataSet groupBy(GroupByTimePlan groupByTimePlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
          IOException {

    if (logger.isDebugEnabled()) {
      logger.debug("paths:" + groupByTimePlan.getPaths() + " level:" + groupByTimePlan.getLevel());
    }

    GroupByEngineDataSet dataSet = null;
    IExpression expression = groupByTimePlan.getExpression();
    List<PartialPath> selectedSeries = groupByTimePlan.getDeduplicatedPaths();
    GlobalTimeExpression timeExpression = getTimeExpression(groupByTimePlan);

    if (expression == null) {
      expression = timeExpression;
    } else {
      expression = BinaryExpression.and(expression, timeExpression);
    }

    // optimize expression to an executable one
    IExpression optimizedExpression =
        ExpressionOptimizer.getInstance().optimize(expression, new ArrayList<>(selectedSeries));
    groupByTimePlan.setExpression(optimizedExpression);

    if (optimizedExpression.getType() == ExpressionType.GLOBAL_TIME) {
      dataSet = getGroupByWithoutValueFilterDataSet(context, groupByTimePlan);
    } else {
      dataSet = getGroupByWithValueFilterDataSet(context, groupByTimePlan);
    }

    // we support group by level for count operation
    // details at https://issues.apache.org/jira/browse/IOTDB-622
    // and UserGuide/Operation Manual/DML
    if (groupByTimePlan.getLevel() >= 0) {
      return groupByLevelWithoutTimeIntervalDataSet(context, groupByTimePlan, dataSet);
    }
    return dataSet;
  }

  private GlobalTimeExpression getTimeExpression(GroupByTimePlan plan) {
    if (plan.isSlidingStepByMonth() || plan.isIntervalByMonth()) {
      return new GlobalTimeExpression(
          (new GroupByMonthFilter(
              plan.getInterval(),
              plan.getSlidingStep(),
              plan.getStartTime(),
              plan.getEndTime(),
              plan.isSlidingStepByMonth(),
              plan.isIntervalByMonth())));
    } else {
      return new GlobalTimeExpression(
          new GroupByFilter(
              plan.getInterval(), plan.getSlidingStep(), plan.getStartTime(), plan.getEndTime()));
    }
  }

  protected GroupByWithoutValueFilterDataSet getGroupByWithoutValueFilterDataSet(
      QueryContext context, GroupByTimePlan plan)
      throws StorageEngineException, QueryProcessException {
    return new GroupByWithoutValueFilterDataSet(context, plan);
  }

  protected GroupByWithValueFilterDataSet getGroupByWithValueFilterDataSet(
      QueryContext context, GroupByTimePlan plan)
      throws StorageEngineException, QueryProcessException {
    return new GroupByWithValueFilterDataSet(context, plan);
  }

  protected GroupByTimeDataSet groupByLevelWithoutTimeIntervalDataSet(
      QueryContext context, GroupByTimePlan plan, GroupByEngineDataSet dataSet)
      throws QueryProcessException, IOException {
    return new GroupByTimeDataSet(context, plan, dataSet);
  }

  @Override
  public QueryDataSet fill(FillQueryPlan fillQueryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    List<PartialPath> fillPaths = fillQueryPlan.getDeduplicatedPaths();
    List<TSDataType> dataTypes = fillQueryPlan.getDeduplicatedDataTypes();
    long queryTime = fillQueryPlan.getQueryTime();
    Map<TSDataType, IFill> fillType = fillQueryPlan.getFillType();

    FillQueryExecutor fillQueryExecutor =
        getFillExecutor(fillPaths, dataTypes, queryTime, fillType);
    return fillQueryExecutor.execute(context, fillQueryPlan);
  }

  protected FillQueryExecutor getFillExecutor(
      List<PartialPath> fillPaths,
      List<TSDataType> dataTypes,
      long queryTime,
      Map<TSDataType, IFill> fillType) {
    return new FillQueryExecutor(fillPaths, dataTypes, queryTime, fillType);
  }

  @Override
  public QueryDataSet groupByFill(GroupByTimeFillPlan groupByFillPlan, QueryContext context)
      throws QueryFilterOptimizationException, StorageEngineException, QueryProcessException,
          IOException {
    GroupByEngineDataSet groupByEngineDataSet =
        (GroupByEngineDataSet) groupBy(groupByFillPlan, context);
    return new GroupByFillDataSet(
        groupByFillPlan.getDeduplicatedPaths(),
        groupByFillPlan.getDeduplicatedDataTypes(),
        groupByEngineDataSet,
        groupByFillPlan.getFillType(),
        context,
        groupByFillPlan);
  }

  @Override
  public QueryDataSet lastQuery(LastQueryPlan lastQueryPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    LastQueryExecutor lastQueryExecutor = getLastQueryExecutor(lastQueryPlan);
    return lastQueryExecutor.execute(context, lastQueryPlan);
  }

  @Override
  public QueryDataSet indexQuery(QueryIndexPlan queryPlan, QueryContext context)
      throws QueryIndexException, StorageEngineException {
    QueryIndexExecutor executor = new QueryIndexExecutor(queryPlan, context);
    return executor.executeIndexQuery();
  }

  protected LastQueryExecutor getLastQueryExecutor(LastQueryPlan lastQueryPlan) {
    return new LastQueryExecutor(lastQueryPlan);
  }

  @Override
  public QueryDataSet udtfQuery(UDTFPlan udtfPlan, QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException, InterruptedException {
    IExpression expression = udtfPlan.getExpression();
    IExpression optimizedExpression;
    try {
      optimizedExpression =
          expression == null
              ? null
              : ExpressionOptimizer.getInstance()
                  .optimize(expression, new ArrayList<>(udtfPlan.getDeduplicatedPaths()));
    } catch (QueryFilterOptimizationException e) {
      throw new StorageEngineException(e.getMessage());
    }
    udtfPlan.setExpression(optimizedExpression);

    boolean withValueFilter =
        optimizedExpression != null && optimizedExpression.getType() != ExpressionType.GLOBAL_TIME;
    UDTFQueryExecutor udtfQueryExecutor = new UDTFQueryExecutor(udtfPlan);

    if (udtfPlan.isAlignByTime()) {
      return withValueFilter
          ? udtfQueryExecutor.executeWithValueFilterAlignByTime(context)
          : udtfQueryExecutor.executeWithoutValueFilterAlignByTime(context);
    } else {
      return withValueFilter
          ? udtfQueryExecutor.executeWithValueFilterNonAlign(context)
          : udtfQueryExecutor.executeWithoutValueFilterNonAlign(context);
    }
  }
}
