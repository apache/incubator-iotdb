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
package org.apache.iotdb.db.qp.sql;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEvent;
import org.apache.iotdb.db.exception.index.UnsupportedIndexTypeException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.FilterConstant;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.AggregationQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.DeleteDataOperator;
import org.apache.iotdb.db.qp.logical.crud.FillClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.FillQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromComponent;
import org.apache.iotdb.db.qp.logical.crud.GroupByClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.GroupByFillClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.GroupByFillQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.GroupByQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.InOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.logical.crud.LastQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectComponent;
import org.apache.iotdb.db.qp.logical.crud.SpecialClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.UDFQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.WhereComponent;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator.AlterType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.logical.sys.ClearCacheOperator;
import org.apache.iotdb.db.qp.logical.sys.CountOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateContinuousQueryOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateFunctionOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateIndexOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateSnapshotOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateTriggerOperator;
import org.apache.iotdb.db.qp.logical.sys.DataAuthOperator;
import org.apache.iotdb.db.qp.logical.sys.DeletePartitionOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.DropContinuousQueryOperator;
import org.apache.iotdb.db.qp.logical.sys.DropFunctionOperator;
import org.apache.iotdb.db.qp.logical.sys.DropIndexOperator;
import org.apache.iotdb.db.qp.logical.sys.DropTriggerOperator;
import org.apache.iotdb.db.qp.logical.sys.FlushOperator;
import org.apache.iotdb.db.qp.logical.sys.KillQueryOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator.LoadConfigurationOperatorType;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadFilesOperator;
import org.apache.iotdb.db.qp.logical.sys.MergeOperator;
import org.apache.iotdb.db.qp.logical.sys.MoveFileOperator;
import org.apache.iotdb.db.qp.logical.sys.RemoveFileOperator;
import org.apache.iotdb.db.qp.logical.sys.SetStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.SetTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowChildNodesOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowChildPathsOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowContinuousQueriesOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowDevicesOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowFunctionsOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowLockInfoOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowMergeStatusOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTriggersOperator;
import org.apache.iotdb.db.qp.logical.sys.StartTriggerOperator;
import org.apache.iotdb.db.qp.logical.sys.StopTriggerOperator;
import org.apache.iotdb.db.qp.logical.sys.TracingOperator;
import org.apache.iotdb.db.qp.logical.sys.UnSetTTLOperator;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AliasClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AlignByDeviceClauseOrDisableAlignContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AlignByDeviceClauseOrDisableAlignStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AlterClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AlterTimeseriesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AlterUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AndExpressionContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AttributeClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.AttributeClausesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ClearcacheContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ConstantContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CountDevicesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CountNodesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CountStorageGroupContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CountTimeseriesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CqGroupByTimeClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CqSelectIntoClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CreateContinuousQueryStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CreateFunctionContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CreateIndexContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CreateRoleContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CreateSnapshotContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CreateTimeseriesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CreateTriggerContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.CreateUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DateExpressionContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DeletePartitionContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DeleteStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DeleteStorageGroupContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DeleteTimeseriesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DropContinuousQueryStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DropFunctionContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DropIndexContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DropRoleContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DropTriggerContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.DropUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ExpressionContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.FillClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.FillStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.FlushContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.FromClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.FullMergeContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.FullPathContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.FunctionAttributeContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GrantRoleContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GrantRoleToUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GrantUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GrantWatermarkEmbeddingContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GroupByFillClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GroupByFillStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GroupByLevelClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GroupByLevelStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GroupByTimeClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.GroupByTimeStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.InClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.IndexPredicateClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.IndexWithClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.InsertColumnsSpecContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.InsertMultiValueContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.InsertStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.InsertValuesSpecContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.KillQueryContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.LimitClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.LimitStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ListAllRoleOfUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ListAllUserOfRoleContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ListPrivilegesRoleContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ListPrivilegesUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ListRoleContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ListRolePrivilegesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ListUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ListUserPrivilegesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.LoadConfigurationStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.LoadFilesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.LoadStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.MeasurementNameContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.MeasurementValueContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.MergeContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.MoveFileContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.NodeNameContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.NodeNameWithoutStarContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.OffsetClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.OrExpressionContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.OrderByTimeClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.OrderByTimeStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.PredicateContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.PrefixPathContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.PrivilegesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.PropertyContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.PropertyValueContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.RemoveFileContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ResampleClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ResultColumnContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.RevokeRoleContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.RevokeRoleFromUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.RevokeUserContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.RevokeWatermarkEmbeddingContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.RootOrIdContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SelectClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SelectStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SequenceClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SetStorageGroupContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SetTTLStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowAllTTLStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowChildNodesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowChildPathsContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowContinuousQueriesStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowDevicesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowFlushTaskInfoContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowFunctionsContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowLockInfoContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowMergeStatusContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowQueryProcesslistContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowStorageGroupContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowTTLStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowTimeseriesContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowTriggersContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowVersionContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.ShowWhereClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SingleStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SlimitClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SlimitStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SoffsetClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SpecialLimitStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.StartTriggerContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.StopTriggerContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.StringLiteralContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.SuffixPathContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.TagClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.TimeIntervalContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.TopClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.TracingOffContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.TracingOnContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.TriggerAttributeContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.TypeClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.UnsetTTLStatementContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.WhereClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.WithoutNullClauseContext;
import org.apache.iotdb.db.qp.sql.SqlBaseParser.WithoutNullStatementContext;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.db.query.executor.fill.LinearFill;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.binary.AdditionExpression;
import org.apache.iotdb.db.query.expression.binary.DivisionExpression;
import org.apache.iotdb.db.query.expression.binary.ModuloExpression;
import org.apache.iotdb.db.query.expression.binary.MultiplicationExpression;
import org.apache.iotdb.db.query.expression.binary.SubtractionExpression;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.NegationExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;

import org.antlr.v4.runtime.tree.TerminalNode;

import java.io.File;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;
import static org.apache.iotdb.db.index.common.IndexConstant.TOP_K;
import static org.apache.iotdb.db.qp.constant.SQLConstant.TIME_PATH;
import static org.apache.iotdb.db.qp.constant.SQLConstant.TOK_KILL_QUERY;

public class IoTDBSqlVisitor extends SqlBaseBaseVisitor<Operator> {

  private static final String DELETE_RANGE_ERROR_MSG =
      "For delete statement, where clause can only contain atomic expressions like : "
          + "time > XXX, time <= XXX, or two atomic expressions connected by 'AND'";

  private static final Pattern cqLevelNodePattern = Pattern.compile("\\$\\{\\w+}");

  private ZoneId zoneId;
  private QueryOperator queryOp;

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  @Override
  public Operator visitSingleStatement(SingleStatementContext ctx) {
    Operator operator = visit(ctx.statement());
    if (ctx.DEBUG() != null) {
      operator.setDebug(true);
    }
    return operator;
  }

  @Override
  public Operator visitCreateTimeseries(CreateTimeseriesContext ctx) {
    CreateTimeSeriesOperator createTimeSeriesOperator =
        new CreateTimeSeriesOperator(SQLConstant.TOK_METADATA_CREATE);
    createTimeSeriesOperator.setPath(parseFullPath(ctx.fullPath()));
    if (ctx.alias() != null) {
      createTimeSeriesOperator.setAlias(ctx.alias().ID().getText());
    }
    if (ctx.attributeClauses() != null) {
      parseAttributeClauses(ctx.attributeClauses(), createTimeSeriesOperator);
    }
    return createTimeSeriesOperator;
  }

  @Override
  public Operator visitDeleteTimeseries(DeleteTimeseriesContext ctx) {
    List<PartialPath> deletePaths = new ArrayList<>();
    List<PrefixPathContext> prefixPaths = ctx.prefixPath();
    for (PrefixPathContext prefixPath : prefixPaths) {
      deletePaths.add(parsePrefixPath(prefixPath));
    }
    DeleteTimeSeriesOperator deleteTimeSeriesOperator =
        new DeleteTimeSeriesOperator(SQLConstant.TOK_METADATA_DELETE);
    deleteTimeSeriesOperator.setDeletePathList(deletePaths);
    return deleteTimeSeriesOperator;
  }

  @Override
  public Operator visitAlterTimeseries(AlterTimeseriesContext ctx) {
    AlterTimeSeriesOperator alterTimeSeriesOperator =
        new AlterTimeSeriesOperator(SQLConstant.TOK_METADATA_ALTER);
    alterTimeSeriesOperator.setPath(parseFullPath(ctx.fullPath()));
    parseAlterClause(ctx.alterClause(), alterTimeSeriesOperator);
    return alterTimeSeriesOperator;
  }

  @Override
  public Operator visitInsertStatement(InsertStatementContext ctx) {
    InsertOperator insertOp = new InsertOperator(SQLConstant.TOK_INSERT);
    insertOp.setDevice(parsePrefixPath(ctx.prefixPath()));
    parseInsertColumnSpec(ctx.insertColumnsSpec(), insertOp);
    parseInsertValuesSpec(ctx.insertValuesSpec(), insertOp);
    return insertOp;
  }

  @Override
  public Operator visitDeleteStatement(DeleteStatementContext ctx) {
    DeleteDataOperator deleteDataOp = new DeleteDataOperator(SQLConstant.TOK_DELETE);
    List<PrefixPathContext> prefixPaths = ctx.prefixPath();
    for (PrefixPathContext prefixPath : prefixPaths) {
      deleteDataOp.addPath(parsePrefixPath(prefixPath));
    }
    if (ctx.whereClause() != null) {
      WhereComponent whereComponent = parseWhereClause(ctx.whereClause());
      Pair<Long, Long> timeInterval = parseDeleteTimeInterval(whereComponent.getFilterOperator());
      deleteDataOp.setStartTime(timeInterval.left);
      deleteDataOp.setEndTime(timeInterval.right);
    } else {
      deleteDataOp.setStartTime(Long.MIN_VALUE);
      deleteDataOp.setEndTime(Long.MAX_VALUE);
    }
    return deleteDataOp;
  }

  @Override
  public Operator visitSetStorageGroup(SetStorageGroupContext ctx) {
    SetStorageGroupOperator setStorageGroupOperator =
        new SetStorageGroupOperator(SQLConstant.TOK_METADATA_SET_FILE_LEVEL);
    PartialPath path = parsePrefixPath(ctx.prefixPath());
    setStorageGroupOperator.setPath(path);
    return setStorageGroupOperator;
  }

  @Override
  public Operator visitDeleteStorageGroup(DeleteStorageGroupContext ctx) {
    List<PartialPath> deletePaths = new ArrayList<>();
    List<PrefixPathContext> prefixPaths = ctx.prefixPath();
    for (PrefixPathContext prefixPath : prefixPaths) {
      deletePaths.add(parsePrefixPath(prefixPath));
    }
    DeleteStorageGroupOperator deleteStorageGroupOperator =
        new DeleteStorageGroupOperator(SQLConstant.TOK_METADATA_DELETE_FILE_LEVEL);
    deleteStorageGroupOperator.setDeletePathList(deletePaths);
    return deleteStorageGroupOperator;
  }

  @Override
  public Operator visitCreateIndex(CreateIndexContext ctx) {
    CreateIndexOperator createIndexOp = new CreateIndexOperator(SQLConstant.TOK_CREATE_INDEX);
    List<PrefixPathContext> prefixPaths = Collections.singletonList(ctx.prefixPath());
    for (PrefixPathContext prefixPath : prefixPaths) {
      createIndexOp.addPath(parsePrefixPath(prefixPath));
    }
    parseIndexWithClause(ctx.indexWithClause(), createIndexOp);
    if (ctx.whereClause() != null) {
      FilterOperator filterOp = parseWhereClause(ctx.whereClause()).getFilterOperator();
      long indexTime = parseCreateIndexFilter(filterOp);
      createIndexOp.setTime(indexTime);
    }
    return createIndexOp;
  }

  /** for create index command, time should only have an end time. */
  private long parseCreateIndexFilter(FilterOperator filterOperator) {
    if (filterOperator.getFilterType() != FilterType.GREATERTHAN
        && filterOperator.getFilterType() != FilterType.GREATERTHANOREQUALTO) {
      throw new SQLParserException(
          "For create index command, where clause must be like : time > XXX or time >= XXX");
    }
    long time = Long.parseLong(((BasicFunctionOperator) filterOperator).getValue());
    if (filterOperator.getFilterType() == FilterType.LESSTHAN) {
      time = time - 1;
    }
    return time;
  }

  public void parseIndexWithClause(IndexWithClauseContext ctx, CreateIndexOperator createIndexOp) {
    IndexType indexType;
    try {
      indexType = IndexType.getIndexType(ctx.indexName.getText());
    } catch (UnsupportedIndexTypeException e) {
      throw new SQLParserException(ctx.indexName.getText());
    }

    List<PropertyContext> properties = ctx.property();
    Map<String, String> props = new HashMap<>(properties.size(), 1);
    if (ctx.property(0) != null) {
      for (PropertyContext property : properties) {
        String k = property.ID().getText().toUpperCase();
        String v = property.propertyValue().getText().toUpperCase();
        v = IndexUtils.removeQuotation(v);
        props.put(k, v);
      }
    }
    createIndexOp.setIndexType(indexType);
    createIndexOp.setProps(props);
  }

  @Override
  public Operator visitDropIndex(DropIndexContext ctx) {
    DropIndexOperator dropIndexOperator = new DropIndexOperator(SQLConstant.TOK_DROP_INDEX);
    List<PrefixPathContext> prefixPaths = Collections.singletonList(ctx.prefixPath());
    for (PrefixPathContext prefixPath : prefixPaths) {
      dropIndexOperator.addPath(parsePrefixPath(prefixPath));
    }
    try {
      dropIndexOperator.setIndexType(IndexType.getIndexType(ctx.indexName.getText()));
    } catch (UnsupportedIndexTypeException e) {
      throw new SQLParserException(ctx.indexName.getText());
    }
    return dropIndexOperator;
  }

  @Override
  public Operator visitCreateFunction(CreateFunctionContext ctx) {
    CreateFunctionOperator createFunctionOperator =
        new CreateFunctionOperator(SQLConstant.TOK_FUNCTION_CREATE);
    createFunctionOperator.setTemporary(ctx.TEMPORARY() != null);
    createFunctionOperator.setUdfName(ctx.udfName.getText());
    createFunctionOperator.setClassName(removeStringQuote(ctx.className.getText()));
    return createFunctionOperator;
  }

  @Override
  public Operator visitDropFunction(DropFunctionContext ctx) {
    DropFunctionOperator dropFunctionOperator =
        new DropFunctionOperator(SQLConstant.TOK_FUNCTION_DROP);
    dropFunctionOperator.setUdfName(ctx.udfName.getText());
    return dropFunctionOperator;
  }

  @Override
  public Operator visitShowFunctions(ShowFunctionsContext ctx) {
    ShowFunctionsOperator showFunctionsOperator =
        new ShowFunctionsOperator(SQLConstant.TOK_SHOW_FUNCTIONS);
    showFunctionsOperator.setShowTemporary(ctx.TEMPORARY() != null);
    return showFunctionsOperator;
  }

  @Override
  public Operator visitCreateTrigger(CreateTriggerContext ctx) {
    CreateTriggerOperator createTriggerOperator =
        new CreateTriggerOperator(SQLConstant.TOK_TRIGGER_CREATE);
    createTriggerOperator.setTriggerName(ctx.triggerName.getText());
    createTriggerOperator.setEvent(
        ctx.triggerEventClause().BEFORE() != null
            ? TriggerEvent.BEFORE_INSERT
            : TriggerEvent.AFTER_INSERT);
    createTriggerOperator.setFullPath(parseFullPath(ctx.fullPath()));
    createTriggerOperator.setClassName(removeStringQuote(ctx.className.getText()));
    if (ctx.triggerAttributeClause() != null) {
      for (TriggerAttributeContext triggerAttributeContext :
          ctx.triggerAttributeClause().triggerAttribute()) {
        createTriggerOperator.addAttribute(
            removeStringQuote(triggerAttributeContext.key.getText()),
            removeStringQuote(triggerAttributeContext.value.getText()));
      }
    }
    return createTriggerOperator;
  }

  @Override
  public Operator visitDropTrigger(DropTriggerContext ctx) {
    DropTriggerOperator dropTriggerOperator = new DropTriggerOperator(SQLConstant.TOK_TRIGGER_DROP);
    dropTriggerOperator.setTriggerName(ctx.triggerName.getText());
    return dropTriggerOperator;
  }

  @Override
  public Operator visitStartTrigger(StartTriggerContext ctx) {
    StartTriggerOperator startTriggerOperator =
        new StartTriggerOperator(SQLConstant.TOK_TRIGGER_START);
    startTriggerOperator.setTriggerName(ctx.triggerName.getText());
    return startTriggerOperator;
  }

  @Override
  public Operator visitStopTrigger(StopTriggerContext ctx) {
    StopTriggerOperator stopTriggerOperator = new StopTriggerOperator(SQLConstant.TOK_TRIGGER_STOP);
    stopTriggerOperator.setTriggerName(ctx.triggerName.getText());
    return stopTriggerOperator;
  }

  @Override
  public Operator visitShowTriggers(ShowTriggersContext ctx) {
    return new ShowTriggersOperator(SQLConstant.TOK_SHOW_TRIGGERS);
  }

  @Override
  public Operator visitMerge(MergeContext ctx) {
    return new MergeOperator(SQLConstant.TOK_MERGE);
  }

  @Override
  public Operator visitFlush(FlushContext ctx) {
    FlushOperator flushOperator = new FlushOperator(SQLConstant.TOK_FLUSH);
    if (ctx.booleanClause() != null) {
      flushOperator.setSeq(Boolean.parseBoolean(ctx.booleanClause().getText()));
    }
    if (ctx.prefixPath(0) != null) {
      List<PartialPath> storageGroups = new ArrayList<>();
      for (PrefixPathContext prefixPathContext : ctx.prefixPath()) {
        storageGroups.add(parsePrefixPath(prefixPathContext));
      }
      flushOperator.setStorageGroupList(storageGroups);
    }
    return flushOperator;
  }

  @Override
  public Operator visitFullMerge(FullMergeContext ctx) {
    return new MergeOperator(SQLConstant.TOK_FULL_MERGE);
  }

  @Override
  public Operator visitClearcache(ClearcacheContext ctx) {
    return new ClearCacheOperator(SQLConstant.TOK_CLEAR_CACHE);
  }

  @Override
  public Operator visitCreateUser(CreateUserContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE, AuthorOperator.AuthorType.CREATE_USER);
    authorOperator.setUserName(ctx.ID().getText());
    authorOperator.setPassWord(removeStringQuote(ctx.password.getText()));
    return authorOperator;
  }

  @Override
  public Operator visitAlterUser(AlterUserContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(
            SQLConstant.TOK_AUTHOR_UPDATE_USER, AuthorOperator.AuthorType.UPDATE_USER);
    if (ctx.ID() != null) {
      authorOperator.setUserName(ctx.ID().getText());
    } else {
      authorOperator.setUserName(ctx.ROOT().getText());
    }
    authorOperator.setNewPassword(removeStringQuote(ctx.password.getText()));
    return authorOperator;
  }

  @Override
  public Operator visitDropUser(DropUserContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP, AuthorOperator.AuthorType.DROP_USER);
    authorOperator.setUserName(ctx.ID().getText());
    return authorOperator;
  }

  @Override
  public Operator visitCreateRole(CreateRoleContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_CREATE, AuthorOperator.AuthorType.CREATE_ROLE);
    authorOperator.setRoleName(ctx.ID().getText());
    return authorOperator;
  }

  @Override
  public Operator visitDropRole(DropRoleContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_DROP, AuthorOperator.AuthorType.DROP_ROLE);
    authorOperator.setRoleName(ctx.ID().getText());
    return authorOperator;
  }

  @Override
  public Operator visitGrantUser(GrantUserContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT, AuthorOperator.AuthorType.GRANT_USER);
    authorOperator.setUserName(ctx.ID().getText());
    authorOperator.setPrivilegeList(parsePrivilege(ctx.privileges()));
    authorOperator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    return authorOperator;
  }

  @Override
  public Operator visitGrantRole(GrantRoleContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT, AuthorType.GRANT_ROLE);
    authorOperator.setRoleName(ctx.ID().getText());
    authorOperator.setPrivilegeList(parsePrivilege(ctx.privileges()));
    authorOperator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    return authorOperator;
  }

  @Override
  public Operator visitRevokeUser(RevokeUserContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT, AuthorType.REVOKE_USER);
    authorOperator.setUserName(ctx.ID().getText());
    authorOperator.setPrivilegeList(parsePrivilege(ctx.privileges()));
    authorOperator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    return authorOperator;
  }

  @Override
  public Operator visitRevokeRole(RevokeRoleContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT, AuthorType.REVOKE_ROLE);
    authorOperator.setRoleName(ctx.ID().getText());
    authorOperator.setPrivilegeList(parsePrivilege(ctx.privileges()));
    authorOperator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    return authorOperator;
  }

  @Override
  public Operator visitGrantRoleToUser(GrantRoleToUserContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(
            SQLConstant.TOK_AUTHOR_GRANT, AuthorOperator.AuthorType.GRANT_ROLE_TO_USER);
    authorOperator.setRoleName(ctx.roleName.getText());
    authorOperator.setUserName(ctx.userName.getText());
    return authorOperator;
  }

  @Override
  public Operator visitRevokeRoleFromUser(RevokeRoleFromUserContext ctx) {
    AuthorOperator authorOperator =
        new AuthorOperator(SQLConstant.TOK_AUTHOR_GRANT, AuthorType.REVOKE_ROLE_FROM_USER);
    authorOperator.setRoleName(ctx.roleName.getText());
    authorOperator.setUserName(ctx.userName.getText());
    return authorOperator;
  }

  @Override
  public Operator visitLoadStatement(LoadStatementContext ctx) {
    if (ctx.prefixPath().nodeName().size() < 3) {
      throw new SQLParserException("data load command: child count < 3\n");
    }

    String csvPath = ctx.stringLiteral().getText();
    StringContainer sc = new StringContainer(TsFileConstant.PATH_SEPARATOR);
    List<NodeNameContext> nodeNames = ctx.prefixPath().nodeName();
    sc.addTail(ctx.prefixPath().ROOT().getText());
    for (NodeNameContext nodeName : nodeNames) {
      sc.addTail(nodeName.getText());
    }
    return new LoadDataOperator(
        SQLConstant.TOK_DATALOAD, removeStringQuote(csvPath), sc.toString());
  }

  @Override
  public Operator visitGrantWatermarkEmbedding(GrantWatermarkEmbeddingContext ctx) {
    List<RootOrIdContext> rootOrIdList = ctx.rootOrId();
    List<String> users = new ArrayList<>();
    for (RootOrIdContext rootOrId : rootOrIdList) {
      users.add(rootOrId.getText());
    }
    return new DataAuthOperator(SQLConstant.TOK_GRANT_WATERMARK_EMBEDDING, users);
  }

  @Override
  public Operator visitRevokeWatermarkEmbedding(RevokeWatermarkEmbeddingContext ctx) {
    List<RootOrIdContext> rootOrIdList = ctx.rootOrId();
    List<String> users = new ArrayList<>();
    for (RootOrIdContext rootOrId : rootOrIdList) {
      users.add(rootOrId.getText());
    }
    return new DataAuthOperator(SQLConstant.TOK_REVOKE_WATERMARK_EMBEDDING, users);
  }

  @Override
  public Operator visitListUser(ListUserContext ctx) {
    return new AuthorOperator(SQLConstant.TOK_LIST, AuthorOperator.AuthorType.LIST_USER);
  }

  @Override
  public Operator visitListRole(ListRoleContext ctx) {
    return new AuthorOperator(SQLConstant.TOK_LIST, AuthorOperator.AuthorType.LIST_ROLE);
  }

  @Override
  public Operator visitListPrivilegesUser(ListPrivilegesUserContext ctx) {
    AuthorOperator operator =
        new AuthorOperator(SQLConstant.TOK_LIST, AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
    operator.setUserName(ctx.rootOrId().getText());
    operator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    return operator;
  }

  @Override
  public Operator visitListPrivilegesRole(ListPrivilegesRoleContext ctx) {
    AuthorOperator operator =
        new AuthorOperator(SQLConstant.TOK_LIST, AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
    operator.setRoleName((ctx.ID().getText()));
    operator.setNodeNameList(parsePrefixPath(ctx.prefixPath()));
    return operator;
  }

  @Override
  public Operator visitListUserPrivileges(ListUserPrivilegesContext ctx) {
    AuthorOperator operator =
        new AuthorOperator(SQLConstant.TOK_LIST, AuthorOperator.AuthorType.LIST_USER_PRIVILEGE);
    operator.setUserName(ctx.rootOrId().getText());
    return operator;
  }

  @Override
  public Operator visitListRolePrivileges(ListRolePrivilegesContext ctx) {
    AuthorOperator operator =
        new AuthorOperator(SQLConstant.TOK_LIST, AuthorOperator.AuthorType.LIST_ROLE_PRIVILEGE);
    operator.setRoleName(ctx.ID().getText());
    return operator;
  }

  @Override
  public Operator visitListAllRoleOfUser(ListAllRoleOfUserContext ctx) {
    AuthorOperator operator =
        new AuthorOperator(SQLConstant.TOK_LIST, AuthorOperator.AuthorType.LIST_USER_ROLES);
    operator.setUserName(ctx.rootOrId().getText());
    return operator;
  }

  @Override
  public Operator visitListAllUserOfRole(ListAllUserOfRoleContext ctx) {
    AuthorOperator operator =
        new AuthorOperator(SQLConstant.TOK_LIST, AuthorOperator.AuthorType.LIST_ROLE_USERS);
    operator.setRoleName((ctx.ID().getText()));
    return operator;
  }

  @Override
  public Operator visitSetTTLStatement(SetTTLStatementContext ctx) {
    SetTTLOperator operator = new SetTTLOperator(SQLConstant.TOK_SET);
    operator.setStorageGroup(parsePrefixPath(ctx.prefixPath()));
    operator.setDataTTL(Long.parseLong(ctx.INT().getText()));
    return operator;
  }

  @Override
  public Operator visitUnsetTTLStatement(UnsetTTLStatementContext ctx) {
    UnSetTTLOperator operator = new UnSetTTLOperator(SQLConstant.TOK_UNSET);
    operator.setStorageGroup(parsePrefixPath(ctx.prefixPath()));
    return operator;
  }

  @Override
  public Operator visitShowTTLStatement(ShowTTLStatementContext ctx) {
    List<PartialPath> storageGroups = new ArrayList<>();
    List<PrefixPathContext> prefixPathList = ctx.prefixPath();
    for (PrefixPathContext prefixPath : prefixPathList) {
      storageGroups.add(parsePrefixPath(prefixPath));
    }
    return new ShowTTLOperator(storageGroups);
  }

  @Override
  public Operator visitShowAllTTLStatement(ShowAllTTLStatementContext ctx) {
    List<PartialPath> storageGroups = new ArrayList<>();
    return new ShowTTLOperator(storageGroups);
  }

  @Override
  public Operator visitShowFlushTaskInfo(ShowFlushTaskInfoContext ctx) {
    return new ShowOperator(SQLConstant.TOK_FLUSH_TASK_INFO);
  }

  @Override
  public Operator visitShowVersion(ShowVersionContext ctx) {
    return new ShowOperator(SQLConstant.TOK_VERSION);
  }

  @Override
  public Operator visitShowTimeseries(ShowTimeseriesContext ctx) {
    boolean orderByHeat = ctx.LATEST() != null;
    ShowTimeSeriesOperator showTimeSeriesOperator;
    if (ctx.prefixPath() != null) {
      showTimeSeriesOperator =
          new ShowTimeSeriesOperator(
              SQLConstant.TOK_TIMESERIES, parsePrefixPath(ctx.prefixPath()), orderByHeat);
    } else {
      showTimeSeriesOperator =
          new ShowTimeSeriesOperator(
              SQLConstant.TOK_TIMESERIES,
              new PartialPath(SQLConstant.getSingleRootArray()),
              orderByHeat);
    }
    if (ctx.showWhereClause() != null) {
      parseShowWhereClause(ctx.showWhereClause(), showTimeSeriesOperator);
    }
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), showTimeSeriesOperator);
    }
    return showTimeSeriesOperator;
  }

  @Override
  public Operator visitShowQueryProcesslist(ShowQueryProcesslistContext ctx) {
    return new ShowOperator(SQLConstant.TOK_QUERY_PROCESSLIST);
  }

  @Override
  public Operator visitKillQuery(KillQueryContext ctx) {
    KillQueryOperator killQueryOperator = new KillQueryOperator(TOK_KILL_QUERY);
    if (ctx.INT() != null) {
      killQueryOperator.setQueryId(Integer.parseInt(ctx.INT().getText()));
    }
    return killQueryOperator;
  }

  @Override
  public Operator visitShowStorageGroup(ShowStorageGroupContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowStorageGroupOperator(
          SQLConstant.TOK_STORAGE_GROUP, parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowStorageGroupOperator(
          SQLConstant.TOK_STORAGE_GROUP, new PartialPath(SQLConstant.getSingleRootArray()));
    }
  }

  @Override
  public Operator visitShowLockInfo(ShowLockInfoContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowLockInfoOperator(SQLConstant.TOK_LOCK_INFO, parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowLockInfoOperator(
          SQLConstant.TOK_LOCK_INFO, new PartialPath(SQLConstant.getSingleRootArray()));
    }
  }

  @Override
  public Operator visitShowChildPaths(ShowChildPathsContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowChildPathsOperator(
          SQLConstant.TOK_CHILD_PATHS, parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowChildPathsOperator(
          SQLConstant.TOK_CHILD_PATHS, new PartialPath(SQLConstant.getSingleRootArray()));
    }
  }

  @Override
  public Operator visitShowChildNodes(ShowChildNodesContext ctx) {
    if (ctx.prefixPath() != null) {
      return new ShowChildNodesOperator(
          SQLConstant.TOK_CHILD_NODES, parsePrefixPath(ctx.prefixPath()));
    } else {
      return new ShowChildNodesOperator(
          SQLConstant.TOK_CHILD_NODES, new PartialPath(SQLConstant.getSingleRootArray()));
    }
  }

  @Override
  public Operator visitShowDevices(ShowDevicesContext ctx) {
    ShowDevicesOperator showDevicesOperator;
    if (ctx.prefixPath() != null) {
      showDevicesOperator =
          new ShowDevicesOperator(SQLConstant.TOK_DEVICES, parsePrefixPath(ctx.prefixPath()));
    } else {
      showDevicesOperator =
          new ShowDevicesOperator(
              SQLConstant.TOK_DEVICES, new PartialPath(SQLConstant.getSingleRootArray()));
    }
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), showDevicesOperator);
    }
    // show devices wtih storage group
    if (ctx.WITH() != null) {
      showDevicesOperator.setSgCol(true);
    }
    return showDevicesOperator;
  }

  @Override
  public Operator visitShowMergeStatus(ShowMergeStatusContext ctx) {
    return new ShowMergeStatusOperator(SQLConstant.TOK_SHOW_MERGE_STATUS);
  }

  @Override
  public Operator visitTracingOn(TracingOnContext ctx) {
    return new TracingOperator(SQLConstant.TOK_TRACING, true);
  }

  @Override
  public Operator visitTracingOff(TracingOffContext ctx) {
    return new TracingOperator(SQLConstant.TOK_TRACING, false);
  }

  @Override
  public Operator visitCountTimeseries(CountTimeseriesContext ctx) {
    PrefixPathContext pathContext = ctx.prefixPath();
    PartialPath path =
        (pathContext != null
            ? parsePrefixPath(pathContext)
            : new PartialPath(SQLConstant.getSingleRootArray()));
    if (ctx.INT() != null) {
      return new CountOperator(
          SQLConstant.TOK_COUNT_NODE_TIMESERIES, path, Integer.parseInt(ctx.INT().getText()));
    } else {
      return new CountOperator(SQLConstant.TOK_COUNT_TIMESERIES, path);
    }
  }

  @Override
  public Operator visitCountDevices(CountDevicesContext ctx) {
    PrefixPathContext pathContext = ctx.prefixPath();
    PartialPath path =
        (pathContext != null
            ? parsePrefixPath(pathContext)
            : new PartialPath(SQLConstant.getSingleRootArray()));
    return new CountOperator(SQLConstant.TOK_COUNT_DEVICES, path);
  }

  @Override
  public Operator visitCountStorageGroup(CountStorageGroupContext ctx) {
    PrefixPathContext pathContext = ctx.prefixPath();
    PartialPath path =
        (pathContext != null
            ? parsePrefixPath(pathContext)
            : new PartialPath(SQLConstant.getSingleRootArray()));
    return new CountOperator(SQLConstant.TOK_COUNT_STORAGE_GROUP, path);
  }

  @Override
  public Operator visitCountNodes(CountNodesContext ctx) {
    return new CountOperator(
        SQLConstant.TOK_COUNT_NODES,
        parsePrefixPath(ctx.prefixPath()),
        Integer.parseInt(ctx.INT().getText()));
  }

  @Override
  public Operator visitLoadConfigurationStatement(LoadConfigurationStatementContext ctx) {
    if (ctx.GLOBAL() != null) {
      return new LoadConfigurationOperator(LoadConfigurationOperatorType.GLOBAL);
    } else {
      return new LoadConfigurationOperator(LoadConfigurationOperatorType.LOCAL);
    }
  }

  @Override
  public Operator visitLoadFiles(LoadFilesContext ctx) {
    if (ctx.autoCreateSchema() != null) {
      if (ctx.autoCreateSchema().INT() != null) {
        return new LoadFilesOperator(
            new File(removeStringQuote(ctx.stringLiteral().getText())),
            Boolean.parseBoolean(ctx.autoCreateSchema().booleanClause().getText()),
            Integer.parseInt(ctx.autoCreateSchema().INT().getText()));
      } else {
        return new LoadFilesOperator(
            new File(removeStringQuote(ctx.stringLiteral().getText())),
            Boolean.parseBoolean(ctx.autoCreateSchema().booleanClause().getText()),
            IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel());
      }
    } else {
      return new LoadFilesOperator(
          new File(removeStringQuote(ctx.stringLiteral().getText())),
          true,
          IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel());
    }
  }

  @Override
  public Operator visitRemoveFile(RemoveFileContext ctx) {
    return new RemoveFileOperator(new File(removeStringQuote(ctx.stringLiteral().getText())));
  }

  @Override
  public Operator visitMoveFile(MoveFileContext ctx) {
    return new MoveFileOperator(
        new File(removeStringQuote(ctx.stringLiteral(0).getText())),
        new File(removeStringQuote(ctx.stringLiteral(1).getText())));
  }

  @Override
  public Operator visitDeletePartition(DeletePartitionContext ctx) {
    DeletePartitionOperator deletePartitionOperator =
        new DeletePartitionOperator(SQLConstant.TOK_DELETE_PARTITION);
    deletePartitionOperator.setStorageGroupName(parsePrefixPath(ctx.prefixPath()));
    Set<Long> idSet = new HashSet<>();
    for (TerminalNode terminalNode : ctx.INT()) {
      idSet.add(Long.parseLong(terminalNode.getText()));
    }
    deletePartitionOperator.setPartitionIds(idSet);
    return deletePartitionOperator;
  }

  @Override
  public Operator visitCreateSnapshot(CreateSnapshotContext ctx) {
    return new CreateSnapshotOperator(SQLConstant.TOK_CREATE_SCHEMA_SNAPSHOT);
  }

  @Override
  public Operator visitSelectStatement(SelectStatementContext ctx) {
    // 1. Visit special clause first to initialize different query operator
    if (ctx.specialClause() != null) {
      queryOp = (QueryOperator) visit(ctx.specialClause());
    }
    // 2. There is no special clause in query statement.
    if (queryOp == null) {
      queryOp = new QueryOperator();
    }
    // 3. Visit select, from, where in sequence
    parseSelectClause(ctx.selectClause());
    parseFromClause(ctx.fromClause());
    if (ctx.whereClause() != null) {
      WhereComponent whereComponent = parseWhereClause(ctx.whereClause());
      if (whereComponent != null) {
        queryOp.setWhereComponent(whereComponent);
      }
    }

    return queryOp;
  }

  public void parseSelectClause(SelectClauseContext ctx) {
    SelectComponent selectComponent = new SelectComponent(zoneId);
    if (ctx.topClause() != null) {
      // TODO: parse info of top clause into selectOp
      visitTopClause(ctx.topClause());
    } else if (ctx.LAST() != null) {
      queryOp = new LastQueryOperator(queryOp);
    }

    boolean isFirstElement = true;
    for (ResultColumnContext resultColumnContext : ctx.resultColumn()) {
      selectComponent.addResultColumn(parseResultColumn(resultColumnContext));
      // judge query type according to the first select element
      if (!hasDecidedQueryType()) {
        if (selectComponent.hasAggregationFunction()) {
          queryOp = new AggregationQueryOperator(queryOp);
        } else if (selectComponent.hasTimeSeriesGeneratingFunction()) {
          queryOp = new UDFQueryOperator(queryOp);
        }
      }
    }

    queryOp.setSelectComponent(selectComponent);
  }

  private boolean hasDecidedQueryType() {
    return queryOp instanceof GroupByQueryOperator
        || queryOp instanceof FillQueryOperator
        || queryOp instanceof LastQueryOperator
        || queryOp instanceof AggregationQueryOperator
        || queryOp instanceof UDFQueryOperator;
  }

  @Override
  public Operator visitTopClause(TopClauseContext ctx) {
    Map<String, Object> props = new HashMap<>();
    int top = Integer.parseInt(ctx.INT().getText());
    if (top < 0) {
      throw new SQLParserException("TOP <N>: N should be greater than 0.");
    }
    props.put(TOP_K, top);
    queryOp.setProps(props);
    return queryOp;
  }

  private ResultColumn parseResultColumn(ResultColumnContext resultColumnContext) {
    return new ResultColumn(
        parseExpression(resultColumnContext.expression()),
        resultColumnContext.AS() == null ? null : resultColumnContext.ID().getText());
  }

  @Override
  public Operator visitDropContinuousQueryStatement(DropContinuousQueryStatementContext ctx) {
    DropContinuousQueryOperator dropContinuousQueryOperator =
        new DropContinuousQueryOperator(SQLConstant.TOK_CONTINUOUS_QUERY_DROP);
    dropContinuousQueryOperator.setContinuousQueryName(ctx.continuousQueryName.getText());
    return dropContinuousQueryOperator;
  }

  @Override
  public Operator visitShowContinuousQueriesStatement(ShowContinuousQueriesStatementContext ctx) {
    return new ShowContinuousQueriesOperator(SQLConstant.TOK_SHOW_CONTINUOUS_QUERIES);
  }

  @Override
  public Operator visitCreateContinuousQueryStatement(CreateContinuousQueryStatementContext ctx) {
    CreateContinuousQueryOperator createContinuousQueryOperator =
        new CreateContinuousQueryOperator(SQLConstant.TOK_CONTINUOUS_QUERY_CREATE);

    createContinuousQueryOperator.setQuerySql(ctx.getText());

    createContinuousQueryOperator.setContinuousQueryName(ctx.continuousQueryName.getText());

    if (ctx.resampleClause() != null) {
      parseResampleClause(ctx.resampleClause(), createContinuousQueryOperator);
    }

    parseCqSelectIntoClause(ctx.cqSelectIntoClause(), createContinuousQueryOperator);

    StringBuilder sb = new StringBuilder();
    sb.append("select ");
    sb.append(ctx.cqSelectIntoClause().selectClause().getText().substring(6));
    sb.append(" from ");
    sb.append(ctx.cqSelectIntoClause().fromClause().prefixPath(0).getText());

    sb.append(" group by ([now() - ");
    String groupByInterval = ctx.cqSelectIntoClause().cqGroupByTimeClause().DURATION().getText();
    if (createContinuousQueryOperator.getForInterval() == 0) {
      sb.append(groupByInterval);
    } else {
      List<TerminalNode> durations = ctx.resampleClause().DURATION();
      sb.append(durations.get(durations.size() - 1).getText());
    }
    sb.append(", now()), ");
    sb.append(groupByInterval);
    sb.append(")");
    if (queryOp.isGroupByLevel()) {
      sb.append(", level = ");
      sb.append(queryOp.getSpecialClauseComponent().getLevel());
    }
    createContinuousQueryOperator.setQuerySql(sb.toString());

    if (createContinuousQueryOperator.getEveryInterval() == 0) {
      createContinuousQueryOperator.setEveryInterval(
          ((GroupByClauseComponent) queryOp.getSpecialClauseComponent()).getUnit());
    }

    if (createContinuousQueryOperator.getEveryInterval()
        < IoTDBDescriptor.getInstance().getConfig().getContinuousQueryMinimumEveryInterval()) {
      throw new SQLParserException(
          "CQ: every interval should not be lower than the minimum value you configured.");
    }

    if (createContinuousQueryOperator.getForInterval() == 0) {
      createContinuousQueryOperator.setForInterval(
          ((GroupByClauseComponent) queryOp.getSpecialClauseComponent()).getUnit());
    }

    return createContinuousQueryOperator;
  }

  public void parseResampleClause(
      ResampleClauseContext ctx, CreateContinuousQueryOperator operator) {

    if (ctx.DURATION().size() == 1) {
      if (ctx.EVERY() != null) {
        operator.setEveryInterval(
            DatetimeUtils.convertDurationStrToLong(ctx.DURATION(0).getText()));
      } else if (ctx.FOR() != null) {
        operator.setForInterval(DatetimeUtils.convertDurationStrToLong(ctx.DURATION(0).getText()));
      }
    } else if (ctx.DURATION().size() == 2) {
      operator.setEveryInterval(DatetimeUtils.convertDurationStrToLong(ctx.DURATION(0).getText()));
      operator.setForInterval(DatetimeUtils.convertDurationStrToLong(ctx.DURATION(1).getText()));
    }
  }

  public void parseCqSelectIntoClause(
      CqSelectIntoClauseContext ctx, CreateContinuousQueryOperator createContinuousQueryOperator) {

    queryOp = new GroupByQueryOperator();

    parseSelectClause(ctx.selectClause());
    parseFromClause(ctx.fromClause());

    if (queryOp.getSelectComponent().getResultColumns().size() > 1) {
      throw new SQLParserException("CQ: CQ currently does not support multiple result columns.");
    }

    if (queryOp.getFromComponent().getPrefixPaths().size() > 1) {
      throw new SQLParserException("CQ: CQ currently does not support multiple series .");
    }

    parseCqGroupByTimeClause(ctx.cqGroupByTimeClause());

    int fromLen = queryOp.getFromComponent().getPrefixPaths().get(0).getNodeLength();
    int queryLevel = queryOp.getSpecialClauseComponent().getLevel();
    if (queryLevel >= fromLen) {
      throw new SQLParserException("CQ: Level should not exceed the <from_prefix> length.");
    }

    PartialPath targetPath = null;

    int trueLevel = queryLevel;
    if (trueLevel == -1) {
      trueLevel = fromLen - 1;
    }

    if (ctx.fullPath() != null) {
      targetPath = parseFullPath(ctx.fullPath());
      Matcher m = cqLevelNodePattern.matcher(targetPath.getFullPath());
      while (m.find()) {
        String param = m.group();
        int nodeIndex = 0;
        try {
          nodeIndex = Integer.parseInt(param.substring(2, param.length() - 1).trim());
        } catch (NumberFormatException e) {
          throw new SQLParserException("CQ: x of ${x} should be an integer.");
        }
        if (nodeIndex < 1 || nodeIndex > trueLevel) {
          throw new SQLParserException(
              "CQ: x of ${x} should be greater than 0 and equal to or less than <level> or the length of queried path prefix.");
        }
      }
    } else if (ctx.nodeNameWithoutStar() != null) {

      List<String> targetNodes = new ArrayList<>();

      targetNodes.add("root");

      for (int i = 1; i <= trueLevel; i++) {
        targetNodes.add("${" + i + "}");
      }
      targetNodes.add(ctx.nodeNameWithoutStar().getText());
      targetPath = new PartialPath(targetNodes.toArray(new String[0]));
    }

    createContinuousQueryOperator.setTargetPath(targetPath);
    createContinuousQueryOperator.setQueryOperator(queryOp);
  }

  public void parseCqGroupByTimeClause(CqGroupByTimeClauseContext ctx) {

    GroupByClauseComponent groupByClauseComponent = new GroupByClauseComponent();

    groupByClauseComponent.setUnit(
        parseTimeUnitOrSlidingStep(ctx.DURATION().getText(), true, groupByClauseComponent));

    groupByClauseComponent.setSlidingStep(groupByClauseComponent.getUnit());
    groupByClauseComponent.setSlidingStepByMonth(groupByClauseComponent.isIntervalByMonth());

    groupByClauseComponent.setLeftCRightO(true);

    if (ctx.LEVEL() != null && ctx.INT() != null) {
      groupByClauseComponent.setLevel(Integer.parseInt(ctx.INT().getText()));
    }

    queryOp.setSpecialClauseComponent(groupByClauseComponent);
  }

  @SuppressWarnings("squid:S3776")
  private Expression parseExpression(ExpressionContext context) {
    // unary
    if (context.functionName != null) {
      return parseFunctionExpression(context);
    }
    if (context.suffixPath() != null) {
      return new TimeSeriesOperand(parseSuffixPath(context.suffixPath()));
    }
    if (context.literal != null) {
      return new TimeSeriesOperand(new PartialPath(new String[] {context.literal.getText()}));
    }
    if (context.unary != null) {
      return context.MINUS() != null
          ? new NegationExpression(parseExpression(context.expression(0)))
          : parseExpression(context.expression(0));
    }

    // binary
    Expression leftExpression = parseExpression(context.leftExpression);
    Expression rightExpression = parseExpression(context.rightExpression);
    if (context.STAR() != null) {
      return new MultiplicationExpression(leftExpression, rightExpression);
    }
    if (context.DIV() != null) {
      return new DivisionExpression(leftExpression, rightExpression);
    }
    if (context.MOD() != null) {
      return new ModuloExpression(leftExpression, rightExpression);
    }
    if (context.PLUS() != null) {
      return new AdditionExpression(leftExpression, rightExpression);
    }
    if (context.MINUS() != null) {
      return new SubtractionExpression(leftExpression, rightExpression);
    }

    throw new UnsupportedOperationException();
  }

  private Expression parseFunctionExpression(ExpressionContext functionClause) {
    FunctionExpression functionExpression =
        new FunctionExpression(functionClause.functionName.getText());

    // expressions
    for (ExpressionContext expression : functionClause.expression()) {
      functionExpression.addExpression(parseExpression(expression));
    }

    // attributes
    for (FunctionAttributeContext functionAttribute : functionClause.functionAttribute()) {
      functionExpression.addAttribute(
          removeStringQuote(functionAttribute.functionAttributeKey.getText()),
          removeStringQuote(functionAttribute.functionAttributeValue.getText()));
    }

    return functionExpression;
  }

  @Override
  public Operator visitSpecialLimitStatement(SpecialLimitStatementContext ctx) {
    return visit(ctx.specialLimit());
  }

  @Override
  public Operator visitLimitStatement(LimitStatementContext ctx) {
    if (queryOp == null) {
      queryOp = new QueryOperator();
    }
    parseLimitClause(ctx.limitClause(), queryOp);
    if (ctx.slimitClause() != null) {
      parseSlimitClause(ctx.slimitClause());
    }
    if (ctx.alignByDeviceClauseOrDisableAlign() != null) {
      parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    }
    return queryOp;
  }

  @Override
  public Operator visitSlimitStatement(SlimitStatementContext ctx) {
    if (queryOp == null) {
      queryOp = new QueryOperator();
    }
    parseSlimitClause(ctx.slimitClause());
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), queryOp);
    }
    if (ctx.alignByDeviceClauseOrDisableAlign() != null) {
      parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    }
    return queryOp;
  }

  @Override
  public Operator visitAlignByDeviceClauseOrDisableAlignStatement(
      AlignByDeviceClauseOrDisableAlignStatementContext ctx) {
    if (queryOp == null) {
      queryOp = new QueryOperator();
    }
    parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    return queryOp;
  }

  private void parseAlignByDeviceClauseOrDisableAlign(
      AlignByDeviceClauseOrDisableAlignContext ctx) {
    SpecialClauseComponent specialClauseComponent = queryOp.getSpecialClauseComponent();
    if (specialClauseComponent == null) {
      specialClauseComponent = new SpecialClauseComponent();
    }
    if (ctx.alignByDeviceClause() != null) {
      parseAlignByDeviceClause(specialClauseComponent);
    } else {
      parseDisableAlign(specialClauseComponent);
    }
    queryOp.setSpecialClauseComponent(specialClauseComponent);
  }

  @Override
  public Operator visitWithoutNullStatement(WithoutNullStatementContext ctx) {
    if (queryOp == null) {
      queryOp = new QueryOperator();
    }
    parseWithoutNullClause(ctx.withoutNullClause());
    if (ctx.limitClause() != null) {
      parseLimitClause(ctx.limitClause(), queryOp);
    }
    if (ctx.slimitClause() != null) {
      parseSlimitClause(ctx.slimitClause());
    }
    if (ctx.alignByDeviceClauseOrDisableAlign() != null) {
      parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    }
    return queryOp;
  }

  private void parseWithoutNullClause(WithoutNullClauseContext ctx) {
    SpecialClauseComponent specialClauseComponent = queryOp.getSpecialClauseComponent();
    if (specialClauseComponent == null) {
      specialClauseComponent = new SpecialClauseComponent();
    }
    specialClauseComponent.setWithoutAnyNull(ctx.ANY() != null);
    specialClauseComponent.setWithoutAllNull(ctx.ALL() != null);
    queryOp.setSpecialClauseComponent(specialClauseComponent);
  }

  @Override
  public Operator visitOrderByTimeStatement(OrderByTimeStatementContext ctx) {
    queryOp = new QueryOperator();
    parseOrderByTimeClause(ctx.orderByTimeClause());
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryOp;
  }

  @Override
  public Operator visitGroupByTimeStatement(GroupByTimeStatementContext ctx) {
    queryOp = new GroupByQueryOperator();
    parseGroupByTimeClause(ctx.groupByTimeClause());
    if (ctx.orderByTimeClause() != null) {
      parseOrderByTimeClause(ctx.orderByTimeClause());
    }
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryOp;
  }

  @Override
  public Operator visitGroupByFillStatement(GroupByFillStatementContext ctx) {
    queryOp = new GroupByFillQueryOperator();
    parseGroupByFillClause(ctx.groupByFillClause());
    if (ctx.orderByTimeClause() != null) {
      parseOrderByTimeClause(ctx.orderByTimeClause());
    }
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryOp;
  }

  @Override
  public Operator visitFillStatement(FillStatementContext ctx) {
    queryOp = new FillQueryOperator();
    parseFillClause(ctx.fillClause());
    if (ctx.slimitClause() != null) {
      parseSlimitClause(ctx.slimitClause());
    }
    if (ctx.alignByDeviceClauseOrDisableAlign() != null) {
      parseAlignByDeviceClauseOrDisableAlign(ctx.alignByDeviceClauseOrDisableAlign());
    }
    return queryOp;
  }

  @Override
  public Operator visitGroupByLevelStatement(GroupByLevelStatementContext ctx) {
    queryOp = new AggregationQueryOperator();
    parseGroupByLevelClause(ctx.groupByLevelClause());
    if (ctx.orderByTimeClause() != null) {
      parseOrderByTimeClause(ctx.orderByTimeClause());
    }
    if (ctx.specialLimit() != null) {
      return visit(ctx.specialLimit());
    }
    return queryOp;
  }

  public void parseFromClause(FromClauseContext ctx) {
    FromComponent fromComponent = new FromComponent();
    List<PrefixPathContext> prefixFromPaths = ctx.prefixPath();
    for (PrefixPathContext prefixFromPath : prefixFromPaths) {
      PartialPath path = parsePrefixPath(prefixFromPath);
      fromComponent.addPrefixTablePath(path);
    }
    queryOp.setFromComponent(fromComponent);
  }

  private void parseIndexPredicate(IndexPredicateClauseContext ctx) {
    Map<String, Object> props;
    PartialPath path;
    if (ctx.suffixPath() != null) {
      path = parseSuffixPath(ctx.suffixPath());
    } else {
      path = parseFullPath(ctx.fullPath());
    }
    if (ctx.LIKE() != null) {
      // whole matching case
      if (queryOp.getSelectComponent().getResultColumns().size() != 1) {
        throw new SQLParserException("Index query statement allows only one select path");
      }
      if (!path.equals(
          queryOp.getSelectComponent().getResultColumns().get(0).getExpression().toString())) {
        throw new SQLParserException(
            "In the index query statement, "
                + "the path in select element and the index predicate should be same");
      }
      if (queryOp.getProps() != null) {
        props = queryOp.getProps();
      } else {
        props = new HashMap<>();
      }
      props.put(PATTERN, parseSequence(ctx.sequenceClause(0)));
      queryOp.setIndexType(IndexType.RTREE_PAA);
    } else if (ctx.CONTAIN() != null) {
      // subsequence matching case
      List<double[]> compositePattern = new ArrayList<>();
      List<Double> thresholds = new ArrayList<>();
      for (int i = 0; i < ctx.sequenceClause().size(); i++) {
        compositePattern.add(parseSequence(ctx.sequenceClause(i)));
        thresholds.add(Double.parseDouble(ctx.constant(i).getText()));
      }
      if (queryOp.getProps() != null) {
        props = queryOp.getProps();
      } else {
        props = new HashMap<>();
      }
      List<ResultColumn> resultColumns = new ArrayList<>();
      resultColumns.add(new ResultColumn(new TimeSeriesOperand(path)));
      queryOp.getSelectComponent().setResultColumns(resultColumns);
      props.put(PATTERN, compositePattern);
      props.put(THRESHOLD, thresholds);
      queryOp.setIndexType(IndexType.ELB_INDEX);
    } else {
      throw new SQLParserException("Unknown index predicate: " + ctx);
    }
    queryOp.setProps(props);
  }

  private double[] parseSequence(SequenceClauseContext ctx) {
    int seqLen = ctx.constant().size();
    double[] sequence = new double[seqLen];
    for (int i = 0; i < seqLen; i++) {
      sequence[i] = Double.parseDouble(ctx.constant(i).getText());
    }
    return sequence;
  }

  public void parseGroupByLevelClause(GroupByLevelClauseContext ctx) {
    SpecialClauseComponent groupByLevelClauseComponent = new SpecialClauseComponent();
    groupByLevelClauseComponent.setLevel(Integer.parseInt(ctx.INT().getText()));
    queryOp.setSpecialClauseComponent(groupByLevelClauseComponent);
  }

  public void parseFillClause(FillClauseContext ctx) {
    FillClauseComponent fillClauseComponent = new FillClauseComponent();
    List<TypeClauseContext> list = ctx.typeClause();
    Map<TSDataType, IFill> fillTypes = new EnumMap<>(TSDataType.class);
    for (TypeClauseContext typeClause : list) {
      if (typeClause.ALL() != null) {
        if (typeClause.linearClause() != null) {
          throw new SQLParserException("fill all doesn't support linear fill");
        }
        parseAllTypeClause(typeClause, fillTypes);
        break;
      } else {
        parsePrimitiveTypeClause(typeClause, fillTypes);
      }
    }
    fillClauseComponent.setFillTypes(fillTypes);
    queryOp.setSpecialClauseComponent(fillClauseComponent);
  }

  private void parseAllTypeClause(TypeClauseContext ctx, Map<TSDataType, IFill> fillTypes) {
    IFill fill;
    long preRange;
    if (ctx.previousUntilLastClause() != null) {
      if (ctx.previousUntilLastClause().DURATION() != null) {
        preRange =
            DatetimeUtils.convertDurationStrToLong(
                ctx.previousUntilLastClause().DURATION().getText());
      } else {
        preRange = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
      }
      fill = new PreviousFill(preRange, true);
    } else {
      if (ctx.previousClause().DURATION() != null) {
        preRange =
            DatetimeUtils.convertDurationStrToLong(ctx.previousClause().DURATION().getText());
      } else {
        preRange = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();
      }
      fill = new PreviousFill(preRange);
    }
    for (TSDataType tsDataType : TSDataType.values()) {
      fillTypes.put(tsDataType, fill.copy());
    }
  }

  private void parseLimitClause(LimitClauseContext ctx, Operator operator) {
    int limit;
    try {
      limit = Integer.parseInt(ctx.INT().getText());
    } catch (NumberFormatException e) {
      throw new SQLParserException("Out of range. LIMIT <N>: N should be Int32.");
    }
    if (limit <= 0) {
      throw new SQLParserException("LIMIT <N>: N should be greater than 0.");
    }
    if (operator instanceof ShowTimeSeriesOperator) {
      ((ShowTimeSeriesOperator) operator).setLimit(limit);
    } else if (operator instanceof ShowDevicesOperator) {
      ((ShowDevicesOperator) operator).setLimit(limit);
    } else {
      SpecialClauseComponent specialClauseComponent = queryOp.getSpecialClauseComponent();
      if (specialClauseComponent == null) {
        specialClauseComponent = new SpecialClauseComponent();
      }
      specialClauseComponent.setRowLimit(limit);
      queryOp.setSpecialClauseComponent(specialClauseComponent);
    }
    if (ctx.offsetClause() != null) {
      parseOffsetClause(ctx.offsetClause(), operator);
    }
  }

  private void parseOffsetClause(OffsetClauseContext ctx, Operator operator) {
    int offset;
    try {
      offset = Integer.parseInt(ctx.INT().getText());
    } catch (NumberFormatException e) {
      throw new SQLParserException(
          "Out of range. OFFSET <OFFSETValue>: OFFSETValue should be Int32.");
    }
    if (offset < 0) {
      throw new SQLParserException("OFFSET <OFFSETValue>: OFFSETValue should >= 0.");
    }
    if (operator instanceof ShowTimeSeriesOperator) {
      ((ShowTimeSeriesOperator) operator).setOffset(offset);
    } else if (operator instanceof ShowDevicesOperator) {
      ((ShowDevicesOperator) operator).setOffset(offset);
    } else {
      SpecialClauseComponent specialClauseComponent = queryOp.getSpecialClauseComponent();
      if (specialClauseComponent == null) {
        specialClauseComponent = new SpecialClauseComponent();
      }
      specialClauseComponent.setRowOffset(offset);
      queryOp.setSpecialClauseComponent(specialClauseComponent);
    }
  }

  private void parseSlimitClause(SlimitClauseContext ctx) {
    int slimit;
    try {
      slimit = Integer.parseInt(ctx.INT().getText());
    } catch (NumberFormatException e) {
      throw new SQLParserException("Out of range. SLIMIT <SN>: SN should be Int32.");
    }
    if (slimit <= 0) {
      throw new SQLParserException("SLIMIT <SN>: SN should be greater than 0.");
    }
    SpecialClauseComponent specialClauseComponent = queryOp.getSpecialClauseComponent();
    if (specialClauseComponent == null) {
      specialClauseComponent = new SpecialClauseComponent();
    }
    specialClauseComponent.setSeriesLimit(slimit);
    queryOp.setSpecialClauseComponent(specialClauseComponent);
    if (ctx.soffsetClause() != null) {
      parseSoffsetClause(ctx.soffsetClause(), queryOp);
    }
  }

  public void parseSoffsetClause(SoffsetClauseContext ctx, QueryOperator queryOp) {
    int soffset;
    try {
      soffset = Integer.parseInt(ctx.INT().getText());
    } catch (NumberFormatException e) {
      throw new SQLParserException(
          "Out of range. SOFFSET <SOFFSETValue>: SOFFSETValue should be Int32.");
    }
    if (soffset < 0) {
      throw new SQLParserException("SOFFSET <SOFFSETValue>: SOFFSETValue should >= 0.");
    }
    SpecialClauseComponent specialClauseComponent = queryOp.getSpecialClauseComponent();
    if (specialClauseComponent == null) {
      specialClauseComponent = new SpecialClauseComponent();
    }
    specialClauseComponent.setSeriesOffset(soffset);
    queryOp.setSpecialClauseComponent(specialClauseComponent);
  }

  private void parseGroupByTimeClause(GroupByTimeClauseContext ctx) {
    GroupByClauseComponent groupByClauseComponent = new GroupByClauseComponent();
    groupByClauseComponent.setLeftCRightO(ctx.timeInterval().LS_BRACKET() != null);
    // parse timeUnit
    groupByClauseComponent.setUnit(
        parseTimeUnitOrSlidingStep(ctx.DURATION(0).getText(), true, groupByClauseComponent));
    // parse sliding step
    if (ctx.DURATION().size() == 2) {
      groupByClauseComponent.setSlidingStep(
          parseTimeUnitOrSlidingStep(ctx.DURATION(1).getText(), false, groupByClauseComponent));
      if (groupByClauseComponent.getSlidingStep() < groupByClauseComponent.getUnit()) {
        throw new SQLParserException(
            "The third parameter sliding step shouldn't be smaller than the second parameter time interval.");
      }
    } else {
      groupByClauseComponent.setSlidingStep(groupByClauseComponent.getUnit());
      groupByClauseComponent.setSlidingStepByMonth(groupByClauseComponent.isIntervalByMonth());
    }

    parseTimeInterval(ctx.timeInterval(), groupByClauseComponent);

    if (ctx.INT() != null) {
      groupByClauseComponent.setLevel(Integer.parseInt(ctx.INT().getText()));
    }
    queryOp.setSpecialClauseComponent(groupByClauseComponent);
  }

  private void parseGroupByFillClause(GroupByFillClauseContext ctx) {
    GroupByFillClauseComponent groupByFillClauseComponent = new GroupByFillClauseComponent();
    groupByFillClauseComponent.setLeftCRightO(ctx.timeInterval().LS_BRACKET() != null);

    // parse timeUnit
    groupByFillClauseComponent.setUnit(
        DatetimeUtils.convertDurationStrToLong(ctx.DURATION().getText()));
    groupByFillClauseComponent.setSlidingStep(groupByFillClauseComponent.getUnit());

    parseTimeInterval(ctx.timeInterval(), groupByFillClauseComponent);

    List<TypeClauseContext> list = ctx.typeClause();
    Map<TSDataType, IFill> fillTypes = new EnumMap<>(TSDataType.class);
    for (TypeClauseContext typeClause : list) {
      // group by fill doesn't support linear fill
      if (typeClause.linearClause() != null) {
        throw new SQLParserException("group by fill doesn't support linear fill");
      }
      // all type use the same fill way
      if (typeClause.ALL() != null) {
        parseAllTypeClause(typeClause, fillTypes);
        break;
      } else {
        parsePrimitiveTypeClause(typeClause, fillTypes);
      }
    }
    groupByFillClauseComponent.setFillTypes(fillTypes);
    queryOp.setSpecialClauseComponent(groupByFillClauseComponent);
  }

  private void parsePrimitiveTypeClause(TypeClauseContext ctx, Map<TSDataType, IFill> fillTypes) {
    TSDataType dataType = parseType(ctx.dataType().getText());
    if (ctx.linearClause() != null && dataType == TSDataType.TEXT) {
      throw new SQLParserException(
          String.format(
              "type %s cannot use %s fill function",
              dataType, ctx.linearClause().LINEAR().getText()));
    }

    int defaultFillInterval = IoTDBDescriptor.getInstance().getConfig().getDefaultFillInterval();

    if (ctx.linearClause() != null) { // linear
      if (ctx.linearClause().DURATION(0) != null) {
        long beforeRange =
            DatetimeUtils.convertDurationStrToLong(ctx.linearClause().DURATION(0).getText());
        long afterRange =
            DatetimeUtils.convertDurationStrToLong(ctx.linearClause().DURATION(1).getText());
        fillTypes.put(dataType, new LinearFill(beforeRange, afterRange));
      } else {
        fillTypes.put(dataType, new LinearFill(defaultFillInterval, defaultFillInterval));
      }
    } else if (ctx.previousClause() != null) { // previous
      if (ctx.previousClause().DURATION() != null) {
        long preRange =
            DatetimeUtils.convertDurationStrToLong(ctx.previousClause().DURATION().getText());
        fillTypes.put(dataType, new PreviousFill(preRange));
      } else {
        fillTypes.put(dataType, new PreviousFill(defaultFillInterval));
      }
    } else { // previous until last
      if (ctx.previousUntilLastClause().DURATION() != null) {
        long preRange =
            DatetimeUtils.convertDurationStrToLong(
                ctx.previousUntilLastClause().DURATION().getText());
        fillTypes.put(dataType, new PreviousFill(preRange, true));
      } else {
        fillTypes.put(dataType, new PreviousFill(defaultFillInterval, true));
      }
    }
  }

  /** parse datatype node. */
  private TSDataType parseType(String datatype) {
    String type = datatype.toLowerCase();
    switch (type) {
      case "int32":
        return TSDataType.INT32;
      case "int64":
        return TSDataType.INT64;
      case "float":
        return TSDataType.FLOAT;
      case "double":
        return TSDataType.DOUBLE;
      case "boolean":
        return TSDataType.BOOLEAN;
      case "text":
        return TSDataType.TEXT;
      default:
        throw new SQLParserException("not a valid fill type : " + type);
    }
  }

  private void parseOrderByTimeClause(OrderByTimeClauseContext ctx) {
    if (ctx.DESC() != null) {
      SpecialClauseComponent specialClauseComponent = queryOp.getSpecialClauseComponent();
      if (specialClauseComponent == null) {
        specialClauseComponent = new SpecialClauseComponent();
      }
      specialClauseComponent.setAscending(false);
      queryOp.setSpecialClauseComponent(specialClauseComponent);
    }
  }

  private void parseAlignByDeviceClause(SpecialClauseComponent specialClauseComponent) {
    specialClauseComponent.setAlignByDevice(true);
  }

  private void parseDisableAlign(SpecialClauseComponent specialClauseComponent) {
    specialClauseComponent.setAlignByTime(false);
  }

  private void parseTimeInterval(
      TimeIntervalContext timeInterval, GroupByClauseComponent groupByClauseComponent) {
    long startTime;
    long endTime;
    long currentTime = DatetimeUtils.currentTime();
    if (timeInterval.timeValue(0).INT() != null) {
      startTime = Long.parseLong(timeInterval.timeValue(0).INT().getText());
    } else if (timeInterval.timeValue(0).dateExpression() != null) {
      startTime = parseDateExpression(timeInterval.timeValue(0).dateExpression(), currentTime);
    } else {
      startTime = parseTimeFormat(timeInterval.timeValue(0).dateFormat().getText(), currentTime);
    }
    if (timeInterval.timeValue(1).INT() != null) {
      endTime = Long.parseLong(timeInterval.timeValue(1).INT().getText());
    } else if (timeInterval.timeValue(1).dateExpression() != null) {
      endTime = parseDateExpression(timeInterval.timeValue(1).dateExpression(), currentTime);
    } else {
      endTime = parseTimeFormat(timeInterval.timeValue(1).dateFormat().getText(), currentTime);
    }

    groupByClauseComponent.setStartTime(startTime);
    groupByClauseComponent.setEndTime(endTime);
    if (startTime >= endTime) {
      throw new SQLParserException("Start time should be smaller than endTime in GroupBy");
    }
  }

  private void parseShowWhereClause(ShowWhereClauseContext ctx, ShowTimeSeriesOperator operator) {
    PropertyValueContext propertyValueContext;
    if (ctx.containsExpression() != null) {
      operator.setContains(true);
      propertyValueContext = ctx.containsExpression().propertyValue();
      operator.setKey(ctx.containsExpression().ID().getText());
    } else {
      operator.setContains(false);
      propertyValueContext = ctx.property().propertyValue();
      operator.setKey(ctx.property().ID().getText());
    }
    String value;
    if (propertyValueContext.stringLiteral() != null) {
      value = removeStringQuote(propertyValueContext.getText());
    } else {
      value = propertyValueContext.getText();
    }
    operator.setValue(value);
  }

  private String[] parsePrivilege(PrivilegesContext ctx) {
    List<StringLiteralContext> privilegeList = ctx.stringLiteral();
    List<String> privileges = new ArrayList<>();
    for (StringLiteralContext privilege : privilegeList) {
      privileges.add(removeStringQuote(privilege.getText()));
    }
    return privileges.toArray(new String[0]);
  }

  private Pair<Long, Long> parseDeleteTimeInterval(FilterOperator filterOperator) {
    if (!filterOperator.isLeaf() && filterOperator.getFilterType() != FilterType.KW_AND) {
      throw new SQLParserException(DELETE_RANGE_ERROR_MSG);
    }

    if (filterOperator.isLeaf()) {
      return calcOperatorInterval(filterOperator);
    }

    List<FilterOperator> children = filterOperator.getChildren();
    FilterOperator lOperator = children.get(0);
    FilterOperator rOperator = children.get(1);
    if (!lOperator.isLeaf() || !rOperator.isLeaf()) {
      throw new SQLParserException(DELETE_RANGE_ERROR_MSG);
    }

    Pair<Long, Long> leftOpInterval = calcOperatorInterval(lOperator);
    Pair<Long, Long> rightOpInterval = calcOperatorInterval(rOperator);
    Pair<Long, Long> parsedInterval =
        new Pair<>(
            Math.max(leftOpInterval.left, rightOpInterval.left),
            Math.min(leftOpInterval.right, rightOpInterval.right));
    if (parsedInterval.left > parsedInterval.right) {
      throw new SQLParserException(
          "Invalid delete range: [" + parsedInterval.left + ", " + parsedInterval.right + "]");
    }
    return parsedInterval;
  }

  private Pair<Long, Long> calcOperatorInterval(FilterOperator filterOperator) {
    long time = Long.parseLong(((BasicFunctionOperator) filterOperator).getValue());
    switch (filterOperator.getFilterType()) {
      case LESSTHAN:
        return new Pair<>(Long.MIN_VALUE, time - 1);
      case LESSTHANOREQUALTO:
        return new Pair<>(Long.MIN_VALUE, time);
      case GREATERTHAN:
        return new Pair<>(time + 1, Long.MAX_VALUE);
      case GREATERTHANOREQUALTO:
        return new Pair<>(time, Long.MAX_VALUE);
      case EQUAL:
        return new Pair<>(time, time);
      default:
        throw new SQLParserException(DELETE_RANGE_ERROR_MSG);
    }
  }

  public WhereComponent parseWhereClause(WhereClauseContext ctx) {
    if (ctx.indexPredicateClause() != null) {
      parseIndexPredicate(ctx.indexPredicateClause());
      return null;
    }
    FilterOperator whereOp = new FilterOperator();
    whereOp.addChildOperator(parseOrExpression(ctx.orExpression()));
    return new WhereComponent(whereOp.getChildren().get(0));
  }

  private FilterOperator parseOrExpression(OrExpressionContext ctx) {
    if (ctx.andExpression().size() == 1) {
      return parseAndExpression(ctx.andExpression(0));
    }
    FilterOperator binaryOp = new FilterOperator(FilterType.KW_OR);
    if (ctx.andExpression().size() > 2) {
      binaryOp.addChildOperator(parseAndExpression(ctx.andExpression(0)));
      binaryOp.addChildOperator(parseAndExpression(ctx.andExpression(1)));
      for (int i = 2; i < ctx.andExpression().size(); i++) {
        FilterOperator op = new FilterOperator(FilterType.KW_OR);
        op.addChildOperator(binaryOp);
        op.addChildOperator(parseAndExpression(ctx.andExpression(i)));
        binaryOp = op;
      }
    } else {
      for (AndExpressionContext andExpressionContext : ctx.andExpression()) {
        binaryOp.addChildOperator(parseAndExpression(andExpressionContext));
      }
    }
    return binaryOp;
  }

  private FilterOperator parseAndExpression(AndExpressionContext ctx) {
    if (ctx.predicate().size() == 1) {
      return parsePredicate(ctx.predicate(0));
    }
    FilterOperator binaryOp = new FilterOperator(FilterType.KW_AND);
    int size = ctx.predicate().size();
    if (size > 2) {
      binaryOp.addChildOperator(parsePredicate(ctx.predicate(0)));
      binaryOp.addChildOperator(parsePredicate(ctx.predicate(1)));
      for (int i = 2; i < size; i++) {
        FilterOperator op = new FilterOperator(FilterType.KW_AND);
        op.addChildOperator(binaryOp);
        op.addChildOperator(parsePredicate(ctx.predicate(i)));
        binaryOp = op;
      }
    } else {
      for (PredicateContext predicateContext : ctx.predicate()) {
        binaryOp.addChildOperator(parsePredicate(predicateContext));
      }
    }
    return binaryOp;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private FilterOperator parsePredicate(PredicateContext ctx) {
    if (ctx.OPERATOR_NOT() != null) {
      FilterOperator notOp = new FilterOperator(FilterType.KW_NOT);
      notOp.addChildOperator(parseOrExpression(ctx.orExpression()));
      return notOp;
    } else if (ctx.LR_BRACKET() != null && ctx.OPERATOR_NOT() == null) {
      return parseOrExpression(ctx.orExpression());
    } else {
      PartialPath path = null;
      if (ctx.TIME() != null || ctx.TIMESTAMP() != null) {
        path = new PartialPath(SQLConstant.getSingleTimeArray());
      }
      if (ctx.fullPath() != null) {
        path = parseFullPath(ctx.fullPath());
      }
      if (ctx.suffixPath() != null) {
        path = parseSuffixPath(ctx.suffixPath());
      }
      if (path == null) {
        throw new SQLParserException("Path is null, please check the sql.");
      }
      if (ctx.inClause() != null) {
        return parseInOperator(ctx.inClause(), path);
      } else {
        return parseBasicFunctionOperator(ctx, path);
      }
    }
  }

  private FilterOperator parseInOperator(InClauseContext ctx, PartialPath path) {
    Set<String> values = new HashSet<>();
    boolean not = ctx.OPERATOR_NOT() != null;
    for (ConstantContext constant : ctx.constant()) {
      if (constant.dateExpression() != null) {
        if (!path.equals(TIME_PATH)) {
          throw new SQLParserException(path.getFullPath(), "Date can only be used to time");
        }
        values.add(Long.toString(parseDateExpression(constant.dateExpression())));
      } else {
        values.add(constant.getText());
      }
    }
    return new InOperator(FilterType.IN, path, not, values);
  }

  private FilterOperator parseBasicFunctionOperator(PredicateContext ctx, PartialPath path) {
    BasicFunctionOperator basic;
    if (ctx.constant().dateExpression() != null) {
      if (!path.equals(TIME_PATH)) {
        throw new SQLParserException(path.getFullPath(), "Date can only be used to time");
      }
      basic =
          new BasicFunctionOperator(
              FilterConstant.lexerToFilterType.get(ctx.comparisonOperator().type.getType()),
              path,
              Long.toString(parseDateExpression(ctx.constant().dateExpression())));
    } else {
      basic =
          new BasicFunctionOperator(
              FilterConstant.lexerToFilterType.get(ctx.comparisonOperator().type.getType()),
              path,
              ctx.constant().getText());
    }
    return basic;
  }

  /**
   * parse time expression, which is addition and subtraction expression of duration time, now() or
   * DataTimeFormat time.
   *
   * <p>eg. now() + 1d - 2h
   */
  private Long parseDateExpression(DateExpressionContext ctx) {
    long time;
    time = parseTimeFormat(ctx.getChild(0).getText());
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if (ctx.getChild(i).getText().equals("+")) {
        time += DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      } else {
        time -= DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      }
    }
    return time;
  }

  private Long parseDateExpression(DateExpressionContext ctx, long currentTime) {
    long time;
    time = parseTimeFormat(ctx.getChild(0).getText(), currentTime);
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if (ctx.getChild(i).getText().equals("+")) {
        time += DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      } else {
        time -= DatetimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText());
      }
    }
    return time;
  }

  /**
   * parse time unit or sliding step in group by query.
   *
   * @param durationStr represent duration string like: 12d8m9ns, 1y1d, etc.
   * @return time in milliseconds, microseconds, or nanoseconds depending on the profile
   */
  private long parseTimeUnitOrSlidingStep(
      String durationStr, boolean isParsingTimeUnit, GroupByClauseComponent groupByComponent) {
    if (durationStr.toLowerCase().contains("mo")) {
      if (isParsingTimeUnit) {
        groupByComponent.setIntervalByMonth(true);
      } else {
        groupByComponent.setSlidingStepByMonth(true);
      }
    }
    return DatetimeUtils.convertDurationStrToLong(durationStr);
  }

  private PartialPath parseSuffixPath(SuffixPathContext ctx) {
    List<NodeNameContext> nodeNames = ctx.nodeName();
    String[] path = new String[nodeNames.size()];
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i] = nodeNames.get(i).getText();
    }
    return new PartialPath(path);
  }

  private void parseInsertColumnSpec(InsertColumnsSpecContext ctx, InsertOperator insertOp) {
    List<MeasurementNameContext> measurementNames = ctx.measurementName();
    List<String> measurementList = new ArrayList<>();
    for (MeasurementNameContext measurementName : measurementNames) {
      String measurement = measurementName.getText();
      measurementList.add(measurement);
    }
    insertOp.setMeasurementList(measurementList.toArray(new String[0]));
  }

  private void parseInsertValuesSpec(InsertValuesSpecContext ctx, InsertOperator insertOp) {
    List<InsertMultiValueContext> insertMultiValues = ctx.insertMultiValue();
    List<String> valueList = new ArrayList<>();
    long[] timeArray = new long[insertMultiValues.size()];
    for (int i = 0; i < insertMultiValues.size(); i++) {
      long timestamp;
      if (insertMultiValues.get(i).dateFormat() != null) {
        timestamp = parseTimeFormat(insertMultiValues.get(i).dateFormat().getText());
      } else if (insertMultiValues.get(i).INT() != null) {
        timestamp = Long.parseLong(insertMultiValues.get(i).INT().getText());
      } else if (insertMultiValues.size() != 1) {
        throw new SQLParserException("need timestamps when insert multi rows");
      } else {
        timestamp = parseTimeFormat("now()");
      }
      timeArray[i] = timestamp;
      List<MeasurementValueContext> values = insertMultiValues.get(i).measurementValue();
      for (MeasurementValueContext value : values) {
        for (ConstantContext counstant : value.constant()) {
          valueList.add(counstant.getText());
        }
      }
    }
    insertOp.setTimes(timeArray);
    insertOp.setValueList(valueList.toArray(new String[0]));
  }

  private void parseAlterClause(
      AlterClauseContext ctx, AlterTimeSeriesOperator alterTimeSeriesOperator) {
    Map<String, String> alterMap = new HashMap<>();
    // rename
    if (ctx.RENAME() != null) {
      alterTimeSeriesOperator.setAlterType(AlterType.RENAME);
      alterMap.put(ctx.beforeName.getText(), ctx.currentName.getText());
    } else if (ctx.SET() != null) {
      // set
      alterTimeSeriesOperator.setAlterType(AlterType.SET);
      setMap(ctx, alterMap);
    } else if (ctx.DROP() != null) {
      // drop
      alterTimeSeriesOperator.setAlterType(AlterType.DROP);
      for (TerminalNode dropId : ctx.ID()) {
        alterMap.put(dropId.getText(), null);
      }
    } else if (ctx.TAGS() != null) {
      // add tag
      alterTimeSeriesOperator.setAlterType(AlterType.ADD_TAGS);
      setMap(ctx, alterMap);
    } else if (ctx.ATTRIBUTES() != null) {
      // add attribute
      alterTimeSeriesOperator.setAlterType(AlterType.ADD_ATTRIBUTES);
      setMap(ctx, alterMap);
    } else {
      // upsert
      alterTimeSeriesOperator.setAlterType(AlterType.UPSERT);
      if (ctx.aliasClause() != null) {
        parseAliasClause(ctx.aliasClause(), alterTimeSeriesOperator);
      }
      if (ctx.tagClause() != null) {
        parseTagClause(ctx.tagClause(), alterTimeSeriesOperator);
      }
      if (ctx.attributeClause() != null) {
        parseAttributeClause(ctx.attributeClause(), alterTimeSeriesOperator);
      }
    }
    alterTimeSeriesOperator.setAlterMap(alterMap);
  }

  public void parseAliasClause(
      AliasClauseContext ctx, AlterTimeSeriesOperator alterTimeSeriesOperator) {
    if (alterTimeSeriesOperator != null && ctx.ID() != null) {
      alterTimeSeriesOperator.setAlias(ctx.ID().getText());
    }
  }

  private void setMap(SqlBaseParser.AlterClauseContext ctx, Map<String, String> alterMap) {
    List<PropertyContext> tagsList = ctx.property();
    if (ctx.property(0) != null) {
      for (PropertyContext property : tagsList) {
        String value;
        if (property.propertyValue().stringLiteral() != null) {
          value = removeStringQuote(property.propertyValue().getText());
        } else {
          value = property.propertyValue().getText();
        }
        alterMap.put(property.ID().getText(), value);
      }
    }
  }

  private String removeStringQuote(String src) {
    if (src.charAt(0) == '\'' && src.charAt(src.length() - 1) == '\'') {
      return src.substring(1, src.length() - 1);
    } else if (src.charAt(0) == '\"' && src.charAt(src.length() - 1) == '\"') {
      return src.substring(1, src.length() - 1);
    } else {
      throw new SQLParserException("error format for string with quote:" + src);
    }
  }

  private PartialPath parsePrefixPath(PrefixPathContext ctx) {
    List<NodeNameContext> nodeNames = ctx.nodeName();
    String[] path = new String[nodeNames.size() + 1];
    path[0] = ctx.ROOT().getText();
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i + 1] = nodeNames.get(i).getText();
    }
    return new PartialPath(path);
  }

  public void parseAttributeClauses(
      AttributeClausesContext ctx, CreateTimeSeriesOperator createTimeSeriesOperator) {
    final String dataType = ctx.dataType().getChild(0).getText().toUpperCase();
    final TSDataType tsDataType = TSDataType.valueOf(dataType);
    createTimeSeriesOperator.setDataType(tsDataType);

    final IoTDBDescriptor ioTDBDescriptor = IoTDBDescriptor.getInstance();
    TSEncoding encoding = ioTDBDescriptor.getDefaultEncodingByType(tsDataType);
    if (Objects.nonNull(ctx.encoding())) {
      String encodingString = ctx.encoding().getChild(0).getText().toUpperCase();
      encoding = TSEncoding.valueOf(encodingString);
    }
    createTimeSeriesOperator.setEncoding(encoding);

    CompressionType compressor;
    List<PropertyContext> properties = ctx.property();
    if (ctx.compressor() != null) {
      compressor = CompressionType.valueOf(ctx.compressor().getText().toUpperCase());
    } else {
      compressor = TSFileDescriptor.getInstance().getConfig().getCompressor();
    }
    Map<String, String> props = null;
    if (ctx.property(0) != null) {
      props = new HashMap<>(properties.size());
      for (PropertyContext property : properties) {
        props.put(
            property.ID().getText().toLowerCase(),
            property.propertyValue().getText().toLowerCase());
      }
    }
    createTimeSeriesOperator.setCompressor(compressor);
    createTimeSeriesOperator.setProps(props);
    if (ctx.tagClause() != null) {
      parseTagClause(ctx.tagClause(), createTimeSeriesOperator);
    }
    if (ctx.attributeClause() != null) {
      parseAttributeClause(ctx.attributeClause(), createTimeSeriesOperator);
    }
  }

  public void parseAttributeClause(AttributeClauseContext ctx, Operator operator) {
    Map<String, String> attributes = extractMap(ctx.property(), ctx.property(0));
    if (operator instanceof CreateTimeSeriesOperator) {
      ((CreateTimeSeriesOperator) operator).setAttributes(attributes);
    } else if (operator instanceof AlterTimeSeriesOperator) {
      ((AlterTimeSeriesOperator) operator).setAttributesMap(attributes);
    }
  }

  public void parseTagClause(TagClauseContext ctx, Operator operator) {
    Map<String, String> tags = extractMap(ctx.property(), ctx.property(0));
    if (operator instanceof CreateTimeSeriesOperator) {
      ((CreateTimeSeriesOperator) operator).setTags(tags);
    } else if (operator instanceof AlterTimeSeriesOperator) {
      ((AlterTimeSeriesOperator) operator).setTagsMap(tags);
    }
  }

  private Map<String, String> extractMap(
      List<PropertyContext> property2, PropertyContext property3) {
    String value;
    Map<String, String> tags = new HashMap<>(property2.size());
    if (property3 != null) {
      for (PropertyContext property : property2) {
        if (property.propertyValue().stringLiteral() != null) {
          value = removeStringQuote(property.propertyValue().getText());
        } else {
          value = property.propertyValue().getText();
        }
        tags.put(property.ID().getText(), value);
      }
    }
    return tags;
  }

  private PartialPath parseFullPath(FullPathContext ctx) {
    List<NodeNameWithoutStarContext> nodeNamesWithoutStar = ctx.nodeNameWithoutStar();
    String[] path = new String[nodeNamesWithoutStar.size() + 1];
    int i = 0;
    if (ctx.ROOT() != null) {
      path[0] = ctx.ROOT().getText();
    }
    for (NodeNameWithoutStarContext nodeNameWithoutStar : nodeNamesWithoutStar) {
      i++;
      path[i] = nodeNameWithoutStar.getText();
    }
    return new PartialPath(path);
  }

  /** function for parsing time format. */
  public long parseTimeFormat(String timestampStr) throws SQLParserException {
    if (timestampStr == null || timestampStr.trim().equals("")) {
      throw new SQLParserException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return DatetimeUtils.currentTime();
    }
    try {
      return DatetimeUtils.convertDatetimeStrToLong(timestampStr, zoneId);
    } catch (Exception e) {
      throw new SQLParserException(
          String.format(
              "Input time format %s error. "
                  + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
                  + "refer to user document for more info.",
              timestampStr));
    }
  }

  public long parseTimeFormat(String timestampStr, long currentTime) throws SQLParserException {
    if (timestampStr == null || timestampStr.trim().equals("")) {
      throw new SQLParserException("input timestamp cannot be empty");
    }
    if (timestampStr.equalsIgnoreCase(SQLConstant.NOW_FUNC)) {
      return currentTime;
    }
    try {
      return DatetimeUtils.convertDatetimeStrToLong(timestampStr, zoneId);
    } catch (Exception e) {
      throw new SQLParserException(
          String.format(
              "Input time format %s error. "
                  + "Input like yyyy-MM-dd HH:mm:ss, yyyy-MM-ddTHH:mm:ss or "
                  + "refer to user document for more info.",
              timestampStr));
    }
  }
}
