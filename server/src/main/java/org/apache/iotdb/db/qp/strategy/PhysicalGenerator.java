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
package org.apache.iotdb.db.qp.strategy;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.engine.compaction.heavyhitter.QueryHitterManager;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.DeleteDataOperator;
import org.apache.iotdb.db.qp.logical.crud.FillClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.FillQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.GroupByClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.GroupByFillClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.GroupByFillQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.GroupByQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.InsertOperator;
import org.apache.iotdb.db.qp.logical.crud.LastQueryOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SpecialClauseComponent;
import org.apache.iotdb.db.qp.logical.crud.WhereComponent;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.CountOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateFunctionOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateIndexOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateTriggerOperator;
import org.apache.iotdb.db.qp.logical.sys.DataAuthOperator;
import org.apache.iotdb.db.qp.logical.sys.DeletePartitionOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.DeleteTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.DropFunctionOperator;
import org.apache.iotdb.db.qp.logical.sys.DropIndexOperator;
import org.apache.iotdb.db.qp.logical.sys.DropTriggerOperator;
import org.apache.iotdb.db.qp.logical.sys.FlushOperator;
import org.apache.iotdb.db.qp.logical.sys.KillQueryOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator.LoadConfigurationOperatorType;
import org.apache.iotdb.db.qp.logical.sys.LoadDataOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadFilesOperator;
import org.apache.iotdb.db.qp.logical.sys.MoveFileOperator;
import org.apache.iotdb.db.qp.logical.sys.RemoveFileOperator;
import org.apache.iotdb.db.qp.logical.sys.SetStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.SetTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowChildNodesOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowChildPathsOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowDevicesOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowFunctionsOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowLockInfoOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowStorageGroupOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTTLOperator;
import org.apache.iotdb.db.qp.logical.sys.ShowTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.StartTriggerOperator;
import org.apache.iotdb.db.qp.logical.sys.StopTriggerOperator;
import org.apache.iotdb.db.qp.logical.sys.TracingOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.DeletePartitionPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.ClearCachePlan;
import org.apache.iotdb.db.qp.physical.sys.CountPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateSnapshotPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.DropIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.KillQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.qp.physical.sys.LoadDataPlan;
import org.apache.iotdb.db.qp.physical.sys.MergePlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildNodesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowFunctionsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowLockInfoPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowMergeStatusPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan.ShowContentType;
import org.apache.iotdb.db.qp.physical.sys.ShowQueryProcesslistPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTriggersPlan;
import org.apache.iotdb.db.qp.physical.sys.StartTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StopTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.TracingPlan;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Used to convert logical operator to physical plan */
public class PhysicalGenerator {

  public PhysicalPlan transformToPhysicalPlan(Operator operator, int fetchSize)
      throws QueryProcessException {
    PhysicalPlan physicalPlan = doTransformation(operator, fetchSize);
    physicalPlan.setDebug(operator.isDebug());
    return physicalPlan;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private PhysicalPlan doTransformation(Operator operator, int fetchSize)
      throws QueryProcessException {
    switch (operator.getType()) {
      case AUTHOR:
        AuthorOperator author = (AuthorOperator) operator;
        try {
          return new AuthorPlan(
              author.getAuthorType(),
              author.getUserName(),
              author.getRoleName(),
              author.getPassWord(),
              author.getNewPassword(),
              author.getPrivilegeList(),
              author.getNodeName());
        } catch (AuthException e) {
          throw new QueryProcessException(e.getMessage());
        }
      case GRANT_WATERMARK_EMBEDDING:
      case REVOKE_WATERMARK_EMBEDDING:
        DataAuthOperator dataAuthOperator = (DataAuthOperator) operator;
        return new DataAuthPlan(dataAuthOperator.getType(), dataAuthOperator.getUsers());
      case LOADDATA:
        LoadDataOperator loadData = (LoadDataOperator) operator;
        return new LoadDataPlan(loadData.getInputFilePath(), loadData.getMeasureType());
      case METADATA:
      case SET_STORAGE_GROUP:
        SetStorageGroupOperator setStorageGroup = (SetStorageGroupOperator) operator;
        return new SetStorageGroupPlan(setStorageGroup.getPath());
      case DELETE_STORAGE_GROUP:
        DeleteStorageGroupOperator deleteStorageGroup = (DeleteStorageGroupOperator) operator;
        return new DeleteStorageGroupPlan(deleteStorageGroup.getDeletePathList());
      case CREATE_TIMESERIES:
        CreateTimeSeriesOperator createOperator = (CreateTimeSeriesOperator) operator;
        if (createOperator.getTags() != null
            && !createOperator.getTags().isEmpty()
            && createOperator.getAttributes() != null
            && !createOperator.getAttributes().isEmpty()) {
          for (String tagKey : createOperator.getTags().keySet()) {
            if (createOperator.getAttributes().containsKey(tagKey)) {
              throw new QueryProcessException(
                  String.format(
                      "Tag and attribute shouldn't have the same property key [%s]", tagKey));
            }
          }
        }
        return new CreateTimeSeriesPlan(
            createOperator.getPath(),
            createOperator.getDataType(),
            createOperator.getEncoding(),
            createOperator.getCompressor(),
            createOperator.getProps(),
            createOperator.getTags(),
            createOperator.getAttributes(),
            createOperator.getAlias());
      case DELETE_TIMESERIES:
        DeleteTimeSeriesOperator deletePath = (DeleteTimeSeriesOperator) operator;
        return new DeleteTimeSeriesPlan(deletePath.getDeletePathList());
      case CREATE_INDEX:
        CreateIndexOperator createIndexOp = (CreateIndexOperator) operator;
        return new CreateIndexPlan(
            createIndexOp.getPaths(),
            createIndexOp.getProps(),
            createIndexOp.getTime(),
            createIndexOp.getIndexType());
      case DROP_INDEX:
        DropIndexOperator dropIndexOp = (DropIndexOperator) operator;
        return new DropIndexPlan(dropIndexOp.getPaths(), dropIndexOp.getIndexType());
      case ALTER_TIMESERIES:
        AlterTimeSeriesOperator alterTimeSeriesOperator = (AlterTimeSeriesOperator) operator;
        return new AlterTimeSeriesPlan(
            alterTimeSeriesOperator.getPath(),
            alterTimeSeriesOperator.getAlterType(),
            alterTimeSeriesOperator.getAlterMap(),
            alterTimeSeriesOperator.getAlias(),
            alterTimeSeriesOperator.getTagsMap(),
            alterTimeSeriesOperator.getAttributesMap());
      case DELETE:
        DeleteDataOperator delete = (DeleteDataOperator) operator;
        return new DeletePlan(delete.getStartTime(), delete.getEndTime(), delete.getPaths());
      case INSERT:
        InsertOperator insert = (InsertOperator) operator;
        int measurementsNum = 0;
        for (String measurement : insert.getMeasurementList()) {
          if (measurement.startsWith("(") && measurement.endsWith(")")) {
            measurementsNum += measurement.replace("(", "").replace(")", "").split(",").length;
          } else {
            measurementsNum++;
          }
        }
        if (measurementsNum == 0 || (insert.getValueList().length % measurementsNum != 0)) {
          throw new SQLParserException(
              String.format(
                  "the measurementList's size %d is not consistent with the valueList's size %d",
                  measurementsNum, insert.getValueList().length));
        }
        if (measurementsNum == insert.getValueList().length) {
          return new InsertRowPlan(
              insert.getDevice(),
              insert.getTimes()[0],
              insert.getMeasurementList(),
              insert.getValueList());
        }
        InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
        for (int i = 0; i < insert.getTimes().length; i++) {
          insertRowsPlan.addOneInsertRowPlan(
              new InsertRowPlan(
                  insert.getDevice(),
                  insert.getTimes()[i],
                  insert.getMeasurementList(),
                  Arrays.copyOfRange(
                      insert.getValueList(), i * measurementsNum, (i + 1) * measurementsNum)),
              i);
        }
        return insertRowsPlan;
      case MERGE:
        if (operator.getTokenIntType() == SQLConstant.TOK_FULL_MERGE) {
          return new MergePlan(OperatorType.FULL_MERGE);
        } else {
          return new MergePlan();
        }
      case FLUSH:
        FlushOperator flushOperator = (FlushOperator) operator;
        return new FlushPlan(flushOperator.isSeq(), flushOperator.getStorageGroupList());
      case TRACING:
        TracingOperator tracingOperator = (TracingOperator) operator;
        return new TracingPlan(tracingOperator.isTracingOn());
      case QUERY:
        return transformQuery((QueryOperator) operator);
      case TTL:
        switch (operator.getTokenIntType()) {
          case SQLConstant.TOK_SET:
            SetTTLOperator setTTLOperator = (SetTTLOperator) operator;
            return new SetTTLPlan(setTTLOperator.getStorageGroup(), setTTLOperator.getDataTTL());
          case SQLConstant.TOK_UNSET:
            SetTTLOperator unsetTTLOperator = (SetTTLOperator) operator;
            return new SetTTLPlan(unsetTTLOperator.getStorageGroup());
          case SQLConstant.TOK_SHOW:
            ShowTTLOperator showTTLOperator = (ShowTTLOperator) operator;
            return new ShowTTLPlan(showTTLOperator.getStorageGroups());
          default:
            throw new LogicalOperatorException(
                String.format(
                    "not supported operator type %s in ttl operation.", operator.getType()));
        }
      case LOAD_CONFIGURATION:
        LoadConfigurationOperatorType type =
            ((LoadConfigurationOperator) operator).getLoadConfigurationOperatorType();
        return generateLoadConfigurationPlan(type);
      case SHOW:
        switch (operator.getTokenIntType()) {
          case SQLConstant.TOK_FLUSH_TASK_INFO:
            return new ShowPlan(ShowContentType.FLUSH_TASK_INFO);
          case SQLConstant.TOK_VERSION:
            return new ShowPlan(ShowContentType.VERSION);
          case SQLConstant.TOK_TIMESERIES:
            ShowTimeSeriesOperator showTimeSeriesOperator = (ShowTimeSeriesOperator) operator;
            ShowTimeSeriesPlan showTimeSeriesPlan =
                new ShowTimeSeriesPlan(
                    showTimeSeriesOperator.getPath(),
                    showTimeSeriesOperator.getLimit(),
                    showTimeSeriesOperator.getOffset(),
                    fetchSize);
            showTimeSeriesPlan.setIsContains(showTimeSeriesOperator.isContains());
            showTimeSeriesPlan.setKey(showTimeSeriesOperator.getKey());
            showTimeSeriesPlan.setValue(showTimeSeriesOperator.getValue());
            showTimeSeriesPlan.setOrderByHeat(showTimeSeriesOperator.isOrderByHeat());
            return showTimeSeriesPlan;
          case SQLConstant.TOK_STORAGE_GROUP:
            return new ShowStorageGroupPlan(
                ShowContentType.STORAGE_GROUP, ((ShowStorageGroupOperator) operator).getPath());
          case SQLConstant.TOK_LOCK_INFO:
            return new ShowLockInfoPlan(
                ShowContentType.LOCK_INFO, ((ShowLockInfoOperator) operator).getPath());
          case SQLConstant.TOK_DEVICES:
            ShowDevicesOperator showDevicesOperator = (ShowDevicesOperator) operator;
            return new ShowDevicesPlan(
                showDevicesOperator.getPath(),
                showDevicesOperator.getLimit(),
                showDevicesOperator.getOffset(),
                fetchSize,
                showDevicesOperator.hasSgCol());
          case SQLConstant.TOK_COUNT_DEVICES:
            return new CountPlan(
                ShowContentType.COUNT_DEVICES, ((CountOperator) operator).getPath());
          case SQLConstant.TOK_COUNT_STORAGE_GROUP:
            return new CountPlan(
                ShowContentType.COUNT_STORAGE_GROUP, ((CountOperator) operator).getPath());
          case SQLConstant.TOK_COUNT_NODE_TIMESERIES:
            return new CountPlan(
                ShowContentType.COUNT_NODE_TIMESERIES,
                ((CountOperator) operator).getPath(),
                ((CountOperator) operator).getLevel());
          case SQLConstant.TOK_COUNT_NODES:
            return new CountPlan(
                ShowContentType.COUNT_NODES,
                ((CountOperator) operator).getPath(),
                ((CountOperator) operator).getLevel());
          case SQLConstant.TOK_COUNT_TIMESERIES:
            return new CountPlan(
                ShowContentType.COUNT_TIMESERIES, ((CountOperator) operator).getPath());
          case SQLConstant.TOK_CHILD_PATHS:
            return new ShowChildPathsPlan(
                ShowContentType.CHILD_PATH, ((ShowChildPathsOperator) operator).getPath());
          case SQLConstant.TOK_CHILD_NODES:
            return new ShowChildNodesPlan(
                ShowContentType.CHILD_NODE, ((ShowChildNodesOperator) operator).getPath());
          case SQLConstant.TOK_QUERY_PROCESSLIST:
            return new ShowQueryProcesslistPlan(ShowContentType.QUERY_PROCESSLIST);
          case SQLConstant.TOK_SHOW_FUNCTIONS:
            return new ShowFunctionsPlan(((ShowFunctionsOperator) operator).showTemporary());
          case SQLConstant.TOK_SHOW_TRIGGERS:
            return new ShowTriggersPlan();
          default:
            throw new LogicalOperatorException(
                String.format(
                    "not supported operator type %s in show operation.", operator.getType()));
        }
      case LOAD_FILES:
        return new OperateFilePlan(
            ((LoadFilesOperator) operator).getFile(),
            OperatorType.LOAD_FILES,
            ((LoadFilesOperator) operator).isAutoCreateSchema(),
            ((LoadFilesOperator) operator).getSgLevel());
      case REMOVE_FILE:
        return new OperateFilePlan(
            ((RemoveFileOperator) operator).getFile(), OperatorType.REMOVE_FILE);
      case MOVE_FILE:
        return new OperateFilePlan(
            ((MoveFileOperator) operator).getFile(),
            ((MoveFileOperator) operator).getTargetDir(),
            OperatorType.MOVE_FILE);
      case CLEAR_CACHE:
        return new ClearCachePlan();
      case SHOW_MERGE_STATUS:
        return new ShowMergeStatusPlan();
      case DELETE_PARTITION:
        DeletePartitionOperator op = (DeletePartitionOperator) operator;
        return new DeletePartitionPlan(op.getStorageGroupName(), op.getPartitionId());
      case CREATE_SCHEMA_SNAPSHOT:
        return new CreateSnapshotPlan();
      case KILL:
        return new KillQueryPlan(((KillQueryOperator) operator).getQueryId());
      case CREATE_FUNCTION:
        CreateFunctionOperator createFunctionOperator = (CreateFunctionOperator) operator;
        return new CreateFunctionPlan(
            createFunctionOperator.isTemporary(),
            createFunctionOperator.getUdfName(),
            createFunctionOperator.getClassName());
      case DROP_FUNCTION:
        DropFunctionOperator dropFunctionOperator = (DropFunctionOperator) operator;
        return new DropFunctionPlan(dropFunctionOperator.getUdfName());
      case CREATE_TRIGGER:
        CreateTriggerOperator createTriggerOperator = (CreateTriggerOperator) operator;
        return new CreateTriggerPlan(
            createTriggerOperator.getTriggerName(),
            createTriggerOperator.getEvent(),
            createTriggerOperator.getFullPath(),
            createTriggerOperator.getClassName(),
            createTriggerOperator.getAttributes());
      case DROP_TRIGGER:
        return new DropTriggerPlan(((DropTriggerOperator) operator).getTriggerName());
      case START_TRIGGER:
        return new StartTriggerPlan(((StartTriggerOperator) operator).getTriggerName());
      case STOP_TRIGGER:
        return new StopTriggerPlan(((StopTriggerOperator) operator).getTriggerName());
      default:
        throw new LogicalOperatorException(operator.getType().toString(), "");
    }
  }

  protected PhysicalPlan generateLoadConfigurationPlan(LoadConfigurationOperatorType type)
      throws QueryProcessException {
    switch (type) {
      case GLOBAL:
        return new LoadConfigurationPlan(LoadConfigurationPlanType.GLOBAL);
      case LOCAL:
        return new LoadConfigurationPlan(LoadConfigurationPlanType.LOCAL);
      default:
        throw new QueryProcessException(
            String.format("Unrecognized load configuration operator type, %s", type.name()));
    }
  }

  /**
   * get types for path list
   *
   * @return pair.left is the type of column in result set, pair.right is the real type of the
   *     measurement
   */
  protected Pair<List<TSDataType>, List<TSDataType>> getSeriesTypes(
      List<PartialPath> paths, String aggregation) throws MetadataException {
    List<TSDataType> measurementDataTypes = SchemaUtils.getSeriesTypesByPaths(paths, (String) null);
    // if the aggregation function is null, the type of column in result set
    // is equal to the real type of the measurement
    if (aggregation == null) {
      return new Pair<>(measurementDataTypes, measurementDataTypes);
    } else {
      // if the aggregation function is not null,
      // we should recalculate the type of column in result set
      List<TSDataType> columnDataTypes = SchemaUtils.getSeriesTypesByPaths(paths, aggregation);
      return new Pair<>(columnDataTypes, measurementDataTypes);
    }
  }

  public List<TSDataType> getSeriesTypes(List<PartialPath> paths) throws MetadataException {
    return SchemaUtils.getSeriesTypesByPaths(paths);
  }

  interface Transform {
    QueryPlan transform(QueryOperator queryOperator) throws QueryProcessException;
  }

  /** agg physical plan transform */
  public static class AggPhysicalPlanRule implements Transform {

    @Override
    public QueryPlan transform(QueryOperator queryOperator) throws QueryProcessException {
      AggregationPlan queryPlan;
      if (queryOperator.hasTimeSeriesGeneratingFunction()) {
        throw new QueryProcessException(
            "User-defined and built-in hybrid aggregation is not supported.");
      }
      if (queryOperator instanceof GroupByFillQueryOperator) {
        queryPlan = new GroupByTimeFillPlan();
      } else if (queryOperator instanceof GroupByQueryOperator) {
        queryPlan = new GroupByTimePlan();
      } else {
        queryPlan = new AggregationPlan();
      }

      queryPlan.setPaths(queryOperator.getSelectComponent().getPaths());
      queryPlan.setAggregations(queryOperator.getSelectComponent().getAggregationFunctions());

      if (queryOperator instanceof GroupByQueryOperator) {
        GroupByTimePlan groupByTimePlan = (GroupByTimePlan) queryPlan;
        GroupByClauseComponent groupByClauseComponent =
            (GroupByClauseComponent) queryOperator.getSpecialClauseComponent();
        groupByTimePlan.setInterval(groupByClauseComponent.getUnit());
        groupByTimePlan.setIntervalByMonth(groupByClauseComponent.isIntervalByMonth());
        groupByTimePlan.setSlidingStep(groupByClauseComponent.getSlidingStep());
        groupByTimePlan.setSlidingStepByMonth(groupByClauseComponent.isSlidingStepByMonth());
        groupByTimePlan.setLeftCRightO(groupByClauseComponent.isLeftCRightO());
        if (!groupByClauseComponent.isLeftCRightO()) {
          groupByTimePlan.setStartTime(groupByClauseComponent.getStartTime() + 1);
          groupByTimePlan.setEndTime(groupByClauseComponent.getEndTime() + 1);
        } else {
          groupByTimePlan.setStartTime(groupByClauseComponent.getStartTime());
          groupByTimePlan.setEndTime(groupByClauseComponent.getEndTime());
        }
      }
      if (queryOperator instanceof GroupByFillQueryOperator) {
        GroupByFillClauseComponent groupByFillClauseComponent =
            (GroupByFillClauseComponent) queryOperator.getSpecialClauseComponent();
        ((GroupByTimeFillPlan) queryPlan).setFillType(groupByFillClauseComponent.getFillTypes());
        for (String aggregation : queryPlan.getAggregations()) {
          if (!SQLConstant.LAST_VALUE.equals(aggregation)) {
            throw new QueryProcessException("Group By Fill only support last_value function");
          }
        }
      } else if (queryOperator.isGroupByLevel()) {
        queryPlan.setLevel(queryOperator.getSpecialClauseComponent().getLevel());
        try {
          if (!verifyAllAggregationDataTypesEqual(queryOperator)) {
            throw new QueryProcessException("Aggregate among unmatched data types");
          }
        } catch (MetadataException e) {
          throw new QueryProcessException(e);
        }
      }
      return queryPlan;
    }
  }

  /** fill physical plan transfrom */
  public static class FillPhysicalPlanRule implements Transform {

    @Override
    public QueryPlan transform(QueryOperator queryOperator) throws QueryProcessException {
      FillQueryOperator fillQueryOperator = (FillQueryOperator) queryOperator;
      if (queryOperator.hasTimeSeriesGeneratingFunction()) {
        throw new QueryProcessException("Fill functions are not supported in UDF queries.");
      }
      FillQueryPlan queryPlan = new FillQueryPlan();
      FilterOperator timeFilter = fillQueryOperator.getWhereComponent().getFilterOperator();
      if (!timeFilter.isSingle()) {
        throw new QueryProcessException("Slice query must select a single time point");
      }
      long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
      queryPlan.setQueryTime(time);
      queryPlan.setFillType(
          ((FillClauseComponent) fillQueryOperator.getSpecialClauseComponent()).getFillTypes());
      return queryPlan;
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private PhysicalPlan transformQuery(QueryOperator queryOperator) throws QueryProcessException {
    QueryPlan queryPlan;

    if (queryOperator.hasAggregationFunction()) {
      queryPlan = new AggPhysicalPlanRule().transform(queryOperator);
    } else if (queryOperator instanceof FillQueryOperator) {
      queryPlan = new FillPhysicalPlanRule().transform(queryOperator);
    } else if (queryOperator instanceof LastQueryOperator) {
      queryPlan = new LastQueryPlan();
    } else if (queryOperator.getIndexType() != null) {
      queryPlan = new QueryIndexPlan();
    } else if (queryOperator.hasTimeSeriesGeneratingFunction()) {
      queryPlan = new UDTFPlan(queryOperator.getSelectComponent().getZoneId());
      ((UDTFPlan) queryPlan)
          .constructUdfExecutors(queryOperator.getSelectComponent().getResultColumns());
    } else {
      queryPlan = new RawDataQueryPlan();
    }

    if (queryOperator.getSpecialClauseComponent() != null
        && queryOperator.getSpecialClauseComponent().isAlignByDevice()) {
      queryPlan = getAlignQueryPlan(queryOperator, queryPlan);
    } else {
      queryPlan.setPaths(queryOperator.getSelectComponent().getPaths());
      // Last query result set will not be affected by alignment
      if (queryPlan instanceof LastQueryPlan && !queryOperator.isAlignByTime()) {
        throw new QueryProcessException("Disable align cannot be applied to LAST query.");
      }
      queryPlan.setAlignByTime(queryOperator.isAlignByTime());

      // transform filter operator to expression
      WhereComponent whereComponent = queryOperator.getWhereComponent();

      if (whereComponent != null) {
        FilterOperator filterOperator = whereComponent.getFilterOperator();
        List<PartialPath> filterPaths = new ArrayList<>(filterOperator.getPathSet());
        try {
          List<TSDataType> seriesTypes = getSeriesTypes(filterPaths);
          HashMap<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
          for (int i = 0; i < filterPaths.size(); i++) {
            ((RawDataQueryPlan) queryPlan).addFilterPathInDeviceToMeasurements(filterPaths.get(i));
            pathTSDataTypeHashMap.put(filterPaths.get(i), seriesTypes.get(i));
          }
          IExpression expression = filterOperator.transformToExpression(pathTSDataTypeHashMap);
          ((RawDataQueryPlan) queryPlan).setExpression(expression);
        } catch (MetadataException e) {
          throw new LogicalOptimizeException(e.getMessage());
        }
      }
    }

    if (queryOperator.getIndexType() != null) {
      if (queryPlan instanceof QueryIndexPlan) {
        ((QueryIndexPlan) queryPlan).setIndexType(queryOperator.getIndexType());
        ((QueryIndexPlan) queryPlan).setProps(queryOperator.getProps());
      }
      return queryPlan;
    }

    queryPlan.setResultColumns(queryOperator.getSelectComponent().getResultColumns());

    try {
      List<PartialPath> paths = queryPlan.getPaths();
      List<TSDataType> dataTypes = getSeriesTypes(paths);
      queryPlan.setDataTypes(dataTypes);

      queryPlan.deduplicate(this);

      // estimate time series' query frequency
      if (IoTDBDescriptor.getInstance().getConfig().getCompactionStrategy()
              == CompactionStrategy.HITTER_LEVEL_COMPACTION
          && queryPlan instanceof RawDataQueryPlan) {
        QueryHitterManager.getInstance()
            .submitTask(
                QueryHitterManager.getInstance()
                .new HitterTask(((RawDataQueryPlan) queryPlan).getDeduplicatedPaths()));
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }

    if (queryOperator.getSpecialClauseComponent() != null) {
      SpecialClauseComponent specialClauseComponent = queryOperator.getSpecialClauseComponent();
      queryPlan.setWithoutAllNull(specialClauseComponent.isWithoutAllNull());
      queryPlan.setWithoutAnyNull(specialClauseComponent.isWithoutAnyNull());
      queryPlan.setRowLimit(specialClauseComponent.getRowLimit());
      queryPlan.setRowOffset(specialClauseComponent.getRowOffset());
      queryPlan.setAscending(specialClauseComponent.isAscending());
    }

    return queryPlan;
  }

  @SuppressWarnings("squid:S3777") // Suppress high Cognitive Complexity warning
  private QueryPlan getAlignQueryPlan(QueryOperator queryOperator, QueryPlan queryPlan)
      throws QueryProcessException {
    // below is the core realization of ALIGN_BY_DEVICE sql logic
    AlignByDevicePlan alignByDevicePlan = new AlignByDevicePlan();
    if (queryPlan instanceof GroupByTimePlan) {
      alignByDevicePlan.setGroupByTimePlan((GroupByTimePlan) queryPlan);
    } else if (queryPlan instanceof FillQueryPlan) {
      alignByDevicePlan.setFillQueryPlan((FillQueryPlan) queryPlan);
    } else if (queryPlan instanceof AggregationPlan) {
      if (((AggregationPlan) queryPlan).getLevel() >= 0) {
        throw new QueryProcessException("group by level does not support align by device now.");
      }
      alignByDevicePlan.setAggregationPlan((AggregationPlan) queryPlan);
    }

    List<PartialPath> prefixPaths = queryOperator.getFromComponent().getPrefixPaths();
    // remove stars in fromPaths and get deviceId with deduplication
    List<PartialPath> devices = this.removeStarsInDeviceWithUnique(prefixPaths);
    List<ResultColumn> resultColumns = queryOperator.getSelectComponent().getResultColumns();
    List<String> originAggregations = queryOperator.getSelectComponent().getAggregationFunctions();

    // to record result measurement columns
    List<String> measurements = new ArrayList<>();
    Map<String, String> measurementAliasMap = new HashMap<>();
    // to check the same measurement of different devices having the same datatype
    // record the data type of each column of result set
    Map<String, TSDataType> columnDataTypeMap = new HashMap<>();
    Map<String, MeasurementType> measurementTypeMap = new HashMap<>();

    // to record the real type of the corresponding measurement
    Map<String, TSDataType> measurementDataTypeMap = new HashMap<>();
    List<PartialPath> paths = new ArrayList<>();

    for (int i = 0; i < resultColumns.size(); i++) { // per suffix in SELECT
      ResultColumn resultColumn = resultColumns.get(i);
      Expression suffixExpression = resultColumn.getExpression();
      PartialPath suffixPath =
          suffixExpression instanceof TimeSeriesOperand
              ? ((TimeSeriesOperand) suffixExpression).getPath()
              : (((FunctionExpression) suffixExpression).getPaths().get(0));

      // to record measurements in the loop of a suffix path
      Set<String> measurementSetOfGivenSuffix = new LinkedHashSet<>();

      // if const measurement
      if (suffixPath.getMeasurement().startsWith("'")) {
        measurements.add(suffixPath.getMeasurement());
        measurementTypeMap.put(suffixPath.getMeasurement(), MeasurementType.Constant);
        continue;
      }

      for (PartialPath device : devices) { // per device in FROM after deduplication
        PartialPath fullPath = device.concatPath(suffixPath);
        try {
          // remove stars in SELECT to get actual paths
          List<PartialPath> actualPaths = getMatchedTimeseries(fullPath);
          if (resultColumn.hasAlias()) {
            if (actualPaths.size() == 1) {
              String columnName = actualPaths.get(0).getMeasurement();
              if (originAggregations != null && !originAggregations.isEmpty()) {
                measurementAliasMap.put(
                    originAggregations.get(i) + "(" + columnName + ")", resultColumn.getAlias());
              } else {
                measurementAliasMap.put(columnName, resultColumn.getAlias());
              }
            } else if (actualPaths.size() >= 2) {
              throw new QueryProcessException(
                  "alias '"
                      + resultColumn.getAlias()
                      + "' can only be matched with one time series");
            }
          }

          // for actual non exist path
          if (originAggregations != null && actualPaths.isEmpty() && originAggregations.isEmpty()) {
            String nonExistMeasurement = fullPath.getMeasurement();
            if (measurementSetOfGivenSuffix.add(nonExistMeasurement)
                && measurementTypeMap.get(nonExistMeasurement) != MeasurementType.Exist) {
              measurementTypeMap.put(fullPath.getMeasurement(), MeasurementType.NonExist);
            }
          }

          // Get data types with and without aggregate functions (actual time series) respectively
          // Data type with aggregation function `columnDataTypes` is used for:
          //  1. Data type consistency check 2. Header calculation, output result set
          // The actual data type of the time series `measurementDataTypes` is used for
          //  the actual query in the AlignByDeviceDataSet
          String aggregation =
              originAggregations != null && !originAggregations.isEmpty()
                  ? originAggregations.get(i)
                  : null;

          Pair<List<TSDataType>, List<TSDataType>> pair = getSeriesTypes(actualPaths, aggregation);
          List<TSDataType> columnDataTypes = pair.left;
          List<TSDataType> measurementDataTypes = pair.right;
          for (int pathIdx = 0; pathIdx < actualPaths.size(); pathIdx++) {
            PartialPath path = new PartialPath(actualPaths.get(pathIdx).getNodes());

            // check datatype consistency
            // a example of inconsistency: select s0 from root.sg1.d1, root.sg1.d2 align by
            // device,
            // while root.sg1.d1.s0 is INT32 and root.sg1.d2.s0 is FLOAT.
            String measurementChecked;
            if (originAggregations != null && !originAggregations.isEmpty()) {
              measurementChecked = originAggregations.get(i) + "(" + path.getMeasurement() + ")";
            } else {
              measurementChecked = path.getMeasurement();
            }
            TSDataType columnDataType = columnDataTypes.get(pathIdx);
            if (columnDataTypeMap.containsKey(measurementChecked)) {
              if (!columnDataType.equals(columnDataTypeMap.get(measurementChecked))) {
                throw new QueryProcessException(
                    "The data types of the same measurement column should be the same across "
                        + "devices in ALIGN_BY_DEVICE sql. For more details please refer to the "
                        + "SQL document.");
              }
            } else {
              columnDataTypeMap.put(measurementChecked, columnDataType);
              measurementDataTypeMap.put(measurementChecked, measurementDataTypes.get(pathIdx));
            }

            // This step indicates that the measurement exists under the device and is correct,
            // First, update measurementSetOfGivenSuffix which is distinct
            // Then if this measurement is recognized as NonExist before，update it to Exist
            if (measurementSetOfGivenSuffix.add(measurementChecked)
                || measurementTypeMap.get(measurementChecked) != MeasurementType.Exist) {
              measurementTypeMap.put(measurementChecked, MeasurementType.Exist);
            }

            // update paths
            paths.add(path);
          }

        } catch (MetadataException e) {
          throw new LogicalOptimizeException(
              String.format(
                      "Error when getting all paths of a full path: %s", fullPath.getFullPath())
                  + e.getMessage());
        }
      }

      // update measurements
      // Note that in the loop of a suffix path, set is used.
      // And across the loops of suffix paths, list is used.
      // e.g. select *,s1 from root.sg.d0, root.sg.d1
      // for suffix *, measurementSetOfGivenSuffix = {s1,s2,s3}
      // for suffix s1, measurementSetOfGivenSuffix = {s1}
      // therefore the final measurements is [s1,s2,s3,s1].
      measurements.addAll(measurementSetOfGivenSuffix);
    }

    // slimit trim on the measurementColumnList
    if (queryOperator.getSpecialClauseComponent().hasSlimit()) {
      int seriesSlimit = queryOperator.getSpecialClauseComponent().getSeriesLimit();
      int seriesOffset = queryOperator.getSpecialClauseComponent().getSeriesOffset();
      measurements = slimitTrimColumn(measurements, seriesSlimit, seriesOffset);
    }

    // assigns to alignByDevicePlan
    alignByDevicePlan.setMeasurements(measurements);
    alignByDevicePlan.setMeasurementAliasMap(measurementAliasMap);
    alignByDevicePlan.setDevices(devices);
    alignByDevicePlan.setColumnDataTypeMap(columnDataTypeMap);
    alignByDevicePlan.setMeasurementTypeMap(measurementTypeMap);
    alignByDevicePlan.setMeasurementDataTypeMap(measurementDataTypeMap);
    alignByDevicePlan.setPaths(paths);

    // get deviceToFilterMap
    WhereComponent whereComponent = queryOperator.getWhereComponent();
    if (whereComponent != null) {
      alignByDevicePlan.setDeviceToFilterMap(
          concatFilterByDevice(devices, whereComponent.getFilterOperator()));
    }

    queryPlan = alignByDevicePlan;
    return queryPlan;
  }

  // e.g. translate "select * from root.ln.d1, root.ln.d2 where s1 < 20 AND s2 > 10" to
  // [root.ln.d1 -> root.ln.d1.s1 < 20 AND root.ln.d1.s2 > 10,
  //  root.ln.d2 -> root.ln.d2.s1 < 20 AND root.ln.d2.s2 > 10)]
  private Map<String, IExpression> concatFilterByDevice(
      List<PartialPath> devices, FilterOperator operator) throws QueryProcessException {
    Map<String, IExpression> deviceToFilterMap = new HashMap<>();
    Set<PartialPath> filterPaths = new HashSet<>();
    for (PartialPath device : devices) {
      FilterOperator newOperator = operator.copy();
      concatFilterPath(device, newOperator, filterPaths);
      // transform to a list so it can be indexed
      List<PartialPath> filterPathList = new ArrayList<>(filterPaths);
      try {
        List<TSDataType> seriesTypes = getSeriesTypes(filterPathList);
        Map<PartialPath, TSDataType> pathTSDataTypeHashMap = new HashMap<>();
        for (int i = 0; i < filterPathList.size(); i++) {
          pathTSDataTypeHashMap.put(filterPathList.get(i), seriesTypes.get(i));
        }
        deviceToFilterMap.put(
            device.getFullPath(), newOperator.transformToExpression(pathTSDataTypeHashMap));
        filterPaths.clear();
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }

    return deviceToFilterMap;
  }

  private List<PartialPath> removeStarsInDeviceWithUnique(List<PartialPath> paths)
      throws LogicalOptimizeException {
    List<PartialPath> retDevices;
    Set<PartialPath> deviceSet = new LinkedHashSet<>();
    try {
      for (PartialPath path : paths) {
        Set<PartialPath> tempDS = getMatchedDevices(path);
        deviceSet.addAll(tempDS);
      }
      retDevices = new ArrayList<>(deviceSet);
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
    }
    return retDevices;
  }

  private void concatFilterPath(
      PartialPath prefix, FilterOperator operator, Set<PartialPath> filterPaths) {
    if (!operator.isLeaf()) {
      for (FilterOperator child : operator.getChildren()) {
        concatFilterPath(prefix, child, filterPaths);
      }
      return;
    }
    BasicFunctionOperator basicOperator = (BasicFunctionOperator) operator;
    PartialPath filterPath = basicOperator.getSinglePath();

    // do nothing in the cases of "where time > 5" or "where root.d1.s1 > 5"
    if (SQLConstant.isReservedPath(filterPath)
        || filterPath.getFirstNode().startsWith(SQLConstant.ROOT)) {
      filterPaths.add(filterPath);
      return;
    }

    PartialPath concatPath = prefix.concatPath(filterPath);
    filterPaths.add(concatPath);
    basicOperator.setSinglePath(concatPath);
  }

  public Pair<List<PartialPath>, Map<String, Integer>> getSeriesSchema(List<PartialPath> paths)
      throws MetadataException {
    return IoTDB.metaManager.getSeriesSchemas(paths);
  }

  private List<String> slimitTrimColumn(List<String> columnList, int seriesLimit, int seriesOffset)
      throws QueryProcessException {
    int size = columnList.size();

    // check parameter range
    if (seriesOffset >= size) {
      String errorMessage =
          "The value of SOFFSET (%d) is equal to or exceeds the number of sequences (%d) that can actually be returned.";
      throw new QueryProcessException(String.format(errorMessage, seriesOffset, size));
    }
    int endPosition = seriesOffset + seriesLimit;
    if (endPosition > size) {
      endPosition = size;
    }

    // trim seriesPath list
    return new ArrayList<>(columnList.subList(seriesOffset, endPosition));
  }

  private static boolean verifyAllAggregationDataTypesEqual(QueryOperator queryOperator)
      throws MetadataException {
    List<String> aggregations = queryOperator.getSelectComponent().getAggregationFunctions();
    if (aggregations.isEmpty()) {
      return true;
    }

    List<PartialPath> paths = queryOperator.getSelectComponent().getPaths();
    List<TSDataType> dataTypes = SchemaUtils.getSeriesTypesByPaths(paths);
    String aggType = aggregations.get(0);
    switch (aggType) {
      case SQLConstant.MIN_VALUE:
      case SQLConstant.MAX_VALUE:
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return dataTypes.stream().allMatch(dataTypes.get(0)::equals);
      default:
        return true;
    }
  }

  protected List<PartialPath> getMatchedTimeseries(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesPath(path);
  }

  protected Set<PartialPath> getMatchedDevices(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getDevices(path);
  }
}
