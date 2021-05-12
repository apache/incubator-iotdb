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

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.flush.pool.FlushTaskPoolManager;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeManager.TaskStatus;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.TimePartitionFilter;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.QueryIdNotExsitException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.exception.UDFRegistrationException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator;
import org.apache.iotdb.db.qp.logical.sys.AuthorOperator.AuthorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePartitionPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.SetDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.physical.sys.AlterTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CountPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.DataAuthPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.FlushPlan;
import org.apache.iotdb.db.qp.physical.sys.KillQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.MergePlan;
import org.apache.iotdb.db.qp.physical.sys.OperateFilePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildNodesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowChildPathsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowFunctionsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.StartTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StopTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.TracingPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.control.QueryTimeManager.QueryInfo;
import org.apache.iotdb.db.query.control.TracingManager;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.dataset.ShowDevicesDataSet;
import org.apache.iotdb.db.query.dataset.ShowTimeseriesDataSet;
import org.apache.iotdb.db.query.dataset.SingleDataSet;
import org.apache.iotdb.db.query.executor.IQueryRouter;
import org.apache.iotdb.db.query.executor.QueryRouter;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationInformation;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.tools.TsFileRewriteTool;
import org.apache.iotdb.db.utils.AuthUtils;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.EmptyDataSet;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_CANCELLED;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_CHILD_NODES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_CHILD_PATHS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_COLUMN;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_COUNT;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_CREATED_TIME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_DEVICES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_DONE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_FUNCTION_CLASS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_FUNCTION_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_FUNCTION_TYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ITEM;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PRIVILEGE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PROGRESS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ROLE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TASK_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TTL;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_USER;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;
import static org.apache.iotdb.db.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDAF;
import static org.apache.iotdb.db.conf.IoTDBConstant.FUNCTION_TYPE_BUILTIN_UDTF;
import static org.apache.iotdb.db.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDAF;
import static org.apache.iotdb.db.conf.IoTDBConstant.FUNCTION_TYPE_EXTERNAL_UDTF;
import static org.apache.iotdb.db.conf.IoTDBConstant.FUNCTION_TYPE_NATIVE;
import static org.apache.iotdb.db.conf.IoTDBConstant.QUERY_ID;
import static org.apache.iotdb.db.conf.IoTDBConstant.STATEMENT;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class PlanExecutor implements IPlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(PlanExecutor.class);
  // for data query
  protected IQueryRouter queryRouter;
  // for administration
  private IAuthorizer authorizer;

  private static final String INSERT_MEASUREMENTS_FAILED_MESSAGE = "failed to insert measurements ";

  public PlanExecutor() throws QueryProcessException {
    queryRouter = new QueryRouter();
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  @Override
  public QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException, QueryFilterOptimizationException,
          QueryProcessException, MetadataException, InterruptedException {
    if (queryPlan instanceof QueryPlan) {
      return processDataQuery((QueryPlan) queryPlan, context);
    } else if (queryPlan instanceof AuthorPlan) {
      return processAuthorQuery((AuthorPlan) queryPlan);
    } else if (queryPlan instanceof ShowPlan) {
      return processShowQuery((ShowPlan) queryPlan, context);
    } else {
      throw new QueryProcessException(String.format("Unrecognized query plan %s", queryPlan));
    }
  }

  @Override
  public boolean processNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    switch (plan.getOperatorType()) {
      case DELETE:
        delete((DeletePlan) plan);
        return true;
      case INSERT:
        insert((InsertRowPlan) plan);
        return true;
      case BATCH_INSERT_ONE_DEVICE:
        insert((InsertRowsOfOneDevicePlan) plan);
        return true;
      case BATCH_INSERT_ROWS:
        insert((InsertRowsPlan) plan);
        return true;
      case BATCHINSERT:
        insertTablet((InsertTabletPlan) plan);
        return true;
      case MULTI_BATCH_INSERT:
        insertTablet((InsertMultiTabletPlan) plan);
        return true;
      case CREATE_ROLE:
      case DELETE_ROLE:
      case CREATE_USER:
      case REVOKE_USER_ROLE:
      case REVOKE_ROLE_PRIVILEGE:
      case REVOKE_USER_PRIVILEGE:
      case GRANT_ROLE_PRIVILEGE:
      case GRANT_USER_PRIVILEGE:
      case GRANT_USER_ROLE:
      case MODIFY_PASSWORD:
      case DELETE_USER:
        AuthorPlan author = (AuthorPlan) plan;
        return operateAuthor(author);
      case GRANT_WATERMARK_EMBEDDING:
        return operateWatermarkEmbedding(((DataAuthPlan) plan).getUsers(), true);
      case REVOKE_WATERMARK_EMBEDDING:
        return operateWatermarkEmbedding(((DataAuthPlan) plan).getUsers(), false);
      case DELETE_TIMESERIES:
        return deleteTimeSeries((DeleteTimeSeriesPlan) plan);
      case CREATE_TIMESERIES:
        return createTimeSeries((CreateTimeSeriesPlan) plan);
      case CREATE_ALIGNED_TIMESERIES:
        return createAlignedTimeSeries((CreateAlignedTimeSeriesPlan) plan);
      case CREATE_MULTI_TIMESERIES:
        return createMultiTimeSeries((CreateMultiTimeSeriesPlan) plan);
      case ALTER_TIMESERIES:
        return alterTimeSeries((AlterTimeSeriesPlan) plan);
      case SET_STORAGE_GROUP:
        return setStorageGroup((SetStorageGroupPlan) plan);
      case DELETE_STORAGE_GROUP:
        return deleteStorageGroups((DeleteStorageGroupPlan) plan);
      case TTL:
        operateTTL((SetTTLPlan) plan);
        return true;
      case LOAD_CONFIGURATION:
        loadConfiguration((LoadConfigurationPlan) plan);
        return true;
      case LOAD_FILES:
        operateLoadFiles((OperateFilePlan) plan);
        return true;
      case REMOVE_FILE:
        operateRemoveFile((OperateFilePlan) plan);
        return true;
      case MOVE_FILE:
        operateMoveFile((OperateFilePlan) plan);
        return true;
      case FLUSH:
        operateFlush((FlushPlan) plan);
        return true;
      case MERGE:
      case FULL_MERGE:
        operateMerge((MergePlan) plan);
        return true;
      case TRACING:
        operateTracing((TracingPlan) plan);
        return true;
      case CLEAR_CACHE:
        operateClearCache();
        return true;
      case DELETE_PARTITION:
        DeletePartitionPlan p = (DeletePartitionPlan) plan;
        TimePartitionFilter filter =
            (storageGroupName, partitionId) ->
                storageGroupName.equals(
                        ((DeletePartitionPlan) plan).getStorageGroupName().getFullPath())
                    && p.getPartitionId().contains(partitionId);
        StorageEngine.getInstance()
            .removePartitions(((DeletePartitionPlan) plan).getStorageGroupName(), filter);
        return true;
      case CREATE_SCHEMA_SNAPSHOT:
        operateCreateSnapshot();
        return true;
      case CREATE_FUNCTION:
        return operateCreateFunction((CreateFunctionPlan) plan);
      case DROP_FUNCTION:
        return operateDropFunction((DropFunctionPlan) plan);
      case CREATE_TRIGGER:
        return operateCreateTrigger((CreateTriggerPlan) plan);
      case DROP_TRIGGER:
        return operateDropTrigger((DropTriggerPlan) plan);
      case START_TRIGGER:
        return operateStartTrigger((StartTriggerPlan) plan);
      case STOP_TRIGGER:
        return operateStopTrigger((StopTriggerPlan) plan);
      case CREATE_INDEX:
        throw new QueryProcessException("Create index hasn't been supported yet");
      case DROP_INDEX:
        throw new QueryProcessException("Drop index hasn't been supported yet");
      case KILL:
        try {
          operateKillQuery((KillQueryPlan) plan);
        } catch (QueryIdNotExsitException e) {
          throw new QueryProcessException(e.getMessage());
        }
        return true;
      case CREATE_TEMPLATE:
        return createDeviceTemplate((CreateTemplatePlan) plan);
      case SET_DEVICE_TEMPLATE:
        return setDeviceTemplate((SetDeviceTemplatePlan) plan);
      default:
        throw new UnsupportedOperationException(
            String.format("operation %s is not supported", plan.getOperatorType()));
    }
  }

  private boolean createDeviceTemplate(CreateTemplatePlan createTemplatePlan)
      throws QueryProcessException {
    try {
      IoTDB.metaManager.createDeviceTemplate(createTemplatePlan);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return true;
  }

  private boolean setDeviceTemplate(SetDeviceTemplatePlan setDeviceTemplatePlan)
      throws QueryProcessException {
    try {
      IoTDB.metaManager.setDeviceTemplate(setDeviceTemplatePlan);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return true;
  }

  private boolean operateCreateFunction(CreateFunctionPlan plan) throws UDFRegistrationException {
    UDFRegistrationService.getInstance()
        .register(plan.getUdfName(), plan.getClassName(), plan.isTemporary(), true);
    return true;
  }

  private boolean operateDropFunction(DropFunctionPlan plan) throws UDFRegistrationException {
    UDFRegistrationService.getInstance().deregister(plan.getUdfName());
    return true;
  }

  private boolean operateCreateTrigger(CreateTriggerPlan plan)
      throws TriggerManagementException, TriggerExecutionException {
    TriggerRegistrationService.getInstance().register(plan);
    return true;
  }

  private boolean operateDropTrigger(DropTriggerPlan plan) throws TriggerManagementException {
    TriggerRegistrationService.getInstance().deregister(plan);
    return true;
  }

  private boolean operateStartTrigger(StartTriggerPlan plan)
      throws TriggerManagementException, TriggerExecutionException {
    TriggerRegistrationService.getInstance().activate(plan);
    return true;
  }

  private boolean operateStopTrigger(StopTriggerPlan plan) throws TriggerManagementException {
    TriggerRegistrationService.getInstance().inactivate(plan);
    return true;
  }

  private void operateMerge(MergePlan plan) throws StorageEngineException {
    if (plan.getOperatorType() == OperatorType.FULL_MERGE) {
      StorageEngine.getInstance().mergeAll(true);
    } else {
      StorageEngine.getInstance().mergeAll(false);
    }
  }

  private void operateClearCache() {
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
  }

  private void operateCreateSnapshot() {
    IoTDB.metaManager.createMTreeSnapshot();
  }

  private void operateKillQuery(KillQueryPlan killQueryPlan) throws QueryIdNotExsitException {
    QueryTimeManager queryTimeManager = QueryTimeManager.getInstance();
    long killQueryId = killQueryPlan.getQueryId();
    if (killQueryId != -1) {
      if (queryTimeManager.getQueryInfoMap().get(killQueryId) != null) {
        queryTimeManager.killQuery(killQueryId);
      } else {
        throw new QueryIdNotExsitException(
            String.format(
                "Query Id %d is not exist, please check it.", killQueryPlan.getQueryId()));
      }
    } else {
      // if queryId is not specified, kill all running queries
      if (!queryTimeManager.getQueryInfoMap().isEmpty()) {
        synchronized (queryTimeManager.getQueryInfoMap()) {
          List<Long> queryIdList = new ArrayList<>(queryTimeManager.getQueryInfoMap().keySet());
          for (Long queryId : queryIdList) {
            queryTimeManager.killQuery(queryId);
          }
        }
      }
    }
  }

  /** when tracing off need Close the stream */
  private void operateTracing(TracingPlan plan) {
    IoTDBDescriptor.getInstance().getConfig().setEnablePerformanceTracing(plan.isTracingOn());
    if (!plan.isTracingOn()) {
      TracingManager.getInstance().close();
    } else {
      if (!TracingManager.getInstance().getWriterStatus()) {
        TracingManager.getInstance().openTracingWriteStream();
      }
    }
  }

  private void operateFlush(FlushPlan plan) throws StorageGroupNotSetException {
    if (plan.getPaths().isEmpty()) {
      StorageEngine.getInstance().syncCloseAllProcessor();
    } else {
      flushSpecifiedStorageGroups(plan);
    }

    if (!plan.getPaths().isEmpty()) {
      List<PartialPath> noExistSg = checkStorageGroupExist(plan.getPaths());
      if (!noExistSg.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        noExistSg.forEach(storageGroup -> sb.append(storageGroup.getFullPath()).append(","));
        throw new StorageGroupNotSetException(sb.subSequence(0, sb.length() - 1).toString(), true);
      }
    }
  }

  public static void flushSpecifiedStorageGroups(FlushPlan plan)
      throws StorageGroupNotSetException {
    Map<PartialPath, List<Pair<Long, Boolean>>> storageGroupMap =
        plan.getStorageGroupPartitionIds();
    for (Entry<PartialPath, List<Pair<Long, Boolean>>> entry : storageGroupMap.entrySet()) {
      PartialPath storageGroupName = entry.getKey();
      // normal flush
      if (entry.getValue() == null) {
        if (plan.isSeq() == null) {
          StorageEngine.getInstance()
              .closeStorageGroupProcessor(storageGroupName, true, plan.isSync());
          StorageEngine.getInstance()
              .closeStorageGroupProcessor(storageGroupName, false, plan.isSync());
        } else {
          StorageEngine.getInstance()
              .closeStorageGroupProcessor(storageGroupName, plan.isSeq(), plan.isSync());
        }
      }
      // partition specified flush, for snapshot flush plan
      else {
        List<Pair<Long, Boolean>> partitionIdSequencePairs = entry.getValue();
        for (Pair<Long, Boolean> pair : partitionIdSequencePairs) {
          StorageEngine.getInstance()
              .closeStorageGroupProcessor(storageGroupName, pair.left, pair.right, true);
        }
      }
    }
  }

  protected QueryDataSet processDataQuery(QueryPlan queryPlan, QueryContext context)
      throws StorageEngineException, QueryFilterOptimizationException, QueryProcessException,
          IOException, InterruptedException {
    QueryDataSet queryDataSet;
    if (queryPlan instanceof AlignByDevicePlan) {
      queryDataSet = getAlignByDeviceDataSet((AlignByDevicePlan) queryPlan, context, queryRouter);
    } else {
      if (queryPlan.getPaths() == null || queryPlan.getPaths().isEmpty()) {
        // no time series are selected, return EmptyDataSet
        return new EmptyDataSet();
      } else if (queryPlan instanceof UDTFPlan) {
        UDTFPlan udtfPlan = (UDTFPlan) queryPlan;
        queryDataSet = queryRouter.udtfQuery(udtfPlan, context);
      } else if (queryPlan instanceof GroupByTimeFillPlan) {
        GroupByTimeFillPlan groupByFillPlan = (GroupByTimeFillPlan) queryPlan;
        queryDataSet = queryRouter.groupByFill(groupByFillPlan, context);
      } else if (queryPlan instanceof GroupByTimePlan) {
        GroupByTimePlan groupByTimePlan = (GroupByTimePlan) queryPlan;
        queryDataSet = queryRouter.groupBy(groupByTimePlan, context);
      } else if (queryPlan instanceof QueryIndexPlan) {
        throw new QueryProcessException("Query index hasn't been supported yet");
      } else if (queryPlan instanceof AggregationPlan) {
        AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
        queryDataSet = queryRouter.aggregate(aggregationPlan, context);
      } else if (queryPlan instanceof FillQueryPlan) {
        FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
        queryDataSet = queryRouter.fill(fillQueryPlan, context);
      } else if (queryPlan instanceof LastQueryPlan) {
        queryDataSet = queryRouter.lastQuery((LastQueryPlan) queryPlan, context);
      } else {
        queryDataSet = queryRouter.rawDataQuery((RawDataQueryPlan) queryPlan, context);
      }
    }
    queryDataSet.setRowLimit(queryPlan.getRowLimit());
    queryDataSet.setRowOffset(queryPlan.getRowOffset());
    return queryDataSet;
  }

  protected AlignByDeviceDataSet getAlignByDeviceDataSet(
      AlignByDevicePlan plan, QueryContext context, IQueryRouter router) {
    return new AlignByDeviceDataSet(plan, context, router);
  }

  protected QueryDataSet processShowQuery(ShowPlan showPlan, QueryContext context)
      throws QueryProcessException, MetadataException {
    switch (showPlan.getShowContentType()) {
      case TTL:
        return processShowTTLQuery((ShowTTLPlan) showPlan);
      case FLUSH_TASK_INFO:
        return processShowFlushTaskInfo();
      case VERSION:
        return processShowVersion();
      case TIMESERIES:
        return processShowTimeseries((ShowTimeSeriesPlan) showPlan, context);
      case STORAGE_GROUP:
        return processShowStorageGroup((ShowStorageGroupPlan) showPlan);
      case DEVICES:
        return processShowDevices((ShowDevicesPlan) showPlan);
      case CHILD_PATH:
        return processShowChildPaths((ShowChildPathsPlan) showPlan);
      case CHILD_NODE:
        return processShowChildNodes((ShowChildNodesPlan) showPlan);
      case COUNT_TIMESERIES:
        return processCountTimeSeries((CountPlan) showPlan);
      case COUNT_NODE_TIMESERIES:
        return processCountNodeTimeSeries((CountPlan) showPlan);
      case COUNT_DEVICES:
        return processCountDevices((CountPlan) showPlan);
      case COUNT_STORAGE_GROUP:
        return processCountStorageGroup((CountPlan) showPlan);
      case COUNT_NODES:
        return processCountNodes((CountPlan) showPlan);
      case MERGE_STATUS:
        return processShowMergeStatus();
      case QUERY_PROCESSLIST:
        return processShowQueryProcesslist();
      case FUNCTIONS:
        return processShowFunctions((ShowFunctionsPlan) showPlan);
      case TRIGGERS:
        return processShowTriggers();
      default:
        throw new QueryProcessException(String.format("Unrecognized show plan %s", showPlan));
    }
  }

  private QueryDataSet processCountNodes(CountPlan countPlan) throws MetadataException {
    int num = getNodesNumInGivenLevel(countPlan.getPath(), countPlan.getLevel());
    return createSingleDataSet(COLUMN_COUNT, TSDataType.INT32, num);
  }

  private QueryDataSet processCountNodeTimeSeries(CountPlan countPlan) throws MetadataException {
    // get the nodes that need to group by first
    List<PartialPath> nodes = getNodesList(countPlan.getPath(), countPlan.getLevel());
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_COLUMN, false), new PartialPath(COLUMN_COUNT, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.INT32));
    for (PartialPath columnPath : nodes) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(columnPath.getFullPath()));
      Field field1 = new Field(TSDataType.INT32);
      // get the count of every group
      field1.setIntV(getPathsNum(columnPath));
      record.addField(field);
      record.addField(field1);
      listDataSet.putRecord(record);
    }
    return listDataSet;
  }

  private QueryDataSet processCountDevices(CountPlan countPlan) throws MetadataException {
    int num = getDevicesNum(countPlan.getPath());
    return createSingleDataSet(COLUMN_DEVICES, TSDataType.INT32, num);
  }

  private QueryDataSet processCountStorageGroup(CountPlan countPlan) throws MetadataException {
    int num = getStorageGroupNum(countPlan.getPath());
    return createSingleDataSet(COLUMN_STORAGE_GROUP, TSDataType.INT32, num);
  }

  private QueryDataSet createSingleDataSet(String columnName, TSDataType columnType, Object val) {
    SingleDataSet singleDataSet =
        new SingleDataSet(
            Collections.singletonList(new PartialPath(columnName, false)),
            Collections.singletonList(columnType));
    Field field = new Field(columnType);
    switch (columnType) {
      case TEXT:
        field.setBinaryV(((Binary) val));
        break;
      case FLOAT:
        field.setFloatV(((float) val));
        break;
      case INT32:
        field.setIntV(((int) val));
        break;
      case INT64:
        field.setLongV(((long) val));
        break;
      case DOUBLE:
        field.setDoubleV(((double) val));
        break;
      case BOOLEAN:
        field.setBoolV(((boolean) val));
        break;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type" + columnType);
    }
    RowRecord record = new RowRecord(0);
    record.addField(field);
    singleDataSet.setRecord(record);
    return singleDataSet;
  }

  private int getDevicesNum(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getDevicesNum(path);
  }

  private int getStorageGroupNum(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getStorageGroupNum(path);
  }

  protected int getPathsNum(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesCount(path);
  }

  protected int getNodesNumInGivenLevel(PartialPath path, int level) throws MetadataException {
    return IoTDB.metaManager.getNodesCountInGivenLevel(path, level);
  }

  protected List<PartialPath> getPathsName(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesPath(path);
  }

  protected List<PartialPath> getNodesList(PartialPath schemaPattern, int level)
      throws MetadataException {
    return IoTDB.metaManager.getNodesList(schemaPattern, level);
  }

  private QueryDataSet processCountTimeSeries(CountPlan countPlan) throws MetadataException {
    int num = getPathsNum(countPlan.getPath());
    return createSingleDataSet(COLUMN_COUNT, TSDataType.INT32, num);
  }

  private QueryDataSet processShowDevices(ShowDevicesPlan showDevicesPlan)
      throws MetadataException {
    return new ShowDevicesDataSet(showDevicesPlan);
  }

  protected Set<PartialPath> getDevices(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getDevices(path);
  }

  private QueryDataSet processShowChildPaths(ShowChildPathsPlan showChildPathsPlan)
      throws MetadataException {
    Set<String> childPathsList = getPathNextChildren(showChildPathsPlan.getPath());
    ListDataSet listDataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_CHILD_PATHS, false)),
            Collections.singletonList(TSDataType.TEXT));
    for (String s : childPathsList) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(s));
      record.addField(field);
      listDataSet.putRecord(record);
    }
    return listDataSet;
  }

  protected Set<String> getPathNextChildren(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getChildNodePathInNextLevel(path);
  }

  private QueryDataSet processShowChildNodes(ShowChildNodesPlan showChildNodesPlan)
      throws MetadataException {
    // getNodeNextChildren
    Set<String> childNodesList = getNodeNextChildren(showChildNodesPlan.getPath());
    ListDataSet listDataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_CHILD_NODES, false)),
            Collections.singletonList(TSDataType.TEXT));
    for (String s : childNodesList) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(s));
      record.addField(field);
      listDataSet.putRecord(record);
    }
    return listDataSet;
  }

  protected Set<String> getNodeNextChildren(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getChildNodeInNextLevel(path);
  }

  protected List<PartialPath> getStorageGroupNames(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getStorageGroupPaths(path);
  }

  private QueryDataSet processShowStorageGroup(ShowStorageGroupPlan showStorageGroupPlan)
      throws MetadataException {
    ListDataSet listDataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_STORAGE_GROUP, false)),
            Collections.singletonList(TSDataType.TEXT));
    List<PartialPath> storageGroupList = getStorageGroupNames(showStorageGroupPlan.getPath());
    addToDataSet(storageGroupList, listDataSet);
    return listDataSet;
  }

  private void addToDataSet(Collection<PartialPath> paths, ListDataSet dataSet) {
    for (PartialPath s : paths) {
      RowRecord record = new RowRecord(0);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(s.getFullPath()));
      record.addField(field);
      dataSet.putRecord(record);
    }
  }

  private QueryDataSet processShowTimeseries(
      ShowTimeSeriesPlan showTimeSeriesPlan, QueryContext context) throws MetadataException {
    return new ShowTimeseriesDataSet(showTimeSeriesPlan, context);
  }

  protected List<StorageGroupMNode> getAllStorageGroupNodes() {
    return IoTDB.metaManager.getAllStorageGroupNodes();
  }

  private QueryDataSet processShowTTLQuery(ShowTTLPlan showTTLPlan) {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_STORAGE_GROUP, false), new PartialPath(COLUMN_TTL, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.INT64));
    List<PartialPath> selectedSgs = showTTLPlan.getStorageGroups();

    List<StorageGroupMNode> storageGroups = getAllStorageGroupNodes();
    int timestamp = 0;
    for (StorageGroupMNode mNode : storageGroups) {
      PartialPath sgName = mNode.getPartialPath();
      if (!selectedSgs.isEmpty() && !selectedSgs.contains(sgName)) {
        continue;
      }
      RowRecord rowRecord = new RowRecord(timestamp++);
      Field sg = new Field(TSDataType.TEXT);
      Field ttl;
      sg.setBinaryV(new Binary(sgName.getFullPath()));
      if (mNode.getDataTTL() != Long.MAX_VALUE) {
        ttl = new Field(TSDataType.INT64);
        ttl.setLongV(mNode.getDataTTL());
      } else {
        ttl = null;
      }
      rowRecord.addField(sg);
      rowRecord.addField(ttl);
      listDataSet.putRecord(rowRecord);
    }

    return listDataSet;
  }

  private QueryDataSet processShowVersion() {
    SingleDataSet singleDataSet =
        new SingleDataSet(
            Collections.singletonList(new PartialPath(IoTDBConstant.COLUMN_VERSION, false)),
            Collections.singletonList(TSDataType.TEXT));
    Field field = new Field(TSDataType.TEXT);
    field.setBinaryV(new Binary(IoTDBConstant.VERSION));
    RowRecord rowRecord = new RowRecord(0);
    rowRecord.addField(field);
    singleDataSet.setRecord(rowRecord);
    return singleDataSet;
  }

  private QueryDataSet processShowFlushTaskInfo() {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_ITEM, false), new PartialPath(COLUMN_VALUE, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));

    int timestamp = 0;
    addRowRecordForShowQuery(
        listDataSet,
        timestamp++,
        "total number of flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getTotalTasks()));
    addRowRecordForShowQuery(
        listDataSet,
        timestamp++,
        "number of working flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getWorkingTasksNumber()));
    addRowRecordForShowQuery(
        listDataSet,
        timestamp,
        "number of waiting flush tasks",
        Integer.toString(FlushTaskPoolManager.getInstance().getWaitingTasksNumber()));
    return listDataSet;
  }

  private QueryDataSet processShowFunctions(ShowFunctionsPlan showPlan)
      throws QueryProcessException {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_FUNCTION_NAME, false),
                new PartialPath(COLUMN_FUNCTION_TYPE, false),
                new PartialPath(COLUMN_FUNCTION_CLASS, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT));

    appendUDFs(listDataSet, showPlan);
    appendNativeFunctions(listDataSet, showPlan);

    listDataSet.sort(
        (r1, r2) ->
            String.CASE_INSENSITIVE_ORDER.compare(
                r1.getFields().get(0).getStringValue(), r2.getFields().get(0).getStringValue()));
    return listDataSet;
  }

  @SuppressWarnings("squid:S3776")
  private void appendUDFs(ListDataSet listDataSet, ShowFunctionsPlan showPlan)
      throws QueryProcessException {
    for (UDFRegistrationInformation info :
        UDFRegistrationService.getInstance().getRegistrationInformation()) {
      if (showPlan.showTemporary() && !info.isTemporary()) {
        continue;
      }

      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(info.getFunctionName()), TSDataType.TEXT);
      String functionType = "";
      try {
        if (info.isBuiltin()) {
          if (info.isUDTF()) {
            functionType = FUNCTION_TYPE_BUILTIN_UDTF;
          } else if (info.isUDAF()) {
            functionType = FUNCTION_TYPE_BUILTIN_UDAF;
          }
        } else {
          if (info.isUDTF()) {
            functionType = FUNCTION_TYPE_EXTERNAL_UDTF;
          } else if (info.isUDAF()) {
            functionType = FUNCTION_TYPE_EXTERNAL_UDAF;
          }
        }
      } catch (InstantiationException
          | InvocationTargetException
          | NoSuchMethodException
          | IllegalAccessException e) {
        throw new QueryProcessException(e.toString());
      }
      rowRecord.addField(Binary.valueOf(functionType), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(info.getClassName()), TSDataType.TEXT);
      listDataSet.putRecord(rowRecord);
    }
  }

  private void appendNativeFunctions(ListDataSet listDataSet, ShowFunctionsPlan showPlan) {
    if (showPlan.showTemporary()) {
      return;
    }

    final Binary functionType = Binary.valueOf(FUNCTION_TYPE_NATIVE);
    final Binary className = Binary.valueOf("");
    for (String functionName : SQLConstant.getNativeFunctionNames()) {
      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(functionName.toUpperCase()), TSDataType.TEXT);
      rowRecord.addField(functionType, TSDataType.TEXT);
      rowRecord.addField(className, TSDataType.TEXT);
      listDataSet.putRecord(rowRecord);
    }
  }

  private QueryDataSet processShowTriggers() {
    return TriggerRegistrationService.getInstance().show();
  }

  private void addRowRecordForShowQuery(
      ListDataSet listDataSet, int timestamp, String item, String value) {
    RowRecord rowRecord = new RowRecord(timestamp);
    Field itemField = new Field(TSDataType.TEXT);
    itemField.setBinaryV(new Binary(item));
    Field valueField = new Field(TSDataType.TEXT);
    valueField.setBinaryV(new Binary(value));
    rowRecord.addField(itemField);
    rowRecord.addField(valueField);
    listDataSet.putRecord(rowRecord);
  }

  @Override
  public void delete(DeletePlan deletePlan) throws QueryProcessException {
    for (PartialPath path : deletePlan.getPaths()) {
      delete(
          path,
          deletePlan.getDeleteStartTime(),
          deletePlan.getDeleteEndTime(),
          deletePlan.getIndex());
    }
  }

  private void operateLoadFiles(OperateFilePlan plan) throws QueryProcessException {
    File file = plan.getFile();
    if (!file.exists()) {
      throw new QueryProcessException(
          String.format("File path %s doesn't exists.", file.getPath()));
    }
    if (file.isDirectory()) {
      recursionFileDir(file, plan);
    } else {
      loadFile(file, plan);
    }
  }

  private void recursionFileDir(File curFile, OperateFilePlan plan) throws QueryProcessException {
    File[] files = curFile.listFiles();
    for (File file : files) {
      if (file.isDirectory()) {
        recursionFileDir(file, plan);
      } else {
        loadFile(file, plan);
      }
    }
  }

  private void loadFile(File file, OperateFilePlan plan) throws QueryProcessException {
    if (!file.getName().endsWith(TSFILE_SUFFIX)) {
      return;
    }
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    try {
      // check file
      RestorableTsFileIOWriter restorableTsFileIOWriter = new RestorableTsFileIOWriter(file);
      if (restorableTsFileIOWriter.hasCrashed()) {
        restorableTsFileIOWriter.close();
        throw new QueryProcessException(
            String.format(
                "Cannot load file %s because the file has crashed.", file.getAbsolutePath()));
      }
      Map<Path, IMeasurementSchema> schemaMap = new HashMap<>();

      List<ChunkGroupMetadata> chunkGroupMetadataList = new ArrayList<>();
      try (TsFileSequenceReader reader = new TsFileSequenceReader(file, false)) {
        reader.selfCheck(schemaMap, chunkGroupMetadataList, false);
      }

      FileLoaderUtils.checkTsFileResource(tsFileResource);
      if (UpgradeUtils.isNeedUpgrade(tsFileResource)) {
        throw new QueryProcessException(
            String.format(
                "Cannot load file %s because the file's version is old which needs to be upgraded.",
                file.getAbsolutePath()));
      }

      // create schemas if they doesn't exist
      if (plan.isAutoCreateSchema()) {
        createSchemaAutomatically(chunkGroupMetadataList, schemaMap, plan.getSgLevel());
      }

      List<TsFileResource> splitResources = new ArrayList();
      if (tsFileResource.isSpanMultiTimePartitions()) {
        logger.info(
            "try to split the tsFile={} du to it spans multi partitions",
            tsFileResource.getTsFile().getPath());
        TsFileRewriteTool.rewriteTsFile(tsFileResource, splitResources);
        logger.info(
            "after split, the old tsFile was split to {} new tsFiles", splitResources.size());
      }

      if (splitResources.isEmpty()) {
        splitResources.add(tsFileResource);
      }

      for (TsFileResource resource : splitResources) {
        StorageEngine.getInstance().loadNewTsFile(resource);
      }
    } catch (Exception e) {
      throw new QueryProcessException(
          String.format("Cannot load file %s because %s", file.getAbsolutePath(), e.getMessage()));
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void createSchemaAutomatically(
      List<ChunkGroupMetadata> chunkGroupMetadataList,
      Map<Path, IMeasurementSchema> knownSchemas,
      int sgLevel)
      throws QueryProcessException, MetadataException, IOException {
    if (chunkGroupMetadataList.isEmpty()) {
      return;
    }

    Set<PartialPath> registeredSeries = new HashSet<>();
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      String device = chunkGroupMetadata.getDevice();
      MNode node =
          IoTDB.metaManager.getDeviceNodeWithAutoCreate(
                  new PartialPath(device), true, true, sgLevel)
              .left;
      for (ChunkMetadata chunkMetadata : chunkGroupMetadata.getChunkMetadataList()) {
        PartialPath series =
            new PartialPath(
                chunkGroupMetadata.getDevice()
                    + TsFileConstant.PATH_SEPARATOR
                    + chunkMetadata.getMeasurementUid());
        if (!registeredSeries.contains(series)) {
          registeredSeries.add(series);
          IMeasurementSchema schema =
              knownSchemas.get(new Path(series.getDevice(), series.getMeasurement()));
          if (schema == null) {
            throw new MetadataException(
                String.format(
                    "Can not get the schema of measurement [%s]",
                    chunkMetadata.getMeasurementUid()));
          }
          if (!node.hasChild(chunkMetadata.getMeasurementUid())) {
            IoTDB.metaManager.createTimeseries(
                series,
                schema.getType(),
                schema.getEncodingType(),
                schema.getCompressor(),
                Collections.emptyMap());
          } else if (!(node.getChild(chunkMetadata.getMeasurementUid())
              instanceof MeasurementMNode)) {
            throw new QueryProcessException(
                String.format("Current Path is not leaf node. %s", series));
          }
        }
      }
    }
  }

  private void operateRemoveFile(OperateFilePlan plan) throws QueryProcessException {
    try {
      if (!StorageEngine.getInstance().deleteTsfile(plan.getFile())) {
        throw new QueryProcessException(
            String.format("File %s doesn't exist.", plan.getFile().getName()));
      }
    } catch (StorageEngineException | IllegalPathException e) {
      throw new QueryProcessException(
          String.format("Cannot remove file because %s", e.getMessage()));
    }
  }

  private void operateMoveFile(OperateFilePlan plan) throws QueryProcessException {
    if (!plan.getTargetDir().exists() || !plan.getTargetDir().isDirectory()) {
      throw new QueryProcessException(
          String.format("Target dir %s is invalid.", plan.getTargetDir().getPath()));
    }
    try {
      if (!StorageEngine.getInstance().moveTsfile(plan.getFile(), plan.getTargetDir())) {
        throw new QueryProcessException(
            String.format("File %s doesn't exist.", plan.getFile().getName()));
      }
    } catch (StorageEngineException | IllegalPathException e) {
      throw new QueryProcessException(
          String.format(
              "Cannot move file %s to target directory %s because %s",
              plan.getFile().getPath(), plan.getTargetDir().getPath(), e.getMessage()));
    }
  }

  private void operateTTL(SetTTLPlan plan) throws QueryProcessException {
    try {
      IoTDB.metaManager.setTTL(plan.getStorageGroup(), plan.getDataTTL());
      StorageEngine.getInstance().setTTL(plan.getStorageGroup(), plan.getDataTTL());
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    } catch (IOException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  @Override
  public void update(PartialPath path, long startTime, long endTime, String value) {
    throw new UnsupportedOperationException("update is not supported now");
  }

  @Override
  public void delete(PartialPath path, long startTime, long endTime, long planIndex)
      throws QueryProcessException {
    try {
      StorageEngine.getInstance().delete(path, startTime, endTime, planIndex);
    } catch (StorageEngineException e) {
      throw new QueryProcessException(e);
    }
  }

  protected MNode getSeriesSchemas(InsertPlan insertPlan) throws MetadataException {
    try {
      return IoTDB.metaManager.getSeriesSchemasAndReadLockDevice(insertPlan);
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  private void checkFailedMeasurments(InsertPlan plan)
      throws PathNotExistException, StorageEngineException {
    // check if all path not exist exceptions
    List<String> failedPaths = plan.getFailedMeasurements();
    List<Exception> exceptions = plan.getFailedExceptions();
    boolean isPathNotExistException = true;
    for (Exception e : exceptions) {
      Throwable curException = e;
      while (curException.getCause() != null) {
        curException = curException.getCause();
      }
      if (!(curException instanceof PathNotExistException)) {
        isPathNotExistException = false;
        break;
      }
    }
    if (isPathNotExistException) {
      throw new PathNotExistException(failedPaths);
    } else {
      throw new StorageEngineException(
          INSERT_MEASUREMENTS_FAILED_MESSAGE
              + plan.getFailedMeasurements()
              + (!exceptions.isEmpty() ? (" caused by " + exceptions.get(0).getMessage()) : ""));
    }
  }

  @Override
  public void insert(InsertRowsOfOneDevicePlan insertRowsOfOneDevicePlan)
      throws QueryProcessException {
    try {
      for (InsertRowPlan plan : insertRowsOfOneDevicePlan.getRowPlans()) {
        plan.setMeasurementMNodes(new MeasurementMNode[plan.getMeasurements().length]);
        // check whether types are match
        getSeriesSchemas(plan);
        // we do not need to infer data type for insertRowsOfOneDevicePlan
      }
      // ok, we can begin to write data into the engine..
      StorageEngine.getInstance().insert(insertRowsOfOneDevicePlan);

      List<String> notExistedPaths = null;
      List<String> failedMeasurements = null;

      // If there are some exceptions, we assume they caused by the same reason.
      Exception exception = null;
      for (InsertRowPlan plan : insertRowsOfOneDevicePlan.getRowPlans()) {
        if (plan.getFailedMeasurements() != null) {
          if (notExistedPaths == null) {
            notExistedPaths = new ArrayList<>();
            failedMeasurements = new ArrayList<>();
          }
          // check if all path not exist exceptions
          List<String> failedPaths = plan.getFailedMeasurements();
          List<Exception> exceptions = plan.getFailedExceptions();
          boolean isPathNotExistException = true;
          for (Exception e : exceptions) {
            exception = e;
            Throwable curException = e;
            while (curException.getCause() != null) {
              curException = curException.getCause();
            }
            if (!(curException instanceof PathNotExistException)) {
              isPathNotExistException = false;
              break;
            }
          }
          if (isPathNotExistException) {
            notExistedPaths.addAll(failedPaths);
          } else {
            failedMeasurements.addAll(plan.getFailedMeasurements());
          }
        }
      }
      if (notExistedPaths != null && !notExistedPaths.isEmpty()) {
        throw new PathNotExistException(notExistedPaths);
      } else if (notExistedPaths != null && !failedMeasurements.isEmpty()) {
        throw new StorageEngineException(
            "failed to insert points "
                + failedMeasurements
                + (exception != null ? (" caused by " + exception.getMessage()) : ""));
      }

    } catch (StorageEngineException | MetadataException e) {
      throw new QueryProcessException(e);
    }
  }

  @Override
  public void insert(InsertRowsPlan plan) throws QueryProcessException {
    for (int i = 0; i < plan.getInsertRowPlanList().size(); i++) {
      if (plan.getResults().containsKey(i)) {
        continue;
      }
      try {
        insert(plan.getInsertRowPlanList().get(i));
      } catch (QueryProcessException e) {
        plan.getResults().put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }
    if (!plan.getResults().isEmpty()) {
      throw new BatchProcessException(plan.getFailingStatus());
    }
  }

  @Override
  public void insert(InsertRowPlan insertRowPlan) throws QueryProcessException {
    try {
      insertRowPlan.setMeasurementMNodes(
          new MeasurementMNode[insertRowPlan.getMeasurements().length]);
      // When insert data with sql statement, the data types will be null here.
      // We need to predicted the data types first
      if (insertRowPlan.getDataTypes()[0] == null) {
        for (int i = 0; i < insertRowPlan.getDataTypes().length; i++) {
          insertRowPlan.getDataTypes()[i] =
              TypeInferenceUtils.getPredictedDataType(
                  insertRowPlan.getValues()[i], insertRowPlan.isNeedInferType());
        }
      }
      // check whether types are match
      getSeriesSchemas(insertRowPlan);
      insertRowPlan.transferType();
      StorageEngine.getInstance().insert(insertRowPlan);
      if (insertRowPlan.getFailedMeasurements() != null) {
        checkFailedMeasurments(insertRowPlan);
      }
    } catch (StorageEngineException | MetadataException e) {
      if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
        StatMonitor.getInstance().updateFailedStatValue();
      }
      throw new QueryProcessException(e);
    } catch (Exception e) {
      // update failed statistics
      if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
        StatMonitor.getInstance().updateFailedStatValue();
      }
      throw e;
    }
  }

  @Override
  public void insertTablet(InsertMultiTabletPlan insertMultiTabletPlan)
      throws QueryProcessException {
    for (int i = 0; i < insertMultiTabletPlan.getInsertTabletPlanList().size(); i++) {
      if (insertMultiTabletPlan.getResults().containsKey(i)) {
        continue;
      }
      insertTablet(insertMultiTabletPlan.getInsertTabletPlanList().get(i));
    }
  }

  @Override
  public void insertTablet(InsertTabletPlan insertTabletPlan) throws QueryProcessException {
    try {
      insertTabletPlan.setMeasurementMNodes(
          new MeasurementMNode[insertTabletPlan.getMeasurements().length]);
      getSeriesSchemas(insertTabletPlan);
      StorageEngine.getInstance().insertTablet(insertTabletPlan);
      if (insertTabletPlan.getFailedMeasurements() != null) {
        checkFailedMeasurments(insertTabletPlan);
      }
    } catch (StorageEngineException | MetadataException e) {
      if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
        StatMonitor.getInstance().updateFailedStatValue();
      }
      throw new QueryProcessException(e);
    } catch (Exception e) {
      // update failed statistics
      if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
        StatMonitor.getInstance().updateFailedStatValue();
      }
      throw e;
    }
  }

  private boolean operateAuthor(AuthorPlan author) throws QueryProcessException {
    AuthorOperator.AuthorType authorType = author.getAuthorType();
    String userName = author.getUserName();
    String roleName = author.getRoleName();
    String password = author.getPassword();
    String newPassword = author.getNewPassword();
    Set<Integer> permissions = author.getPermissions();
    PartialPath nodeName = author.getNodeName();
    try {
      switch (authorType) {
        case UPDATE_USER:
          authorizer.updateUserPassword(userName, newPassword);
          break;
        case CREATE_USER:
          authorizer.createUser(userName, password);
          break;
        case CREATE_ROLE:
          authorizer.createRole(roleName);
          break;
        case DROP_USER:
          authorizer.deleteUser(userName);
          break;
        case DROP_ROLE:
          authorizer.deleteRole(roleName);
          break;
        case GRANT_ROLE:
          for (int i : permissions) {
            authorizer.grantPrivilegeToRole(roleName, nodeName.getFullPath(), i);
          }
          break;
        case GRANT_USER:
          for (int i : permissions) {
            authorizer.grantPrivilegeToUser(userName, nodeName.getFullPath(), i);
          }
          break;
        case GRANT_ROLE_TO_USER:
          authorizer.grantRoleToUser(roleName, userName);
          break;
        case REVOKE_USER:
          for (int i : permissions) {
            authorizer.revokePrivilegeFromUser(userName, nodeName.getFullPath(), i);
          }
          break;
        case REVOKE_ROLE:
          for (int i : permissions) {
            authorizer.revokePrivilegeFromRole(roleName, nodeName.getFullPath(), i);
          }
          break;
        case REVOKE_ROLE_FROM_USER:
          authorizer.revokeRoleFromUser(roleName, userName);
          break;
        default:
          throw new QueryProcessException("Unsupported operation " + authorType);
      }
    } catch (AuthException e) {
      throw new QueryProcessException(e.getMessage(), true);
    }
    return true;
  }

  private boolean operateWatermarkEmbedding(List<String> users, boolean useWatermark)
      throws QueryProcessException {
    try {
      for (String user : users) {
        authorizer.setUserUseWaterMark(user, useWatermark);
      }
    } catch (AuthException e) {
      throw new QueryProcessException(e.getMessage());
    }
    return true;
  }

  private boolean createTimeSeries(CreateTimeSeriesPlan createTimeSeriesPlan)
      throws QueryProcessException {
    try {
      IoTDB.metaManager.createTimeseries(createTimeSeriesPlan);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return true;
  }

  private boolean createAlignedTimeSeries(CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan)
      throws QueryProcessException {
    try {
      IoTDB.metaManager.createAlignedTimeSeries(createAlignedTimeSeriesPlan);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return true;
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  private boolean createMultiTimeSeries(CreateMultiTimeSeriesPlan multiPlan)
      throws BatchProcessException {
    int dataTypeIdx = 0;
    for (int i = 0; i < multiPlan.getPaths().size(); i++) {
      if (multiPlan.getResults().containsKey(i)) {
        continue;
      }
      PartialPath path = multiPlan.getPaths().get(i);
      String measurement = path.getMeasurement();
      if (measurement.contains("(") && measurement.contains(",")) {
        PartialPath devicePath = path.getDevicePath();
        List<String> measurements = MetaUtils.getMeasurementsInPartialPath(path);
        List<TSDataType> dataTypes = new ArrayList<>();
        List<TSEncoding> encodings = new ArrayList<>();
        for (int j = 0; j < measurements.size(); j++) {
          dataTypes.add(multiPlan.getDataTypes().get(dataTypeIdx));
          encodings.add(multiPlan.getEncodings().get(dataTypeIdx));
          dataTypeIdx++;
        }
        CreateAlignedTimeSeriesPlan plan =
            new CreateAlignedTimeSeriesPlan(
                devicePath,
                measurements,
                dataTypes,
                encodings,
                multiPlan.getCompressors().get(i),
                null);
        try {
          createAlignedTimeSeries(plan);
        } catch (QueryProcessException e) {
          multiPlan.getResults().put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        }
      } else {
        CreateTimeSeriesPlan plan =
            new CreateTimeSeriesPlan(
                multiPlan.getPaths().get(i),
                multiPlan.getDataTypes().get(i),
                multiPlan.getEncodings().get(i),
                multiPlan.getCompressors().get(i),
                multiPlan.getProps() == null ? null : multiPlan.getProps().get(i),
                multiPlan.getTags() == null ? null : multiPlan.getTags().get(i),
                multiPlan.getAttributes() == null ? null : multiPlan.getAttributes().get(i),
                multiPlan.getAlias() == null ? null : multiPlan.getAlias().get(i));
        dataTypeIdx++;
        try {
          createTimeSeries(plan);
        } catch (QueryProcessException e) {
          multiPlan.getResults().put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
        }
      }
    }
    if (!multiPlan.getResults().isEmpty()) {
      throw new BatchProcessException(multiPlan.getFailingStatus());
    }
    return true;
  }

  protected boolean deleteTimeSeries(DeleteTimeSeriesPlan deleteTimeSeriesPlan)
      throws QueryProcessException {
    List<PartialPath> deletePathList = deleteTimeSeriesPlan.getPaths();
    for (int i = 0; i < deletePathList.size(); i++) {
      PartialPath path = deletePathList.get(i);
      try {
        StorageEngine.getInstance().deleteTimeseries(path, deleteTimeSeriesPlan.getIndex());
        String failed = IoTDB.metaManager.deleteTimeseries(path);
        if (failed != null) {
          deleteTimeSeriesPlan
              .getResults()
              .put(i, RpcUtils.getStatus(TSStatusCode.NODE_DELETE_FAILED_ERROR, failed));
        }
      } catch (StorageEngineException | MetadataException e) {
        deleteTimeSeriesPlan
            .getResults()
            .put(i, RpcUtils.getStatus(e.getErrorCode(), e.getMessage()));
      }
    }
    if (!deleteTimeSeriesPlan.getResults().isEmpty()) {
      throw new BatchProcessException(deleteTimeSeriesPlan.getFailingStatus());
    }
    return true;
  }

  private boolean alterTimeSeries(AlterTimeSeriesPlan alterTimeSeriesPlan)
      throws QueryProcessException {
    PartialPath path = alterTimeSeriesPlan.getPath();
    Map<String, String> alterMap = alterTimeSeriesPlan.getAlterMap();
    try {
      switch (alterTimeSeriesPlan.getAlterType()) {
        case RENAME:
          String beforeName = alterMap.keySet().iterator().next();
          String currentName = alterMap.get(beforeName);
          IoTDB.metaManager.renameTagOrAttributeKey(beforeName, currentName, path);
          break;
        case SET:
          IoTDB.metaManager.setTagsOrAttributesValue(alterMap, path);
          break;
        case DROP:
          IoTDB.metaManager.dropTagsOrAttributes(alterMap.keySet(), path);
          break;
        case ADD_TAGS:
          IoTDB.metaManager.addTags(alterMap, path);
          break;
        case ADD_ATTRIBUTES:
          IoTDB.metaManager.addAttributes(alterMap, path);
          break;
        case UPSERT:
          IoTDB.metaManager.upsertTagsAndAttributes(
              alterTimeSeriesPlan.getAlias(),
              alterTimeSeriesPlan.getTagsMap(),
              alterTimeSeriesPlan.getAttributesMap(),
              path);
          break;
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    } catch (IOException e) {
      throw new QueryProcessException(
          String.format(
              "Something went wrong while read/write the [%s]'s tag/attribute info.",
              path.getFullPath()));
    }
    return true;
  }

  public boolean setStorageGroup(SetStorageGroupPlan setStorageGroupPlan)
      throws QueryProcessException {
    PartialPath path = setStorageGroupPlan.getPath();
    try {
      IoTDB.metaManager.setStorageGroup(path);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return true;
  }

  protected boolean deleteStorageGroups(DeleteStorageGroupPlan deleteStorageGroupPlan)
      throws QueryProcessException {
    List<PartialPath> deletePathList = new ArrayList<>();
    try {
      for (PartialPath storageGroupPath : deleteStorageGroupPlan.getPaths()) {
        List<PartialPath> allRelatedStorageGroupPath =
            IoTDB.metaManager.getStorageGroupPaths(storageGroupPath);
        if (allRelatedStorageGroupPath.isEmpty()) {
          throw new PathNotExistException(storageGroupPath.getFullPath(), true);
        }
        for (PartialPath path : allRelatedStorageGroupPath) {
          StorageEngine.getInstance().deleteStorageGroup(path);
          deletePathList.add(path);
        }
      }
      IoTDB.metaManager.deleteStorageGroups(deletePathList);
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    return true;
  }

  protected QueryDataSet processAuthorQuery(AuthorPlan plan) throws QueryProcessException {
    AuthorType authorType = plan.getAuthorType();
    String userName = plan.getUserName();
    String roleName = plan.getRoleName();
    PartialPath path = plan.getNodeName();

    ListDataSet dataSet;

    try {
      switch (authorType) {
        case LIST_ROLE:
          dataSet = executeListRole(plan);
          break;
        case LIST_USER:
          dataSet = executeListUser(plan);
          break;
        case LIST_ROLE_USERS:
          dataSet = executeListRoleUsers(roleName);
          break;
        case LIST_USER_ROLES:
          dataSet = executeListUserRoles(userName);
          break;
        case LIST_ROLE_PRIVILEGE:
          dataSet = executeListRolePrivileges(roleName, path);
          break;
        case LIST_USER_PRIVILEGE:
          dataSet = executeListUserPrivileges(userName, path);
          break;
        default:
          throw new QueryProcessException("Unsupported operation " + authorType);
      }
    } catch (AuthException e) {
      throw new QueryProcessException(e.getMessage());
    }
    return dataSet;
  }

  private ListDataSet executeListRole(AuthorPlan plan) throws AuthException {
    ListDataSet dataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_ROLE, false)),
            Collections.singletonList(TSDataType.TEXT));

    // check if current user is granted list_role privilege
    boolean hasListRolePrivilege =
        AuthorityChecker.check(
            plan.getLoginUserName(),
            Collections.emptyList(),
            plan.getOperatorType(),
            plan.getLoginUserName());
    if (!hasListRolePrivilege) {
      return dataSet;
    }

    List<String> roleList = authorizer.listAllRoles();
    addToDataSet(roleList, dataSet);
    return dataSet;
  }

  private void addToDataSet(List<String> strResults, ListDataSet dataSet) {
    int index = 0;
    for (String role : strResults) {
      RowRecord record = new RowRecord(index++);
      Field field = new Field(TSDataType.TEXT);
      field.setBinaryV(new Binary(role));
      record.addField(field);
      dataSet.putRecord(record);
    }
  }

  private ListDataSet executeListUser(AuthorPlan plan) throws AuthException {
    ListDataSet dataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_USER, false)),
            Collections.singletonList(TSDataType.TEXT));

    // check if current user is granted list_user privilege
    boolean hasListUserPrivilege =
        AuthorityChecker.check(
            plan.getLoginUserName(),
            Collections.singletonList((plan.getNodeName())),
            plan.getOperatorType(),
            plan.getLoginUserName());
    if (!hasListUserPrivilege) {
      return dataSet;
    }

    List<String> userList = authorizer.listAllUsers();
    addToDataSet(userList, dataSet);
    return dataSet;
  }

  private ListDataSet executeListRoleUsers(String roleName) throws AuthException {
    Role role = authorizer.getRole(roleName);
    if (role == null) {
      throw new AuthException("No such role : " + roleName);
    }
    ListDataSet dataSet =
        new ListDataSet(
            Collections.singletonList(new PartialPath(COLUMN_USER, false)),
            Collections.singletonList(TSDataType.TEXT));
    List<String> userList = authorizer.listAllUsers();
    int index = 0;
    for (String userN : userList) {
      User userObj = authorizer.getUser(userN);
      if (userObj != null && userObj.hasRole(roleName)) {
        RowRecord record = new RowRecord(index++);
        Field field = new Field(TSDataType.TEXT);
        field.setBinaryV(new Binary(userN));
        record.addField(field);
        dataSet.putRecord(record);
      }
    }
    return dataSet;
  }

  private ListDataSet executeListUserRoles(String userName) throws AuthException {
    User user = authorizer.getUser(userName);
    if (user != null) {
      ListDataSet dataSet =
          new ListDataSet(
              Collections.singletonList(new PartialPath(COLUMN_ROLE, false)),
              Collections.singletonList(TSDataType.TEXT));
      int index = 0;
      for (String roleN : user.getRoleList()) {
        RowRecord record = new RowRecord(index++);
        Field field = new Field(TSDataType.TEXT);
        field.setBinaryV(new Binary(roleN));
        record.addField(field);
        dataSet.putRecord(record);
      }
      return dataSet;
    } else {
      throw new AuthException("No such user : " + userName);
    }
  }

  private ListDataSet executeListRolePrivileges(String roleName, PartialPath path)
      throws AuthException {
    Role role = authorizer.getRole(roleName);
    if (role != null) {
      List<PartialPath> headerList = new ArrayList<>();
      List<TSDataType> typeList = new ArrayList<>();
      headerList.add(new PartialPath(COLUMN_PRIVILEGE, false));
      typeList.add(TSDataType.TEXT);
      ListDataSet dataSet = new ListDataSet(headerList, typeList);
      int index = 0;
      for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
        if (path == null || AuthUtils.pathBelongsTo(path.getFullPath(), pathPrivilege.getPath())) {
          RowRecord record = new RowRecord(index++);
          Field field = new Field(TSDataType.TEXT);
          field.setBinaryV(new Binary(pathPrivilege.toString()));
          record.addField(field);
          dataSet.putRecord(record);
        }
      }
      return dataSet;
    } else {
      throw new AuthException("No such role : " + roleName);
    }
  }

  private ListDataSet executeListUserPrivileges(String userName, PartialPath path)
      throws AuthException {
    User user = authorizer.getUser(userName);
    if (user == null) {
      throw new AuthException("No such user : " + userName);
    }
    List<PartialPath> headerList = new ArrayList<>();
    List<TSDataType> typeList = new ArrayList<>();
    headerList.add(new PartialPath(COLUMN_ROLE, false));
    headerList.add(new PartialPath(COLUMN_PRIVILEGE, false));
    typeList.add(TSDataType.TEXT);
    typeList.add(TSDataType.TEXT);
    ListDataSet dataSet = new ListDataSet(headerList, typeList);
    int index = 0;
    for (PathPrivilege pathPrivilege : user.getPrivilegeList()) {
      if (path == null || AuthUtils.pathBelongsTo(path.getFullPath(), pathPrivilege.getPath())) {
        RowRecord record = new RowRecord(index++);
        Field roleF = new Field(TSDataType.TEXT);
        roleF.setBinaryV(new Binary(""));
        record.addField(roleF);
        Field privilegeF = new Field(TSDataType.TEXT);
        privilegeF.setBinaryV(new Binary(pathPrivilege.toString()));
        record.addField(privilegeF);
        dataSet.putRecord(record);
      }
    }
    for (String roleN : user.getRoleList()) {
      Role role = authorizer.getRole(roleN);
      if (role == null) {
        continue;
      }
      for (PathPrivilege pathPrivilege : role.getPrivilegeList()) {
        if (path == null || AuthUtils.pathBelongsTo(path.getFullPath(), pathPrivilege.getPath())) {
          RowRecord record = new RowRecord(index++);
          Field roleF = new Field(TSDataType.TEXT);
          roleF.setBinaryV(new Binary(roleN));
          record.addField(roleF);
          Field privilegeF = new Field(TSDataType.TEXT);
          privilegeF.setBinaryV(new Binary(pathPrivilege.toString()));
          record.addField(privilegeF);
          dataSet.putRecord(record);
        }
      }
    }
    return dataSet;
  }

  protected String deleteTimeSeries(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.deleteTimeseries(path);
  }

  @SuppressWarnings("unused") // for the distributed version
  protected void loadConfiguration(LoadConfigurationPlan plan) throws QueryProcessException {
    IoTDBDescriptor.getInstance().loadHotModifiedProps();
  }

  private QueryDataSet processShowMergeStatus() {
    List<PartialPath> headerList = new ArrayList<>();
    List<TSDataType> typeList = new ArrayList<>();
    headerList.add(new PartialPath(COLUMN_STORAGE_GROUP, false));
    headerList.add(new PartialPath(COLUMN_TASK_NAME, false));
    headerList.add(new PartialPath(COLUMN_CREATED_TIME, false));
    headerList.add(new PartialPath(COLUMN_PROGRESS, false));
    headerList.add(new PartialPath(COLUMN_CANCELLED, false));
    headerList.add(new PartialPath(COLUMN_DONE, false));

    typeList.add(TSDataType.TEXT);
    typeList.add(TSDataType.TEXT);
    typeList.add(TSDataType.TEXT);
    typeList.add(TSDataType.TEXT);
    typeList.add(TSDataType.BOOLEAN);
    typeList.add(TSDataType.BOOLEAN);
    ListDataSet dataSet = new ListDataSet(headerList, typeList);
    Map<String, List<TaskStatus>>[] taskStatus = MergeManager.getINSTANCE().collectTaskStatus();
    for (Map<String, List<TaskStatus>> statusMap : taskStatus) {
      for (Entry<String, List<TaskStatus>> stringListEntry : statusMap.entrySet()) {
        for (TaskStatus status : stringListEntry.getValue()) {
          dataSet.putRecord(toRowRecord(status, stringListEntry.getKey()));
        }
      }
    }
    return dataSet;
  }

  public RowRecord toRowRecord(TaskStatus status, String storageGroup) {
    RowRecord record = new RowRecord(0);
    record.addField(new Binary(storageGroup), TSDataType.TEXT);
    record.addField(new Binary(status.getTaskName()), TSDataType.TEXT);
    record.addField(new Binary(status.getCreatedTime()), TSDataType.TEXT);
    record.addField(new Binary(status.getProgress()), TSDataType.TEXT);
    record.addField(status.isCancelled(), TSDataType.BOOLEAN);
    record.addField(status.isDone(), TSDataType.BOOLEAN);
    return record;
  }

  private QueryDataSet processShowQueryProcesslist() {
    ListDataSet listDataSet =
        new ListDataSet(
            Arrays.asList(new PartialPath(QUERY_ID, false), new PartialPath(STATEMENT, false)),
            Arrays.asList(TSDataType.INT64, TSDataType.TEXT));
    QueryTimeManager queryTimeManager = QueryTimeManager.getInstance();
    for (Entry<Long, QueryInfo> queryInfo : queryTimeManager.getQueryInfoMap().entrySet()) {
      RowRecord record = new RowRecord(queryInfo.getValue().getStartTime());
      record.addField(queryInfo.getKey(), TSDataType.INT64);
      record.addField(new Binary(queryInfo.getValue().getStatement()), TSDataType.TEXT);
      listDataSet.putRecord(record);
    }
    return listDataSet;
  }

  /**
   * @param storageGroups the storage groups to check
   * @return List of PartialPath the storage groups that not exist
   */
  List<PartialPath> checkStorageGroupExist(List<PartialPath> storageGroups) {
    List<PartialPath> noExistSg = new ArrayList<>();
    if (storageGroups == null) {
      return noExistSg;
    }
    for (PartialPath storageGroup : storageGroups) {
      if (!IoTDB.metaManager.isStorageGroup(storageGroup)) {
        noExistSg.add(storageGroup);
      }
    }
    return noExistSg;
  }
}
