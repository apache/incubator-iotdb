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
package org.apache.iotdb.db.service;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.cost.statistic.Operation;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.QueryInBatchStatementException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metrics.server.SqlArgument;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertMultiTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.UDFPlan;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateMultiTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowQueryProcesslistPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.control.TracingManager;
import org.apache.iotdb.db.query.dataset.AlignByDeviceDataSet;
import org.apache.iotdb.db.query.dataset.DirectAlignByTimeDataSet;
import org.apache.iotdb.db.query.dataset.DirectNonAlignDataSet;
import org.apache.iotdb.db.query.dataset.UDTFDataSet;
import org.apache.iotdb.db.tools.watermark.GroupedLSBWatermarkEncoder;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCancelOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSQueryNonAlignDataSet;
import org.apache.iotdb.service.rpc.thrift.TSRawDataQueryReq;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.thrift.TException;
import org.apache.thrift.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** Thrift RPC implementation at server side. */
public class TSServiceImpl implements TSIService.Iface, ServerContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSServiceImpl.class);
  private static final Logger SLOW_SQL_LOGGER = LoggerFactory.getLogger("SLOW_SQL");
  private static final Logger QUERY_FREQUENCY_LOGGER = LoggerFactory.getLogger("QUERY_FREQUENCY");
  private static final Logger DETAILED_FAILURE_QUERY_TRACE_LOGGER =
      LoggerFactory.getLogger("DETAILED_FAILURE_QUERY_TRACE");
  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.AUDIT_LOGGER_NAME);

  private static final String INFO_NOT_LOGIN = "{}: Not login. ";
  private static final String INFO_PARSING_SQL_ERROR =
      "Error occurred while parsing SQL to physical plan: ";
  private static final String INFO_CHECK_METADATA_ERROR = "Check metadata error: ";
  private static final String INFO_QUERY_PROCESS_ERROR = "Error occurred in query process: ";
  private static final String INFO_NOT_ALLOWED_IN_BATCH_ERROR =
      "The query statement is not allowed in batch: ";

  private static final String INFO_INTERRUPT_ERROR =
      "Current Thread interrupted when dealing with request {}";

  private static final int MAX_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getQueryCacheSizeInMetric();
  private static final int DELETE_SIZE = 20;
  private static final int DEFAULT_FETCH_SIZE = 10000;
  private static final long MS_TO_MONTH = 30 * 86400_000L;

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final boolean enableMetric = config.isEnableMetricService();

  private static final List<SqlArgument> sqlArgumentList = new ArrayList<>(MAX_SIZE);
  protected Planner processor;
  protected IPlanExecutor executor;

  // Record the username for every rpc connection (session).
  private final Map<Long, String> sessionIdUsernameMap = new ConcurrentHashMap<>();
  private final Map<Long, ZoneId> sessionIdZoneIdMap = new ConcurrentHashMap<>();

  // The sessionId is unique in one IoTDB instance.
  private final AtomicLong sessionIdGenerator = new AtomicLong();
  // The statementId is unique in one IoTDB instance.
  private final AtomicLong statementIdGenerator = new AtomicLong();

  // (sessionId -> Set(statementId))
  private final Map<Long, Set<Long>> sessionId2StatementId = new ConcurrentHashMap<>();
  // (statementId -> Set(queryId))
  private final Map<Long, Set<Long>> statementId2QueryId = new ConcurrentHashMap<>();
  // (queryId -> QueryDataSet)
  private final Map<Long, QueryDataSet> queryId2DataSet = new ConcurrentHashMap<>();

  // When the client abnormally exits, we can still know who to disconnect
  private final ThreadLocal<Long> currSessionId = new ThreadLocal<>();

  public static final TSProtocolVersion CURRENT_RPC_VERSION =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;

  private static final AtomicInteger queryCount = new AtomicInteger(0);

  private QueryTimeManager queryTimeManager = QueryTimeManager.getInstance();

  public TSServiceImpl() throws QueryProcessException {
    processor = new Planner();
    executor = new PlanExecutor();

    ScheduledExecutorService timedQuerySqlCountThread =
        Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "timedQuerySqlCountThread"));
    timedQuerySqlCountThread.scheduleAtFixedRate(
        () -> {
          if (queryCount.get() != 0) {
            QUERY_FREQUENCY_LOGGER.info(
                "Query count in current 1 minute {} ", queryCount.getAndSet(0));
          }
        },
        config.getFrequencyIntervalInMinute(),
        config.getFrequencyIntervalInMinute(),
        TimeUnit.MINUTES);
  }

  public static List<SqlArgument> getSqlArgumentList() {
    return sqlArgumentList;
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    boolean status;
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }
    String loginMessage = null;
    try {
      status = authorizer.login(req.getUsername(), req.getPassword());
    } catch (AuthException e) {
      LOGGER.info("meet error while logging in.", e);
      status = false;
      loginMessage = e.getMessage();
    }
    TSStatus tsStatus;
    long sessionId = -1;
    if (status) {
      // check the version compatibility
      boolean compatible = checkCompatibility(req.getClient_protocol());
      if (!compatible) {
        tsStatus =
            RpcUtils.getStatus(
                TSStatusCode.INCOMPATIBLE_VERSION,
                "The version is incompatible, please upgrade to " + IoTDBConstant.VERSION);
        TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus, CURRENT_RPC_VERSION);
        resp.setSessionId(sessionId);
        return resp;
      }

      tsStatus = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Login successfully");
      sessionId = sessionIdGenerator.incrementAndGet();
      sessionIdUsernameMap.put(sessionId, req.getUsername());
      sessionIdZoneIdMap.put(sessionId, ZoneId.of(req.getZoneId()));
      currSessionId.set(sessionId);
      AUDIT_LOGGER.info("User {} opens Session-{}", req.getUsername(), sessionId);
      LOGGER.info(
          "{}: Login status: {}. User : {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          tsStatus.message,
          req.getUsername());
    } else {
      tsStatus =
          RpcUtils.getStatus(
              TSStatusCode.WRONG_LOGIN_PASSWORD_ERROR,
              loginMessage != null ? loginMessage : "Authentication failed.");
      AUDIT_LOGGER.info(
          "User {} opens Session failed with an incorrect password", req.getUsername());
    }

    TSOpenSessionResp resp = new TSOpenSessionResp(tsStatus, CURRENT_RPC_VERSION);
    return resp.setSessionId(sessionId);
  }

  private boolean checkCompatibility(TSProtocolVersion version) {
    return version.equals(CURRENT_RPC_VERSION);
  }

  @Override
  public TSStatus closeSession(TSCloseSessionReq req) {
    long sessionId = req.getSessionId();
    AUDIT_LOGGER.info("Session-{} is closing", sessionId);

    currSessionId.remove();
    sessionIdZoneIdMap.remove(sessionId);

    for (long statementId : sessionId2StatementId.getOrDefault(sessionId, Collections.emptySet())) {
      for (long queryId : statementId2QueryId.getOrDefault(statementId, Collections.emptySet())) {
        releaseQueryResourceNoExceptions(queryId);
      }
    }

    return new TSStatus(
        sessionIdUsernameMap.remove(sessionId) == null
            ? RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR)
            : RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSStatus cancelOperation(TSCancelOperationReq req) {
    // TODO implement
    return RpcUtils.getStatus(TSStatusCode.QUERY_NOT_ALLOWED, "Cancellation is not implemented");
  }

  @Override
  public TSStatus closeOperation(TSCloseOperationReq req) {
    if (!checkLogin(req.getSessionId())) {
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "{}: receive close operation from Session {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          currSessionId.get());
    }

    try {
      // ResultSet close
      if (req.isSetStatementId() && req.isSetQueryId()) {
        releaseQueryResourceNoExceptions(req.queryId);
        // clear the statementId2QueryId map
        if (statementId2QueryId.containsKey(req.getStatementId())) {
          statementId2QueryId.get(req.getStatementId()).remove(req.getQueryId());
        }
      } else {
        // statement close
        Set<Long> queryIdSet = statementId2QueryId.remove(req.getStatementId());
        if (queryIdSet != null) {
          for (long queryId : queryIdSet) {
            releaseQueryResourceNoExceptions(queryId);
          }
        }
        // clear the sessionId2StatementId map
        if (sessionId2StatementId.containsKey(req.getSessionId())) {
          sessionId2StatementId.get(req.getSessionId()).remove(req.getStatementId());
        }
      }
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "executing closeOperation", TSStatusCode.CLOSE_OPERATION_ERROR);
    }
  }

  /** release single operation resource */
  protected void releaseQueryResource(long queryId) throws StorageEngineException {
    // remove the corresponding Physical Plan
    QueryDataSet dataSet = queryId2DataSet.remove(queryId);
    if (dataSet instanceof UDTFDataSet) {
      ((UDTFDataSet) dataSet).finalizeUDFs(queryId);
    }
    QueryResourceManager.getInstance().endQuery(queryId);
  }

  private void releaseQueryResourceNoExceptions(long queryId) {
    if (queryId != -1) {
      try {
        releaseQueryResource(queryId);
      } catch (Exception e) {
        LOGGER.warn("Error occurred while releasing query resource: ", e);
      }
    }
  }

  @Override
  public TSFetchMetadataResp fetchMetadata(TSFetchMetadataReq req) {
    TSFetchMetadataResp resp = new TSFetchMetadataResp();

    if (!checkLogin(req.getSessionId())) {
      return resp.setStatus(RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR));
    }

    TSStatus status;
    try {
      switch (req.getType()) {
        case "METADATA_IN_JSON":
          resp.setMetadataInJson(getMetadataInString());
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "COLUMN":
          resp.setDataType(getSeriesTypeByPath(new PartialPath(req.getColumnPath())).toString());
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        case "ALL_COLUMNS":
          resp.setColumnsList(
              getPaths(new PartialPath(req.getColumnPath())).stream()
                  .map(PartialPath::getFullPath)
                  .collect(Collectors.toList()));
          status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
          break;
        default:
          status = RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, req.getType());
          break;
      }
    } catch (MetadataException e) {
      LOGGER.error(
          String.format("Failed to fetch timeseries %s's metadata", req.getColumnPath()), e);
      status = RpcUtils.getStatus(TSStatusCode.METADATA_ERROR, e.getMessage());
    } catch (Exception e) {
      status =
          onNPEOrUnexpectedException(
              e, "executing fetchMetadata", TSStatusCode.INTERNAL_SERVER_ERROR);
    }
    return resp.setStatus(status);
  }

  private String getMetadataInString() {
    return IoTDB.metaManager.getMetadataInString();
  }

  protected List<PartialPath> getPaths(PartialPath path) throws MetadataException {
    return IoTDB.metaManager.getAllTimeseriesPath(path);
  }

  private boolean executeInsertRowsPlan(InsertRowsPlan insertRowsPlan, List<TSStatus> result) {
    long t1 = System.currentTimeMillis();
    TSStatus tsStatus = executeNonQueryPlan(insertRowsPlan);
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_ROWS_PLAN_IN_BATCH, t1);
    int startIndex = result.size();
    if (startIndex > 0) {
      startIndex = startIndex - 1;
    }
    for (int i = 0; i < insertRowsPlan.getRowCount(); i++) {
      result.add(RpcUtils.SUCCESS_STATUS);
    }
    if (tsStatus.subStatus != null) {
      for (Entry<Integer, TSStatus> entry : insertRowsPlan.getResults().entrySet()) {
        result.set(startIndex + entry.getKey(), entry.getValue());
      }
    }
    return tsStatus.getCode() == RpcUtils.SUCCESS_STATUS.getCode();
  }

  private boolean executeMultiTimeSeriesPlan(
      CreateMultiTimeSeriesPlan multiPlan, List<TSStatus> result) {
    long t1 = System.currentTimeMillis();
    TSStatus tsStatus = executeNonQueryPlan(multiPlan);
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_MULTI_TIMESERIES_PLAN_IN_BATCH, t1);

    int startIndex = result.size();
    if (startIndex > 0) {
      startIndex = startIndex - 1;
    }
    for (int k = 0; k < multiPlan.getPaths().size(); k++) {
      result.add(RpcUtils.SUCCESS_STATUS);
    }
    if (tsStatus.subStatus != null) {
      for (Entry<Integer, TSStatus> entry : multiPlan.getResults().entrySet()) {
        result.set(startIndex + entry.getKey(), entry.getValue());
      }
    }
    return tsStatus.getCode() == RpcUtils.SUCCESS_STATUS.getCode();
  }

  private void initMultiTimeSeriesPlan(CreateMultiTimeSeriesPlan multiPlan) {
    if (multiPlan.getPaths() == null) {
      List<PartialPath> paths = new ArrayList<>();
      List<TSDataType> tsDataTypes = new ArrayList<>();
      List<TSEncoding> tsEncodings = new ArrayList<>();
      List<CompressionType> tsCompressionTypes = new ArrayList<>();
      List<Map<String, String>> tagsList = new ArrayList<>();
      List<Map<String, String>> attributesList = new ArrayList<>();
      List<String> aliasList = new ArrayList<>();
      multiPlan.setPaths(paths);
      multiPlan.setDataTypes(tsDataTypes);
      multiPlan.setEncodings(tsEncodings);
      multiPlan.setCompressors(tsCompressionTypes);
      multiPlan.setTags(tagsList);
      multiPlan.setAttributes(attributesList);
      multiPlan.setAlias(aliasList);
    }
  }

  private void setMultiTimeSeriesPlan(
      CreateMultiTimeSeriesPlan multiPlan, CreateTimeSeriesPlan createTimeSeriesPlan) {
    PartialPath path = createTimeSeriesPlan.getPath();
    TSDataType type = createTimeSeriesPlan.getDataType();
    TSEncoding encoding = createTimeSeriesPlan.getEncoding();
    CompressionType compressor = createTimeSeriesPlan.getCompressor();
    Map<String, String> tags = createTimeSeriesPlan.getTags();
    Map<String, String> attributes = createTimeSeriesPlan.getAttributes();
    String alias = createTimeSeriesPlan.getAlias();

    multiPlan.getPaths().add(path);
    multiPlan.getDataTypes().add(type);
    multiPlan.getEncodings().add(encoding);
    multiPlan.getCompressors().add(compressor);
    multiPlan.getTags().add(tags);
    multiPlan.getAttributes().add(attributes);
    multiPlan.getAlias().add(alias);
  }

  private boolean executeBatchList(List executeList, List<TSStatus> result) {
    boolean isAllSuccessful = true;
    for (int j = 0; j < executeList.size(); j++) {
      Object planObject = executeList.get(j);
      if (InsertRowsPlan.class.isInstance(planObject)) {
        if (!executeInsertRowsPlan((InsertRowsPlan) planObject, result)) {
          isAllSuccessful = false;
        }
      } else if (CreateMultiTimeSeriesPlan.class.isInstance(planObject)) {
        if (!executeMultiTimeSeriesPlan((CreateMultiTimeSeriesPlan) planObject, result)) {
          isAllSuccessful = false;
        }
      }
    }
    return isAllSuccessful;
  }

  @Override
  public TSStatus executeBatchStatement(TSExecuteBatchStatementReq req) {
    long t1 = System.currentTimeMillis();
    List<TSStatus> result = new ArrayList<>();
    boolean isAllSuccessful = true;
    if (!checkLogin(req.getSessionId())) {
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    InsertRowsPlan insertRowsPlan;
    int index = 0;
    List executeList = new ArrayList();
    OperatorType lastOperatorType = null;
    CreateMultiTimeSeriesPlan multiPlan;
    for (int i = 0; i < req.getStatements().size(); i++) {
      String statement = req.getStatements().get(i);
      try {
        PhysicalPlan physicalPlan =
            processor.parseSQLToPhysicalPlan(
                statement, sessionIdZoneIdMap.get(req.getSessionId()), DEFAULT_FETCH_SIZE);
        if (physicalPlan.isQuery()) {
          throw new QueryInBatchStatementException(statement);
        }

        if (physicalPlan.getOperatorType().equals(OperatorType.INSERT)) {
          if (OperatorType.INSERT == lastOperatorType) {
            insertRowsPlan = (InsertRowsPlan) executeList.get(executeList.size() - 1);
          } else {
            insertRowsPlan = new InsertRowsPlan();
            executeList.add(insertRowsPlan);
            index = 0;
          }
          lastOperatorType = OperatorType.INSERT;
          insertRowsPlan.addOneInsertRowPlan((InsertRowPlan) physicalPlan, index);
          index++;
          if (i == req.getStatements().size() - 1) {
            if (!executeBatchList(executeList, result)) {
              isAllSuccessful = false;
            }
          }
        } else if (physicalPlan.getOperatorType().equals(OperatorType.CREATE_TIMESERIES)) {
          if (OperatorType.CREATE_TIMESERIES == lastOperatorType) {
            multiPlan = (CreateMultiTimeSeriesPlan) executeList.get(executeList.size() - 1);
          } else {
            multiPlan = new CreateMultiTimeSeriesPlan();
            executeList.add(multiPlan);
          }
          lastOperatorType = OperatorType.CREATE_TIMESERIES;
          initMultiTimeSeriesPlan(multiPlan);

          CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) physicalPlan;
          setMultiTimeSeriesPlan(multiPlan, createTimeSeriesPlan);
          if (i == req.getStatements().size() - 1) {
            if (!executeBatchList(executeList, result)) {
              isAllSuccessful = false;
            }
          }
        } else {
          lastOperatorType = physicalPlan.getOperatorType();
          if (executeList.size() > 0) {
            if (!executeBatchList(executeList, result)) {
              isAllSuccessful = false;
            }
            executeList.clear();
          }
          long t2 = System.currentTimeMillis();
          TSExecuteStatementResp resp = executeUpdateStatement(physicalPlan, req.getSessionId());
          Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_ONE_SQL_IN_BATCH, t2);
          result.add(resp.status);
          if (resp.getStatus().code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            isAllSuccessful = false;
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error occurred when executing executeBatchStatement: ", e);
        TSStatus status = tryCatchQueryException(e);
        if (status != null) {
          result.add(status);
          isAllSuccessful = false;
        } else {
          result.add(
              onNPEOrUnexpectedException(
                  e, "executing " + statement, TSStatusCode.INTERNAL_SERVER_ERROR));
        }
      }
    }
    Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_JDBC_BATCH, t1);
    return isAllSuccessful
        ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute batch statements successfully")
        : RpcUtils.getStatus(result);
  }

  @Override
  public TSExecuteStatementResp executeStatement(TSExecuteStatementReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NOT_LOGIN_ERROR);
      }

      String statement = req.getStatement();
      PhysicalPlan physicalPlan =
          processor.parseSQLToPhysicalPlan(
              statement, sessionIdZoneIdMap.get(req.getSessionId()), req.fetchSize);

      physicalPlan.setLoginUserName(sessionIdUsernameMap.get(req.getSessionId()));

      return physicalPlan.isQuery()
          ? internalExecuteQueryStatement(
              statement,
              req.statementId,
              physicalPlan,
              req.fetchSize,
              req.timeout,
              sessionIdUsernameMap.get(req.getSessionId()),
              req.isEnableRedirectQuery())
          : executeUpdateStatement(physicalPlan, req.getSessionId());
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSExecuteStatementResp(onQueryException(e, "executing executeStatement"));
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(onQueryException(e, "executing executeStatement"));
    }
  }

  @Override
  public TSExecuteStatementResp executeQueryStatement(TSExecuteStatementReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NOT_LOGIN_ERROR);
      }

      String statement = req.getStatement();
      PhysicalPlan physicalPlan =
          processor.parseSQLToPhysicalPlan(
              statement, sessionIdZoneIdMap.get(req.getSessionId()), req.fetchSize);

      physicalPlan.setLoginUserName(sessionIdUsernameMap.get(req.getSessionId()));

      return physicalPlan.isQuery()
          ? internalExecuteQueryStatement(
              statement,
              req.statementId,
              physicalPlan,
              req.fetchSize,
              req.timeout,
              sessionIdUsernameMap.get(req.getSessionId()),
              req.isEnableRedirectQuery())
          : RpcUtils.getTSExecuteStatementResp(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "executing executeQueryStatement"));
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "executing executeQueryStatement"));
    }
  }

  @Override
  public TSExecuteStatementResp executeRawDataQuery(TSRawDataQueryReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NOT_LOGIN_ERROR);
      }

      PhysicalPlan physicalPlan =
          processor.rawDataQueryReqToPhysicalPlan(req, sessionIdZoneIdMap.get(req.getSessionId()));
      physicalPlan.setLoginUserName(sessionIdUsernameMap.get(req.getSessionId()));
      return physicalPlan.isQuery()
          ? internalExecuteQueryStatement(
              "",
              req.statementId,
              physicalPlan,
              req.fetchSize,
              config.getQueryTimeoutThreshold(),
              sessionIdUsernameMap.get(req.getSessionId()),
              req.isEnableRedirectQuery())
          : RpcUtils.getTSExecuteStatementResp(
              TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is not a query statement.");
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "executing executeRawDataQuery"));
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(
          onQueryException(e, "executing executeRawDataQuery"));
    }
  }

  /**
   * @param plan must be a plan for Query: FillQueryPlan, AggregationPlan, GroupByTimePlan, UDFPlan,
   *     some AuthorPlan
   */
  @SuppressWarnings({"squid:S3776", "squid:S1141"}) // Suppress high Cognitive Complexity warning
  private TSExecuteStatementResp internalExecuteQueryStatement(
      String statement,
      long statementId,
      PhysicalPlan plan,
      int fetchSize,
      long timeout,
      String username,
      boolean enableRedirect)
      throws QueryProcessException, SQLException, StorageEngineException,
          QueryFilterOptimizationException, MetadataException, IOException, InterruptedException,
          TException, AuthException {
    queryCount.incrementAndGet();
    AUDIT_LOGGER.debug("Session {} execute Query: {}", currSessionId.get(), statement);
    long startTime = System.currentTimeMillis();
    long queryId = -1;
    try {

      // pair.left = fetchSize, pair.right = deduplicatedNum
      Pair<Integer, Integer> p = getMemoryParametersFromPhysicalPlan(plan, fetchSize);
      fetchSize = p.left;

      // generate the queryId for the operation
      queryId = generateQueryId(true, fetchSize, p.right);
      // register query info to queryTimeManager
      if (!(plan instanceof ShowQueryProcesslistPlan)) {
        queryTimeManager.registerQuery(queryId, startTime, statement, timeout);
      }
      if (plan instanceof QueryPlan && config.isEnablePerformanceTracing()) {
        TracingManager tracingManager = TracingManager.getInstance();
        if (!(plan instanceof AlignByDevicePlan)) {
          tracingManager.writeQueryInfo(queryId, statement, startTime, plan.getPaths().size());
        } else {
          tracingManager.writeQueryInfo(queryId, statement, startTime);
        }
      }

      statementId2QueryId
          .computeIfAbsent(statementId, k -> new CopyOnWriteArraySet<>())
          .add(queryId);

      if (plan instanceof AuthorPlan) {
        plan.setLoginUserName(username);
      }

      TSExecuteStatementResp resp = null;
      // execute it before createDataSet since it may change the content of query plan
      if (plan instanceof QueryPlan && !(plan instanceof UDFPlan)) {
        resp = getQueryColumnHeaders(plan, username);
      }
      if (plan instanceof QueryPlan) {
        ((QueryPlan) plan).setEnableRedirect(enableRedirect);
      }
      // create and cache dataset
      QueryDataSet newDataSet = createQueryDataSet(queryId, plan, fetchSize);

      if (newDataSet.getEndPoint() != null && enableRedirect) {
        // redirect query
        LOGGER.debug(
            "need to redirect {} {} to node {}", statement, queryId, newDataSet.getEndPoint());
        TSStatus status = new TSStatus();
        status.setRedirectNode(
            new EndPoint(newDataSet.getEndPoint().getIp(), newDataSet.getEndPoint().getPort()));
        status.setCode(TSStatusCode.NEED_REDIRECTION.getStatusCode());
        resp.setStatus(status);
        resp.setQueryId(queryId);
        return resp;
      }

      if (plan instanceof ShowPlan || plan instanceof AuthorPlan) {
        resp = getListDataSetHeaders(newDataSet);
      } else if (plan instanceof UDFPlan) {
        resp = getQueryColumnHeaders(plan, username);
      }

      resp.setOperationType(plan.getOperatorType().toString());
      if (plan.getOperatorType() == OperatorType.AGGREGATION) {
        resp.setIgnoreTimeStamp(true);
      } else if (plan instanceof ShowQueryProcesslistPlan) {
        resp.setIgnoreTimeStamp(false);
      }

      if (newDataSet instanceof DirectNonAlignDataSet) {
        resp.setNonAlignQueryDataSet(fillRpcNonAlignReturnData(fetchSize, newDataSet, username));
      } else {
        try {
          TSQueryDataSet tsQueryDataSet = fillRpcReturnData(fetchSize, newDataSet, username);
          resp.setQueryDataSet(tsQueryDataSet);
        } catch (RedirectException e) {
          LOGGER.debug("need to redirect {} {} to {}", statement, queryId, e.getEndPoint());
          if (enableRedirect) {
            // redirect query
            TSStatus status = new TSStatus();
            status.setRedirectNode(e.getEndPoint());
            status.setCode(TSStatusCode.NEED_REDIRECTION.getStatusCode());
            resp.setStatus(status);
            resp.setQueryId(queryId);
            return resp;
          } else {
            LOGGER.error(
                "execute {} error, if session does not support redirect,"
                    + " should not throw redirection exception.",
                statement,
                e);
          }
        }
      }
      resp.setQueryId(queryId);

      if (plan instanceof AlignByDevicePlan && config.isEnablePerformanceTracing()) {
        TracingManager.getInstance()
            .writePathsNum(queryId, ((AlignByDeviceDataSet) newDataSet).getPathsNum());
      }

      if (enableMetric) {
        long endTime = System.currentTimeMillis();
        SqlArgument sqlArgument = new SqlArgument(resp, plan, statement, startTime, endTime);
        synchronized (sqlArgumentList) {
          sqlArgumentList.add(sqlArgument);
          if (sqlArgumentList.size() >= MAX_SIZE) {
            sqlArgumentList.subList(0, DELETE_SIZE).clear();
          }
        }
      }

      // remove query info in QueryTimeManager
      if (!(plan instanceof ShowQueryProcesslistPlan)) {
        queryTimeManager.unRegisterQuery(queryId);
      }

      LOGGER.info(
          "{}, {}, {}", plan.getOperatorType(), plan.getLoginUserName(), config.getRpcAddress());

      return resp;
    } catch (Exception e) {
      releaseQueryResourceNoExceptions(queryId);
      throw e;
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_QUERY, startTime);
      long costTime = System.currentTimeMillis() - startTime;
      if (costTime >= config.getSlowQueryThreshold()) {
        SLOW_SQL_LOGGER.info("Cost: {} ms, sql is {}", costTime, statement);
      }
      if (config.isDebugOn()) {
        SLOW_SQL_LOGGER.info(
            "ChunkCache used memory proportion: {}\n"
                + "TimeSeriesMetadataCache used memory proportion: {}",
            ChunkCache.getInstance().getUsedMemoryProportion(),
            TimeSeriesMetadataCache.getInstance().getUsedMemoryProportion());
      }
    }
  }

  /**
   * get fetchSize and deduplicatedPathNum that are used for memory estimation
   *
   * @return Pair - fetchSize, deduplicatedPathNum
   */
  private Pair<Integer, Integer> getMemoryParametersFromPhysicalPlan(
      PhysicalPlan plan, int fetchSizeBefore) {
    // In case users forget to set this field in query, use the default value
    int fetchSize = fetchSizeBefore == 0 ? DEFAULT_FETCH_SIZE : fetchSizeBefore;
    int deduplicatedPathNum = -1;
    if (plan instanceof GroupByTimePlan) {
      fetchSize = Math.min(getFetchSizeForGroupByTimePlan((GroupByTimePlan) plan), fetchSize);
    } else if (plan.getOperatorType() == OperatorType.AGGREGATION) {
      // the actual row number of aggregation query is 1
      fetchSize = 1;
    }
    if (plan instanceof AlignByDevicePlan) {
      deduplicatedPathNum = ((AlignByDevicePlan) plan).getMeasurements().size();
    } else if (plan instanceof LastQueryPlan) {
      // dataset of last query consists of three column: time column + value column = 1
      // deduplicatedPathNum
      // and we assume that the memory which sensor name takes equals to 1 deduplicatedPathNum
      deduplicatedPathNum = 2;
      // last query's actual row number should be the minimum between the number of series and
      // fetchSize
      fetchSize = Math.min(((LastQueryPlan) plan).getDeduplicatedPaths().size(), fetchSize);
    } else if (plan instanceof RawDataQueryPlan) {
      deduplicatedPathNum = ((RawDataQueryPlan) plan).getDeduplicatedPaths().size();
    }
    return new Pair<>(fetchSize, deduplicatedPathNum);
  }

  /*
  calculate fetch size for group by time plan
   */
  private int getFetchSizeForGroupByTimePlan(GroupByTimePlan plan) {
    int rows = (int) ((plan.getEndTime() - plan.getStartTime()) / plan.getInterval());
    // rows gets 0 is caused by: the end time - the start time < the time interval.
    if (rows == 0 && plan.isIntervalByMonth()) {
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(plan.getStartTime());
      calendar.add(Calendar.MONTH, (int) (plan.getInterval() / MS_TO_MONTH));
      rows = calendar.getTimeInMillis() <= plan.getEndTime() ? 1 : 0;
    }
    return rows;
  }

  private TSExecuteStatementResp getListDataSetHeaders(QueryDataSet dataSet) {
    return StaticResps.getNoTimeExecuteResp(
        dataSet.getPaths().stream().map(Path::getFullPath).collect(Collectors.toList()),
        dataSet.getDataTypes().stream().map(Enum::toString).collect(Collectors.toList()));
  }

  /** get ResultSet schema */
  private TSExecuteStatementResp getQueryColumnHeaders(PhysicalPlan physicalPlan, String username)
      throws AuthException, TException, QueryProcessException, MetadataException {

    List<String> respColumns = new ArrayList<>();
    List<String> columnsTypes = new ArrayList<>();

    // check permissions
    if (!checkAuthorization(physicalPlan.getPaths(), physicalPlan, username)) {
      return RpcUtils.getTSExecuteStatementResp(
          RpcUtils.getStatus(
              TSStatusCode.NO_PERMISSION_ERROR,
              "No permissions for this operation " + physicalPlan.getOperatorType()));
    }

    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(TSStatusCode.SUCCESS_STATUS);

    // align by device query
    QueryPlan plan = (QueryPlan) physicalPlan;
    if (plan instanceof AlignByDevicePlan) {
      getAlignByDeviceQueryHeaders((AlignByDevicePlan) plan, respColumns, columnsTypes);
    } else if (plan instanceof LastQueryPlan) {
      // Last Query should return different respond instead of the static one
      // because the query dataset and query id is different although the header of last query is
      // same.
      return StaticResps.LAST_RESP.deepCopy();
    } else if (plan instanceof AggregationPlan && ((AggregationPlan) plan).getLevel() >= 0) {
      Map<String, AggregateResult> finalPaths = ((AggregationPlan) plan).getAggPathByLevel();
      for (Map.Entry<String, AggregateResult> entry : finalPaths.entrySet()) {
        respColumns.add(entry.getKey());
        columnsTypes.add(entry.getValue().getResultDataType().toString());
      }
    } else {
      getWideQueryHeaders(plan, respColumns, columnsTypes);
      resp.setColumnNameIndexMap(plan.getPathToIndex());
    }
    resp.setColumns(respColumns);
    resp.setDataTypeList(columnsTypes);
    return resp;
  }

  // wide means not align by device
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void getWideQueryHeaders(
      QueryPlan plan, List<String> respColumns, List<String> columnTypes)
      throws TException, MetadataException {
    // Restore column header of aggregate to func(column_name), only
    // support single aggregate function for now
    List<PartialPath> paths = plan.getPaths();
    List<TSDataType> seriesTypes = new ArrayList<>();
    switch (plan.getOperatorType()) {
      case QUERY:
      case FILL:
        for (PartialPath path : paths) {
          String column;
          if (path.isTsAliasExists()) {
            column = path.getTsAlias();
          } else {
            column =
                path.isMeasurementAliasExists() ? path.getFullPathWithAlias() : path.getFullPath();
          }
          respColumns.add(column);
          seriesTypes.add(getSeriesTypeByPath(path));
        }
        break;
      case AGGREGATION:
      case GROUPBYTIME:
      case GROUP_BY_FILL:
        List<String> aggregations = plan.getAggregations();
        if (aggregations.size() != paths.size()) {
          for (int i = 1; i < paths.size(); i++) {
            aggregations.add(aggregations.get(0));
          }
        }
        for (int i = 0; i < paths.size(); i++) {
          PartialPath path = paths.get(i);
          String column;
          if (path.isTsAliasExists()) {
            column = path.getTsAlias();
          } else {
            column =
                path.isMeasurementAliasExists()
                    ? aggregations.get(i) + "(" + paths.get(i).getFullPathWithAlias() + ")"
                    : aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")";
          }
          respColumns.add(column);
        }
        seriesTypes = getSeriesTypesByPaths(paths, aggregations);
        break;
      case UDTF:
        seriesTypes = new ArrayList<>();
        UDTFPlan udtfPlan = (UDTFPlan) plan;
        for (int i = 0; i < paths.size(); i++) {
          respColumns.add(
              paths.get(i) != null
                  ? paths.get(i).getFullPath()
                  : udtfPlan
                      .getExecutorByOriginalOutputColumnIndex(i)
                      .getContext()
                      .getColumnName());
          seriesTypes.add(
              paths.get(i) != null
                  ? udtfPlan.getDataTypes().get(i)
                  : udtfPlan
                      .getExecutorByOriginalOutputColumnIndex(i)
                      .getConfigurations()
                      .getOutputDataType());
        }
        break;
      default:
        throw new TException("unsupported query type: " + plan.getOperatorType());
    }

    for (TSDataType seriesType : seriesTypes) {
      columnTypes.add(seriesType.toString());
    }
  }

  private void getAlignByDeviceQueryHeaders(
      AlignByDevicePlan plan, List<String> respColumns, List<String> columnTypes) {
    // set columns in TSExecuteStatementResp.
    respColumns.add(SQLConstant.ALIGNBY_DEVICE_COLUMN_NAME);

    // get column types and do deduplication
    columnTypes.add(TSDataType.TEXT.toString()); // the DEVICE column of ALIGN_BY_DEVICE result
    List<TSDataType> deduplicatedColumnsType = new ArrayList<>();
    deduplicatedColumnsType.add(TSDataType.TEXT); // the DEVICE column of ALIGN_BY_DEVICE result

    Set<String> deduplicatedMeasurements = new LinkedHashSet<>();
    Map<String, TSDataType> measurementDataTypeMap = plan.getColumnDataTypeMap();

    // build column header with constant and non exist column and deduplication
    List<String> measurements = plan.getMeasurements();
    Map<String, String> measurementAliasMap = plan.getMeasurementAliasMap();
    Map<String, MeasurementType> measurementTypeMap = plan.getMeasurementTypeMap();
    for (String measurement : measurements) {
      TSDataType type = TSDataType.TEXT;
      switch (measurementTypeMap.get(measurement)) {
        case Exist:
          type = measurementDataTypeMap.get(measurement);
          break;
        case NonExist:
        case Constant:
          type = TSDataType.TEXT;
      }
      respColumns.add(measurementAliasMap.getOrDefault(measurement, measurement));
      columnTypes.add(type.toString());

      if (!deduplicatedMeasurements.contains(measurement)) {
        deduplicatedMeasurements.add(measurement);
        deduplicatedColumnsType.add(type);
      }
    }

    // save deduplicated measurementColumn names and types in QueryPlan for the next stage to use.
    // i.e., used by AlignByDeviceDataSet constructor in `fetchResults` stage.
    plan.setMeasurements(new ArrayList<>(deduplicatedMeasurements));
    plan.setDataTypes(deduplicatedColumnsType);

    // set these null since they are never used henceforth in ALIGN_BY_DEVICE query processing.
    plan.setPaths(null);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSFetchResultsResp fetchResults(TSFetchResultsReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getTSFetchResultsResp(TSStatusCode.NOT_LOGIN_ERROR);
      }

      if (!queryId2DataSet.containsKey(req.queryId)) {
        return RpcUtils.getTSFetchResultsResp(
            RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, "Has not executed query"));
      }

      // register query info to queryTimeManager
      queryTimeManager.registerQuery(
          req.queryId, System.currentTimeMillis(), req.statement, req.timeout);

      QueryDataSet queryDataSet = queryId2DataSet.get(req.queryId);
      if (req.isAlign) {
        TSQueryDataSet result =
            fillRpcReturnData(req.fetchSize, queryDataSet, sessionIdUsernameMap.get(req.sessionId));
        boolean hasResultSet = result.bufferForTime().limit() != 0;
        if (!hasResultSet) {
          releaseQueryResourceNoExceptions(req.queryId);
        }
        TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
        resp.setHasResultSet(hasResultSet);
        resp.setQueryDataSet(result);
        resp.setIsAlign(true);

        queryTimeManager.unRegisterQuery(req.queryId);
        return resp;
      } else {
        TSQueryNonAlignDataSet nonAlignResult =
            fillRpcNonAlignReturnData(
                req.fetchSize, queryDataSet, sessionIdUsernameMap.get(req.sessionId));
        boolean hasResultSet = false;
        for (ByteBuffer timeBuffer : nonAlignResult.getTimeList()) {
          if (timeBuffer.limit() != 0) {
            hasResultSet = true;
            break;
          }
        }
        if (!hasResultSet) {
          queryId2DataSet.remove(req.queryId);
        }
        TSFetchResultsResp resp = RpcUtils.getTSFetchResultsResp(TSStatusCode.SUCCESS_STATUS);
        resp.setHasResultSet(hasResultSet);
        resp.setNonAlignQueryDataSet(nonAlignResult);
        resp.setIsAlign(false);

        queryTimeManager.unRegisterQuery(req.queryId);
        return resp;
      }
    } catch (InterruptedException e) {
      LOGGER.error(INFO_INTERRUPT_ERROR, req, e);
      Thread.currentThread().interrupt();
      return RpcUtils.getTSFetchResultsResp(
          onNPEOrUnexpectedException(
              e, "executing fetchResults", TSStatusCode.INTERNAL_SERVER_ERROR));
    } catch (Exception e) {
      releaseQueryResourceNoExceptions(req.queryId);
      return RpcUtils.getTSFetchResultsResp(
          onNPEOrUnexpectedException(
              e, "executing fetchResults", TSStatusCode.INTERNAL_SERVER_ERROR));
    }
  }

  private TSQueryDataSet fillRpcReturnData(
      int fetchSize, QueryDataSet queryDataSet, String userName)
      throws TException, AuthException, IOException, InterruptedException, QueryProcessException {
    WatermarkEncoder encoder = getWatermarkEncoder(userName);
    return queryDataSet instanceof DirectAlignByTimeDataSet
        ? ((DirectAlignByTimeDataSet) queryDataSet).fillBuffer(fetchSize, encoder)
        : QueryDataSetUtils.convertQueryDataSetByFetchSize(queryDataSet, fetchSize, encoder);
  }

  private TSQueryNonAlignDataSet fillRpcNonAlignReturnData(
      int fetchSize, QueryDataSet queryDataSet, String userName)
      throws TException, AuthException, IOException, QueryProcessException, InterruptedException {
    WatermarkEncoder encoder = getWatermarkEncoder(userName);
    return ((DirectNonAlignDataSet) queryDataSet).fillBuffer(fetchSize, encoder);
  }

  private WatermarkEncoder getWatermarkEncoder(String userName) throws TException, AuthException {
    IAuthorizer authorizer;
    try {
      authorizer = BasicAuthorizer.getInstance();
    } catch (AuthException e) {
      throw new TException(e);
    }

    WatermarkEncoder encoder = null;
    if (config.isEnableWatermark() && authorizer.isUserUseWaterMark(userName)) {
      if (config.getWatermarkMethodName().equals(IoTDBConfig.WATERMARK_GROUPED_LSB)) {
        encoder = new GroupedLSBWatermarkEncoder(config);
      } else {
        throw new UnSupportedDataTypeException(
            String.format(
                "Watermark method is not supported yet: %s", config.getWatermarkMethodName()));
      }
    }
    return encoder;
  }

  /** create QueryDataSet and buffer it for fetchResults */
  private QueryDataSet createQueryDataSet(long queryId, PhysicalPlan physicalPlan, int fetchSize)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException,
          IOException, MetadataException, SQLException, TException, InterruptedException {

    QueryContext context = genQueryContext(queryId);
    QueryDataSet queryDataSet = executor.processQuery(physicalPlan, context);
    queryDataSet.setFetchSize(fetchSize);
    queryId2DataSet.put(queryId, queryDataSet);
    return queryDataSet;
  }

  protected QueryContext genQueryContext(long queryId) {
    return new QueryContext(queryId);
  }

  @Override
  public TSExecuteStatementResp executeUpdateStatement(TSExecuteStatementReq req) {
    if (!checkLogin(req.getSessionId())) {
      return RpcUtils.getTSExecuteStatementResp(TSStatusCode.NOT_LOGIN_ERROR);
    }

    try {
      return executeUpdateStatement(req.getStatement(), req.getSessionId());
    } catch (Exception e) {
      return RpcUtils.getTSExecuteStatementResp(onQueryException(e, "executing update statement"));
    }
  }

  private TSExecuteStatementResp executeUpdateStatement(PhysicalPlan plan, long sessionId) {
    TSStatus status = checkAuthority(plan, sessionId);
    if (status != null) {
      return new TSExecuteStatementResp(status);
    }

    status = executeNonQueryPlan(plan);
    TSExecuteStatementResp resp = RpcUtils.getTSExecuteStatementResp(status);
    long queryId = generateQueryId(false, DEFAULT_FETCH_SIZE, -1);
    return resp.setQueryId(queryId);
  }

  private boolean executeNonQuery(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return executor.processNonQuery(plan);
  }

  private TSExecuteStatementResp executeUpdateStatement(String statement, long sessionId)
      throws QueryProcessException {
    PhysicalPlan physicalPlan =
        processor.parseSQLToPhysicalPlan(
            statement, sessionIdZoneIdMap.get(sessionId), DEFAULT_FETCH_SIZE);
    return physicalPlan.isQuery()
        ? RpcUtils.getTSExecuteStatementResp(
            TSStatusCode.EXECUTE_STATEMENT_ERROR, "Statement is a query statement.")
        : executeUpdateStatement(physicalPlan, sessionId);
  }

  /**
   * Check whether current user has logged in.
   *
   * @return true: If logged in; false: If not logged in
   */
  private boolean checkLogin(long sessionId) {
    boolean isLoggedIn = sessionIdUsernameMap.get(sessionId) != null;
    if (!isLoggedIn) {
      LOGGER.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
    }
    return isLoggedIn;
  }

  private boolean checkAuthorization(List<PartialPath> paths, PhysicalPlan plan, String username)
      throws AuthException {
    String targetUser = null;
    if (plan instanceof AuthorPlan) {
      targetUser = ((AuthorPlan) plan).getUserName();
    }
    return AuthorityChecker.check(username, paths, plan.getOperatorType(), targetUser);
  }

  protected void handleClientExit() {
    Long sessionId = currSessionId.get();
    if (sessionId != null) {
      TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
      closeSession(req);
    }
  }

  @Override
  public TSGetTimeZoneResp getTimeZone(long sessionId) {
    try {
      ZoneId zoneId = sessionIdZoneIdMap.get(sessionId);
      return new TSGetTimeZoneResp(
          RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS),
          zoneId != null ? zoneId.toString() : "Unknown time zone");
    } catch (Exception e) {
      return new TSGetTimeZoneResp(
          onNPEOrUnexpectedException(
              e, "generating time zone", TSStatusCode.GENERATE_TIME_ZONE_ERROR),
          "Unknown time zone");
    }
  }

  @Override
  public TSStatus setTimeZone(TSSetTimeZoneReq req) {
    try {
      sessionIdZoneIdMap.put(req.getSessionId(), ZoneId.of(req.getTimeZone()));
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(e, "setting time zone", TSStatusCode.SET_TIME_ZONE_ERROR);
    }
  }

  @Override
  public ServerProperties getProperties() {
    ServerProperties properties = new ServerProperties();
    properties.setVersion(IoTDBConstant.VERSION);
    LOGGER.info("IoTDB server version: {}", IoTDBConstant.VERSION);
    properties.setSupportedTimeAggregationOperations(new ArrayList<>());
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MAX_TIME);
    properties.getSupportedTimeAggregationOperations().add(IoTDBConstant.MIN_TIME);
    properties.setTimestampPrecision(
        IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision());
    return properties;
  }

  @Override
  public TSStatus insertRecords(TSInsertRecordsReq req) {
    if (!checkLogin(req.getSessionId())) {
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session {} insertRecords, first device {}, first time {}",
          currSessionId.get(),
          req.deviceIds.get(0),
          req.getTimestamps().get(0));
    }
    boolean allCheckSuccess = true;
    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    for (int i = 0; i < req.deviceIds.size(); i++) {
      try {
        InsertRowPlan plan =
            new InsertRowPlan(
                new PartialPath(req.getDeviceIds().get(i)),
                req.getTimestamps().get(i),
                req.getMeasurementsList().get(i).toArray(new String[0]),
                req.valuesList.get(i));
        TSStatus status = checkAuthority(plan, req.getSessionId());
        if (status != null) {
          insertRowsPlan.getResults().put(i, status);
          allCheckSuccess = false;
        }
        insertRowsPlan.addOneInsertRowPlan(plan, i);
      } catch (Exception e) {
        allCheckSuccess = false;
        insertRowsPlan
            .getResults()
            .put(
                i,
                onNPEOrUnexpectedException(
                    e, "inserting records", TSStatusCode.INTERNAL_SERVER_ERROR));
      }
    }
    TSStatus tsStatus = executeNonQueryPlan(insertRowsPlan);

    return judgeFinalTsStatus(
        allCheckSuccess, tsStatus, insertRowsPlan.getResults(), req.deviceIds.size());
  }

  private TSStatus judgeFinalTsStatus(
      boolean allCheckSuccess,
      TSStatus executeTsStatus,
      Map<Integer, TSStatus> checkTsStatus,
      int totalRowCount) {

    if (allCheckSuccess) {
      return executeTsStatus;
    }

    if (executeTsStatus.subStatus == null) {
      TSStatus[] tmpSubTsStatus = new TSStatus[totalRowCount];
      Arrays.fill(tmpSubTsStatus, RpcUtils.SUCCESS_STATUS);
      executeTsStatus.subStatus = Arrays.asList(tmpSubTsStatus);
    }
    for (Entry<Integer, TSStatus> entry : checkTsStatus.entrySet()) {
      executeTsStatus.subStatus.set(entry.getKey(), entry.getValue());
    }
    return RpcUtils.getStatus(executeTsStatus.subStatus);
  }

  @Override
  public TSStatus insertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq req) {
    if (!checkLogin(req.getSessionId())) {
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session {} insertRecords, device {}, first time {}",
          currSessionId.get(),
          req.deviceId,
          req.getTimestamps().get(0));
    }

    List<TSStatus> statusList = new ArrayList<>();
    try {
      InsertRowsOfOneDevicePlan plan =
          new InsertRowsOfOneDevicePlan(
              new PartialPath(req.getDeviceId()),
              req.getTimestamps().toArray(new Long[0]),
              req.getMeasurementsList(),
              req.getValuesList().toArray(new ByteBuffer[0]));
      TSStatus status = checkAuthority(plan, req.getSessionId());
      statusList.add(status != null ? status : executeNonQueryPlan(plan));
    } catch (Exception e) {
      statusList.add(
          onNPEOrUnexpectedException(
              e, "inserting records of one device", TSStatusCode.INTERNAL_SERVER_ERROR));
    }

    TSStatus resp = RpcUtils.getStatus(statusList);
    for (TSStatus status : resp.subStatus) {
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return resp;
      }
    }

    resp.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());

    return resp;
  }

  @Override
  public TSStatus insertStringRecords(TSInsertStringRecordsReq req) {
    if (!checkLogin(req.getSessionId())) {
      return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
    }

    if (AUDIT_LOGGER.isDebugEnabled()) {
      AUDIT_LOGGER.debug(
          "Session {} insertRecords, first device {}, first time {}",
          currSessionId.get(),
          req.deviceIds.get(0),
          req.getTimestamps().get(0));
    }

    boolean allCheckSuccess = true;
    InsertRowsPlan insertRowsPlan = new InsertRowsPlan();
    for (int i = 0; i < req.deviceIds.size(); i++) {
      InsertRowPlan plan = new InsertRowPlan();
      try {
        plan.setDeviceId(new PartialPath(req.getDeviceIds().get(i)));
        plan.setTime(req.getTimestamps().get(i));
        addMeasurementAndValue(plan, req.getMeasurementsList().get(i), req.getValuesList().get(i));
        plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
        plan.setNeedInferType(true);
        TSStatus status = checkAuthority(plan, req.getSessionId());
        if (status != null) {
          insertRowsPlan.getResults().put(i, status);
          allCheckSuccess = false;
        }
        insertRowsPlan.addOneInsertRowPlan(plan, i);
      } catch (Exception e) {
        insertRowsPlan
            .getResults()
            .put(
                i,
                onNPEOrUnexpectedException(
                    e, "inserting string records", TSStatusCode.INTERNAL_SERVER_ERROR));
        allCheckSuccess = false;
      }
    }
    TSStatus tsStatus = executeNonQueryPlan(insertRowsPlan);

    return judgeFinalTsStatus(
        allCheckSuccess, tsStatus, insertRowsPlan.getResults(), req.deviceIds.size());
  }

  private void addMeasurementAndValue(
      InsertRowPlan insertRowPlan, List<String> measurements, List<String> values) {
    List<String> newMeasurements = new ArrayList<>(measurements.size());
    List<Object> newValues = new ArrayList<>(values.size());

    for (int i = 0; i < measurements.size(); ++i) {
      String value = values.get(i);
      if (value.isEmpty()) {
        continue;
      }
      newMeasurements.add(measurements.get(i));
      newValues.add(value);
    }

    insertRowPlan.setValues(newValues.toArray(new Object[0]));
    insertRowPlan.setMeasurements(newMeasurements.toArray(new String[0]));
  }

  @Override
  public TSStatus testInsertTablet(TSInsertTabletReq req) {
    LOGGER.debug("Test insert batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertTablets(TSInsertTabletsReq req) {
    LOGGER.debug("Test insert batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecord(TSInsertRecordReq req) {
    LOGGER.debug("Test insert row request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertStringRecord(TSInsertStringRecordReq req) {
    LOGGER.debug("Test insert string record request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecords(TSInsertRecordsReq req) {
    LOGGER.debug("Test insert row in batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertRecordsOfOneDevice(TSInsertRecordsOfOneDeviceReq req) {
    LOGGER.debug("Test insert rows in batch request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus testInsertStringRecords(TSInsertStringRecordsReq req) {
    LOGGER.debug("Test insert string records request receive.");
    return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
  }

  @Override
  public TSStatus insertRecord(TSInsertRecordReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      AUDIT_LOGGER.debug(
          "Session {} insertRecord, device {}, time {}",
          currSessionId.get(),
          req.getDeviceId(),
          req.getTimestamp());

      InsertRowPlan plan =
          new InsertRowPlan(
              new PartialPath(req.getDeviceId()),
              req.getTimestamp(),
              req.getMeasurements().toArray(new String[0]),
              req.values);

      TSStatus status = checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "inserting a record", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus insertStringRecord(TSInsertStringRecordReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      AUDIT_LOGGER.debug(
          "Session {} insertRecord, device {}, time {}",
          currSessionId.get(),
          req.getDeviceId(),
          req.getTimestamp());

      InsertRowPlan plan = new InsertRowPlan();
      plan.setDeviceId(new PartialPath(req.getDeviceId()));
      plan.setTime(req.getTimestamp());
      plan.setMeasurements(req.getMeasurements().toArray(new String[0]));
      plan.setDataTypes(new TSDataType[plan.getMeasurements().length]);
      plan.setValues(req.getValues().toArray(new Object[0]));
      plan.setNeedInferType(true);

      TSStatus status = checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "inserting a string record", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus deleteData(TSDeleteDataReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      DeletePlan plan = new DeletePlan();
      plan.setDeleteStartTime(req.getStartTime());
      plan.setDeleteEndTime(req.getEndTime());
      List<PartialPath> paths = new ArrayList<>();
      for (String path : req.getPaths()) {
        paths.add(new PartialPath(path));
      }
      plan.addPaths(paths);

      TSStatus status = checkAuthority(plan, req.getSessionId());
      return status != null ? new TSStatus(status) : new TSStatus(executeNonQueryPlan(plan));
    } catch (Exception e) {
      return onNPEOrUnexpectedException(e, "deleting data", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus insertTablet(TSInsertTabletReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      InsertTabletPlan insertTabletPlan =
          new InsertTabletPlan(new PartialPath(req.deviceId), req.measurements);
      insertTabletPlan.setTimes(QueryDataSetUtils.readTimesFromBuffer(req.timestamps, req.size));
      insertTabletPlan.setColumns(
          QueryDataSetUtils.readValuesFromBuffer(
              req.values, req.types, req.measurements.size(), req.size));
      insertTabletPlan.setRowCount(req.size);
      insertTabletPlan.setDataTypes(req.types);

      TSStatus status = checkAuthority(insertTabletPlan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(insertTabletPlan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "inserting tablet", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  @Override
  public TSStatus insertTablets(TSInsertTabletsReq req) {
    long t1 = System.currentTimeMillis();
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      return insertTabletsInternal(req);
    } catch (NullPointerException e) {
      LOGGER.error("{}: error occurs when insertTablets", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "inserting tablets", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    } finally {
      Measurement.INSTANCE.addOperationLatency(Operation.EXECUTE_RPC_BATCH_INSERT, t1);
    }
  }

  private InsertTabletPlan constructInsertTabletPlan(TSInsertTabletsReq req, int i)
      throws IllegalPathException {
    InsertTabletPlan insertTabletPlan =
        new InsertTabletPlan(new PartialPath(req.deviceIds.get(i)), req.measurementsList.get(i));
    insertTabletPlan.setTimes(
        QueryDataSetUtils.readTimesFromBuffer(req.timestampsList.get(i), req.sizeList.get(i)));
    insertTabletPlan.setColumns(
        QueryDataSetUtils.readValuesFromBuffer(
            req.valuesList.get(i),
            req.typesList.get(i),
            req.measurementsList.get(i).size(),
            req.sizeList.get(i)));
    insertTabletPlan.setRowCount(req.sizeList.get(i));
    insertTabletPlan.setDataTypes(req.typesList.get(i));
    return insertTabletPlan;
  }

  /** construct one InsertMultiTabletPlan and process it */
  public TSStatus insertTabletsInternal(TSInsertTabletsReq req) throws IllegalPathException {
    List<InsertTabletPlan> insertTabletPlanList = new ArrayList<>();
    InsertMultiTabletPlan insertMultiTabletPlan = new InsertMultiTabletPlan();
    for (int i = 0; i < req.deviceIds.size(); i++) {
      InsertTabletPlan insertTabletPlan = constructInsertTabletPlan(req, i);
      TSStatus status = checkAuthority(insertTabletPlan, req.getSessionId());
      if (status != null) {
        // not authorized
        insertMultiTabletPlan.getResults().put(i, status);
      }
      insertTabletPlanList.add(insertTabletPlan);
    }

    insertMultiTabletPlan.setInsertTabletPlanList(insertTabletPlanList);
    return executeNonQueryPlan(insertMultiTabletPlan);
  }

  @Override
  public TSStatus setStorageGroup(long sessionId, String storageGroup) {
    try {
      if (!checkLogin(sessionId)) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      SetStorageGroupPlan plan = new SetStorageGroupPlan(new PartialPath(storageGroup));

      TSStatus status = checkAuthority(plan, sessionId);
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "setting storage group", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus deleteStorageGroups(long sessionId, List<String> storageGroups) {
    try {
      if (!checkLogin(sessionId)) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      List<PartialPath> storageGroupList = new ArrayList<>();
      for (String storageGroup : storageGroups) {
        storageGroupList.add(new PartialPath(storageGroup));
      }
      DeleteStorageGroupPlan plan = new DeleteStorageGroupPlan(storageGroupList);

      TSStatus status = checkAuthority(plan, sessionId);
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "deleting storage group", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus createTimeseries(TSCreateTimeseriesReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug("Session-{} create timeseries {}", currSessionId.get(), req.getPath());
      }

      CreateTimeSeriesPlan plan =
          new CreateTimeSeriesPlan(
              new PartialPath(req.path),
              TSDataType.values()[req.dataType],
              TSEncoding.values()[req.encoding],
              CompressionType.values()[req.compressor],
              req.props,
              req.tags,
              req.attributes,
              req.measurementAlias);

      TSStatus status = checkAuthority(plan, req.getSessionId());
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "creating timeseries", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSStatus createMultiTimeseries(TSCreateMultiTimeseriesReq req) {
    try {
      if (!checkLogin(req.getSessionId())) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      if (AUDIT_LOGGER.isDebugEnabled()) {
        AUDIT_LOGGER.debug(
            "Session-{} create {} timeseries, the first is {}",
            currSessionId.get(),
            req.getPaths().size(),
            req.getPaths().get(0));
      }

      CreateMultiTimeSeriesPlan multiPlan = new CreateMultiTimeSeriesPlan();
      List<PartialPath> paths = new ArrayList<>(req.paths.size());
      List<TSDataType> dataTypes = new ArrayList<>(req.paths.size());
      List<TSEncoding> encodings = new ArrayList<>(req.paths.size());
      List<CompressionType> compressors = new ArrayList<>(req.paths.size());
      List<String> alias = null;
      if (req.measurementAliasList != null) {
        alias = new ArrayList<>(req.paths.size());
      }
      List<Map<String, String>> props = null;
      if (req.propsList != null) {
        props = new ArrayList<>(req.paths.size());
      }
      List<Map<String, String>> tags = null;
      if (req.tagsList != null) {
        tags = new ArrayList<>(req.paths.size());
      }
      List<Map<String, String>> attributes = null;
      if (req.attributesList != null) {
        attributes = new ArrayList<>(req.paths.size());
      }

      // for authority check
      CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan();
      for (int i = 0; i < req.paths.size(); i++) {
        plan.setPath(new PartialPath(req.paths.get(i)));

        TSStatus status = checkAuthority(plan, req.getSessionId());
        if (status != null) {
          // not authorized
          multiPlan.getResults().put(i, status);
        }

        paths.add(new PartialPath(req.paths.get(i)));
        dataTypes.add(TSDataType.values()[req.dataTypes.get(i)]);
        encodings.add(TSEncoding.values()[req.encodings.get(i)]);
        compressors.add(CompressionType.values()[req.compressors.get(i)]);
        if (alias != null) {
          alias.add(req.measurementAliasList.get(i));
        }
        if (props != null) {
          props.add(req.propsList.get(i));
        }
        if (tags != null) {
          tags.add(req.tagsList.get(i));
        }
        if (attributes != null) {
          attributes.add(req.attributesList.get(i));
        }
      }

      multiPlan.setPaths(paths);
      multiPlan.setDataTypes(dataTypes);
      multiPlan.setEncodings(encodings);
      multiPlan.setCompressors(compressors);
      multiPlan.setAlias(alias);
      multiPlan.setProps(props);
      multiPlan.setTags(tags);
      multiPlan.setAttributes(attributes);
      multiPlan.setIndexes(new ArrayList<>());

      return executeNonQueryPlan(multiPlan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "creating multi timeseries", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public TSStatus deleteTimeseries(long sessionId, List<String> paths) {
    try {
      if (!checkLogin(sessionId)) {
        return RpcUtils.getStatus(TSStatusCode.NOT_LOGIN_ERROR);
      }

      List<PartialPath> pathList = new ArrayList<>();
      for (String path : paths) {
        pathList.add(new PartialPath(path));
      }
      DeleteTimeSeriesPlan plan = new DeleteTimeSeriesPlan(pathList);

      TSStatus status = checkAuthority(plan, sessionId);
      return status != null ? status : executeNonQueryPlan(plan);
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "deleting timeseries", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
  }

  @Override
  public long requestStatementId(long sessionId) {
    long statementId = statementIdGenerator.incrementAndGet();
    sessionId2StatementId
        .computeIfAbsent(sessionId, s -> new CopyOnWriteArraySet<>())
        .add(statementId);
    return statementId;
  }

  private TSStatus checkAuthority(PhysicalPlan plan, long sessionId) {
    List<PartialPath> paths = plan.getPaths();
    plan.setLoginUserName(sessionIdUsernameMap.get(sessionId));
    try {
      if (!checkAuthorization(paths, plan, sessionIdUsernameMap.get(sessionId))) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION_ERROR,
            "No permissions for this operation " + plan.getOperatorType());
      }
    } catch (AuthException e) {
      LOGGER.warn("meet error while checking authorization.", e);
      return RpcUtils.getStatus(TSStatusCode.UNINITIALIZED_AUTH_ERROR, e.getMessage());
    } catch (Exception e) {
      return onNPEOrUnexpectedException(
          e, "checking authority", TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return null;
  }

  protected TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    boolean isSuccessful;
    try {
      plan.checkIntegrity();
      isSuccessful = executeNonQuery(plan);
    } catch (Exception e) {
      return onNonQueryException(e, "executing non query plan");
    }

    return isSuccessful
        ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
        : RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }

  private long generateQueryId(boolean isDataQuery, int fetchSize, int deduplicatedPathNum) {
    return QueryResourceManager.getInstance()
        .assignQueryId(isDataQuery, fetchSize, deduplicatedPathNum);
  }

  protected List<TSDataType> getSeriesTypesByPaths(
      List<PartialPath> paths, List<String> aggregations) throws MetadataException {
    return SchemaUtils.getSeriesTypesByPaths(paths, aggregations);
  }

  protected TSDataType getSeriesTypeByPath(PartialPath path) throws MetadataException {
    return SchemaUtils.getSeriesTypeByPath(path);
  }

  private TSStatus onQueryException(Exception e, String operation) {
    TSStatus status = tryCatchQueryException(e);
    return status != null
        ? status
        : onNPEOrUnexpectedException(e, operation, TSStatusCode.INTERNAL_SERVER_ERROR);
  }

  private TSStatus tryCatchQueryException(Exception e) {
    if (e instanceof QueryTimeoutRuntimeException) {
      DETAILED_FAILURE_QUERY_TRACE_LOGGER.warn(e.getMessage(), e);
      return RpcUtils.getStatus(TSStatusCode.TIME_OUT, getRootCause(e));
    } else if (e instanceof ParseCancellationException) {
      DETAILED_FAILURE_QUERY_TRACE_LOGGER.warn(INFO_PARSING_SQL_ERROR, e);
      return RpcUtils.getStatus(
          TSStatusCode.SQL_PARSE_ERROR, INFO_PARSING_SQL_ERROR + getRootCause(e));
    } else if (e instanceof SQLParserException) {
      DETAILED_FAILURE_QUERY_TRACE_LOGGER.warn(INFO_CHECK_METADATA_ERROR, e);
      return RpcUtils.getStatus(
          TSStatusCode.METADATA_ERROR, INFO_CHECK_METADATA_ERROR + getRootCause(e));
    } else if (e instanceof QueryProcessException) {
      DETAILED_FAILURE_QUERY_TRACE_LOGGER.warn(INFO_QUERY_PROCESS_ERROR, e);
      return RpcUtils.getStatus(
          TSStatusCode.QUERY_PROCESS_ERROR, INFO_QUERY_PROCESS_ERROR + getRootCause(e));
    } else if (e instanceof QueryInBatchStatementException) {
      DETAILED_FAILURE_QUERY_TRACE_LOGGER.warn(INFO_NOT_ALLOWED_IN_BATCH_ERROR, e);
      return RpcUtils.getStatus(
          TSStatusCode.QUERY_NOT_ALLOWED, INFO_NOT_ALLOWED_IN_BATCH_ERROR + getRootCause(e));
    } else if (e instanceof IoTDBException) {
      DETAILED_FAILURE_QUERY_TRACE_LOGGER.warn(INFO_QUERY_PROCESS_ERROR, e);
      return RpcUtils.getStatus(((IoTDBException) e).getErrorCode(), getRootCause(e));
    }
    return null;
  }

  private TSStatus onNonQueryException(Exception e, String operation) {
    TSStatus status = tryCatchNonQueryException(e);
    return status != null
        ? status
        : onNPEOrUnexpectedException(e, operation, TSStatusCode.INTERNAL_SERVER_ERROR);
  }

  private TSStatus tryCatchNonQueryException(Exception e) {
    String message = "Exception occurred while processing non-query. ";
    if (e instanceof BatchProcessException) {
      LOGGER.warn(message, e);
      return RpcUtils.getStatus(Arrays.asList(((BatchProcessException) e).getFailingStatus()));
    } else if (e instanceof IoTDBException) {
      if (((IoTDBException) e).isUserException()) {
        LOGGER.warn(message + e.getMessage());
      } else {
        LOGGER.warn(message, e);
      }
      return RpcUtils.getStatus(((IoTDBException) e).getErrorCode(), getRootCause(e));
    }
    return null;
  }

  private TSStatus onNPEOrUnexpectedException(
      Exception e, String operation, TSStatusCode statusCode) {
    String message =
        String.format("[%s] Exception occurred while %s. ", statusCode.name(), operation);
    if (e instanceof NullPointerException) {
      LOGGER.error(message, e);
    } else if (e instanceof UnSupportedDataTypeException) {
      LOGGER.warn(e.getMessage());
    } else {
      LOGGER.warn(message, e);
    }
    return RpcUtils.getStatus(statusCode, message + e.getMessage());
  }

  private String getRootCause(Throwable e) {
    while (e.getCause() != null) {
      e = e.getCause();
    }
    return e.getMessage();
  }
}
