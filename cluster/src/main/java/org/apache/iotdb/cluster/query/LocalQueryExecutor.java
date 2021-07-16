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

import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.ReaderNotFoundException;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.metadata.MetaPuller;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.query.filter.SlotTsFileFilter;
import org.apache.iotdb.cluster.query.manage.ClusterQueryManager;
import org.apache.iotdb.cluster.query.reader.ClusterReaderFactory;
import org.apache.iotdb.cluster.query.reader.mult.IMultBatchReader;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.LastQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.MultSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PreviousFillRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.utils.ClusterQueryUtils;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.dataset.groupby.LocalGroupByExecutor;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.executor.LastQueryExecutor;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalQueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(LocalQueryExecutor.class);
  public static final String DEBUG_SHOW_QUERY_ID = "{}: local queryId for {}#{} is {}";
  private DataGroupMember dataGroupMember;
  private ClusterReaderFactory readerFactory;
  private String name;
  private ClusterQueryManager queryManager;

  public LocalQueryExecutor(DataGroupMember dataGroupMember) {
    this.dataGroupMember = dataGroupMember;
    this.readerFactory = new ClusterReaderFactory(dataGroupMember.getMetaGroupMember());
    this.name = dataGroupMember.getName();
    this.queryManager = dataGroupMember.getQueryManager();
  }

  private CMManager getCMManager() {
    return ((CMManager) IoTDB.metaManager);
  }

  /** Return the data of the reader whose id is "readerId", using timestamps in "timeBuffer". */
  public ByteBuffer fetchSingleSeriesByTimestamps(long readerId, long[] timestamps, int length)
      throws ReaderNotFoundException, IOException {
    IReaderByTimestamp reader = dataGroupMember.getQueryManager().getReaderByTimestamp(readerId);
    if (reader == null) {
      throw new ReaderNotFoundException(readerId);
    }
    Object[] values = reader.getValuesInTimestamps(timestamps, length);
    if (values != null) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

      SerializeUtils.serializeObjects(values, dataOutputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    } else {
      return ByteBuffer.allocate(0);
    }
  }

  /**
   * Fetch a batch from the reader whose id is "readerId".
   *
   * @param readerId
   */
  public ByteBuffer fetchSingleSeries(long readerId) throws ReaderNotFoundException, IOException {
    IBatchReader reader = dataGroupMember.getQueryManager().getReader(readerId);
    if (reader == null) {
      throw new ReaderNotFoundException(readerId);
    }

    if (reader.hasNextBatch()) {
      BatchData batchData = reader.nextBatch();

      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

      SerializeUtils.serializeBatchData(batchData, dataOutputStream);
      logger.debug(
          "{}: Send results of reader {}, size:{}",
          dataGroupMember.getName(),
          readerId,
          batchData.length());
      return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    } else {
      return ByteBuffer.allocate(0);
    }
  }

  /**
   * Fetch a batch from the reader whose id is "readerId".
   *
   * @param readerId reader id
   * @param paths mult series path
   */
  public Map<String, ByteBuffer> fetchMultSeries(long readerId, List<String> paths)
      throws ReaderNotFoundException, IOException {
    IMultBatchReader reader =
        (IMultBatchReader) dataGroupMember.getQueryManager().getReader(readerId);
    if (reader == null) {
      throw new ReaderNotFoundException(readerId);
    }

    Map<String, ByteBuffer> pathByteBuffers = Maps.newHashMap();

    for (String path : paths) {
      ByteBuffer byteBuffer;
      if (reader.hasNextBatch(path)) {
        BatchData batchData = reader.nextBatch(path);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

        SerializeUtils.serializeBatchData(batchData, dataOutputStream);
        logger.debug(
            "{}: Send results of reader {}, size:{}",
            dataGroupMember.getName(),
            readerId,
            batchData.length());
        byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
      } else {
        byteBuffer = ByteBuffer.allocate(0);
      }
      pathByteBuffers.put(path, byteBuffer);
    }
    return pathByteBuffers;
  }

  /**
   * Create an IBatchReader of a path, register it in the query manager to get a reader id for it
   * and send the id back to the requester. If the reader does not have any data, an id of -1 will
   * be returned.
   *
   * @param request
   */
  public long querySingleSeries(SingleSeriesQueryRequest request)
      throws CheckConsistencyException, QueryProcessException, StorageEngineException, IOException {
    logger.debug(
        "{}: {} is querying {}, queryId: {}",
        name,
        request.getRequester(),
        request.getPath(),
        request.getQueryId());
    dataGroupMember.syncLeaderWithConsistencyCheck(false);

    PartialPath path = null;
    try {
      path = new PartialPath(request.getPath());
    } catch (IllegalPathException e) {
      // ignore
    }
    TSDataType dataType = TSDataType.values()[request.getDataTypeOrdinal()];
    Filter timeFilter = null;
    Filter valueFilter = null;
    if (request.isSetTimeFilterBytes()) {
      timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
    }
    if (request.isSetValueFilterBytes()) {
      valueFilter = FilterFactory.deserialize(request.valueFilterBytes);
    }
    Set<String> deviceMeasurements = request.getDeviceMeasurements();

    // the same query from a requester correspond to a context here
    RemoteQueryContext queryContext =
        queryManager.getQueryContext(request.getRequester(), request.getQueryId());
    logger.debug(
        DEBUG_SHOW_QUERY_ID,
        name,
        request.getQueryId(),
        request.getPath(),
        queryContext.getQueryId());
    IBatchReader batchReader =
        readerFactory.getSeriesBatchReader(
            path,
            deviceMeasurements,
            dataType,
            timeFilter,
            valueFilter,
            queryContext,
            dataGroupMember,
            request.ascending,
            request.requiredSlots);

    // if the reader contains no data, send a special id of -1 to prevent the requester from
    // meaninglessly fetching data
    if (batchReader != null && batchReader.hasNextBatch()) {
      long readerId = queryManager.registerReader(batchReader);
      queryContext.registerLocalReader(readerId);
      logger.debug(
          "{}: Build a reader of {} for {}#{}, readerId: {}",
          name,
          path,
          request.getRequester(),
          request.getQueryId(),
          readerId);
      return readerId;
    } else {
      logger.debug(
          "{}: There is no data of {} for {}#{}",
          name,
          path,
          request.getRequester(),
          request.getQueryId());

      if (batchReader != null) {
        batchReader.close();
      }
      return -1;
    }
  }

  /**
   * Create an IBatchReader of a path, register it in the query manager to get a reader id for it
   * and send the id back to the requester. If the reader does not have any data, an id of -1 will
   * be returned.
   *
   * @param request
   */
  public long queryMultSeries(MultSeriesQueryRequest request)
      throws CheckConsistencyException, QueryProcessException, StorageEngineException, IOException {
    logger.debug(
        "{}: {} is querying {}, queryId: {}",
        name,
        request.getRequester(),
        request.getPath(),
        request.getQueryId());
    dataGroupMember.syncLeaderWithConsistencyCheck(false);

    List<PartialPath> paths = Lists.newArrayList();
    request
        .getPath()
        .forEach(
            fullPath -> {
              try {
                if (fullPath.contains("$#$")) {
                  String[] array = fullPath.split(":");
                  List<PartialPath> subSensorsPathList = new ArrayList<>();
                  for (int i = 1; i < array.length; i++) {
                    subSensorsPathList.add(new PartialPath(array[i]));
                  }
                  paths.add(new VectorPartialPath(array[0], subSensorsPathList));
                } else {
                  paths.add(new PartialPath(fullPath));
                }
              } catch (IllegalPathException e) {
                logger.warn("Failed to create partial path, fullPath is {}.", fullPath, e);
              }
            });

    List<TSDataType> dataTypes = Lists.newArrayList();
    request.getDataTypeOrdinal().forEach(dataType -> dataTypes.add(TSDataType.values()[dataType]));

    Filter timeFilter = null;
    Filter valueFilter = null;
    if (request.isSetTimeFilterBytes()) {
      timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
    }
    if (request.isSetValueFilterBytes()) {
      valueFilter = FilterFactory.deserialize(request.valueFilterBytes);
    }
    Map<String, Set<String>> deviceMeasurements = request.getDeviceMeasurements();

    // the same query from a requester correspond to a context here
    RemoteQueryContext queryContext =
        queryManager.getQueryContext(request.getRequester(), request.getQueryId());
    logger.debug(
        DEBUG_SHOW_QUERY_ID,
        name,
        request.getQueryId(),
        request.getPath(),
        queryContext.getQueryId());
    IBatchReader batchReader =
        readerFactory.getMultSeriesBatchReader(
            paths,
            deviceMeasurements,
            dataTypes,
            timeFilter,
            valueFilter,
            queryContext,
            dataGroupMember,
            request.ascending);

    // if the reader contains no data, send a special id of -1 to prevent the requester from
    // meaninglessly fetching data
    if (batchReader != null && batchReader.hasNextBatch()) {
      long readerId = queryManager.registerReader(batchReader);
      queryContext.registerLocalReader(readerId);
      logger.debug(
          "{}: Build a reader of {} for {}#{}, readerId: {}",
          name,
          paths,
          request.getRequester(),
          request.getQueryId(),
          readerId);
      return readerId;
    } else {
      logger.debug(
          "{}: There is no data of {} for {}#{}",
          name,
          paths,
          request.getRequester(),
          request.getQueryId());

      if (batchReader != null) {
        batchReader.close();
      }
      return -1;
    }
  }

  /**
   * Send the timeseries schemas of some prefix paths to the requester. The schemas will be sent in
   * the form of a list of MeasurementSchema, but notice the measurements in them are the full
   * paths.
   *
   * @param request
   */
  public PullSchemaResp queryTimeSeriesSchema(PullSchemaRequest request)
      throws CheckConsistencyException, MetadataException {
    // try to synchronize with the leader first in case that some schema logs are accepted but
    // not committed yet
    dataGroupMember.syncLeaderWithConsistencyCheck(false);

    // collect local timeseries schemas and send to the requester
    // the measurements in them are the full paths.
    List<String> prefixPaths = request.getPrefixPaths();
    List<TimeseriesSchema> timeseriesSchemas = new ArrayList<>();
    collectTimeseriesSchema(prefixPaths, timeseriesSchemas);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Collected {} schemas for {} and other {} paths",
          name,
          timeseriesSchemas.size(),
          prefixPaths.get(0),
          prefixPaths.size() - 1);
    }

    PullSchemaResp resp = new PullSchemaResp();
    // serialize the schemas
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.writeInt(timeseriesSchemas.size());
      for (TimeseriesSchema timeseriesSchema : timeseriesSchemas) {
        timeseriesSchema.serializeTo(dataOutputStream);
      }
    } catch (IOException ignored) {
      // unreachable for we are using a ByteArrayOutputStream
    }
    resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
    return resp;
  }

  /**
   * Send the timeseries schemas of some prefix paths to the requester. The schemas will be sent in
   * the form of a list of MeasurementSchema, but notice the measurements in them are the full
   * paths.
   *
   * @param request
   */
  public PullSchemaResp queryMeasurementSchema(PullSchemaRequest request) // pullMeasurementSchemas
      throws CheckConsistencyException, MetadataException {
    // try to synchronize with the leader first in case that some schema logs are accepted but
    // not committed yet
    dataGroupMember.syncLeaderWithConsistencyCheck(false);

    // collect local timeseries schemas and send to the requester
    // the measurements in them are the full paths.
    List<String> prefixPaths = request.getPrefixPaths();
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

    collectSeries(prefixPaths, measurementSchemas);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Collected {} schemas for {} and other {} paths",
          name,
          measurementSchemas.size(),
          prefixPaths.get(0),
          prefixPaths.size() - 1);
    }

    PullSchemaResp resp = new PullSchemaResp();
    // serialize the schemas
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    try {
      dataOutputStream.writeInt(measurementSchemas.size());
      for (IMeasurementSchema timeseriesSchema : measurementSchemas) {
        timeseriesSchema.partialSerializeTo(dataOutputStream);
      }
    } catch (IOException ignored) {
      // unreachable for we are using a ByteArrayOutputStream
    }
    resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
    return resp;
  }

  private void collectSeries(List<String> prefixPaths, List<IMeasurementSchema> measurementSchemas)
      throws MetadataException {
    // Due to add/remove node, some slots may in the state of PULLING, which will not contains the
    // corresponding schemas.
    // In this case, we need to pull series from previous holder.
    Map<PartitionGroup, List<PartialPath>> prePartitionGroupPathMap = new HashMap<>();

    RaftNode header = dataGroupMember.getHeader();
    Map<Integer, PartitionGroup> slotPreviousHolderMap =
        ((SlotPartitionTable) dataGroupMember.getMetaGroupMember().getPartitionTable())
            .getPreviousNodeMap()
            .get(header);

    for (String prefixPath : prefixPaths) {
      int slot =
          ClusterUtils.getSlotByPathTimeWithSync(
              new PartialPath(prefixPath), dataGroupMember.getMetaGroupMember());
      if (dataGroupMember.getSlotManager().checkSlotInMetaMigrationStatus(slot)
          && slotPreviousHolderMap.containsKey(slot)) {
        prePartitionGroupPathMap
            .computeIfAbsent(slotPreviousHolderMap.get(slot), s -> new ArrayList<>())
            .add(new PartialPath(prefixPath));
      } else {
        getCMManager().collectSeries(new PartialPath(prefixPath), measurementSchemas);
      }
    }

    if (prePartitionGroupPathMap.isEmpty()) {
      return;
    }
    for (Map.Entry<PartitionGroup, List<PartialPath>> partitionGroupListEntry :
        prePartitionGroupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupListEntry.getKey();
      List<PartialPath> paths = partitionGroupListEntry.getValue();
      MetaPuller.getInstance().pullMeasurementSchemas(partitionGroup, paths, measurementSchemas);
    }
  }

  private void collectTimeseriesSchema(
      List<String> prefixPaths, List<TimeseriesSchema> timeseriesSchemas) throws MetadataException {
    // Due to add/remove node, some slots may in the state of PULLING, which will not contains the
    // corresponding schemas.
    // In this case, we need to pull series from previous holder.
    Map<PartitionGroup, List<String>> prePartitionGroupPathMap = new HashMap<>();

    RaftNode header = dataGroupMember.getHeader();
    Map<Integer, PartitionGroup> slotPreviousHolderMap =
        ((SlotPartitionTable) dataGroupMember.getMetaGroupMember().getPartitionTable())
            .getPreviousNodeMap()
            .get(header);

    for (String prefixPath : prefixPaths) {
      int slot =
          ClusterUtils.getSlotByPathTimeWithSync(
              new PartialPath(prefixPath), dataGroupMember.getMetaGroupMember());
      if (dataGroupMember.getSlotManager().checkSlotInMetaMigrationStatus(slot)
          && slotPreviousHolderMap.containsKey(slot)) {
        prePartitionGroupPathMap
            .computeIfAbsent(slotPreviousHolderMap.get(slot), s -> new ArrayList<>())
            .add(prefixPath);
      } else {
        getCMManager().collectTimeseriesSchema(prefixPath, timeseriesSchemas);
      }
    }

    if (prePartitionGroupPathMap.isEmpty()) {
      return;
    }
    for (Map.Entry<PartitionGroup, List<String>> partitionGroupListEntry :
        prePartitionGroupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupListEntry.getKey();
      List<String> paths = partitionGroupListEntry.getValue();
      MetaPuller.getInstance().pullTimeSeriesSchemas(partitionGroup, paths, timeseriesSchemas);
    }
  }

  /**
   * Create an IReaderByTime of a path, register it in the query manager to get a reader id for it
   * and send the id back to the requester. If the reader does not have any data, an id of -1 will
   * be returned.
   *
   * @param request
   */
  public long querySingleSeriesByTimestamp(SingleSeriesQueryRequest request)
      throws CheckConsistencyException, QueryProcessException, StorageEngineException {
    logger.debug(
        "{}: {} is querying {} by timestamp, queryId: {}",
        name,
        request.getRequester(),
        request.getPath(),
        request.getQueryId());
    dataGroupMember.syncLeaderWithConsistencyCheck(false);

    PartialPath path = null;
    try {
      path = new PartialPath(request.getPath());
    } catch (IllegalPathException e) {
      // ignore
    }
    TSDataType dataType = TSDataType.values()[request.dataTypeOrdinal];
    Set<String> deviceMeasurements = request.getDeviceMeasurements();

    RemoteQueryContext queryContext =
        queryManager.getQueryContext(request.getRequester(), request.getQueryId());
    logger.debug(
        DEBUG_SHOW_QUERY_ID,
        name,
        request.getQueryId(),
        request.getPath(),
        queryContext.getQueryId());
    IReaderByTimestamp readerByTimestamp =
        readerFactory.getReaderByTimestamp(
            path,
            deviceMeasurements,
            dataType,
            queryContext,
            dataGroupMember,
            request.ascending,
            request.requiredSlots);
    if (readerByTimestamp != null) {
      long readerId = queryManager.registerReaderByTime(readerByTimestamp);
      queryContext.registerLocalReader(readerId);

      logger.debug(
          "{}: Build a readerByTimestamp of {} for {}, readerId: {}",
          name,
          path,
          request.getRequester(),
          readerId);
      return readerId;
    } else {
      logger.debug(
          "{}: There is no data {} for {}#{}",
          name,
          path,
          request.getRequester(),
          request.getQueryId());
      return -1;
    }
  }

  public ByteBuffer getAllMeasurementSchema(ByteBuffer planBuffer)
      throws CheckConsistencyException, IOException, MetadataException {
    dataGroupMember.syncLeaderWithConsistencyCheck(false);

    ShowTimeSeriesPlan plan = (ShowTimeSeriesPlan) PhysicalPlan.Factory.create(planBuffer);
    List<ShowTimeSeriesResult> allTimeseriesSchema;
    allTimeseriesSchema = getCMManager().showLocalTimeseries(plan, new QueryContext());

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
      dataOutputStream.writeInt(allTimeseriesSchema.size());
      for (ShowTimeSeriesResult result : allTimeseriesSchema) {
        result.serialize(outputStream);
      }
    }
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  public ByteBuffer getDevices(ByteBuffer planBuffer)
      throws CheckConsistencyException, IOException, MetadataException {
    dataGroupMember.syncLeaderWithConsistencyCheck(false);
    ShowDevicesPlan plan = (ShowDevicesPlan) PhysicalPlan.Factory.create(planBuffer);
    List<ShowDevicesResult> allDevicesResult = getCMManager().getLocalDevices(plan);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
      dataOutputStream.writeInt(allDevicesResult.size());
      for (ShowDevicesResult result : allDevicesResult) {
        result.serialize(outputStream);
      }
    }
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  /**
   * Execute aggregations over the given path and return the results to the requester.
   *
   * @param request
   */
  public List<ByteBuffer> getAggrResult(GetAggrResultRequest request)
      throws StorageEngineException, QueryProcessException, IOException {
    logger.debug(
        "{}: {} is querying {} by aggregation, queryId: {}",
        name,
        request.getRequestor(),
        request.getPath(),
        request.getQueryId());

    List<String> aggregations = request.getAggregations();
    TSDataType dataType = TSDataType.values()[request.getDataTypeOrdinal()];
    PartialPath path;
    try {
      path = new PartialPath(request.getPath());
    } catch (IllegalPathException e) {
      logger.error(
          "{}: aggregation has error path: {}, queryId: {}",
          name,
          request.getPath(),
          request.getQueryId());
      throw new QueryProcessException(e);
    }
    Filter timeFilter = null;
    if (request.isSetTimeFilterBytes()) {
      timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
    }
    RemoteQueryContext queryContext =
        queryManager.getQueryContext(request.getRequestor(), request.queryId);
    Set<String> deviceMeasurements = request.getDeviceMeasurements();
    boolean ascending = request.ascending;

    // do the aggregations locally
    List<AggregateResult> results;
    results =
        getAggrResult(
            aggregations, deviceMeasurements, dataType, path, timeFilter, queryContext, ascending);
    logger.trace("{}: aggregation results {}, queryId: {}", name, results, request.getQueryId());

    // serialize and send the results
    List<ByteBuffer> resultBuffers = new ArrayList<>();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    for (AggregateResult result : results) {
      try {
        result.serializeTo(byteArrayOutputStream);
      } catch (IOException e) {
        // ignore since we are using a ByteArrayOutputStream
      }
      resultBuffers.add(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      byteArrayOutputStream.reset();
    }
    return resultBuffers;
  }

  /**
   * Execute "aggregation" over "path" with "timeFilter". This method currently requires strong
   * consistency. Only data managed by this group will be used for aggregation.
   *
   * @param aggregations aggregation names in SQLConstant
   * @param dataType
   * @param path
   * @param timeFilter nullable
   * @param context
   * @return
   * @throws IOException
   * @throws StorageEngineException
   * @throws QueryProcessException
   */
  public List<AggregateResult> getAggrResult(
      List<String> aggregations,
      Set<String> allSensors,
      TSDataType dataType,
      PartialPath path,
      Filter timeFilter,
      QueryContext context,
      boolean ascending)
      throws IOException, StorageEngineException, QueryProcessException {
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new QueryProcessException(e.getMessage());
    }

    ClusterQueryUtils.checkPathExistence(path);
    List<AggregateResult> results = new ArrayList<>();
    for (String aggregation : aggregations) {
      results.add(AggregateResultFactory.getAggrResultByName(aggregation, dataType));
    }
    List<Integer> nodeSlots =
        ((SlotPartitionTable) dataGroupMember.getMetaGroupMember().getPartitionTable())
            .getNodeSlots(dataGroupMember.getHeader());
    if (ascending) {
      AggregationExecutor.aggregateOneSeries(
          path,
          allSensors,
          context,
          timeFilter,
          dataType,
          results,
          null,
          new SlotTsFileFilter(nodeSlots));
    } else {
      AggregationExecutor.aggregateOneSeries(
          path,
          allSensors,
          context,
          timeFilter,
          dataType,
          null,
          results,
          new SlotTsFileFilter(nodeSlots));
    }
    return results;
  }

  /**
   * Check if the given measurements are registered or not
   *
   * @param timeseriesList
   */
  public List<String> getUnregisteredTimeseries(List<String> timeseriesList)
      throws CheckConsistencyException {
    dataGroupMember.syncLeaderWithConsistencyCheck(true);

    List<String> result = new ArrayList<>();
    for (String seriesPath : timeseriesList) {
      try {
        List<PartialPath> path = getCMManager().getAllTimeseriesPath(new PartialPath(seriesPath));
        if (path.size() != 1) {
          throw new MetadataException(
              String.format("Timeseries number of the name [%s] is not 1.", seriesPath));
        }
      } catch (MetadataException e) {
        result.add(seriesPath);
      }
    }
    return result;
  }

  /**
   * Create a local GroupByExecutor that will run aggregations of "aggregationTypes" over "path"
   * with "timeFilter". The method currently requires strong consistency.
   *
   * @param path
   * @param dataType
   * @param timeFilter nullable
   * @param aggregationTypes ordinals of AggregationType
   * @param context
   * @return
   * @throws StorageEngineException
   */
  public LocalGroupByExecutor getGroupByExecutor(
      PartialPath path,
      Set<String> deviceMeasurements,
      TSDataType dataType,
      Filter timeFilter,
      List<Integer> aggregationTypes,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    // pull the newest data
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }

    ClusterQueryUtils.checkPathExistence(path);
    List<Integer> nodeSlots =
        ((SlotPartitionTable) dataGroupMember.getMetaGroupMember().getPartitionTable())
            .getNodeSlots(dataGroupMember.getHeader());
    LocalGroupByExecutor executor =
        new LocalGroupByExecutor(
            path,
            deviceMeasurements,
            dataType,
            context,
            timeFilter,
            new SlotTsFileFilter(nodeSlots),
            ascending);
    for (Integer aggregationType : aggregationTypes) {
      executor.addAggregateResult(
          AggregateResultFactory.getAggrResultByType(
              AggregationType.values()[aggregationType], dataType, ascending));
    }
    return executor;
  }

  /**
   * Create a local GroupByExecutor that will run aggregations of "aggregationTypes" over "path"
   * with "timeFilter", register it in the query manager to generate the executor id, and send it
   * back to the requester.
   *
   * @param request
   */
  public long getGroupByExecutor(GroupByRequest request)
      throws QueryProcessException, StorageEngineException {
    PartialPath path;
    try {
      path = new PartialPath(request.getPath());
    } catch (IllegalPathException e) {
      throw new QueryProcessException(e);
    }
    List<Integer> aggregationTypeOrdinals = request.getAggregationTypeOrdinals();
    TSDataType dataType = TSDataType.values()[request.getDataTypeOrdinal()];
    Filter timeFilter = null;
    if (request.isSetTimeFilterBytes()) {
      timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
    }
    long queryId = request.getQueryId();
    logger.debug(
        "{}: {} is querying {} using group by, queryId: {}",
        name,
        request.getRequestor(),
        path,
        queryId);
    Set<String> deviceMeasurements = request.getDeviceMeasurements();
    boolean ascending = request.ascending;

    RemoteQueryContext queryContext = queryManager.getQueryContext(request.getRequestor(), queryId);
    LocalGroupByExecutor executor =
        getGroupByExecutor(
            path,
            deviceMeasurements,
            dataType,
            timeFilter,
            aggregationTypeOrdinals,
            queryContext,
            ascending);
    if (!executor.isEmpty()) {
      long executorId = queryManager.registerGroupByExecutor(executor);
      logger.debug(
          "{}: Build a GroupByExecutor of {} for {}, executorId: {}",
          name,
          path,
          request.getRequestor(),
          executor);
      queryContext.registerLocalGroupByExecutor(executorId);
      return executorId;
    } else {
      logger.debug(
          "{}: There is no data {} for {}#{}",
          name,
          path,
          request.getRequestor(),
          request.getQueryId());
      return -1;
    }
  }

  /**
   * Fetch the aggregation results between [startTime, endTime] of the executor whose id is
   * "executorId". This method currently requires strong consistency.
   *
   * @param executorId
   * @param startTime
   * @param endTime
   */
  public List<ByteBuffer> getGroupByResult(long executorId, long startTime, long endTime)
      throws ReaderNotFoundException, IOException, QueryProcessException {
    GroupByExecutor executor = queryManager.getGroupByExecutor(executorId);
    if (executor == null) {
      throw new ReaderNotFoundException(executorId);
    }
    List<AggregateResult> results = executor.calcResult(startTime, endTime);
    List<ByteBuffer> resultBuffers = new ArrayList<>();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    for (AggregateResult result : results) {
      result.serializeTo(byteArrayOutputStream);
      resultBuffers.add(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
      byteArrayOutputStream.reset();
    }
    logger.debug(
        "{}: Send results of group by executor {}, size:{}", name, executor, resultBuffers.size());
    return resultBuffers;
  }

  public ByteBuffer peekNextNotNullValue(long executorId, long startTime, long endTime)
      throws ReaderNotFoundException, IOException {
    GroupByExecutor executor = queryManager.getGroupByExecutor(executorId);
    if (executor == null) {
      throw new ReaderNotFoundException(executorId);
    }
    Pair<Long, Object> pair = executor.peekNextNotNullValue(startTime, endTime);
    ByteBuffer resultBuffer;
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeLong(pair.left);
      SerializeUtils.serializeObject(pair.right, dataOutputStream);
      resultBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    }
    logger.debug(
        "{}: Send results of group by executor {}, size:{}", name, executor, resultBuffer.limit());
    return resultBuffer;
  }

  public ByteBuffer previousFill(PreviousFillRequest request)
      throws QueryProcessException, StorageEngineException, IOException, IllegalPathException {
    PartialPath path = new PartialPath(request.getPath());
    TSDataType dataType = TSDataType.values()[request.getDataTypeOrdinal()];
    long queryId = request.getQueryId();
    long queryTime = request.getQueryTime();
    long beforeRange = request.getBeforeRange();
    Node requester = request.getRequester();
    Set<String> deviceMeasurements = request.getDeviceMeasurements();
    RemoteQueryContext queryContext = queryManager.getQueryContext(requester, queryId);

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    TimeValuePair timeValuePair =
        localPreviousFill(path, dataType, queryTime, beforeRange, deviceMeasurements, queryContext);
    SerializeUtils.serializeTVPair(timeValuePair, dataOutputStream);
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  /**
   * Perform a local previous fill and return the fill result.
   *
   * @param path
   * @param dataType
   * @param queryTime
   * @param beforeRange
   * @param deviceMeasurements
   * @param context
   * @return
   * @throws QueryProcessException
   * @throws StorageEngineException
   * @throws IOException
   */
  public TimeValuePair localPreviousFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      long beforeRange,
      Set<String> deviceMeasurements,
      QueryContext context)
      throws QueryProcessException, StorageEngineException, IOException {
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new QueryProcessException(e.getMessage());
    }

    PreviousFill previousFill = new PreviousFill(dataType, queryTime, beforeRange);
    previousFill.configureFill(path, dataType, queryTime, deviceMeasurements, context);
    return previousFill.getFillResult();
  }

  public int getPathCount(List<String> pathsToQuery, int level)
      throws CheckConsistencyException, MetadataException {
    dataGroupMember.syncLeaderWithConsistencyCheck(false);

    int count = 0;
    for (String s : pathsToQuery) {
      if (level == -1) {
        count += getCMManager().getAllTimeseriesCount(new PartialPath(s));
      } else {
        count += getCMManager().getNodesCountInGivenLevel(new PartialPath(s), level);
      }
    }
    return count;
  }

  @SuppressWarnings("java:S1135") // ignore todos
  public ByteBuffer last(LastQueryRequest request)
      throws CheckConsistencyException, QueryProcessException, IOException, StorageEngineException,
          IllegalPathException {
    dataGroupMember.syncLeaderWithConsistencyCheck(false);

    RemoteQueryContext queryContext =
        queryManager.getQueryContext(request.getRequestor(), request.getQueryId());
    List<PartialPath> partialPaths = new ArrayList<>();
    for (String path : request.getPaths()) {
      partialPaths.add(new PartialPath(path));
    }
    List<TSDataType> dataTypes = new ArrayList<>(request.dataTypeOrdinals.size());
    for (Integer dataTypeOrdinal : request.dataTypeOrdinals) {
      dataTypes.add(TSDataType.values()[dataTypeOrdinal]);
    }
    ClusterQueryUtils.checkPathExistence(partialPaths);
    IExpression expression = null;
    if (request.isSetFilterBytes()) {
      Filter filter = FilterFactory.deserialize(request.filterBytes);
      expression = new GlobalTimeExpression(filter);
    }

    List<Pair<Boolean, TimeValuePair>> timeValuePairs =
        LastQueryExecutor.calculateLastPairForSeriesLocally(
            partialPaths, dataTypes, queryContext, expression, request.getDeviceMeasurements());
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    for (Pair<Boolean, TimeValuePair> timeValuePair : timeValuePairs) {
      SerializeUtils.serializeTVPair(timeValuePair.right, dataOutputStream);
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }
}
