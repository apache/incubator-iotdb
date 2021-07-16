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

package org.apache.iotdb.cluster.query.reader;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.EmptyIntervalException;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.query.LocalQueryExecutor;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.filter.SlotTsFileFilter;
import org.apache.iotdb.cluster.query.groupby.RemoteGroupByExecutor;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.query.reader.mult.AbstractMultPointReader;
import org.apache.iotdb.cluster.query.reader.mult.MultBatchReader;
import org.apache.iotdb.cluster.query.reader.mult.MultDataSourceInfo;
import org.apache.iotdb.cluster.query.reader.mult.MultEmptyReader;
import org.apache.iotdb.cluster.query.reader.mult.MultSeriesRawDataPointReader;
import org.apache.iotdb.cluster.query.reader.mult.RemoteMultSeriesReader;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.MultSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.ClusterQueryUtils;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataPointReader;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.query.reader.series.SeriesReaderByTimestamp;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("java:S107")
public class ClusterReaderFactory {

  private static final Logger logger = LoggerFactory.getLogger(ClusterReaderFactory.class);
  private final MetaGroupMember metaGroupMember;

  public ClusterReaderFactory(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  public void syncMetaGroup() throws CheckConsistencyException {
    metaGroupMember.syncLeaderWithConsistencyCheck(false);
  }

  /**
   * Create an IReaderByTimestamp that can read the data of "path" by timestamp in the whole
   * cluster. This will query every group and merge the result from them.
   */
  public IReaderByTimestamp getReaderByTimestamp(
      PartialPath path,
      Set<String> deviceMeasurements,
      TSDataType dataType,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    // get all data groups
    List<PartitionGroup> partitionGroups;
    try {
      partitionGroups = metaGroupMember.routeFilter(null, path);
    } catch (EmptyIntervalException e) {
      logger.warn(e.getMessage());
      partitionGroups = Collections.emptyList();
    }
    logger.debug(
        "{}: Sending query of {} to {} groups",
        metaGroupMember.getName(),
        path,
        partitionGroups.size());
    List<IReaderByTimestamp> readers = new ArrayList<>(partitionGroups.size());
    for (PartitionGroup partitionGroup : partitionGroups) {
      // query each group to get a reader in that group
      IReaderByTimestamp readerByTimestamp =
          getSeriesReaderByTime(
              partitionGroup, path, deviceMeasurements, context, dataType, ascending);
      readers.add(readerByTimestamp);
    }
    return new MergedReaderByTime(readers);
  }

  /**
   * Create a IReaderByTimestamp that read data of "path" by timestamp in the given group. If the
   * local node is a member of that group, query locally. Otherwise create a remote reader pointing
   * to one node in that group.
   */
  private IReaderByTimestamp getSeriesReaderByTime(
      PartitionGroup partitionGroup,
      PartialPath path,
      Set<String> deviceMeasurements,
      QueryContext context,
      TSDataType dataType,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    if (partitionGroup.contains(metaGroupMember.getThisNode())) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember =
          metaGroupMember.getLocalDataMember(partitionGroup.getHeader());
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: creating a local reader for {}#{}",
            metaGroupMember.getName(),
            path.getFullPath(),
            context.getQueryId());
      }
      return getReaderByTimestamp(
          path, deviceMeasurements, dataType, context, dataGroupMember, ascending);
    } else {
      return getRemoteReaderByTimestamp(
          path, deviceMeasurements, dataType, partitionGroup, context, ascending);
    }
  }

  /**
   * Create a IReaderByTimestamp that read data of "path" by timestamp in the given group that does
   * not contain the local node. Send a request to one node in that group to build a reader and use
   * that reader's id to build a remote reader.
   */
  private IReaderByTimestamp getRemoteReaderByTimestamp(
      Path path,
      Set<String> deviceMeasurements,
      TSDataType dataType,
      PartitionGroup partitionGroup,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException {
    SingleSeriesQueryRequest request =
        constructSingleQueryRequest(
            null, null, dataType, path, deviceMeasurements, partitionGroup, context, ascending);

    // reorder the nodes by their communication delays
    List<Node> reorderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    DataSourceInfo dataSourceInfo =
        new DataSourceInfo(
            partitionGroup,
            dataType,
            request,
            (RemoteQueryContext) context,
            metaGroupMember,
            reorderedNodes);

    // try building a reader from one of the nodes
    boolean hasClient = dataSourceInfo.hasNextDataClient(true, Long.MIN_VALUE);
    if (hasClient) {
      return new RemoteSeriesReaderByTimestamp(dataSourceInfo);
    } else if (dataSourceInfo.isNoData()) {
      return new EmptyReader();
    }

    throw new StorageEngineException(
        new RequestTimeOutException("Query by timestamp: " + path + " in " + partitionGroup));
  }

  /**
   * Create a MultSeriesReader that can read the data of "path" with filters in the whole cluster.
   * The data groups that should be queried will be determined by the timeFilter, then for each
   * group a series reader will be created, and finally all such readers will be merged into one.
   *
   * @param paths all path
   * @param deviceMeasurements device to measurements
   * @param dataTypes data type
   * @param timeFilter time filter
   * @param valueFilter value filter
   * @param context query context
   * @param ascending asc or aesc
   * @return
   * @throws StorageEngineException
   * @throws EmptyIntervalException
   */
  public List<AbstractMultPointReader> getMultSeriesReader(
      List<PartialPath> paths,
      Map<String, Set<String>> deviceMeasurements,
      List<TSDataType> dataTypes,
      Filter timeFilter,
      Filter valueFilter,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException, EmptyIntervalException, QueryProcessException {

    Map<PartitionGroup, List<PartialPath>> partitionGroupListMap = Maps.newHashMap();
    for (PartialPath partialPath : paths) {
      List<PartitionGroup> partitionGroups = metaGroupMember.routeFilter(timeFilter, partialPath);
      partitionGroups.forEach(
          partitionGroup ->
              partitionGroupListMap
                  .computeIfAbsent(partitionGroup, n -> new ArrayList<>())
                  .add(partialPath));
    }

    List<AbstractMultPointReader> multPointReaders = Lists.newArrayList();

    // different path of the same partition group are constructed as a AbstractMultPointReader
    // if be local partition, constructed a MultBatchReader
    // if be a remote partition, constructed a RemoteMultSeriesReader
    for (Map.Entry<PartitionGroup, List<PartialPath>> entityPartitionGroup :
        partitionGroupListMap.entrySet()) {
      List<PartialPath> partialPaths = entityPartitionGroup.getValue();
      Map<String, Set<String>> partitionGroupDeviceMeasurements = Maps.newHashMap();
      List<TSDataType> partitionGroupTSDataType = Lists.newArrayList();
      partialPaths.forEach(
          partialPath -> {
            Set<String> measurements =
                deviceMeasurements.getOrDefault(partialPath.getDevice(), Collections.emptySet());
            partitionGroupDeviceMeasurements.put(partialPath.getFullPath(), measurements);
            partitionGroupTSDataType.add(dataTypes.get(paths.lastIndexOf(partialPath)));
          });

      AbstractMultPointReader abstractMultPointReader =
          getMultSeriesReader(
              entityPartitionGroup.getKey(),
              partialPaths,
              partitionGroupTSDataType,
              partitionGroupDeviceMeasurements,
              timeFilter,
              valueFilter,
              context,
              ascending);
      multPointReaders.add(abstractMultPointReader);
    }
    return multPointReaders;
  }

  /**
   * Query one node in "partitionGroup" for data of "path" with "timeFilter" and "valueFilter". If
   * "partitionGroup" contains the local node, a local reader will be returned. Otherwise a remote
   * reader will be returned.
   *
   * @param timeFilter nullable
   * @param valueFilter nullable
   */
  private AbstractMultPointReader getMultSeriesReader(
      PartitionGroup partitionGroup,
      List<PartialPath> partialPaths,
      List<TSDataType> dataTypes,
      Map<String, Set<String>> deviceMeasurements,
      Filter timeFilter,
      Filter valueFilter,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    if (partitionGroup.contains(metaGroupMember.getThisNode())) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember =
          metaGroupMember.getLocalDataMember(
              partitionGroup.getHeader(),
              String.format(
                  "Query: %s, time filter: %s, queryId: %d",
                  partialPaths, timeFilter, context.getQueryId()));
      Map<String, IPointReader> partialPathPointReaderMap = Maps.newHashMap();
      for (int i = 0; i < partialPaths.size(); i++) {
        PartialPath partialPath = partialPaths.get(i);
        IPointReader seriesPointReader =
            getSeriesPointReader(
                partialPath,
                deviceMeasurements.get(partialPath.getFullPath()),
                dataTypes.get(i),
                timeFilter,
                valueFilter,
                context,
                dataGroupMember,
                ascending);
        partialPathPointReaderMap.put(partialPath.getFullPath(), seriesPointReader);
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: creating a local reader for {}#{} of {}",
            metaGroupMember.getName(),
            partialPaths,
            context.getQueryId(),
            partitionGroup.getHeader());
      }
      return new MultSeriesRawDataPointReader(partialPathPointReaderMap);
    } else {
      return getRemoteMultSeriesPointReader(
          timeFilter,
          valueFilter,
          dataTypes,
          partialPaths,
          deviceMeasurements,
          partitionGroup,
          context,
          ascending);
    }
  }

  /**
   * Create a ManagedSeriesReader that can read the data of "path" with filters in the whole
   * cluster. The data groups that should be queried will be determined by the timeFilter, then for
   * each group a series reader will be created, and finally all such readers will be merged into
   * one.
   *
   * @param timeFilter nullable, when null, all data groups will be queried
   * @param valueFilter nullable
   */
  public ManagedSeriesReader getSeriesReader(
      PartialPath path,
      Set<String> deviceMeasurements,
      TSDataType dataType,
      Filter timeFilter,
      Filter valueFilter,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException, EmptyIntervalException {
    // find the groups that should be queried using the timeFilter
    List<PartitionGroup> partitionGroups = metaGroupMember.routeFilter(timeFilter, path);
    logger.debug(
        "{}: Sending data query of {} to {} groups",
        metaGroupMember.getName(),
        path,
        partitionGroups.size());
    ManagedMergeReader mergeReader = new ManagedMergeReader(dataType);
    try {
      // build a reader for each group and merge them
      for (PartitionGroup partitionGroup : partitionGroups) {
        IPointReader seriesReader =
            getSeriesReader(
                partitionGroup,
                path,
                deviceMeasurements,
                timeFilter,
                valueFilter,
                context,
                dataType,
                ascending);
        mergeReader.addReader(seriesReader, 0);
      }
    } catch (IOException | QueryProcessException e) {
      throw new StorageEngineException(e);
    }
    return mergeReader;
  }

  /**
   * Query one node in "partitionGroup" for data of "path" with "timeFilter" and "valueFilter". If
   * "partitionGroup" contains the local node, a local reader will be returned. Otherwise a remote
   * reader will be returned.
   *
   * @param timeFilter nullable
   * @param valueFilter nullable
   */
  private IPointReader getSeriesReader(
      PartitionGroup partitionGroup,
      PartialPath path,
      Set<String> deviceMeasurements,
      Filter timeFilter,
      Filter valueFilter,
      QueryContext context,
      TSDataType dataType,
      boolean ascending)
      throws IOException, StorageEngineException, QueryProcessException {
    if (partitionGroup.contains(metaGroupMember.getThisNode())) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember =
          metaGroupMember.getLocalDataMember(
              partitionGroup.getHeader(),
              String.format(
                  "Query: %s, time filter: %s, queryId: %d",
                  path, timeFilter, context.getQueryId()));
      IPointReader seriesPointReader =
          getSeriesPointReader(
              path,
              deviceMeasurements,
              dataType,
              timeFilter,
              valueFilter,
              context,
              dataGroupMember,
              ascending);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: creating a local reader for {}#{} of {}, empty: {}",
            metaGroupMember.getName(),
            path.getFullPath(),
            context.getQueryId(),
            partitionGroup.getHeader(),
            !seriesPointReader.hasNextTimeValuePair());
      }
      return seriesPointReader;
    } else {
      return getRemoteSeriesPointReader(
          timeFilter,
          valueFilter,
          dataType,
          path,
          deviceMeasurements,
          partitionGroup,
          context,
          ascending);
    }
  }

  /**
   * Create an IPointReader of "path" with “timeFilter” and "valueFilter". A synchronization with
   * the leader will be performed according to consistency level
   *
   * @param path series path
   * @param dataType data type
   * @param timeFilter nullable
   * @param valueFilter nullable
   * @param context query context
   * @return reader
   * @throws StorageEngineException encounter exception
   */
  public IPointReader getSeriesPointReader(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      Filter timeFilter,
      Filter valueFilter,
      QueryContext context,
      DataGroupMember dataGroupMember,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    // pull the newest data
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
    return new SeriesRawDataPointReader(
        getSeriesReader(
            path,
            allSensors,
            dataType,
            timeFilter,
            valueFilter,
            context,
            dataGroupMember.getHeader(),
            ascending));
  }

  /**
   * Create a SeriesReader of "path" with “timeFilter” and "valueFilter". The consistency is not
   * guaranteed here and only data slots managed by the member will be queried.
   *
   * @param path series path
   * @param dataType data type
   * @param timeFilter nullable
   * @param valueFilter nullable
   * @param context query context
   * @return reader for series
   * @throws StorageEngineException encounter exception
   */
  private SeriesReader getSeriesReader(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      Filter timeFilter,
      Filter valueFilter,
      QueryContext context,
      Node header,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    ClusterQueryUtils.checkPathExistence(path);
    List<Integer> nodeSlots =
        ((SlotPartitionTable) metaGroupMember.getPartitionTable()).getNodeSlots(header);
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);
    valueFilter = queryDataSource.updateFilterUsingTTL(valueFilter);
    return new SeriesReader(
        path,
        allSensors,
        dataType,
        context,
        queryDataSource,
        timeFilter,
        valueFilter,
        new SlotTsFileFilter(nodeSlots),
        ascending);
  }

  /**
   * Query a remote node in "partitionGroup" to get the reader of "path" with "timeFilter" and
   * "valueFilter". Firstly, a request will be sent to that node to construct a reader there, then
   * the id of the reader will be returned so that we can fetch data from that node using the reader
   * id.
   *
   * @param timeFilter nullable
   * @param valueFilter nullable
   */
  private AbstractMultPointReader getRemoteMultSeriesPointReader(
      Filter timeFilter,
      Filter valueFilter,
      List<TSDataType> dataType,
      List<PartialPath> paths,
      Map<String, Set<String>> deviceMeasurements,
      PartitionGroup partitionGroup,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException {
    MultSeriesQueryRequest request =
        constructMultQueryRequest(
            timeFilter,
            valueFilter,
            dataType,
            paths,
            deviceMeasurements,
            partitionGroup,
            context,
            ascending);

    // reorder the nodes such that the nodes that suit the query best (have lowest latenct or
    // highest throughput) will be put to the front
    List<Node> orderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);

    MultDataSourceInfo dataSourceInfo =
        new MultDataSourceInfo(
            partitionGroup,
            paths,
            dataType,
            request,
            (RemoteQueryContext) context,
            metaGroupMember,
            orderedNodes);

    boolean hasClient = dataSourceInfo.hasNextDataClient(Long.MIN_VALUE);
    if (hasClient) {
      return new RemoteMultSeriesReader(dataSourceInfo);
    } else if (dataSourceInfo.isNoData()) {
      // there is no satisfying data on the remote node
      Set<String> fullPaths = Sets.newHashSet();
      dataSourceInfo
          .getPartialPaths()
          .forEach(
              partialPath -> {
                fullPaths.add(partialPath.getFullPath());
              });
      return new MultEmptyReader(fullPaths);
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query multi-series: " + paths + " in " + partitionGroup));
  }

  /**
   * Query a remote node in "partitionGroup" to get the reader of "path" with "timeFilter" and
   * "valueFilter". Firstly, a request will be sent to that node to construct a reader there, then
   * the id of the reader will be returned so that we can fetch data from that node using the reader
   * id.
   *
   * @param timeFilter nullable
   * @param valueFilter nullable
   */
  private IPointReader getRemoteSeriesPointReader(
      Filter timeFilter,
      Filter valueFilter,
      TSDataType dataType,
      Path path,
      Set<String> deviceMeasurements,
      PartitionGroup partitionGroup,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException {
    SingleSeriesQueryRequest request =
        constructSingleQueryRequest(
            timeFilter,
            valueFilter,
            dataType,
            path,
            deviceMeasurements,
            partitionGroup,
            context,
            ascending);

    // reorder the nodes such that the nodes that suit the query best (have lowest latenct or
    // highest throughput) will be put to the front
    List<Node> orderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);

    DataSourceInfo dataSourceInfo =
        new DataSourceInfo(
            partitionGroup,
            dataType,
            request,
            (RemoteQueryContext) context,
            metaGroupMember,
            orderedNodes);

    boolean hasClient = dataSourceInfo.hasNextDataClient(false, Long.MIN_VALUE);
    if (hasClient) {
      return new RemoteSimpleSeriesReader(dataSourceInfo);
    } else if (dataSourceInfo.isNoData()) {
      // there is no satisfying data on the remote node
      return new EmptyReader();
    }

    throw new StorageEngineException(
        new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  private MultSeriesQueryRequest constructMultQueryRequest(
      Filter timeFilter,
      Filter valueFilter,
      List<TSDataType> dataTypes,
      List<PartialPath> paths,
      Map<String, Set<String>> deviceMeasurements,
      PartitionGroup partitionGroup,
      QueryContext context,
      boolean ascending) {
    MultSeriesQueryRequest request = new MultSeriesQueryRequest();
    if (timeFilter != null) {
      request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    }
    if (valueFilter != null) {
      request.setValueFilterBytes(SerializeUtils.serializeFilter(valueFilter));
    }

    List<String> fullPaths = Lists.newArrayList();
    paths.forEach(
        path -> {
          fullPaths.add(path.getFullPath());
        });

    List<Integer> dataTypeOrdinals = Lists.newArrayList();
    dataTypes.forEach(dataType -> dataTypeOrdinals.add(dataType.ordinal()));

    request.setPath(fullPaths);
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setRequester(metaGroupMember.getThisNode());
    request.setDataTypeOrdinal(dataTypeOrdinals);
    request.setDeviceMeasurements(deviceMeasurements);
    request.setAscending(ascending);
    return request;
  }

  private SingleSeriesQueryRequest constructSingleQueryRequest(
      Filter timeFilter,
      Filter valueFilter,
      TSDataType dataType,
      Path path,
      Set<String> deviceMeasurements,
      PartitionGroup partitionGroup,
      QueryContext context,
      boolean ascending) {
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    if (timeFilter != null) {
      request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    }
    if (valueFilter != null) {
      request.setValueFilterBytes(SerializeUtils.serializeFilter(valueFilter));
    }
    request.setPath(path.getFullPath());
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setRequester(metaGroupMember.getThisNode());
    request.setDataTypeOrdinal(dataType.ordinal());
    request.setDeviceMeasurements(deviceMeasurements);
    request.setAscending(ascending);
    return request;
  }

  /**
   * Get GroupByExecutors the will executor the aggregations of "aggregationTypes" over "path".
   * First, the groups to be queried will be determined by the timeFilter. Then for group, a local
   * or remote GroupByExecutor will be created and finally all such executors will be returned.
   *
   * @param timeFilter nullable
   */
  public List<GroupByExecutor> getGroupByExecutors(
      PartialPath path,
      Set<String> deviceMeasurements,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      List<Integer> aggregationTypes,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    // make sure the partition table is new
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new QueryProcessException(e.getMessage());
    }
    // find out the groups that should be queried
    List<PartitionGroup> partitionGroups;
    try {
      partitionGroups = metaGroupMember.routeFilter(timeFilter, path);
    } catch (EmptyIntervalException e) {
      logger.info(e.getMessage());
      partitionGroups = Collections.emptyList();
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Sending group by query of {} to {} groups",
          metaGroupMember.getName(),
          path,
          partitionGroups.size());
    }
    // create an executor for each group
    List<GroupByExecutor> executors = new ArrayList<>();
    for (PartitionGroup partitionGroup : partitionGroups) {
      GroupByExecutor groupByExecutor =
          getGroupByExecutor(
              path,
              deviceMeasurements,
              partitionGroup,
              timeFilter,
              context,
              dataType,
              aggregationTypes,
              ascending);
      executors.add(groupByExecutor);
    }
    return executors;
  }

  /**
   * Get a GroupByExecutor that will run "aggregationTypes" over "path" within "partitionGroup". If
   * the local node is a member of the group, a local executor will be created. Otherwise a remote
   * executor will be created.
   *
   * @param timeFilter nullable
   */
  private GroupByExecutor getGroupByExecutor(
      PartialPath path,
      Set<String> deviceMeasurements,
      PartitionGroup partitionGroup,
      Filter timeFilter,
      QueryContext context,
      TSDataType dataType,
      List<Integer> aggregationTypes,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    if (partitionGroup.contains(metaGroupMember.getThisNode())) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember =
          metaGroupMember.getLocalDataMember(partitionGroup.getHeader());
      LocalQueryExecutor localQueryExecutor = new LocalQueryExecutor(dataGroupMember);
      logger.debug(
          "{}: creating a local group by executor for {}#{}",
          metaGroupMember.getName(),
          path.getFullPath(),
          context.getQueryId());
      return localQueryExecutor.getGroupByExecutor(
          path, deviceMeasurements, dataType, timeFilter, aggregationTypes, context, ascending);
    } else {
      return getRemoteGroupByExecutor(
          timeFilter,
          aggregationTypes,
          dataType,
          path,
          deviceMeasurements,
          partitionGroup,
          context,
          ascending);
    }
  }

  /**
   * Get a GroupByExecutor that will run "aggregationTypes" over "path" within a remote group
   * "partitionGroup". Send a request to one node in the group to create an executor there and use
   * the return executor id to fetch result later.
   *
   * @param timeFilter nullable
   */
  private GroupByExecutor getRemoteGroupByExecutor(
      Filter timeFilter,
      List<Integer> aggregationTypes,
      TSDataType dataType,
      Path path,
      Set<String> deviceMeasurements,
      PartitionGroup partitionGroup,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException {
    GroupByRequest request = new GroupByRequest();
    if (timeFilter != null) {
      request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    }
    request.setPath(path.getFullPath());
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setAggregationTypeOrdinals(aggregationTypes);
    request.setDataTypeOrdinal(dataType.ordinal());
    request.setRequestor(metaGroupMember.getThisNode());
    request.setDeviceMeasurements(deviceMeasurements);
    request.setAscending(ascending);

    // select a node with lowest latency or highest throughput with high priority
    List<Node> orderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : orderedNodes) {
      // query a remote node
      logger.debug("{}: querying group by {} from {}", metaGroupMember.getName(), path, node);

      try {
        Long executorId = getRemoteGroupByExecutorId(node, request);

        if (executorId == null) {
          continue;
        }

        if (executorId != -1) {
          // record the queried node to release resources later
          ((RemoteQueryContext) context).registerRemoteNode(node, partitionGroup.getHeader());
          logger.debug(
              "{}: get an executorId {} for {}@{} from {}",
              metaGroupMember.getName(),
              executorId,
              aggregationTypes,
              path,
              node);
          // create a remote executor with the return id
          RemoteGroupByExecutor remoteGroupByExecutor =
              new RemoteGroupByExecutor(
                  executorId, metaGroupMember, node, partitionGroup.getHeader());
          for (Integer aggregationType : aggregationTypes) {
            remoteGroupByExecutor.addAggregateResult(
                AggregateResultFactory.getAggrResultByType(
                    AggregationType.values()[aggregationType], dataType, ascending));
          }
          return remoteGroupByExecutor;
        } else {
          // an id of -1 means there is no satisfying data on the remote node, create an empty
          // reader tp reduce further communication
          logger.debug("{}: no data for {} from {}", metaGroupMember.getName(), path, node);
          return new EmptyReader();
        }
      } catch (TException | IOException e) {
        logger.error("{}: Cannot query {} from {}", metaGroupMember.getName(), path, node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("{}: Cannot query {} from {}", metaGroupMember.getName(), path, node, e);
      }
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  private Long getRemoteGroupByExecutorId(Node node, GroupByRequest request)
      throws IOException, TException, InterruptedException {
    Long executorId;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          metaGroupMember
              .getClientProvider()
              .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
      executorId = SyncClientAdaptor.getGroupByExecutor(client, request);
    } else {
      try (SyncDataClient syncDataClient =
          metaGroupMember
              .getClientProvider()
              .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {

        try {
          executorId = syncDataClient.getGroupByExecutor(request);
        } catch (TException e) {
          // the connection may be broken, close it to avoid it being reused
          syncDataClient.getInputProtocol().getTransport().close();
          throw e;
        }
      }
    }
    return executorId;
  }

  /**
   * Create an IBatchReader of "path" with “timeFilter” and "valueFilter". A synchronization with
   * the leader will be performed according to consistency level
   *
   * @param path
   * @param dataType
   * @param timeFilter nullable
   * @param valueFilter nullable
   * @param context
   * @return an IBatchReader or null if there is no satisfying data
   * @throws StorageEngineException
   */
  public IBatchReader getSeriesBatchReader(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      Filter timeFilter,
      Filter valueFilter,
      QueryContext context,
      DataGroupMember dataGroupMember,
      boolean ascending)
      throws StorageEngineException, QueryProcessException, IOException {
    // pull the newest data
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }

    SeriesReader seriesReader =
        getSeriesReader(
            path,
            allSensors,
            dataType,
            timeFilter,
            valueFilter,
            context,
            dataGroupMember.getHeader(),
            ascending);
    if (seriesReader.isEmpty()) {
      return null;
    }
    return new SeriesRawDataBatchReader(seriesReader);
  }

  /**
   * Create an IBatchReader of "path" with “timeFilter” and "valueFilter". A synchronization with
   * the leader will be performed according to consistency level
   *
   * @param paths
   * @param dataTypes
   * @param timeFilter nullable
   * @param valueFilter nullable
   * @param context
   * @return an IBatchReader or null if there is no satisfying data
   * @throws StorageEngineException
   */
  public IBatchReader getMultSeriesBatchReader(
      List<PartialPath> paths,
      Map<String, Set<String>> allSensors,
      List<TSDataType> dataTypes,
      Filter timeFilter,
      Filter valueFilter,
      QueryContext context,
      DataGroupMember dataGroupMember,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    // pull the newest data
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }

    Map<String, IBatchReader> partialPathBatchReaderMap = Maps.newHashMap();

    for (int i = 0; i < paths.size(); i++) {
      PartialPath partialPath = paths.get(i);
      SeriesReader seriesReader =
          getSeriesReader(
              partialPath,
              allSensors.get(partialPath.getFullPath()),
              dataTypes.get(i),
              timeFilter,
              valueFilter,
              context,
              dataGroupMember.getHeader(),
              ascending);
      partialPathBatchReaderMap.put(
          partialPath.getFullPath(), new SeriesRawDataBatchReader(seriesReader));
    }
    return new MultBatchReader(partialPathBatchReaderMap);
  }

  /**
   * Create an IReaderByTimestamp of "path". A synchronization with the leader will be performed
   * according to consistency level
   *
   * @param path
   * @param dataType
   * @param context
   * @return an IReaderByTimestamp or null if there is no satisfying data
   * @throws StorageEngineException
   */
  public IReaderByTimestamp getReaderByTimestamp(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      DataGroupMember dataGroupMember,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    try {
      dataGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
    SeriesReader seriesReader =
        getSeriesReader(
            path,
            allSensors,
            dataType,
            TimeFilter.gtEq(Long.MIN_VALUE),
            null,
            context,
            dataGroupMember.getHeader(),
            ascending);
    try {
      if (seriesReader.isEmpty()) {
        return null;
      }
    } catch (IOException e) {
      throw new QueryProcessException(e, TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return new SeriesReaderByTimestamp(seriesReader, ascending);
  }
}
