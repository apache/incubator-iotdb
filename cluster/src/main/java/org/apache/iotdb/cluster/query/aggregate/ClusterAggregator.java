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

package org.apache.iotdb.cluster.query.aggregate;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.EmptyIntervalException;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.LocalQueryExecutor;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@SuppressWarnings("java:S107")
public class ClusterAggregator {

  private static final Logger logger = LoggerFactory.getLogger(ClusterAggregator.class);
  private MetaGroupMember metaGroupMember;

  public ClusterAggregator(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  /**
   * Perform "aggregations" over "path" in some data groups and merge the results. The groups to be
   * queried is determined by "timeFilter".
   *
   * @param timeFilter nullable, when null, all groups will be queried
   * @param ascending
   */
  public List<AggregateResult> getAggregateResult(
      PartialPath path,
      Set<String> deviceMeasurements,
      List<String> aggregations,
      TSDataType dataType,
      Filter timeFilter,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException {
    // make sure the partition table is new
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
    // find groups to be queried using timeFilter and path
    List<PartitionGroup> partitionGroups;
    try {
      partitionGroups = metaGroupMember.routeFilter(timeFilter, path);
    } catch (EmptyIntervalException e) {
      logger.info(e.getMessage());
      partitionGroups = Collections.emptyList();
    }
    logger.debug(
        "{}: Sending aggregation query of {} to {} groups",
        metaGroupMember.getName(),
        path,
        partitionGroups.size());
    List<AggregateResult> results = null;
    // get the aggregation result of each group and merge them
    for (PartitionGroup partitionGroup : partitionGroups) {
      List<AggregateResult> groupResult =
          getAggregateResult(
              path,
              deviceMeasurements,
              aggregations,
              dataType,
              timeFilter,
              partitionGroup,
              context,
              ascending);
      if (results == null) {
        // the first results
        results = groupResult;
      } else {
        for (int i = 0; i < results.size(); i++) {
          results.get(i).merge(groupResult.get(i));
        }
      }
    }
    return results;
  }

  /**
   * Perform "aggregations" over "path" in "partitionGroup". If the local node is the member of the
   * group, do it locally, otherwise pull the results from a remote node.
   *
   * @param timeFilter nullable
   */
  private List<AggregateResult> getAggregateResult(
      PartialPath path,
      Set<String> deviceMeasurements,
      List<String> aggregations,
      TSDataType dataType,
      Filter timeFilter,
      PartitionGroup partitionGroup,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException {
    if (!partitionGroup.contains(metaGroupMember.getThisNode())) {
      return getRemoteAggregateResult(
          path,
          deviceMeasurements,
          aggregations,
          dataType,
          timeFilter,
          partitionGroup,
          context,
          ascending);
    } else {
      // perform the aggregations locally
      DataGroupMember dataMember =
          metaGroupMember.getLocalDataMember(partitionGroup.getHeader(), partitionGroup.getId());
      LocalQueryExecutor localQueryExecutor = new LocalQueryExecutor(dataMember);
      try {
        logger.debug(
            "{}: querying aggregation {} of {} in {} locally",
            metaGroupMember.getName(),
            aggregations,
            path,
            partitionGroup.getHeader());
        List<AggregateResult> aggrResult =
            localQueryExecutor.getAggrResult(
                aggregations, deviceMeasurements, dataType, path, timeFilter, context, ascending);
        logger.debug(
            "{}: queried aggregation {} of {} in {} locally are {}",
            metaGroupMember.getName(),
            aggregations,
            path,
            partitionGroup.getHeader(),
            aggrResult);
        return aggrResult;
      } catch (IOException | QueryProcessException e) {
        throw new StorageEngineException(e);
      }
    }
  }

  /**
   * Perform "aggregations" over "path" in a remote data group "partitionGroup". Query one node in
   * the group to get the results.
   *
   * @param timeFilter nullable
   */
  private List<AggregateResult> getRemoteAggregateResult(
      Path path,
      Set<String> deviceMeasurements,
      List<String> aggregations,
      TSDataType dataType,
      Filter timeFilter,
      PartitionGroup partitionGroup,
      QueryContext context,
      boolean ascending)
      throws StorageEngineException {

    GetAggrResultRequest request = new GetAggrResultRequest();
    request.setPath(path.getFullPath());
    request.setAggregations(aggregations);
    request.setDataTypeOrdinal(dataType.ordinal());
    request.setQueryId(context.getQueryId());
    request.setRequestor(metaGroupMember.getThisNode());
    request.setHeader(partitionGroup.getHeader());
    request.setDeviceMeasurements(deviceMeasurements);
    request.setAscending(ascending);
    if (timeFilter != null) {
      request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    }

    // put nodes with lowest delay at first
    List<Node> reorderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : reorderedNodes) {
      logger.debug(
          "{}: querying aggregation {} of {} from {} of {}",
          metaGroupMember.getName(),
          aggregations,
          path,
          node,
          partitionGroup.getHeader());

      try {
        List<ByteBuffer> resultBuffers = getRemoteAggregateResult(node, request);
        if (resultBuffers != null) {
          List<AggregateResult> results = new ArrayList<>(resultBuffers.size());
          for (ByteBuffer resultBuffer : resultBuffers) {
            AggregateResult result = AggregateResult.deserializeFrom(resultBuffer);
            results.add(result);
          }
          // register the queried node to release resources when the query ends
          ((RemoteQueryContext) context).registerRemoteNode(node, partitionGroup.getHeader());
          logger.debug(
              "{}: queried aggregation {} of {} from {} of {} are {}",
              metaGroupMember.getName(),
              aggregations,
              path,
              node,
              partitionGroup.getHeader(),
              results);
          return results;
        }
      } catch (TException | IOException e) {
        logger.error(
            "{}: Cannot query aggregation {} from {}", metaGroupMember.getName(), path, node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("{}: query {} interrupted from {}", metaGroupMember.getName(), path, node, e);
      }
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query aggregate: " + path + " in " + partitionGroup));
  }

  private List<ByteBuffer> getRemoteAggregateResult(Node node, GetAggrResultRequest request)
      throws IOException, TException, InterruptedException {
    List<ByteBuffer> resultBuffers;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client =
          metaGroupMember
              .getClientProvider()
              .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
      // each buffer is an AggregationResult
      resultBuffers = SyncClientAdaptor.getAggrResult(client, request);
    } else {
      try (SyncDataClient syncDataClient =
          metaGroupMember
              .getClientProvider()
              .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {
        try {
          resultBuffers = syncDataClient.getAggrResult(request);
        } catch (TException e) {
          // the connection may be broken, close it to avoid it being reused
          syncDataClient.getInputProtocol().getTransport().close();
          throw e;
        }
      }
    }
    return resultBuffers;
  }
}
