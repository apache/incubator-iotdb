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

package org.apache.iotdb.cluster.query.fill;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.QueryTimeOutException;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PreviousFillRequest;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.PreviousFillHandler;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.fill.PreviousFill;
import org.apache.iotdb.db.utils.TimeValuePairUtils.Intervals;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClusterPreviousFill extends PreviousFill {

  private static final Logger logger = LoggerFactory.getLogger(ClusterPreviousFill.class);
  private MetaGroupMember metaGroupMember;
  private TimeValuePair fillResult;

  ClusterPreviousFill(PreviousFill fill, MetaGroupMember metaGroupMember) {
    super(fill.getDataType(), fill.getQueryTime(), fill.getBeforeRange());
    this.metaGroupMember = metaGroupMember;
  }

  ClusterPreviousFill(
      TSDataType dataType, long queryTime, long beforeRange, MetaGroupMember metaGroupMember) {
    super(dataType, queryTime, beforeRange);
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public void configureFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      Set<String> deviceMeasurements,
      QueryContext context) {
    try {
      fillResult =
          performPreviousFill(
              path, dataType, queryTime, getBeforeRange(), deviceMeasurements, context);
    } catch (StorageEngineException e) {
      logger.error("Failed to configure previous fill for Path {}", path, e);
    }
  }

  @Override
  public TimeValuePair getFillResult() {
    return fillResult;
  }

  private TimeValuePair performPreviousFill(
      PartialPath path,
      TSDataType dataType,
      long queryTime,
      long beforeRange,
      Set<String> deviceMeasurements,
      QueryContext context)
      throws StorageEngineException {
    // make sure the partition table is new
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(false);
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
    // find the groups that should be queried using the time range
    Intervals intervals = new Intervals();
    long lowerBound = beforeRange == -1 ? Long.MIN_VALUE : queryTime - beforeRange;
    intervals.addInterval(lowerBound, queryTime);
    List<PartitionGroup> partitionGroups = metaGroupMember.routeIntervals(intervals, path);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Sending data query of {} to {} groups",
          metaGroupMember.getName(),
          path,
          partitionGroups.size());
    }
    CountDownLatch latch = new CountDownLatch(partitionGroups.size());
    PreviousFillHandler handler = new PreviousFillHandler(latch);

    ExecutorService fillService = Executors.newFixedThreadPool(partitionGroups.size());
    PreviousFillArguments arguments =
        new PreviousFillArguments(path, dataType, queryTime, beforeRange, deviceMeasurements);

    for (PartitionGroup partitionGroup : partitionGroups) {
      fillService.submit(() -> performPreviousFill(arguments, context, partitionGroup, handler));
    }
    fillService.shutdown();
    try {
      fillService.awaitTermination(RaftServer.getReadOperationTimeoutMS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for fill pool to stop", e);
    }
    return handler.getResult();
  }

  private void performPreviousFill(
      PreviousFillArguments arguments,
      QueryContext context,
      PartitionGroup group,
      PreviousFillHandler fillHandler) {
    if (group.contains(metaGroupMember.getThisNode())) {
      localPreviousFill(arguments, context, group, fillHandler);
    } else {
      remotePreviousFill(arguments, context, group, fillHandler);
    }
  }

  private void localPreviousFill(
      PreviousFillArguments arguments,
      QueryContext context,
      PartitionGroup group,
      PreviousFillHandler fillHandler) {
    DataGroupMember localDataMember =
        metaGroupMember.getLocalDataMember(group.getHeader(), group.getId());
    try {
      fillHandler.onComplete(
          localDataMember
              .getLocalQueryExecutor()
              .localPreviousFill(
                  arguments.getPath(),
                  arguments.getDataType(),
                  arguments.getQueryTime(),
                  arguments.getBeforeRange(),
                  arguments.getDeviceMeasurements(),
                  context));
    } catch (QueryProcessException | StorageEngineException | IOException e) {
      fillHandler.onError(e);
    }
  }

  private void remotePreviousFill(
      PreviousFillArguments arguments,
      QueryContext context,
      PartitionGroup group,
      PreviousFillHandler fillHandler) {
    PreviousFillRequest request =
        new PreviousFillRequest(
            arguments.getPath().getFullPath(),
            arguments.getQueryTime(),
            arguments.getBeforeRange(),
            context.getQueryId(),
            metaGroupMember.getThisNode(),
            group.getHeader(),
            arguments.getDataType().ordinal(),
            arguments.getDeviceMeasurements());

    for (Node node : group) {
      ByteBuffer byteBuffer;
      if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
        byteBuffer = remoteAsyncPreviousFill(node, request, arguments);
      } else {
        byteBuffer = remoteSyncPreviousFill(node, request, arguments);
      }

      if (byteBuffer != null) {
        fillHandler.onComplete(byteBuffer);
        return;
      }
    }

    fillHandler.onError(
        new QueryTimeOutException(
            String.format(
                "PreviousFill %s@%d range: %d",
                arguments.getPath().getFullPath(),
                arguments.getQueryTime(),
                arguments.getBeforeRange())));
  }

  private ByteBuffer remoteAsyncPreviousFill(
      Node node, PreviousFillRequest request, PreviousFillArguments arguments) {
    ByteBuffer byteBuffer = null;
    AsyncDataClient asyncDataClient;
    try {
      asyncDataClient =
          metaGroupMember
              .getClientProvider()
              .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
    } catch (IOException e) {
      logger.warn("{}: Cannot connect to {} during previous fill", metaGroupMember, node);
      return null;
    }

    try {
      byteBuffer = SyncClientAdaptor.previousFill(asyncDataClient, request);
    } catch (Exception e) {
      logger.error(
          "{}: Cannot perform previous fill of {} to {}",
          metaGroupMember,
          arguments.getPath(),
          node,
          e);
    }
    return byteBuffer;
  }

  private ByteBuffer remoteSyncPreviousFill(
      Node node, PreviousFillRequest request, PreviousFillArguments arguments) {
    ByteBuffer byteBuffer = null;
    try (SyncDataClient syncDataClient =
        metaGroupMember
            .getClientProvider()
            .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {
      try {
        byteBuffer = syncDataClient.previousFill(request);
      } catch (TException e) {
        // the connection may be broken, close it to avoid it being reused
        syncDataClient.getInputProtocol().getTransport().close();
        throw e;
      }
    } catch (Exception e) {
      logger.error(
          "{}: Cannot perform previous fill of {} to {}",
          metaGroupMember.getName(),
          arguments.getPath(),
          node,
          e);
    }
    return byteBuffer;
  }
}
