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
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * provide client which could connect to all nodes of the partitionGroup. Notice: methods like
 * getter should be called only after nextDataClient() has been called
 */
public class DataSourceInfo {

  private static final Logger logger = LoggerFactory.getLogger(DataSourceInfo.class);

  private long readerId;
  private Node curSource;
  private PartitionGroup partitionGroup;
  private TSDataType dataType;
  private SingleSeriesQueryRequest request;
  private RemoteQueryContext context;
  private MetaGroupMember metaGroupMember;
  private List<Node> nodes;
  private int curPos;
  private boolean isNoData = false;
  private boolean isNoClient = false;

  public DataSourceInfo(
      PartitionGroup group,
      TSDataType dataType,
      SingleSeriesQueryRequest request,
      RemoteQueryContext context,
      MetaGroupMember metaGroupMember,
      List<Node> nodes) {
    this.readerId = -1;
    this.partitionGroup = group;
    this.dataType = dataType;
    this.request = request;
    this.context = context;
    this.metaGroupMember = metaGroupMember;
    this.nodes = nodes;
    // set to the last node so after nextDataClient() is called it will scan from the first node
    this.curPos = nodes.size() - 1;
    this.curSource = nodes.get(curPos);
  }

  public boolean hasNextDataClient(boolean byTimestamp, long timestamp) {
    if (this.nodes.isEmpty()) {
      this.isNoData = false;
      return false;
    }

    int nextNodePos = (this.curPos + 1) % this.nodes.size();
    while (true) {
      Node node = nodes.get(nextNodePos);
      logger.debug("querying {} from {} of {}", request.path, node, partitionGroup.getHeader());
      try {
        Long newReaderId = getReaderId(node, byTimestamp, timestamp);
        if (newReaderId != null) {
          logger.debug("get a readerId {} for {} from {}", newReaderId, request.path, node);
          if (newReaderId != -1) {
            // register the node so the remote resources can be released
            context.registerRemoteNode(node, partitionGroup.getHeader());
            this.readerId = newReaderId;
            this.curSource = node;
            this.curPos = nextNodePos;
            return true;
          } else {
            // the id being -1 means there is no satisfying data on the remote node, create an
            // empty reader to reduce further communication
            this.isNoClient = true;
            this.isNoData = true;
            return false;
          }
        }
      } catch (TException | IOException e) {
        logger.error("Cannot query {} from {}", this.request.path, node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Cannot query {} from {}", this.request.path, node, e);
      }
      nextNodePos = (nextNodePos + 1) % this.nodes.size();
      if (nextNodePos == this.curPos) {
        // has iterate over all nodes
        isNoClient = true;
        break;
      }
    }
    // all nodes are failed
    this.isNoData = false;
    return false;
  }

  private Long getReaderId(Node node, boolean byTimestamp, long timestamp)
      throws TException, InterruptedException, IOException {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      return applyForReaderIdAsync(node, byTimestamp, timestamp);
    }
    return applyForReaderIdSync(node, byTimestamp, timestamp);
  }

  private Long applyForReaderIdAsync(Node node, boolean byTimestamp, long timestamp)
      throws TException, InterruptedException, IOException {
    AsyncDataClient client =
        this.metaGroupMember
            .getClientProvider()
            .getAsyncDataClient(node, RaftServer.getReadOperationTimeoutMS());
    Long newReaderId;
    if (byTimestamp) {
      newReaderId = SyncClientAdaptor.querySingleSeriesByTimestamp(client, request);
    } else {
      newReaderId = SyncClientAdaptor.querySingleSeries(client, request, timestamp);
    }
    return newReaderId;
  }

  private Long applyForReaderIdSync(Node node, boolean byTimestamp, long timestamp)
      throws TException {

    Long newReaderId;
    try (SyncDataClient client =
        this.metaGroupMember
            .getClientProvider()
            .getSyncDataClient(node, RaftServer.getReadOperationTimeoutMS())) {

      try {
        if (byTimestamp) {
          newReaderId = client.querySingleSeriesByTimestamp(request);
        } else {
          Filter newFilter;
          // add timestamp to as a timeFilter to skip the data which has been read
          if (request.isSetTimeFilterBytes()) {
            Filter timeFilter = FilterFactory.deserialize(request.timeFilterBytes);
            newFilter = new AndFilter(timeFilter, TimeFilter.gt(timestamp));
          } else {
            newFilter = TimeFilter.gt(timestamp);
          }
          request.setTimeFilterBytes(SerializeUtils.serializeFilter(newFilter));
          newReaderId = client.querySingleSeries(request);
        }
      } catch (TException e) {
        // the connection may be broken, close it to avoid it being reused
        client.getInputProtocol().getTransport().close();
        throw e;
      }
      return newReaderId;
    }
  }

  public long getReaderId() {
    return this.readerId;
  }

  public TSDataType getDataType() {
    return this.dataType;
  }

  public RaftNode getHeader() {
    return partitionGroup.getHeader();
  }

  Node getCurrentNode() {
    return this.curSource;
  }

  AsyncDataClient getCurAsyncClient(int timeout) throws IOException {
    return isNoClient
        ? null
        : metaGroupMember.getClientProvider().getAsyncDataClient(this.curSource, timeout);
  }

  SyncDataClient getCurSyncClient(int timeout) throws TException {
    return isNoClient
        ? null
        : metaGroupMember.getClientProvider().getSyncDataClient(this.curSource, timeout);
  }

  public boolean isNoData() {
    return this.isNoData;
  }

  private boolean isNoClient() {
    return this.isNoClient;
  }

  @Override
  public String toString() {
    return "DataSourceInfo{"
        + "readerId="
        + readerId
        + ", curSource="
        + curSource
        + ", partitionGroup="
        + partitionGroup
        + ", request="
        + request
        + '}';
  }

  /**
   * Check if there is still any available client and there is still any left data.
   *
   * @return true if there is an available client and data to read, false all data has been read.
   * @throws IOException if all clients are unavailable.
   */
  boolean checkCurClient() throws IOException {
    if (isNoClient()) {
      if (!isNoData()) {
        throw new IOException("no available client.");
      } else {
        // no data
        return false;
      }
    }

    return true;
  }

  boolean switchNode(boolean byTimestamp, long timeOffset) throws IOException {
    boolean hasClient = hasNextDataClient(byTimestamp, timeOffset);
    logger.info("Client failed, changed to {}", curSource);
    if (!hasClient) {
      if (!isNoData()) {
        throw new IOException("no available client.");
      } else {
        // no data
        return false;
      }
    }
    return true;
  }
}
