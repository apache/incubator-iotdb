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

import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.utils.ClientUtils;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RemoteSimpleSeriesReader is a reader without value filter that reads points from a remote side.
 */
public class RemoteSimpleSeriesReader implements IPointReader {

  private static final Logger logger = LoggerFactory.getLogger(RemoteSimpleSeriesReader.class);
  private DataSourceInfo sourceInfo;
  private long lastTimestamp;

  private BatchData cachedBatch;

  private AtomicReference<ByteBuffer> fetchResult = new AtomicReference<>();
  private GenericHandler<ByteBuffer> handler;

  public RemoteSimpleSeriesReader(DataSourceInfo sourceInfo) {
    this.sourceInfo = sourceInfo;
    handler = new GenericHandler<>(sourceInfo.getCurrentNode(), fetchResult);
    lastTimestamp = Long.MIN_VALUE;
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    if (cachedBatch != null && cachedBatch.hasCurrent()) {
      return true;
    }
    fetchBatch();
    return cachedBatch != null && cachedBatch.hasCurrent();
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    if (!hasNextTimeValuePair()) {
      throw new NoSuchElementException();
    }
    this.lastTimestamp = cachedBatch.currentTime();
    TimeValuePair timeValuePair =
        new TimeValuePair(
            cachedBatch.currentTime(),
            TsPrimitiveType.getByType(sourceInfo.getDataType(), cachedBatch.currentValue()));
    cachedBatch.next();
    return timeValuePair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    if (!hasNextTimeValuePair()) {
      throw new NoSuchElementException();
    }
    return new TimeValuePair(
        cachedBatch.currentTime(),
        TsPrimitiveType.getByType(sourceInfo.getDataType(), cachedBatch.currentValue()));
  }

  @Override
  public void close() {
    // closed by Resource manager
  }

  private void fetchBatch() throws IOException {
    if (!sourceInfo.checkCurClient()) {
      cachedBatch = null;
      return;
    }

    ByteBuffer result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      result = fetchResultAsync();
    } else {
      result = fetchResultSync();
    }

    cachedBatch = SerializeUtils.deserializeBatchData(result);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Fetched a batch from {}, size:{}",
          sourceInfo.getCurrentNode(),
          cachedBatch == null ? 0 : cachedBatch.length());
    }
  }

  @SuppressWarnings("java:S2274") // enable timeout
  private ByteBuffer fetchResultAsync() throws IOException {
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        sourceInfo
            .getCurAsyncClient(RaftServer.getReadOperationTimeoutMS())
            .fetchSingleSeries(sourceInfo.getHeader(), sourceInfo.getReaderId(), handler);
        fetchResult.wait(RaftServer.getReadOperationTimeoutMS());
      } catch (TException e) {
        // try other node
        if (!sourceInfo.switchNode(false, lastTimestamp)) {
          return null;
        }
        return fetchResultAsync();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Query {} interrupted", sourceInfo);
        return null;
      }
    }
    return fetchResult.get();
  }

  private ByteBuffer fetchResultSync() throws IOException {
    SyncDataClient curSyncClient = null;
    try {
      curSyncClient = sourceInfo.getCurSyncClient(RaftServer.getReadOperationTimeoutMS());
      return curSyncClient.fetchSingleSeries(sourceInfo.getHeader(), sourceInfo.getReaderId());
    } catch (TException e) {
      if (curSyncClient != null) {
        curSyncClient.getInputProtocol().getTransport().close();
      }
      // try other node
      if (!sourceInfo.switchNode(false, lastTimestamp)) {
        return null;
      }
      return fetchResultSync();
    } finally {
      if (curSyncClient != null) {
        ClientUtils.putBackSyncClient(curSyncClient);
      }
    }
  }

  void clearCurDataForTest() {
    this.cachedBatch = null;
  }
}
