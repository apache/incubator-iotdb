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
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.utils.SerializeUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteSeriesReaderByTimestamp implements IReaderByTimestamp {

  private static final Logger logger = LoggerFactory.getLogger(RemoteSeriesReaderByTimestamp.class);
  private DataSourceInfo sourceInfo;

  private AtomicReference<ByteBuffer> fetchResult = new AtomicReference<>();
  private GenericHandler<ByteBuffer> handler;

  public RemoteSeriesReaderByTimestamp(DataSourceInfo sourceInfo) {
    this.sourceInfo = sourceInfo;
    handler = new GenericHandler<>(sourceInfo.getCurrentNode(), fetchResult);
  }

  @Override
  public Object[] getValuesInTimestamps(long[] timestamps, int length) throws IOException {
    if (!sourceInfo.checkCurClient()) {
      return null;
    }

    ByteBuffer result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      result = fetchResultAsync(timestamps, length);
    } else {
      result = fetchResultSync(timestamps, length);
    }

    return SerializeUtils.deserializeObjects(result);
  }

  @SuppressWarnings("java:S2274") // enable timeout
  private ByteBuffer fetchResultAsync(long[] timestamps, int length) throws IOException {
    // convert long[] to List<Long>, which is used for thrift
    List<Long> timestampList = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      timestampList.add(timestamps[i]);
    }
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        sourceInfo
            .getCurAsyncClient(RaftServer.getReadOperationTimeoutMS())
            .fetchSingleSeriesByTimestamps(
                sourceInfo.getHeader(), sourceInfo.getReaderId(), timestampList, handler);
        fetchResult.wait(RaftServer.getReadOperationTimeoutMS());
      } catch (TException e) {
        // try other node
        if (!sourceInfo.switchNode(true, timestamps[0])) {
          return null;
        }
        return fetchResultAsync(timestamps, length);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Query {} interrupted", sourceInfo);
        return null;
      }
    }
    return fetchResult.get();
  }

  private ByteBuffer fetchResultSync(long[] timestamps, int length) throws IOException {
    SyncDataClient curSyncClient = null;
    // convert long[] to List<Long>, which is used for thrift
    List<Long> timestampList = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      timestampList.add(timestamps[i]);
    }
    try {
      curSyncClient = sourceInfo.getCurSyncClient(RaftServer.getReadOperationTimeoutMS());
      return curSyncClient.fetchSingleSeriesByTimestamps(
          sourceInfo.getHeader(), sourceInfo.getReaderId(), timestampList);
    } catch (TException e) {
      if (curSyncClient != null) {
        curSyncClient.getInputProtocol().getTransport().close();
      }
      // try other node
      if (!sourceInfo.switchNode(true, timestamps[0])) {
        return null;
      }
      return fetchResultSync(timestamps, length);
    } finally {
      if (curSyncClient != null) {
        ClientUtils.putBackSyncClient(curSyncClient);
      }
    }
  }
}
