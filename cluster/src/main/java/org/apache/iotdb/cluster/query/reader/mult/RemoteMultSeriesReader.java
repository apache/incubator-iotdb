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

package org.apache.iotdb.cluster.query.reader.mult;

import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/** mult reader without value filter that reads points from a remote side. */
public class RemoteMultSeriesReader extends AbstractMultPointReader {

  private static final Logger logger = LoggerFactory.getLogger(RemoteMultSeriesReader.class);
  private static final int FETCH_BATCH_DATA_SIZE = 10;

  private MultDataSourceInfo sourceInfo;

  private Map<String, Queue<BatchData>> cachedBatchs;

  private AtomicReference<Map<String, ByteBuffer>> fetchResult = new AtomicReference<>();
  private GenericHandler<Map<String, ByteBuffer>> handler;

  private BatchStrategy batchStrategy;

  private Map<String, BatchData> currentBatchDatas;

  private Map<String, TSDataType> pathToDataType;

  public RemoteMultSeriesReader(MultDataSourceInfo sourceInfo) {
    this.sourceInfo = sourceInfo;
    this.handler = new GenericHandler<>(sourceInfo.getCurrentNode(), fetchResult);
    this.currentBatchDatas = Maps.newHashMap();
    this.batchStrategy = new DefaultBatchStrategy();

    this.cachedBatchs = Maps.newHashMap();
    this.pathToDataType = Maps.newHashMap();
    for (int i = 0; i < sourceInfo.getPartialPaths().size(); i++) {
      String fullPath = sourceInfo.getPartialPaths().get(i).getFullPath();
      this.cachedBatchs.put(fullPath, new ConcurrentLinkedQueue<>());
      this.pathToDataType.put(fullPath, sourceInfo.getDataTypes().get(i));
    }
  }

  @Override
  public synchronized boolean hasNextTimeValuePair(String fullPath) throws IOException {
    BatchData batchData = currentBatchDatas.get(fullPath);
    if (batchData != null && batchData.hasCurrent()) {
      return true;
    }
    fetchBatch();
    return checkPathBatchData(fullPath);
  }

  private synchronized boolean checkPathBatchData(String fullPath) {
    BatchData batchData = cachedBatchs.get(fullPath).peek();
    if (batchData != null && !batchData.isEmpty()) {
      return true;
    }
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair(String fullPath) throws IOException {
    BatchData batchData = currentBatchDatas.get(fullPath);
    if ((batchData == null || !batchData.hasCurrent()) && checkPathBatchData(fullPath)) {
      batchData = cachedBatchs.get(fullPath).poll();
      currentBatchDatas.put(fullPath, batchData);
    }

    if (!hasNextTimeValuePair(fullPath)) {
      throw new NoSuchElementException();
    }

    TimeValuePair timeValuePair =
        new TimeValuePair(
            batchData.currentTime(),
            TsPrimitiveType.getByType(pathToDataType.get(fullPath), batchData.currentValue()));
    batchData.next();
    return timeValuePair;
  }

  @Override
  public Set<String> getAllPaths() {
    return cachedBatchs.keySet();
  }

  /** query resource deal close there is not dealing. */
  @Override
  public void close() {}

  private void fetchBatch() throws IOException {
    if (!sourceInfo.checkCurClient()) {
      cachedBatchs = null;
      return;
    }
    List<String> paths = batchStrategy.selectBatchPaths(this.cachedBatchs);
    if (paths.isEmpty()) return;

    Map<String, ByteBuffer> result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      result = fetchResultAsync(paths);
    } else {
      result = fetchResultSync(paths);
    }

    if (result == null) return;

    for (String path : result.keySet()) {

      BatchData batchData = SerializeUtils.deserializeBatchData(result.get(path));
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Fetched a batch from {}, size:{}",
            sourceInfo.getCurrentNode(),
            batchData == null ? 0 : batchData.length());
      }
      // if data query end, batchData is null,
      // will create empty BatchData, and add queue.
      if (batchData == null) {
        batchData = new BatchData();
      }
      cachedBatchs
          .computeIfAbsent(path, n -> new ConcurrentLinkedQueue<BatchData>())
          .add(batchData);
    }
  }

  @SuppressWarnings("java:S2274") // enable timeout
  private Map<String, ByteBuffer> fetchResultAsync(List<String> paths) throws IOException {
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        sourceInfo
            .getCurAsyncClient(RaftServer.getReadOperationTimeoutMS())
            .fetchMultSeries(sourceInfo.getHeader(), sourceInfo.getReaderId(), paths, handler);
        fetchResult.wait(RaftServer.getReadOperationTimeoutMS());
      } catch (TException | InterruptedException e) {
        logger.error("Failed to fetch result async, connect to {}", sourceInfo, e);
        return null;
      }
    }
    return fetchResult.get();
  }

  private Map<String, ByteBuffer> fetchResultSync(List<String> paths) throws IOException {
    try (SyncDataClient curSyncClient =
        sourceInfo.getCurSyncClient(RaftServer.getReadOperationTimeoutMS())) {
      try {
        return curSyncClient.fetchMultSeries(
            sourceInfo.getHeader(), sourceInfo.getReaderId(), paths);
      } catch (TException e) {
        // the connection may be broken, close it to avoid it being reused
        curSyncClient.getInputProtocol().getTransport().close();
        logger.error("Failed to fetch result sync, connect to {}", sourceInfo, e);
      }
    }
    return null;
  }

  /** select path, which could batch-fetch result */
  interface BatchStrategy {
    List<String> selectBatchPaths(Map<String, Queue<BatchData>> cacheBatchs);
  }

  static class DefaultBatchStrategy implements BatchStrategy {

    @Override
    public List<String> selectBatchPaths(Map<String, Queue<BatchData>> cacheBatchs) {
      List<String> paths = Lists.newArrayList();

      for (String path : cacheBatchs.keySet()) {
        Queue<BatchData> batchDataQueue = cacheBatchs.get(path);
        BatchData batchData = batchDataQueue.peek();

        // data read finished, so can not batch get data
        if (batchData != null && batchData.isEmpty()) {
          continue;
        }

        if (batchDataQueue.size() < FETCH_BATCH_DATA_SIZE) {
          paths.add(path);
        }
      }
      return paths;
    }
  }
}
