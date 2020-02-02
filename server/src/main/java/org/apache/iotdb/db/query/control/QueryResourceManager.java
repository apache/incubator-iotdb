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
package org.apache.iotdb.db.query.control;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.externalsort.serialize.IExternalSortFileDeserializer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>
 * QueryResourceManager manages resource (file streams) used by each query job, and assign Ids to
 * the jobs. During the life cycle of a query, the following methods must be called in strict order:
 * 1. assignQueryId - get an Id for the new query. 2. getQueryDataSource - open files for the job or
 * reuse existing readers. 3. endQueryForGivenJob - release the resource used by this job.
 * </p>
 */
public class QueryResourceManager {

  private AtomicLong queryIdAtom = new AtomicLong();
  private QueryFileManager filePathsManager;
  /**
   * Record temporary files used for external sorting.
   * <p>
   * Key: query job id. Value: temporary file list used for external sorting.
   */
  private Map<Long, List<IExternalSortFileDeserializer>> externalSortFileMap;
  private Map<String, TsFileSequenceReader> fileSequenceReaderMap;

  private QueryResourceManager() {
    filePathsManager = new QueryFileManager();
    externalSortFileMap = new ConcurrentHashMap<>();
    fileSequenceReaderMap = new ConcurrentHashMap<>();
  }

  public static QueryResourceManager getInstance() {
    return QueryTokenManagerHelper.INSTANCE;
  }

  /**
   * Register a new query. When a query request is created firstly, this method must be invoked.
   */
  public long assignQueryId(boolean isDataQuery) {
    long queryId = queryIdAtom.incrementAndGet();
    if (isDataQuery) {
      filePathsManager.addQueryId(queryId);
    }
    return queryId;
  }

  public TsFileSequenceReader getTsFileSequenceReader(String fileAbsolutePath) throws IOException {
    fileSequenceReaderMap.putIfAbsent(fileAbsolutePath, new TsFileSequenceReader(fileAbsolutePath));
    return fileSequenceReaderMap.get(fileAbsolutePath);
  }

  public void releaseAllTsFileSequenceReader() {
    fileSequenceReaderMap.forEach((k, v) -> {
      try {
        v.close();
      } catch (IOException e) {
      }
    });
  }

  /**
   * register temporary file generated by external sort for resource release.
   *
   * @param queryId      query job id
   * @param deserializer deserializer of temporary file in external sort.
   */
  public void registerTempExternalSortFile(long queryId,
      IExternalSortFileDeserializer deserializer) {
    externalSortFileMap.computeIfAbsent(queryId, x -> new ArrayList<>()).add(deserializer);
  }


  public QueryDataSource getQueryDataSource(Path selectedPath,
      QueryContext context, Filter filter) throws StorageEngineException {

    SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(selectedPath,
        filter);
    return StorageEngine
        .getInstance().query(singleSeriesExpression, context, filePathsManager);
  }

  /**
   * Whenever the jdbc request is closed normally or abnormally, this method must be invoked. All
   * query tokens created by this jdbc request must be cleared.
   */
  public void endQuery(long queryId) throws StorageEngineException {
    // close file stream of external sort files, and delete
    if (externalSortFileMap.get(queryId) != null) {
      for (IExternalSortFileDeserializer deserializer : externalSortFileMap.get(queryId)) {
        try {
          deserializer.close();
        } catch (IOException e) {
          throw new StorageEngineException(e.getMessage());
        }
      }
      externalSortFileMap.remove(queryId);
    }
    // remove usage of opened file paths of current thread
    filePathsManager.removeUsedFilesForQuery(queryId);
  }

  private static class QueryTokenManagerHelper {

    private static final QueryResourceManager INSTANCE = new QueryResourceManager();

    private QueryTokenManagerHelper() {
    }
  }
}
