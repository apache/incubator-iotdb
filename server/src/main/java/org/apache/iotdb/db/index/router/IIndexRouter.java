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
package org.apache.iotdb.db.index.router;

import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexProcessorStruct;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;

import java.io.IOException;
import java.util.Map;

/**
 * Singleton pattern.
 *
 * <p>Firstly, IIndexRouter is responsible for index metadata management. More importantly, it is
 * for routing the create/drop/insert/query command to corresponding index processors.
 *
 * <p>IIndexRouter can decouple the mapping relationship, which may be re-designed in future,
 * between {@link org.apache.iotdb.db.index.algorithm.IoTDBIndex IoTDBIndex} and {@link
 * IndexProcessor} from {@link org.apache.iotdb.db.index.IndexManager IndexManager}.
 */
public interface IIndexRouter {

  /**
   * add an new index into the router.
   *
   * @param prefixPath the partial path of given index series
   * @param indexInfo the index infomation.
   * @param func a function to create a new IndexProcessor, if it's not created before.
   * @param doSerialize true to serialize the new information immediately.
   * @return true if adding index information successfully
   */
  boolean addIndexIntoRouter(
      PartialPath prefixPath,
      IndexInfo indexInfo,
      CreateIndexProcessorFunc func,
      boolean doSerialize)
      throws MetadataException;

  /**
   * remove an exist index into the router.
   *
   * @param prefixPath the partial path of given index series
   * @param indexType the type of index to be removed.
   * @return true if removing index information successfully
   */
  boolean removeIndexFromRouter(PartialPath prefixPath, IndexType indexType)
      throws MetadataException, IOException;

  Map<IndexType, IndexInfo> getIndexInfosByIndexSeries(PartialPath indexSeries);

  Iterable<IndexProcessorStruct> getAllIndexProcessorsAndInfo();

  Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath timeSeries);

  /**
   * serialize all index information and processors to the disk
   *
   * @param doClose true if close processors after serialization.
   */
  void serialize(boolean doClose);

  /** deserialize all index information and processors into the memory */
  void deserializeAndReload(CreateIndexProcessorFunc func);

  /**
   * return a subset of the original IIndexRouter for accessing concurrency
   *
   * @param storageGroupPath the path of a storageGroup
   * @return a subset of the original IIndexRouter
   */
  IIndexRouter getRouterByStorageGroup(String storageGroupPath);

  int getIndexNum();

  /**
   * prepare necessary information for index query
   *
   * @param partialPath the query path
   * @param indexType the index type
   * @param context the query context
   * @return the necessary information for this query
   */
  IndexProcessorStruct startQueryAndCheck(
      PartialPath partialPath, IndexType indexType, QueryContext context)
      throws QueryIndexException;

  /**
   * do something when the query end
   *
   * @param indexSeries the query path
   * @param indexType the index type
   * @param context the query context
   */
  void endQuery(PartialPath indexSeries, IndexType indexType, QueryContext context);

  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexRouter getIndexRouter(String routerDir) {
      return new ProtoIndexRouter(routerDir);
    }
  }
}
