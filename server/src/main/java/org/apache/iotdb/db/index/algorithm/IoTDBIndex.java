/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.algorithm;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.index.common.DistSeries;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.feature.IndexFeatureExtractor;
import org.apache.iotdb.db.index.read.IndexQueryDataSet;
import org.apache.iotdb.db.index.read.optimize.IIndexCandidateOrderOptimize;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * For index developers, the indexing framework aims to provide a simple and friendly platform and
 * shield the complex details in other modules.
 *
 * <p>To add a new index methods, developers need inherit {@linkplain IoTDBIndex} or its subclass.
 */
public abstract class IoTDBIndex {

  protected final PartialPath indexSeries;
  protected final IndexType indexType;
  protected final TSDataType tsDataType;

  protected final Map<String, String> props;
  protected IndexFeatureExtractor indexFeatureExtractor;

  protected IoTDBIndex(PartialPath indexSeries, TSDataType tsDataType, IndexInfo indexInfo) {
    this.indexSeries = indexSeries;
    this.indexType = indexInfo.getIndexType();
    this.props = indexInfo.getProps();
    this.tsDataType = tsDataType;
  }

  /**
   * An index should determine which FeatureExtractor it uses and hook it to {@linkplain
   * IoTDBIndex}.indexFeatureExtractor. This method is called when IoTDBIndex is created.
   *
   * @param previous the status data saved in the last closing of the FeatureExtractor
   * @param inQueryMode true if it's during index query, false if it's during index building
   */
  public abstract void initFeatureExtractor(ByteBuffer previous, boolean inQueryMode);

  /** A new item has been pre-processed by the FeatureExtractor, now the index can insert it. */
  public abstract boolean buildNext() throws IndexManagerException;

  /** This index will be closed, it's time to serialize in-memory data to disk for next open. */
  protected abstract void flushIndex();

  /**
   * execute index query and return the result.
   *
   * @param queryProps query conditions
   * @param indexUsable the information of index usability
   * @param context query context provided by IoTDB.
   * @param candidateOrderOptimize an optimizer for the order of visiting candidates
   * @param alignedByTime true if the result series need to aligned by timestamp, otherwise they
   *     will be aligned by their first points
   * @return the result should be consistent with other IoTDB query result.
   */
  public abstract QueryDataSet query(
      Map<String, Object> queryProps,
      IIndexUsable indexUsable,
      QueryContext context,
      IIndexCandidateOrderOptimize candidateOrderOptimize,
      boolean alignedByTime)
      throws QueryIndexException;

  /**
   * In current design, the index building (insert data) only occurs in the memtable flush. When
   * this method is called, a batch of raw data is coming.
   *
   * @param tvList tvList to insert
   * @return FeatureExtractor filled with the given raw data
   */
  public IndexFeatureExtractor startFlushTask(PartialPath partialPath, TVList tvList) {
    this.indexFeatureExtractor.appendNewSrcData(tvList);
    return indexFeatureExtractor;
  }

  /** The flush task has ended. */
  public void endFlushTask() {
    indexFeatureExtractor.clearProcessedSrcData();
  }

  /** Close the index, release resources of the index structure and the feature extractor. */
  public ByteBuffer closeAndRelease() throws IOException {
    flushIndex();
    if (indexFeatureExtractor != null) {
      return indexFeatureExtractor.closeAndRelease();
    } else {
      return ByteBuffer.allocate(0);
    }
  }

  public TSDataType getTsDataType() {
    return tsDataType;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public String toString() {
    return indexType.toString();
  }

  protected QueryDataSet constructSearchDataset(List<DistSeries> res, boolean alignedByTime)
      throws QueryIndexException {
    return constructSearchDataset(
        res, alignedByTime, IoTDBDescriptor.getInstance().getConfig().getMaxIndexQueryResultSize());
  }

  protected QueryDataSet constructSearchDataset(
      List<DistSeries> res, boolean alignedByTime, int numMaxReturnSeries)
      throws QueryIndexException {
    if (alignedByTime) {
      throw new QueryIndexException("Unsupported alignedByTime result");
    }
    // make result paths and types
    numMaxReturnSeries = Math.min(numMaxReturnSeries, res.size());
    List<PartialPath> paths = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    Map<String, Integer> pathToIndex = new HashMap<>();
    int numMinLength = Integer.MAX_VALUE;
    for (int i = 0; i < numMaxReturnSeries; i++) {
      PartialPath series = res.get(i).partialPath;
      paths.add(series);
      pathToIndex.put(series.getFullPath(), i);
      types.add(tsDataType);
      if (res.get(i).tvList.size() < numMinLength) {
        numMinLength = res.get(i).tvList.size();
      }
    }
    IndexQueryDataSet dataSet = new IndexQueryDataSet(paths, types, pathToIndex);
    if (numMaxReturnSeries == 0) {
      return dataSet;
    }
    for (int row = 0; row < numMinLength; row++) {
      RowRecord rowRecord = new RowRecord(row);
      for (int col = 0; col < numMaxReturnSeries; col++) {
        TVList tvList = res.get(col).tvList;
        rowRecord.addField(IndexUtils.getValue(tvList, row), tsDataType);
      }
      dataSet.putRecord(rowRecord);
    }
    return dataSet;
  }
}
