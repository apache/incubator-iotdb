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
package org.apache.iotdb.db.query.reader.seriesRelated;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;


public class RawDataBatchReader implements ManagedSeriesReader {

  private final SeriesReader seriesReader;

  private boolean hasRemaining;
  private boolean managedByQueryManager;

  private BatchData batchData;
  private boolean hasCachedBatchData = false;

  public RawDataBatchReader(SeriesReader seriesReader) {
    this.seriesReader = seriesReader;
  }

  public RawDataBatchReader(Path seriesPath, TSDataType dataType, QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter) {
    this.seriesReader = new SeriesReader(seriesPath, dataType, context, dataSource, timeFilter,
        valueFilter);
  }

  @TestOnly
  public RawDataBatchReader(Path seriesPath, TSDataType dataType, QueryContext context,
      List<TsFileResource> seqFileResource, List<TsFileResource> unseqFileResource,
      Filter timeFilter, Filter valueFilter) {
    this.seriesReader = new SeriesReader(seriesPath, dataType, context, seqFileResource,
        unseqFileResource, timeFilter, valueFilter);
  }

  /**
   * This method overrides the AbstractDataReader.hasNextOverlappedPage for pause reads, to achieve
   * a continuous read
   */
  @Override
  public boolean hasNextBatch() throws IOException {

    if (hasCachedBatchData) {
      return true;
    }

    while (seriesReader.hasNextChunk()) {
      while (seriesReader.hasNextPage()) {
        if (!seriesReader.isPageOverlapped()) {
          batchData = seriesReader.nextPage();
          hasCachedBatchData = true;
          return true;
        }
        if (seriesReader.hasNextOverlappedPage()) {
          batchData = seriesReader.nextOverlappedPage();
          hasCachedBatchData = true;
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    if (hasCachedBatchData || hasNextBatch()) {
      hasCachedBatchData = false;
      return batchData;
    }
    throw new IOException("no next batch");
  }

  @Override
  public void close() throws IOException {
  }


  @Override
  public boolean isManagedByQueryManager() {
    return managedByQueryManager;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    this.managedByQueryManager = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
  }

}
