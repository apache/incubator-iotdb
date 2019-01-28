/**
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
package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.control.QueryDataSourceManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutTimeGenerator;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * IoTDB query executor with global time filter.
 */
public class EngineExecutorWithoutTimeGenerator {

  private QueryExpression queryExpression;
  private long jobId;

  public EngineExecutorWithoutTimeGenerator(long jobId, QueryExpression queryExpression) {
    this.jobId = jobId;
    this.queryExpression = queryExpression;
  }

  /**
   * with global time filter.
   */
  public QueryDataSet executeWithGlobalTimeFilter()
      throws FileNodeManagerException {

    Filter timeFilter = ((GlobalTimeExpression) queryExpression.getExpression()).getFilter();

    List<IReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    QueryTokenManager.getInstance()
        .beginQueryOfGivenQueryPaths(jobId, queryExpression.getSelectedSeries());

    for (Path path : queryExpression.getSelectedSeries()) {

      QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, path);

      // add data type
      try {
        dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
      } catch (PathErrorException e) {
        throw new FileNodeManagerException(e);
      }

      PriorityMergeReader priorityReader = new PriorityMergeReader();

      // sequence reader for one sealed tsfile
      SequenceDataReader tsFilesReader = null;
      try {
        tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(),
            timeFilter);
        priorityReader.addReaderWithPriority(tsFilesReader, PriorityMergeReader.LOW_PRIORITY);
      } catch (IOException e) {
        throw new FileNodeManagerException(e);
      }

      // unseq reader for all chunk groups in unSeqFile
      PriorityMergeReader unSeqMergeReader = null;
      try {
        unSeqMergeReader = SeriesReaderFactory.getInstance()
            .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);
        priorityReader.addReaderWithPriority(unSeqMergeReader, PriorityMergeReader.HIGH_PRIORITY);
      } catch (IOException e) {
        throw new FileNodeManagerException(e);
      }

      readersOfSelectedSeries.add(priorityReader);
    }

    try {
      return new EngineDataSetWithoutTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
          readersOfSelectedSeries);
    } catch (IOException e) {
      throw new FileNodeManagerException(e);
    }
  }

  /**
   * without filter.
   */
  public QueryDataSet executeWithoutFilter()
      throws FileNodeManagerException {

    List<IReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    QueryTokenManager.getInstance()
        .beginQueryOfGivenQueryPaths(jobId, queryExpression.getSelectedSeries());

    for (Path path : queryExpression.getSelectedSeries()) {

      QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(jobId, path);

      // add data type
      try {
        dataTypes.add(MManager.getInstance().getSeriesType(path.getFullPath()));
      } catch (PathErrorException e) {
        throw new FileNodeManagerException(e);
      }

      PriorityMergeReader priorityReader = new PriorityMergeReader();

      // sequence insert data
      SequenceDataReader tsFilesReader = null;
      try {
        tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(),
            null);
        priorityReader.addReaderWithPriority(tsFilesReader, 1);
      } catch (IOException e) {
        throw new FileNodeManagerException(e);
      }

      // unseq insert data
      PriorityMergeReader unSeqMergeReader = null;
      try {
        unSeqMergeReader = SeriesReaderFactory.getInstance()
            .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), null);
        priorityReader.addReaderWithPriority(unSeqMergeReader, 2);
      } catch (IOException e) {
        throw new FileNodeManagerException(e);
      }

      readersOfSelectedSeries.add(priorityReader);
    }

    try {
      return new EngineDataSetWithoutTimeGenerator(queryExpression.getSelectedSeries(), dataTypes,
          readersOfSelectedSeries);
    } catch (IOException e) {
      throw new FileNodeManagerException(e);
    }
  }

}
