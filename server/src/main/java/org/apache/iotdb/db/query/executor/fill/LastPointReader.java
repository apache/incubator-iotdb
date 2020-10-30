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
package org.apache.iotdb.db.query.executor.fill;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class LastPointReader {

  private PartialPath seriesPath;
  long queryTime;
  TSDataType dataType;
  private QueryContext context;

  // measurements of the same device as "seriesPath"
  private Set<String> deviceMeasurements;

  private Filter timeFilter;

  private QueryDataSource dataSource;

  private List<TimeseriesMetadata> unseqTimeseriesMetadataList = new ArrayList<>();;

  public LastPointReader() {

  }

  public LastPointReader(PartialPath seriesPath, TSDataType dataType, Set<String> deviceMeasurements,
      QueryContext context, QueryDataSource dataSource, long queryTime, Filter timeFilter) {
    this.seriesPath = seriesPath;
    this.dataType = dataType;
    this.dataSource = dataSource;
    this.context = context;
    this.queryTime = queryTime;
    this.deviceMeasurements = deviceMeasurements;
    this.timeFilter = timeFilter;
  }

  public TimeValuePair readLastPoint() throws IOException {
    TimeValuePair lastPointResult = retrieveValidLastPointFromSeqFiles();
    UnpackOverlappedUnseqFiles(lastPointResult.getTimestamp());

    long lastVersion = 0;
    PriorityQueue<ChunkMetadata> sortedChunkMetatdataList = sortUnseqChunkMetadatasByEndtime();
    while (!sortedChunkMetatdataList.isEmpty()
        && lastPointResult.getTimestamp() <= sortedChunkMetatdataList.peek().getEndTime()) {
      ChunkMetadata chunkMetadata = sortedChunkMetatdataList.poll();
      TimeValuePair lastChunkPoint = getChunkLastPoint(chunkMetadata);
      if (shouldUpdate(lastPointResult.getTimestamp(), lastVersion,
          lastChunkPoint.getTimestamp(), chunkMetadata.getVersion())) {
        lastPointResult = lastChunkPoint;
        lastVersion = chunkMetadata.getVersion();
      }
    }
    return lastPointResult;
  }

  /** Pick up and cache the last sequence TimeseriesMetadata that satisfies timeFilter */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private TimeValuePair retrieveValidLastPointFromSeqFiles() throws IOException {
    List<TsFileResource> seqFileResource = dataSource.getSeqResources();
    TimeValuePair lastPoint = new TimeValuePair(Long.MIN_VALUE, null);
    for (int index = seqFileResource.size() - 1; index >= 0; index--) {
      TsFileResource resource = seqFileResource.get(index);
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              resource, seriesPath, context, timeFilter, deviceMeasurements);
      if (timeseriesMetadata != null) {
        if (endtimeContainedByTimeFilter(timeseriesMetadata.getStatistics())) {
          return constructLastPair(
              timeseriesMetadata.getStatistics().getEndTime(),
              timeseriesMetadata.getStatistics().getLastValue(),
              dataType);
        } else {
          List<ChunkMetadata> seqChunkMetadataList = timeseriesMetadata.loadChunkMetadataList();
          for (int i = seqChunkMetadataList.size() - 1; i >= 0; i--) {
            lastPoint = getChunkLastPoint(seqChunkMetadataList.get(i));
            // last point of this sequence chunk is valid, quit the loop
            if (lastPoint.getValue() != null) {
              return lastPoint;
            }
          }
        }
      }
    }

    return lastPoint;
  }

  /**
   * find the last TimeseriesMetadata in unseq files and unpack all overlapped unseq files
   */
  private void UnpackOverlappedUnseqFiles(long lBoundTime) throws IOException {
    PriorityQueue<TsFileResource> unseqFileResource =
        sortUnSeqFileResourcesInDecendingOrder(dataSource.getUnseqResources());

    while (!unseqFileResource.isEmpty()
        && (lBoundTime <= unseqFileResource.peek().getEndTime(seriesPath.getDevice()))) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              unseqFileResource.poll(), seriesPath, context, timeFilter, deviceMeasurements);

      if (timeseriesMetadata == null || (!timeseriesMetadata.isModified()
          && timeseriesMetadata.getStatistics().getEndTime() < lBoundTime)) {
        continue;
      }
      unseqTimeseriesMetadataList.add(timeseriesMetadata);
      if (!timeseriesMetadata.isModified()) {
        if (endtimeContainedByTimeFilter(timeseriesMetadata.getStatistics())) {
          lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getEndTime());
        } else {
          lBoundTime = Math.max(lBoundTime, timeseriesMetadata.getStatistics().getStartTime());
        }
      }
    }
  }

  private TimeValuePair getChunkLastPoint(ChunkMetadata chunkMetaData) throws IOException {
    TimeValuePair lastPoint = new TimeValuePair(Long.MIN_VALUE, null);
    if (chunkMetaData == null) {
      return lastPoint;
    }
    Statistics chunkStatistics = chunkMetaData.getStatistics();

    if (!chunkMetaData.isModified() && endtimeContainedByTimeFilter(chunkStatistics)) {
      return constructLastPair(
          chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), dataType);
    }
    List<IPageReader> pageReaders = FileLoaderUtils.loadPageReaderList(chunkMetaData, timeFilter);
    for (int i = pageReaders.size() - 1; i >= 0; i--) {
      IPageReader pageReader = pageReaders.get(i);
      Statistics pageStatistics = pageReader.getStatistics();
      if (!pageReader.isModified() && endtimeContainedByTimeFilter(pageStatistics)) {
        lastPoint = constructLastPair(
            pageStatistics.getEndTime(), pageStatistics.getLastValue(), dataType);
      } else {
        BatchData batchData = pageReader.getAllSatisfiedPageData();
        lastPoint = batchData.getLastPairBeforeOrEqualTimestamp(queryTime);
      }
      if (lastPoint.getValue() != null) {
        return lastPoint;
      }
    }
    return lastPoint;
  }

  private boolean shouldUpdate(long time, long version, long newTime, long newVersion) {
    return time < newTime || (time == newTime && version < newVersion);
  }

  private PriorityQueue<TsFileResource> sortUnSeqFileResourcesInDecendingOrder(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              Long maxTimeOfO1 = o1.getEndTime(seriesPath.getDevice());
              Long maxTimeOfO2 = o2.getEndTime(seriesPath.getDevice());
              return Long.compare(maxTimeOfO2, maxTimeOfO1);
            });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }

  private PriorityQueue<ChunkMetadata> sortUnseqChunkMetadatasByEndtime() throws IOException {
    PriorityQueue<ChunkMetadata> chunkMetadataList =
        new PriorityQueue<>(
            (o1, o2) -> {
              long endTime1 = o1.getEndTime();
              long endTime2 = o2.getEndTime();
              if (endTime1 < endTime2) {
                return 1;
              } else if (endTime1 > endTime2) {
                return -1;
              }
              return Long.compare(o2.getVersion(), o1.getVersion());
            });
    for (TimeseriesMetadata timeseriesMetadata : unseqTimeseriesMetadataList) {
      if (timeseriesMetadata != null) {
        chunkMetadataList.addAll(timeseriesMetadata.loadChunkMetadataList());
      }
    }
    return chunkMetadataList;
  }

  private boolean endtimeContainedByTimeFilter(Statistics statistics) {
    if (timeFilter == null) {
      return true;
    }
    return timeFilter.containStartEndTime(statistics.getEndTime(), statistics.getEndTime());
  }

  private TimeValuePair constructLastPair(long timestamp, Object value, TSDataType dataType) {
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, value));
  }
}
