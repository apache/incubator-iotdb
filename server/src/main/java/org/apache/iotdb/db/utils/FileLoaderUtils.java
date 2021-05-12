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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunk.MemChunkReader;
import org.apache.iotdb.db.query.reader.chunk.metadata.DiskChunkMetadataLoader;
import org.apache.iotdb.db.query.reader.chunk.metadata.MemChunkMetadataLoader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorTimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.VectorChunkReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class FileLoaderUtils {

  private FileLoaderUtils() {}

  public static void checkTsFileResource(TsFileResource tsFileResource) throws IOException {
    if (!tsFileResource.resourceFileExists()) {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFile())) {
        updateTsFileResource(reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    } else {
      tsFileResource.deserialize();
    }
    tsFileResource.setClosed(true);
  }

  public static void updateTsFileResource(
      TsFileSequenceReader reader, TsFileResource tsFileResource) throws IOException {
    for (Entry<String, List<TimeseriesMetadata>> entry :
        reader.getAllTimeseriesMetadata().entrySet()) {
      for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
        tsFileResource.updateStartTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(
            entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
      }
    }
    tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
  }

  /**
   * @param resource TsFile
   * @param seriesPath Timeseries path
   * @param allSensors measurements queried at the same time of this device
   * @param filter any filter, only used to check time range
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static ITimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    // deal with vector
    if (seriesPath instanceof VectorPartialPath) {
      return loadVectorTimeSeriesMetadata(
          resource,
          seriesPath,
          ((VectorPartialPath) seriesPath).getSubSensorsPathList(),
          context,
          filter,
          allSensors);
    }

    // common path
    ITimeSeriesMetadata timeSeriesMetadata;
    if (resource.isClosed()) {
      if (!resource.getTsFile().exists()) {
        return null;
      }
      timeSeriesMetadata =
          TimeSeriesMetadataCache.getInstance()
              .get(
                  new TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey(
                      resource.getTsFile(), seriesPath.getDevice(), seriesPath.getMeasurement()),
                  allSensors,
                  context.isDebug());
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata.setChunkMetadataLoader(
            new DiskChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    } else {
      timeSeriesMetadata = resource.getTimeSeriesMetadata();
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata.setChunkMetadataLoader(
            new MemChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    }

    if (timeSeriesMetadata != null) {
      List<Modification> pathModifications =
          context.getPathModifications(resource.getModFile(), seriesPath);
      timeSeriesMetadata.setModified(!pathModifications.isEmpty());
      if (timeSeriesMetadata.getStatistics().getStartTime()
          > timeSeriesMetadata.getStatistics().getEndTime()) {
        return null;
      }
      if (filter != null
          && !filter.satisfyStartEndTime(
              timeSeriesMetadata.getStatistics().getStartTime(),
              timeSeriesMetadata.getStatistics().getEndTime())) {
        return null;
      }
    }
    return timeSeriesMetadata;
  }

  private static VectorTimeSeriesMetadata loadVectorTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      List<PartialPath> subSensorList,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    VectorTimeSeriesMetadata vectorTimeSeriesMetadata = null;
    if (resource.isClosed()) {
      if (!resource.getTsFile().exists()) {
        return null;
      }
      List<TimeseriesMetadata> timeSeriesMetadata =
          TimeSeriesMetadataCache.getInstance()
              .get(
                  new TimeSeriesMetadataCache.TimeSeriesMetadataCacheKey(
                      resource.getTsFilePath(),
                      seriesPath.getDevice(),
                      seriesPath.getMeasurement()),
                  subSensorList.stream()
                      .map(PartialPath::getMeasurement)
                      .collect(Collectors.toList()),
                  allSensors,
                  context.isDebug());
      if (timeSeriesMetadata != null) {
        timeSeriesMetadata
            .get(0)
            .setChunkMetadataLoader(
                new DiskChunkMetadataLoader(resource, seriesPath, context, filter));
        for (int i = 1; i < timeSeriesMetadata.size(); i++) {
          timeSeriesMetadata
              .get(i)
              .setChunkMetadataLoader(
                  new DiskChunkMetadataLoader(resource, subSensorList.get(i - 1), context, filter));
        }
        vectorTimeSeriesMetadata =
            new VectorTimeSeriesMetadata(
                timeSeriesMetadata.get(0),
                timeSeriesMetadata.subList(1, timeSeriesMetadata.size()));
      }
    } else {
      vectorTimeSeriesMetadata = (VectorTimeSeriesMetadata) resource.getTimeSeriesMetadata();
      if (vectorTimeSeriesMetadata != null) {
        vectorTimeSeriesMetadata.setChunkMetadataLoader(
            new MemChunkMetadataLoader(resource, seriesPath, context, filter));
      }
    }

    if (vectorTimeSeriesMetadata != null) {
      List<Modification> pathModifications =
          context.getPathModifications(resource.getModFile(), seriesPath);
      vectorTimeSeriesMetadata.getTimeseriesMetadata().setModified(!pathModifications.isEmpty());
      if (vectorTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getStartTime()
          > vectorTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getEndTime()) {
        return null;
      }
      if (filter != null
          && !filter.satisfyStartEndTime(
              vectorTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getStartTime(),
              vectorTimeSeriesMetadata.getTimeseriesMetadata().getStatistics().getEndTime())) {
        return null;
      }
      List<TimeseriesMetadata> valueTimeSeriesMetadataList =
          vectorTimeSeriesMetadata.getValueTimeseriesMetadataList();
      for (int i = 0; i < valueTimeSeriesMetadataList.size(); i++) {
        pathModifications =
            context.getPathModifications(resource.getModFile(), subSensorList.get(i));
        valueTimeSeriesMetadataList.get(i).setModified(!pathModifications.isEmpty());
      }
    }
    return vectorTimeSeriesMetadata;
  }

  /**
   * load all chunk metadata of one time series in one file.
   *
   * @param timeSeriesMetadata the corresponding TimeSeriesMetadata in that file.
   */
  public static List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeSeriesMetadata)
      throws IOException {
    return timeSeriesMetadata.loadChunkMetadataList();
  }

  /**
   * load all page readers in one chunk that satisfying the timeFilter
   *
   * @param chunkMetaData the corresponding chunk metadata
   * @param timeFilter it should be a TimeFilter instead of a ValueFilter
   */
  public static List<IPageReader> loadPageReaderList(
      IChunkMetadata chunkMetaData, Filter timeFilter) throws IOException {
    if (chunkMetaData == null) {
      throw new IOException("Can't init null chunkMeta");
    }
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = chunkMetaData.getChunkLoader();
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), timeFilter);
    } else {
      if (chunkMetaData instanceof ChunkMetadata) {
        Chunk chunk = chunkLoader.loadChunk((ChunkMetadata) chunkMetaData);
        chunk.setFromOldFile(chunkMetaData.isFromOldTsFile());
        chunkReader = new ChunkReader(chunk, timeFilter);
        chunkReader.hasNextSatisfiedPage();
      } else {
        VectorChunkMetadata vectorChunkMetadata = (VectorChunkMetadata) chunkMetaData;
        Chunk timeChunk = vectorChunkMetadata.getTimeChunk();
        List<Chunk> valueChunkList = vectorChunkMetadata.getValueChunkList();
        chunkReader = new VectorChunkReader(timeChunk, valueChunkList, timeFilter);
      }
    }
    return chunkReader.loadPageReaderList();
  }

  public static List<IChunkMetadata> getChunkMetadataList(Path path, File file) throws IOException {
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(file, true);
    return new ArrayList<>(tsFileReader.getChunkMetadataList(path));
  }
}
