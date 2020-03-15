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

import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileLoaderUtils {

  public static void checkTsFileResource(TsFileResource tsFileResource) throws IOException {
    if (!tsFileResource.fileExists()) {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader = new TsFileSequenceReader(
          tsFileResource.getFile().getAbsolutePath())) {
        TsFileMetadata metaData = reader.readFileMetadata();
        updateTsFileResource(metaData, reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    } else {
      tsFileResource.deserialize();
    }
  }

  public static void updateTsFileResource(TsFileMetadata metaData, TsFileSequenceReader reader,
      TsFileResource tsFileResource) throws IOException {
    for (String device : metaData.getDeviceMetadataIndex().keySet()) {
      Map<String, TimeseriesMetadata> chunkMetadataListInOneDevice = reader
          .readDeviceMetadata(device);
      for (TimeseriesMetadata timeseriesMetaData : chunkMetadataListInOneDevice.values()) {
        tsFileResource.updateStartTime(device, timeseriesMetaData.getStatistics().getStartTime());
        tsFileResource.updateEndTime(device, timeseriesMetaData.getStatistics().getEndTime());
      }
    }
  }

  public static TimeseriesMetadata loadTimeSeriesMetadata(TsFileResource resource, Path seriesPath, QueryContext context) throws IOException {
    if (resource.isClosed()) {
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(resource.getPath(), resource.isClosed());
      TimeseriesMetadata timeseriesMetadata = reader.readDeviceMetadata(seriesPath.getDevice()).get(seriesPath.getMeasurement());
      return timeseriesMetadata;
    } else {
      List<Modification> pathModifications =
              context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());

      if (resource.getTimeSeriesMetadata() != null) {
        if (!pathModifications.isEmpty()) {
          resource.getTimeSeriesMetadata().setCanUseStatistics(false);
        } else {
          resource.getTimeSeriesMetadata().setCanUseStatistics(true);
        }
      }
      return resource.getTimeSeriesMetadata();

    }
  }

  /**
   * load all ChunkMetadatas belong to the seriesPath and satisfy filter
   */
  public static List<ChunkMetadata> loadChunkMetadataFromTsFileResource(
      TsFileResource resource, Path seriesPath, QueryContext context, Filter timeFilter) throws IOException {
    List<ChunkMetadata> chunkMetadataList = loadChunkMetadataFromTsFileResource(resource, seriesPath, context);

    /*
     * remove not satisfied ChunkMetaData
     */
    chunkMetadataList.removeIf(chunkMetaData -> (timeFilter != null && !timeFilter
        .satisfyStartEndTime(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
        || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());
    return chunkMetadataList;
  }

  /**
   * load all ChunkMetadatas belong to the seriesPath
   */
  public static List<ChunkMetadata> loadChunkMetadataFromTsFileResource(
      TsFileResource resource, Path seriesPath, QueryContext context) throws IOException {
    List<ChunkMetadata> chunkMetadataList;
    if (resource == null) {
      return new ArrayList<>();
    }
    if (resource.isClosed()) {
      chunkMetadataList = DeviceMetaDataCache.getInstance().get(resource, seriesPath);
    } else {
      chunkMetadataList = resource.getChunkMetadataList();
    }
    List<Modification> pathModifications =
        context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(chunkMetadataList, pathModifications);
    }

    TsFileSequenceReader tsFileSequenceReader =
            FileReaderManager.getInstance().get(resource.getPath(), resource.isClosed());
    for (ChunkMetadata data : chunkMetadataList) {
      data.setChunkLoader(new DiskChunkLoader(tsFileSequenceReader));
    }
    List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk();
    if (memChunks != null) {
      for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
        if (!memChunks.isEmpty()) {
          chunkMetadataList.add(readOnlyMemChunk.getChunkMetaData());
        }
      }
    }

    /*
     * remove empty or invalid chunk metadata
     */
    chunkMetadataList.removeIf(chunkMetaData -> (
        chunkMetaData.getStartTime() > chunkMetaData.getEndTime()));
    return chunkMetadataList;
  }
}
