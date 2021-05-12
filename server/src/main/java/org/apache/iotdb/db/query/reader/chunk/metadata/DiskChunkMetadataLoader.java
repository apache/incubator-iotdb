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
package org.apache.iotdb.db.query.reader.chunk.metadata;

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkMetadataLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DiskChunkMetadataLoader implements IChunkMetadataLoader {

  private final TsFileResource resource;
  private final PartialPath seriesPath;
  private final QueryContext context;
  // time filter or value filter, only used to check time range
  private final Filter filter;

  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");

  public DiskChunkMetadataLoader(
      TsFileResource resource, PartialPath seriesPath, QueryContext context, Filter filter) {
    this.resource = resource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.filter = filter;
  }

  @Override
  public List<IChunkMetadata> loadChunkMetadataList(ITimeSeriesMetadata timeseriesMetadata) {
    List<IChunkMetadata> chunkMetadataList = timeseriesMetadata.getChunkMetadataList();

    setDiskChunkLoader(chunkMetadataList, resource, seriesPath, context);

    /*
     * remove not satisfied ChunkMetaData
     */
    chunkMetadataList.removeIf(
        chunkMetaData ->
            (filter != null
                    && !filter.satisfyStartEndTime(
                        chunkMetaData.getStartTime(), chunkMetaData.getEndTime()))
                || chunkMetaData.getStartTime() > chunkMetaData.getEndTime());

    // For chunkMetadata from old TsFile, do not set version
    for (IChunkMetadata metadata : chunkMetadataList) {
      if (!metadata.isFromOldTsFile()) {
        metadata.setVersion(resource.getVersion());
      }
    }

    if (context.isDebug()) {
      DEBUG_LOGGER.info("After removed by filter Chunk meta data list is: ");
      chunkMetadataList.forEach(c -> DEBUG_LOGGER.info(c.toString()));
    }

    return chunkMetadataList;
  }

  @Override
  public boolean isMemChunkMetadataLoader() {
    return false;
  }

  public static void setDiskChunkLoader(
      List<IChunkMetadata> chunkMetadataList,
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context) {
    List<Modification> pathModifications =
        context.getPathModifications(resource.getModFile(), seriesPath);

    if (context.isDebug()) {
      DEBUG_LOGGER.info(
          "Modifications size is {} for file Path: {} ",
          pathModifications.size(),
          resource.getTsFilePath());
      pathModifications.forEach(c -> DEBUG_LOGGER.info(c.toString()));
    }

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(chunkMetadataList, pathModifications);
    }

    if (context.isDebug()) {
      DEBUG_LOGGER.info("After modification Chunk meta data list is: ");
      chunkMetadataList.forEach(c -> DEBUG_LOGGER.info(c.toString()));
    }

    // it is ok, even if it is not thread safe, because the cost of creating a DiskChunkLoader is
    // very cheap.
    chunkMetadataList.forEach(
        chunkMetadata -> {
          if (chunkMetadata.getChunkLoader() == null) {
            chunkMetadata.setFile(resource.getTsFile());
            chunkMetadata.setClosed(resource.isClosed());
            chunkMetadata.setChunkLoader(new DiskChunkLoader(context));
          }
        });
  }
}
