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
package org.apache.iotdb.db.query.factory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSourceV2;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.SeriesReaderWithoutValueFilter;
import org.apache.iotdb.db.query.reader.SeriesReaderWithValueFilter;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.mem.MemChunkReader;
import org.apache.iotdb.db.query.reader.mem.MemChunkReaderByTimestamp;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.db.query.reader.merge.UnsequenceSeriesReader;
import org.apache.iotdb.db.query.reader.merge.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequenceSeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.sequence.SequentialSeriesReader;
import org.apache.iotdb.db.query.reader.unsequence.EngineChunkReader;
import org.apache.iotdb.db.query.reader.unsequence.EngineChunkReaderByTimestamp;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.common.constant.StatisticConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeriesReaderFactoryImpl implements ISeriesReaderFactory {

  private static final Logger logger = LoggerFactory.getLogger(SeriesReaderFactoryImpl.class);

  private SeriesReaderFactoryImpl() {
  }

  public static SeriesReaderFactoryImpl getInstance() {
    return SeriesReaderFactoryHelper.INSTANCE;
  }

  @Override
  public IPointReader createUnseqSeriesReader(Path seriesPath, List<TsFileResourceV2> unseqResources,
                                              QueryContext context,
                                              Filter filter) throws IOException {
    UnsequenceSeriesReader unseqMergeReader = new UnsequenceSeriesReader();

    int priorityValue = 1;

    for (TsFileResourceV2 tsFileResourceV2 : unseqResources) {

      // store only one opened file stream into manager, to avoid too many opened files
      TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
          .get(tsFileResourceV2.getFile().getPath(), tsFileResourceV2.isClosed());

      // get modified chunk metadatas
      List<ChunkMetaData> metaDataList;
      if (tsFileResourceV2.isClosed()) {
        MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
        metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
        // mod
        List<Modification> pathModifications = context
            .getPathModifications(tsFileResourceV2.getModFile(),
                seriesPath.getFullPath());
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
      } else {
        metaDataList = tsFileResourceV2.getChunkMetaDatas();
      }

      // add readers for chunks
      // TODO 下面这段对chunkmetadata过滤考虑复用
      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileReader);
      for (ChunkMetaData chunkMetaData : metaDataList) {

        DigestForFilter digest = new DigestForFilter(chunkMetaData.getStartTime(),
            chunkMetaData.getEndTime(),
            chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MIN_VALUE),
            chunkMetaData.getDigest().getStatistics().get(StatisticConstant.MAX_VALUE),
            chunkMetaData.getTsDataType());

        if (filter != null && !filter.satisfy(digest)) {
          continue;
        }

        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReader chunkReader = filter != null ? new ChunkReaderWithFilter(chunk, filter)
            : new ChunkReaderWithoutFilter(chunk);

        unseqMergeReader.addReaderWithPriority(new EngineChunkReader(chunkReader), priorityValue);

        priorityValue++;
      }

      // add reader for MemTable
      if (!tsFileResourceV2.isClosed()) {
        unseqMergeReader.addReaderWithPriority(
            new MemChunkReader(tsFileResourceV2.getReadOnlyMemChunk(), filter), priorityValue++);
      }
    }

    // TODO add external sort when needed
    return unseqMergeReader;
  }

  private SeriesReaderByTimestamp createUnseqSeriesReaderByTimestamp(Path seriesPath,
                                                                     List<TsFileResourceV2> unseqResources, QueryContext context) throws IOException {
    SeriesReaderByTimestamp unseqMergeReader = new SeriesReaderByTimestamp();

    int priorityValue = 1;

    for (TsFileResourceV2 tsFileResourceV2 : unseqResources) {

      // store only one opened file stream into manager, to avoid too many opened files
      TsFileSequenceReader tsFileReader = FileReaderManager.getInstance()
          .get(tsFileResourceV2.getFile().getPath(), tsFileResourceV2.isClosed());

      List<ChunkMetaData> metaDataList;
      if (tsFileResourceV2.isClosed()) {
        MetadataQuerierByFileImpl metadataQuerier = new MetadataQuerierByFileImpl(tsFileReader);
        metaDataList = metadataQuerier.getChunkMetaDataList(seriesPath);
        // mod
        List<Modification> pathModifications = context
            .getPathModifications(tsFileResourceV2.getModFile(),
                seriesPath.getFullPath());
        if (!pathModifications.isEmpty()) {
          QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
        }
      } else {
        metaDataList = tsFileResourceV2.getChunkMetaDatas();
      }

      // add reader for chunk
      ChunkLoaderImpl chunkLoader = new ChunkLoaderImpl(tsFileReader);
      for (ChunkMetaData chunkMetaData : metaDataList) {

        Chunk chunk = chunkLoader.getChunk(chunkMetaData);
        ChunkReaderByTimestamp chunkReader = new ChunkReaderByTimestamp(chunk);

        unseqMergeReader.addReaderWithPriority(new EngineChunkReaderByTimestamp(chunkReader),
            priorityValue);

        priorityValue++;
      }

      // add reader for MemTable
      if (!tsFileResourceV2.isClosed()) {
        unseqMergeReader.addReaderWithPriority(
            new MemChunkReaderByTimestamp(tsFileResourceV2.getReadOnlyMemChunk()), priorityValue++);
      }
    }

    // TODO add external sort when needed
    return unseqMergeReader;
  }

  @Override
  public List<EngineReaderByTimeStamp> createSeriesReadersByTimestamp(List<Path> paths,
                                                                      QueryContext context) throws FileNodeManagerException, IOException {
    List<EngineReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();

    for (Path path : paths) {

      QueryDataSourceV2 queryDataSource = null;
      try {
        queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSourceV2(path,
                context);
      } catch (ProcessorException e) {
        throw new FileNodeManagerException(e);
      }

      SeriesReaderByTimestamp mergeReaderByTimestamp = new SeriesReaderByTimestamp();

      // reader for sequence data
      SequenceSeriesReaderByTimestamp tsFilesReader = new SequenceSeriesReaderByTimestamp(path,
          queryDataSource.getSeqResources(), context);
      mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

      // reader for unsequence data
      SeriesReaderByTimestamp unseqMergeReader = createUnseqSeriesReaderByTimestamp(path,
          queryDataSource.getUnseqResources(), context);
      mergeReaderByTimestamp.addReaderWithPriority(unseqMergeReader, 2);

      readersOfSelectedSeries.add(mergeReaderByTimestamp);
    }

    return readersOfSelectedSeries;
  }

  @Override
  public IPointReader createSeriesReaderWithoutValueFilter(Path path, Filter timeFilter,
                                                           QueryContext context)
      throws FileNodeManagerException, IOException {
    QueryDataSourceV2 queryDataSource = null;
    try {
      queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSourceV2(path, context);
    } catch (ProcessorException e) {
      throw new FileNodeManagerException(e);
    }

    // sequence reader for one sealed tsfile
    SequentialSeriesReader tsFilesReader;

    tsFilesReader = new SequentialSeriesReader(queryDataSource.getSeriesPath(),
        queryDataSource.getSeqResources(),
        timeFilter, context);

    // unseq reader for all chunk groups in unseqFile
    IPointReader unseqMergeReader = null;
    unseqMergeReader = createUnseqSeriesReader(path, queryDataSource.getUnseqResources(), context,
        timeFilter);

    if (!tsFilesReader.hasNext()) {
      //only have unsequence data.
      return unseqMergeReader;
    } else {
      //merge sequence data with unsequence data.
      return new SeriesReaderWithoutValueFilter(tsFilesReader, unseqMergeReader);
    }
  }

  @Override
  public IPointReader createSeriesReaderWithValueFilter(Path path, Filter filter, QueryContext context)
      throws FileNodeManagerException, IOException {
    QueryDataSourceV2 queryDataSource = null;
    try {
      queryDataSource = QueryResourceManager.getInstance()
          .getQueryDataSourceV2(path, context);
    } catch (ProcessorException e) {
      throw new FileNodeManagerException(e);
    }

    // sequence reader for one sealed tsfile
    SequentialSeriesReader tsFilesReader;

    tsFilesReader = new SequentialSeriesReader(queryDataSource.getSeriesPath(),
        queryDataSource.getSeqResources(),
        filter, context);

    // unseq reader for all chunk groups in unseqFile. Filter for unseqMergeReader is null, because
    // we won't push down filter in unsequence data source.
    IPointReader unseqMergeReader;
    unseqMergeReader = createUnseqSeriesReader(path, queryDataSource.getUnseqResources(), context, null);

    return new SeriesReaderWithValueFilter(tsFilesReader, unseqMergeReader, filter);
  }

  private static class SeriesReaderFactoryHelper {

    private static final SeriesReaderFactoryImpl INSTANCE = new SeriesReaderFactoryImpl();

    private SeriesReaderFactoryHelper() {
    }
  }
}
