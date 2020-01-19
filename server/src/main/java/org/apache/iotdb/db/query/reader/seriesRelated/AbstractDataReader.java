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

import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.MemChunkLoader;
import org.apache.iotdb.db.query.reader.chunkRelated.MemChunkReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.util.*;

public abstract class AbstractDataReader {

  private final QueryDataSource queryDataSource;
  private final QueryContext context;
  private final Path seriesPath;
  private final TSDataType dataType;
  protected Filter filter;

  private final List<TsFileResource> seqFileResource;
  private final PriorityQueue<TsFileResource> unseqFileResource;

  private final List<ChunkMetaData> seqChunkMetadatas = new LinkedList<>();
  private final PriorityQueue<ChunkMetaData> unseqChunkMetadatas =
      new PriorityQueue<>(Comparator.comparingLong(ChunkMetaData::getStartTime));

  private final List<IChunkLoader> openedChunkLoaders = new LinkedList<>();

  protected boolean hasCachedFirstChunkMetadata;
  protected ChunkMetaData firstChunkMetaData;

  protected PriorityQueue<VersionPair<IPageReader>> overlappedPageReaders =
      new PriorityQueue<>(
          Comparator.comparingLong(pageReader -> pageReader.data.getStatistics().getStartTime()));

  private PriorityMergeReader mergeReader = new PriorityMergeReader();

  private boolean hasCachedNextBatch;
  private BatchData cachedBatchData;

  private long currentPageEndTime = Long.MAX_VALUE;

  public AbstractDataReader(
      Path seriesPath, TSDataType dataType, Filter filter, QueryContext context)
      throws StorageEngineException {
    queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, filter);
    this.seriesPath = seriesPath;
    this.context = context;
    this.dataType = dataType;

    this.filter = queryDataSource.setTTL(filter);

    seqFileResource = queryDataSource.getSeqResources();
    unseqFileResource = sortUnSeqFileResources(queryDataSource.getUnseqResources());
  }

  // for test
  public AbstractDataReader(
      Path seriesPath,
      TSDataType dataType,
      Filter filter,
      QueryContext context,
      QueryDataSource dataSource) {
    queryDataSource = dataSource;
    this.seriesPath = seriesPath;
    this.context = context;
    this.dataType = dataType;

    this.filter = queryDataSource.setTTL(filter);

    seqFileResource = queryDataSource.getSeqResources();
    unseqFileResource = sortUnSeqFileResources(queryDataSource.getUnseqResources());
  }

  // for test
  public AbstractDataReader(
      Path seriesPath,
      TSDataType dataType,
      Filter filter,
      QueryContext context,
      List<TsFileResource> seqResources) {
    this.queryDataSource = null;
    this.seriesPath = seriesPath;
    this.context = context;
    this.dataType = dataType;

    this.filter = filter;

    this.seqFileResource = seqResources;
    this.unseqFileResource = new PriorityQueue<>();
  }

  protected boolean hasNextChunk() throws IOException {
    if (hasCachedFirstChunkMetadata) {
      return true;
    }
    // init first chunkReader whose startTime is minimum
    tryToInitFirstChunk();

    return hasCachedFirstChunkMetadata;
  }

  /**
   * Because seq data and unseq data intersect, the minimum startTime taken from two files at a time
   * is used as the reference time to start reading data
   */
  private void tryToInitFirstChunk() throws IOException {
    tryToFillChunkMetadatas();
    hasCachedFirstChunkMetadata = true;
    if (!seqChunkMetadatas.isEmpty() && unseqChunkMetadatas.isEmpty()) {
      // only has seq
      firstChunkMetaData = seqChunkMetadatas.remove(0);
    } else if (seqChunkMetadatas.isEmpty() && !unseqChunkMetadatas.isEmpty()) {
      // only has unseq
      firstChunkMetaData = unseqChunkMetadatas.poll();
    } else if (!seqChunkMetadatas.isEmpty()) {
      // has seq and unseq
      if (seqChunkMetadatas.get(0).getStartTime() <= unseqChunkMetadatas.peek().getStartTime()) {
        firstChunkMetaData = seqChunkMetadatas.remove(0);
      } else {
        firstChunkMetaData = unseqChunkMetadatas.poll();
      }
    } else {
      // no seq nor unseq
      hasCachedFirstChunkMetadata = false;
    }
    tryToFillChunkMetadatas();
  }

  public boolean canUseChunkStatistics() {
    Statistics chunkStatistics = firstChunkMetaData.getStatistics();
    return !mergeReader.hasNextTimeValuePair()
        && (seqChunkMetadatas.isEmpty()
        || chunkStatistics.getEndTime() < seqChunkMetadatas.get(0).getStartTime())
        && (unseqChunkMetadatas.isEmpty()
        || chunkStatistics.getEndTime() < unseqChunkMetadatas.peek().getStartTime())
        && satisfyFilter(chunkStatistics);
  }

  protected boolean satisfyFilter(Statistics statistics) {
    return filter == null
        || filter.containStartEndTime(statistics.getStartTime(), statistics.getEndTime());
  }

  protected boolean hasNextPage() throws IOException {
    if (!overlappedPageReaders.isEmpty()) {
      return true;
    }

    fillOverlappedPageReaders();

    return !overlappedPageReaders.isEmpty();
  }

  private void fillOverlappedPageReaders() throws IOException {
    if (!hasCachedFirstChunkMetadata) {
      return;
    }
    unpackOneChunkMetaData(firstChunkMetaData);
    hasCachedFirstChunkMetadata = false;
    firstChunkMetaData = null;
  }

  private void unpackOneChunkMetaData(ChunkMetaData chunkMetaData) throws IOException {
    initChunkReader(chunkMetaData)
        .getPageReaderList()
        .forEach(
            pageReader ->
                overlappedPageReaders.add(
                    new VersionPair(chunkMetaData.getVersion(), pageReader)));
  }

  public boolean canUseCurrentPageStatistics() {
    Statistics pageStatistics = overlappedPageReaders.peek().data.getStatistics();
    return !mergeReader.hasNextTimeValuePair()
        && (seqChunkMetadatas.isEmpty()
        || pageStatistics.getEndTime() < seqChunkMetadatas.get(0).getStartTime())
        && (unseqChunkMetadatas.isEmpty()
        || pageStatistics.getEndTime() < unseqChunkMetadatas.peek().getStartTime())
        && satisfyFilter(pageStatistics);
  }

  protected boolean hasNextOverlappedPage() throws IOException {

    if (hasCachedNextBatch) {
      return true;
    }

    putAllDirectlyOverlappedPageReadersIntoMergeReader();

    if (mergeReader.hasNextTimeValuePair()) {
      cachedBatchData = new BatchData(dataType);
      currentPageEndTime = mergeReader.getCurrentLargestEndTime();
      while (mergeReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = mergeReader.currentTimeValuePair();
        if (timeValuePair.getTimestamp() > currentPageEndTime) {
          break;
        }
        // unpack all overlapped chunks
        while (true) {
          tryToFillChunkMetadatas();
          boolean hasOverlappedChunkMetadata = false;
          if (!seqChunkMetadatas.isEmpty()
              && timeValuePair.getTimestamp() >= seqChunkMetadatas.get(0).getStartTime()) {
            unpackOneChunkMetaData(seqChunkMetadatas.remove(0));
            hasOverlappedChunkMetadata = true;
          }
          if (!unseqChunkMetadatas.isEmpty()
              && timeValuePair.getTimestamp() >= unseqChunkMetadatas.peek().getStartTime()) {
            unpackOneChunkMetaData(unseqChunkMetadatas.poll());
            hasOverlappedChunkMetadata = true;
          }
          if (!hasOverlappedChunkMetadata) {
            break;
          }
        }

        // put all overlapped pages into merge reader
        while (!overlappedPageReaders.isEmpty()
            && timeValuePair.getTimestamp()
            >= overlappedPageReaders.peek().data.getStatistics().getStartTime()) {
          VersionPair<IPageReader> pageReader = overlappedPageReaders.poll();
          mergeReader.addReader(
              pageReader.data.getAllSatisfiedPageData().getBatchDataIterator(), pageReader.version,
              pageReader.data.getStatistics().getEndTime());
        }

        timeValuePair = mergeReader.nextTimeValuePair();
        cachedBatchData.putAnObject(
            timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
      }
      hasCachedNextBatch = true;
    }
    return hasCachedNextBatch;
  }

  private void putAllDirectlyOverlappedPageReadersIntoMergeReader() throws IOException {

    if (mergeReader.hasNextTimeValuePair()) {
      currentPageEndTime = mergeReader.getCurrentLargestEndTime();
    } else if (!overlappedPageReaders.isEmpty()) {
      // put the first page into merge reader
      currentPageEndTime = overlappedPageReaders.peek().data.getStatistics().getEndTime();
      VersionPair<IPageReader> pageReader = overlappedPageReaders.poll();
      mergeReader.addReader(
          pageReader.data.getAllSatisfiedPageData().getBatchDataIterator(), pageReader.version,
          pageReader.data.getStatistics().getEndTime());
    } else {
      return;
    }

    // unpack all overlapped seq chunk meta data into overlapped page readers
    while (!seqChunkMetadatas.isEmpty()
        && currentPageEndTime >= seqChunkMetadatas.get(0).getStartTime()) {
      unpackOneChunkMetaData(seqChunkMetadatas.remove(0));
      tryToFillChunkMetadatas();
    }
    // unpack all overlapped unseq chunk meta data into overlapped page readers
    while (!unseqChunkMetadatas.isEmpty()
        && currentPageEndTime >= unseqChunkMetadatas.peek().getStartTime()) {
      unpackOneChunkMetaData(unseqChunkMetadatas.poll());
      tryToFillChunkMetadatas();
    }

    // put all page that directly overlapped with first page into merge reader
    while (!overlappedPageReaders.isEmpty()
        && currentPageEndTime >= overlappedPageReaders.peek().data.getStatistics().getStartTime()) {
      VersionPair<IPageReader> pageReader = overlappedPageReaders.poll();
      mergeReader.addReader(
          pageReader.data.getAllSatisfiedPageData().getBatchDataIterator(), pageReader.version,
          pageReader.data.getStatistics().getEndTime());
    }
  }

  protected BatchData nextOverlappedPage() throws IOException {
    if (hasCachedNextBatch || hasNextOverlappedPage()) {
      hasCachedNextBatch = false;
      return cachedBatchData;
    }
    throw new IOException("No more batch data");
  }

  private IChunkReader initChunkReader(ChunkMetaData metaData) throws IOException {
    if (metaData == null) {
      return null;
    }
    IChunkReader chunkReader;
    IChunkLoader chunkLoader = metaData.getChunkLoader();
    openedChunkLoaders.add(chunkLoader);
    if (chunkLoader instanceof MemChunkLoader) {
      MemChunkLoader memChunkLoader = (MemChunkLoader) chunkLoader;
      chunkReader = new MemChunkReader(memChunkLoader.getChunk(), filter);
    } else {
      Chunk chunk = chunkLoader.getChunk(metaData);
      chunkReader = new ChunkReader(chunk, filter);
      chunkReader.hasNextSatisfiedPage();
    }
    return chunkReader;
  }

  private List<ChunkMetaData> loadSatisfiedChunkMetadatas(TsFileResource resource)
      throws IOException {
    List<ChunkMetaData> currentChunkMetaDataList;
    if (resource == null) {
      return new ArrayList<>();
    }
    if (resource.isClosed()) {
      currentChunkMetaDataList = DeviceMetaDataCache.getInstance().get(resource, seriesPath);
    } else {
      currentChunkMetaDataList = resource.getChunkMetaDataList();
    }
    List<Modification> pathModifications =
        context.getPathModifications(resource.getModFile(), seriesPath.getFullPath());

    if (!pathModifications.isEmpty()) {
      QueryUtils.modifyChunkMetaData(currentChunkMetaDataList, pathModifications);
    }

    IChunkLoader chunkLoader = null;

    for (ChunkMetaData data : currentChunkMetaDataList) {
      if (data.getChunkLoader() == null) {
        if (chunkLoader == null) {
          chunkLoader =
              new ChunkLoaderImpl(new TsFileSequenceReader(resource.getFile().getAbsolutePath()));
        }
        data.setChunkLoader(chunkLoader);
      }
    }
    List<ReadOnlyMemChunk> memChunks = resource.getReadOnlyMemChunk();
    if (memChunks != null) {
      for (ReadOnlyMemChunk readOnlyMemChunk : memChunks) {
        if (!memChunks.isEmpty()) {
          currentChunkMetaDataList.add(readOnlyMemChunk.getChunkMetaData());
        }
      }
    }

    if (filter != null) {
      currentChunkMetaDataList.removeIf(
          a -> !filter.satisfyStartEndTime(a.getStartTime(), a.getEndTime()));
    }
    return currentChunkMetaDataList;
  }

  private PriorityQueue<TsFileResource> sortUnSeqFileResources(
      List<TsFileResource> tsFileResources) {
    PriorityQueue<TsFileResource> unseqTsFilesSet =
        new PriorityQueue<>(
            (o1, o2) -> {
              Map<String, Long> startTimeMap = o1.getStartTimeMap();
              Long minTimeOfO1 = startTimeMap.get(seriesPath.getDevice());
              Map<String, Long> startTimeMap2 = o2.getStartTimeMap();
              Long minTimeOfO2 = startTimeMap2.get(seriesPath.getDevice());

              return Long.compare(minTimeOfO1, minTimeOfO2);
            });
    unseqTsFilesSet.addAll(tsFileResources);
    return unseqTsFilesSet;
  }

  /**
   * Because there may be too many files in the scenario used by the user, we cannot open all the
   * chunks at once, which may cause OOM, so we can only unpack one file at a time when needed. This
   * approach is likely to be ubiquitous, but it keeps the system running smoothly
   */
  private void tryToFillChunkMetadatas() throws IOException {
    // Fill sequence chunkMetadatas until it is not empty
    while (seqChunkMetadatas.isEmpty() && !seqFileResource.isEmpty()) {
      seqChunkMetadatas.addAll(loadSatisfiedChunkMetadatas(seqFileResource.remove(0)));
    }
    // Fill unsequence chunkMetadatas until it is not empty
    while (unseqChunkMetadatas.isEmpty() && !unseqFileResource.isEmpty()) {
      unseqChunkMetadatas.addAll(loadSatisfiedChunkMetadatas(unseqFileResource.poll()));
    }
  }

  protected class VersionPair<T> {

    protected long version;
    protected T data;

    public VersionPair(long version, T data) {
      this.version = version;
      this.data = data;
    }
  }

  public void close() throws IOException {
    if (firstChunkMetaData != null) {
      firstChunkMetaData.getChunkLoader().close();
    }
    for (IChunkLoader openedChunkLoader : openedChunkLoaders) {
      openedChunkLoader.close();
    }
  }
}
