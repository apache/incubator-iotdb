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
package org.apache.iotdb.db.query.reader.series;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.universal.DescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader.MergeReaderPriority;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.basic.UnaryFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;

public class SeriesReader {

  // inner class of SeriesReader for order purpose
  private TimeOrderUtils orderUtils;

  private final PartialPath seriesPath;

  // all the sensors in this device;
  private final Set<String> allSensors;
  private final TSDataType dataType;
  private final QueryContext context;

  /*
   * There is at most one is not null between timeFilter and valueFilter
   *
   * timeFilter is pushed down to all pages (seq, unseq) without correctness problem
   *
   * valueFilter is pushed down to non-overlapped page only
   */
  private final Filter timeFilter;
  private final Filter valueFilter;
  /*
   * file cache
   */
  private final List<TsFileResource> seqFileResource;
  private final List<TsFileResource> unseqFileResource;

  /*
   * TimeSeriesMetadata cache
   */
  private TimeseriesMetadata firstTimeSeriesMetadata;
  private final List<TimeseriesMetadata> seqTimeSeriesMetadata = new LinkedList<>();
  private final PriorityQueue<TimeseriesMetadata> unSeqTimeSeriesMetadata;

  /*
   * chunk cache
   */
  private ChunkMetadata firstChunkMetadata;
  private final PriorityQueue<ChunkMetadata> cachedChunkMetadata;

  /*
   * page cache
   */
  private VersionPageReader firstPageReader;
  private final List<VersionPageReader> seqPageReaders = new LinkedList<>();
  private final PriorityQueue<VersionPageReader> unSeqPageReaders;

  /*
   * point cache
   */
  private final PriorityMergeReader mergeReader;

  /*
   * result cache
   */
  private boolean hasCachedNextOverlappedPage;
  private BatchData cachedBatchData;

  public SeriesReader(PartialPath seriesPath, Set<String> allSensors, TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource, Filter timeFilter, Filter valueFilter, TsFileFilter fileFilter,
      boolean ascending) {
    this.seriesPath = seriesPath;
    this.allSensors = allSensors;
    this.dataType = dataType;
    this.context = context;
    QueryUtils.filterQueryDataSource(dataSource, fileFilter);
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
    if (ascending) {
      this.orderUtils = new AscTimeOrderUtils();
      mergeReader = new PriorityMergeReader();
    } else {
      this.orderUtils = new DescTimeOrderUtils();
      mergeReader = new DescPriorityMergeReader();
    }

    this.seqFileResource = new LinkedList<>(dataSource.getSeqResources());
    this.unseqFileResource = sortUnSeqFileResources(dataSource.getUnseqResources());
    unSeqTimeSeriesMetadata = new PriorityQueue<>(orderUtils.comparingLong(
        timeSeriesMetadata -> orderUtils.getOrderTime(timeSeriesMetadata.getStatistics())));
    cachedChunkMetadata = new PriorityQueue<>(orderUtils.comparingLong(
        chunkMetadata -> orderUtils.getOrderTime(chunkMetadata.getStatistics())));
    unSeqPageReaders = new PriorityQueue<>(orderUtils.comparingLong(
        versionPageReader -> orderUtils.getOrderTime(versionPageReader.getStatistics())));
  }

  @TestOnly
  @SuppressWarnings("squid:S107")
  SeriesReader(PartialPath seriesPath, Set<String> allSensors, TSDataType dataType,
      QueryContext context,
      List<TsFileResource> seqFileResource, List<TsFileResource> unseqFileResource,
      Filter timeFilter, Filter valueFilter, boolean ascending) {
    this.seriesPath = seriesPath;
    this.allSensors = allSensors;
    this.dataType = dataType;
    this.context = context;
    this.timeFilter = timeFilter;
    this.valueFilter = valueFilter;
    if (ascending) {
      this.orderUtils = new AscTimeOrderUtils();
      mergeReader = new PriorityMergeReader();
    } else {
      this.orderUtils = new DescTimeOrderUtils();
      mergeReader = new DescPriorityMergeReader();
    }

    this.seqFileResource = new LinkedList<>(seqFileResource);
    this.unseqFileResource = sortUnSeqFileResources(unseqFileResource);
    unSeqTimeSeriesMetadata = new PriorityQueue<>(orderUtils.comparingLong(
        timeSeriesMetadata -> orderUtils.getOrderTime(timeSeriesMetadata.getStatistics())));
    cachedChunkMetadata = new PriorityQueue<>(orderUtils.comparingLong(
        chunkMetadata -> orderUtils.getOrderTime(chunkMetadata.getStatistics())));
    unSeqPageReaders = new PriorityQueue<>(orderUtils.comparingLong(
        versionPageReader -> orderUtils.getOrderTime(versionPageReader.getStatistics())));
  }

  public boolean isEmpty() throws IOException {
    return !(hasNextPage() || hasNextChunk() || hasNextFile());
  }

  boolean hasNextFile() throws IOException {
    if (Thread.interrupted()) {
      throw new QueryTimeoutRuntimeException(
          QueryTimeoutRuntimeException.TIMEOUT_EXCEPTION_MESSAGE);
    }

    if (!unSeqPageReaders.isEmpty()
        || firstPageReader != null
        || mergeReader.hasNextTimeValuePair()) {
      throw new IOException(
          "all cached pages should be consumed first cachedPageReaders.isEmpty() is "
              + unSeqPageReaders.isEmpty()
              + " firstPageReader != null is "
              + (firstPageReader != null)
              + " mergeReader.hasNextTimeValuePair() = "
              + mergeReader.hasNextTimeValuePair());
    }

    if (firstChunkMetadata != null || !cachedChunkMetadata.isEmpty()) {
      throw new IOException("all cached chunks should be consumed first");
    }

    if (firstTimeSeriesMetadata != null) {
      return true;
    }

    // init first time series metadata whose startTime is minimum
    tryToUnpackAllOverlappedFilesToTimeSeriesMetadata();

    return firstTimeSeriesMetadata != null;
  }

  boolean isFileOverlapped() throws IOException {
    if (firstTimeSeriesMetadata == null) {
      throw new IOException("no first file");
    }

    Statistics fileStatistics = firstTimeSeriesMetadata.getStatistics();
    return !seqTimeSeriesMetadata.isEmpty()
        && orderUtils.isOverlapped(fileStatistics, seqTimeSeriesMetadata.get(0).getStatistics())
        || !unSeqTimeSeriesMetadata.isEmpty()
        && orderUtils.isOverlapped(fileStatistics, unSeqTimeSeriesMetadata.peek().getStatistics());
  }

  Statistics currentFileStatistics() {
    return firstTimeSeriesMetadata.getStatistics();
  }

  boolean currentFileModified() throws IOException {
    if (firstTimeSeriesMetadata == null) {
      throw new IOException("no first file");
    }
    return firstTimeSeriesMetadata.isModified();
  }

  void skipCurrentFile() {
    firstTimeSeriesMetadata = null;
  }

  /**
   * This method should be called after hasNextFile() until no next chunk, make sure that all
   * overlapped chunks are consumed
   */
  boolean hasNextChunk() throws IOException {
    if (Thread.interrupted()) {
      throw new QueryTimeoutRuntimeException(
          QueryTimeoutRuntimeException.TIMEOUT_EXCEPTION_MESSAGE);
    }

    if (!unSeqPageReaders.isEmpty()
        || firstPageReader != null
        || mergeReader.hasNextTimeValuePair()) {
      throw new IOException(
          "all cached pages should be consumed first cachedPageReaders.isEmpty() is "
              + unSeqPageReaders.isEmpty()
              + " firstPageReader != null is "
              + (firstPageReader != null)
              + " mergeReader.hasNextTimeValuePair() = "
              + mergeReader.hasNextTimeValuePair());
    }

    if (firstChunkMetadata != null) {
      return true;
    }

    /*
     * construct first chunk metadata
     */
    if (firstTimeSeriesMetadata != null) {
      /*
       * try to unpack all overlapped TimeSeriesMetadata to cachedChunkMetadata
       */
      unpackAllOverlappedTsFilesToTimeSeriesMetadata(
          orderUtils.getOverlapCheckTime(firstTimeSeriesMetadata.getStatistics()));
      unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
          orderUtils.getOverlapCheckTime(firstTimeSeriesMetadata.getStatistics()), true);
    } else {
      /*
       * first time series metadata is already unpacked, consume cached ChunkMetadata
       */
      if (!cachedChunkMetadata.isEmpty()) {
        firstChunkMetadata = cachedChunkMetadata.poll();
        unpackAllOverlappedTsFilesToTimeSeriesMetadata(
            orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()));
        unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
            orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()), false);
      }
    }

    return firstChunkMetadata != null;
  }

  private void unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
      long endpointTime, boolean init) throws IOException {
    while (!seqTimeSeriesMetadata.isEmpty()
        && orderUtils.isOverlapped(endpointTime, seqTimeSeriesMetadata.get(0).getStatistics())) {
      unpackOneTimeSeriesMetadata(seqTimeSeriesMetadata.remove(0));
    }
    while (!unSeqTimeSeriesMetadata.isEmpty()
        && orderUtils.isOverlapped(endpointTime, unSeqTimeSeriesMetadata.peek().getStatistics())) {
      unpackOneTimeSeriesMetadata(unSeqTimeSeriesMetadata.poll());
    }

    if (firstTimeSeriesMetadata != null
        && orderUtils.isOverlapped(endpointTime, firstTimeSeriesMetadata.getStatistics())) {
      unpackOneTimeSeriesMetadata(firstTimeSeriesMetadata);
      firstTimeSeriesMetadata = null;
    }

    if (init && firstChunkMetadata == null && !cachedChunkMetadata.isEmpty()) {
      firstChunkMetadata = cachedChunkMetadata.poll();
    }
  }

  private void unpackOneTimeSeriesMetadata(TimeseriesMetadata timeSeriesMetadata)
      throws IOException {
    List<ChunkMetadata> chunkMetadataList = FileLoaderUtils
        .loadChunkMetadataList(timeSeriesMetadata);
    chunkMetadataList.forEach(chunkMetadata -> chunkMetadata.setSeq(timeSeriesMetadata.isSeq()));

    // try to calculate the total number of chunk and time-value points in chunk
    if (IoTDBDescriptor.getInstance().getConfig().isEnablePerformanceTracing()) {
      QueryResourceManager queryResourceManager = QueryResourceManager.getInstance();
      queryResourceManager.getChunkNumMap()
          .compute(context.getQueryId(),
              (k, v) -> v == null ? chunkMetadataList.size() : v + chunkMetadataList.size());

      long totalChunkSize = chunkMetadataList.stream()
          .mapToLong(chunkMetadata -> chunkMetadata.getStatistics().getCount()).sum();
      queryResourceManager.getChunkSizeMap()
          .compute(context.getQueryId(), (k, v) -> v == null ? totalChunkSize : v + totalChunkSize);
    }

    cachedChunkMetadata.addAll(chunkMetadataList);
  }

  boolean isChunkOverlapped() throws IOException {
    if (firstChunkMetadata == null) {
      throw new IOException("no first chunk");
    }

    Statistics chunkStatistics = firstChunkMetadata.getStatistics();
    return !cachedChunkMetadata.isEmpty()
        && orderUtils.isOverlapped(chunkStatistics, cachedChunkMetadata.peek().getStatistics());
  }

  Statistics currentChunkStatistics() {
    return firstChunkMetadata.getStatistics();
  }

  boolean currentChunkModified() throws IOException {
    if (firstChunkMetadata == null) {
      throw new IOException("no first chunk");
    }
    return firstChunkMetadata.isModified();
  }

  void skipCurrentChunk() {
    firstChunkMetadata = null;
  }

  /**
   * This method should be called after hasNextChunk() until no next page, make sure that all
   * overlapped pages are consumed
   */
  @SuppressWarnings("squid:S3776")
  // Suppress high Cognitive Complexity warning
  boolean hasNextPage() throws IOException {
    if (Thread.interrupted()) {
      throw new QueryTimeoutRuntimeException(
          QueryTimeoutRuntimeException.TIMEOUT_EXCEPTION_MESSAGE);
    }

    /*
     * has overlapped data before
     */
    if (hasCachedNextOverlappedPage) {
      return true;
    } else if (mergeReader.hasNextTimeValuePair()) {
      if (hasNextOverlappedPage()) {
        cachedBatchData = nextOverlappedPage();
        if (cachedBatchData != null && cachedBatchData.hasCurrent()) {
          hasCachedNextOverlappedPage = true;
          return true;
        }
      }
    }

    if (firstPageReader != null) {
      return true;
    }

    /*
     * construct first page reader
     */
    if (firstChunkMetadata != null) {
      /*
       * try to unpack all overlapped ChunkMetadata to cachedPageReaders
       */
      unpackAllOverlappedChunkMetadataToCachedPageReaders(
          orderUtils.getOverlapCheckTime(firstChunkMetadata.getStatistics()), true);
    } else {
      /*
       * first chunk metadata is already unpacked, consume cached pages
       */
      initFirstPageReader();
      if (firstPageReader != null) {
        long endpointTime = orderUtils.getOverlapCheckTime(firstPageReader.getStatistics());
        unpackAllOverlappedTsFilesToTimeSeriesMetadata(endpointTime);
        unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(endpointTime, false);
        unpackAllOverlappedChunkMetadataToCachedPageReaders(endpointTime, false);
      }
    }

    if (firstPageOverlapped()) {
      /*
       * next page is overlapped, read overlapped data and cache it
       */
      if (hasNextOverlappedPage()) {
        cachedBatchData = nextOverlappedPage();
        if (cachedBatchData != null && cachedBatchData.hasCurrent()) {
          hasCachedNextOverlappedPage = true;
          return true;
        }
      }
    }

    // make sure firstPageReader won't be null while cachedPageReaders has more cached page readers
    while (firstPageReader == null && (!seqPageReaders.isEmpty() || !unSeqPageReaders.isEmpty())) {

      initFirstPageReader();
      if (firstPageReader != null) {
        long endpointTime = orderUtils.getOverlapCheckTime(firstPageReader.getStatistics());
        unpackAllOverlappedTsFilesToTimeSeriesMetadata(endpointTime);
        unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(endpointTime, false);
        unpackAllOverlappedChunkMetadataToCachedPageReaders(endpointTime, false);
      }

      if (firstPageOverlapped()) {
        /*
         * next page is overlapped, read overlapped data and cache it
         */
        if (hasNextOverlappedPage()) {
          cachedBatchData = nextOverlappedPage();
          if (cachedBatchData != null && cachedBatchData.hasCurrent()) {
            hasCachedNextOverlappedPage = true;
            return true;
          }
        }
      }
    }
    return firstPageReader != null;
  }

  private boolean firstPageOverlapped() throws IOException {
    if (firstPageReader == null) {
      return false;
    }
    return (!seqPageReaders.isEmpty() && orderUtils
        .isOverlapped(firstPageReader.getStatistics(), seqPageReaders.get(0).getStatistics())) || (
        !unSeqPageReaders.isEmpty() && orderUtils
            .isOverlapped(firstPageReader.getStatistics(), unSeqPageReaders.peek().getStatistics())
            || (mergeReader.hasNextTimeValuePair()
            && mergeReader.currentTimeValuePair().getTimestamp() > firstPageReader.getStatistics()
            .getStartTime()));
  }

  private void unpackAllOverlappedChunkMetadataToCachedPageReaders(long endpointTime, boolean init)
      throws IOException {
    while (!cachedChunkMetadata.isEmpty() &&
        orderUtils.isOverlapped(endpointTime, cachedChunkMetadata.peek().getStatistics())) {
      unpackOneChunkMetaData(cachedChunkMetadata.poll());
    }
    if (firstChunkMetadata != null &&
        orderUtils.isOverlapped(endpointTime, firstChunkMetadata.getStatistics())) {
      unpackOneChunkMetaData(firstChunkMetadata);
      firstChunkMetadata = null;
    }
    if (init && firstPageReader == null && (!seqPageReaders.isEmpty() || !unSeqPageReaders
        .isEmpty())) {
      initFirstPageReader();
    }
  }

  private void unpackOneChunkMetaData(ChunkMetadata chunkMetaData)
      throws IOException {
    FileLoaderUtils.loadPageReaderList(chunkMetaData, timeFilter).forEach(
        pageReader -> {
          if (chunkMetaData.isSeq()) {
            // addLast for asc; addFirst for desc
            if (orderUtils.getAscending()) {
              seqPageReaders
                  .add(new VersionPageReader(chunkMetaData.getVersion(),
                          chunkMetaData.getOffsetOfChunkHeader(), pageReader, true));
            } else {
              seqPageReaders
                  .add(0, new VersionPageReader(chunkMetaData.getVersion(),
                          chunkMetaData.getOffsetOfChunkHeader(), pageReader, true));
            }

          } else {
            unSeqPageReaders
                .add(new VersionPageReader(chunkMetaData.getVersion(),
                        chunkMetaData.getOffsetOfChunkHeader(), pageReader, false));
          }
        });
  }

  /**
   * This method should be called after calling hasNextPage.
   *
   * <p>hasNextPage may cache firstPageReader if it is not overlapped or cached a BatchData if the
   * first page is overlapped
   */
  boolean isPageOverlapped() throws IOException {

    /*
     * has an overlapped page
     */
    if (hasCachedNextOverlappedPage) {
      return true;
    }

    /*
     * has a non-overlapped page in firstPageReader
     */
    if (mergeReader.hasNextTimeValuePair() &&
        ((orderUtils.getAscending() && mergeReader.currentTimeValuePair().getTimestamp() <=
            firstPageReader.getStatistics().getEndTime()) ||
            (!orderUtils.getAscending() && mergeReader.currentTimeValuePair().getTimestamp() >=
                firstPageReader.getStatistics().getStartTime()))) {
      throw new IOException("overlapped data should be consumed first");
    }

    Statistics firstPageStatistics = firstPageReader.getStatistics();

    return !unSeqPageReaders.isEmpty()
        && orderUtils.isOverlapped(firstPageStatistics, unSeqPageReaders.peek().getStatistics());
  }

  Statistics currentPageStatistics() {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getStatistics();
  }

  boolean currentPageModified() throws IOException {
    if (firstPageReader == null) {
      throw new IOException("no first page");
    }
    return firstPageReader.isModified();
  }

  void skipCurrentPage() {
    firstPageReader = null;
  }

  /**
   * This method should only be used when the method isPageOverlapped() return true.
   */
  BatchData nextPage() throws IOException {

    if (!hasNextPage()) {
      throw new IOException("no next page, neither non-overlapped nor overlapped");
    }

    if (hasCachedNextOverlappedPage) {
      hasCachedNextOverlappedPage = false;
      return cachedBatchData;
    } else {

      /*
       * next page is not overlapped, push down value filter if it exists
       */
      if (valueFilter != null) {
        firstPageReader.setFilter(valueFilter);
      }
      BatchData batchData = firstPageReader.getAllSatisfiedPageData(orderUtils.getAscending());
      firstPageReader = null;

      return batchData;
    }
  }

  /**
   * read overlapped data till currentLargestEndTime in mergeReader, if current batch does not
   * contain data, read till next currentLargestEndTime again
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean hasNextOverlappedPage() throws IOException {

    if (hasCachedNextOverlappedPage) {
      return true;
    }

    tryToPutAllDirectlyOverlappedUnseqPageReadersIntoMergeReader();

    while (true) {

      if (mergeReader.hasNextTimeValuePair()) {

        cachedBatchData = BatchDataFactory
            .createBatchData(dataType, orderUtils.getAscending(), true);
        long currentPageEndPointTime = mergeReader.getCurrentReadStopTime();
        if (firstPageReader != null) {
          currentPageEndPointTime = orderUtils
              .getCurrentEndPoint(currentPageEndPointTime, firstPageReader.getStatistics());
        }
        if (!seqPageReaders.isEmpty()) {
          currentPageEndPointTime = orderUtils
              .getCurrentEndPoint(currentPageEndPointTime, seqPageReaders.get(0).getStatistics());
        }
        while (mergeReader.hasNextTimeValuePair()) {

          /*
           * get current first point in mergeReader, this maybe overlapped latter
           */
          TimeValuePair timeValuePair = mergeReader.currentTimeValuePair();

          if (orderUtils.isExcessEndpoint(timeValuePair.getTimestamp(), currentPageEndPointTime)) {
            if (cachedBatchData.hasCurrent() || firstPageReader != null || !seqPageReaders
                .isEmpty()) {
              break;
            }
            currentPageEndPointTime = mergeReader.getCurrentReadStopTime();
          }

          unpackAllOverlappedTsFilesToTimeSeriesMetadata(timeValuePair.getTimestamp());
          unpackAllOverlappedTimeSeriesMetadataToCachedChunkMetadata(
              timeValuePair.getTimestamp(), false);
          unpackAllOverlappedChunkMetadataToCachedPageReaders(timeValuePair.getTimestamp(), false);
          unpackAllOverlappedUnseqPageReadersToMergeReader(timeValuePair.getTimestamp());

          if (firstPageReader != null) {
            if ((orderUtils.getAscending() && timeValuePair.getTimestamp() > firstPageReader
                .getStatistics().getEndTime()) || (!orderUtils.getAscending()
                && timeValuePair.getTimestamp() < firstPageReader.getStatistics().getStartTime())) {
              hasCachedNextOverlappedPage = cachedBatchData.hasCurrent();
              return hasCachedNextOverlappedPage;
            } else {
              mergeReader
                  .addReader(firstPageReader.getAllSatisfiedPageData(orderUtils.getAscending())
                          .getBatchDataIterator(), firstPageReader.version,
                      orderUtils.getOverlapCheckTime(firstPageReader.getStatistics()));
              currentPageEndPointTime = updateEndPointTime(currentPageEndPointTime,
                  firstPageReader);
              firstPageReader = null;
            }
          }

          if (!seqPageReaders.isEmpty()) {
            if ((orderUtils.getAscending() && timeValuePair.getTimestamp() > seqPageReaders.get(0)
                .getStatistics().getEndTime()) || (!orderUtils.getAscending()
                && timeValuePair.getTimestamp() < seqPageReaders.get(0).getStatistics()
                .getStartTime())) {
              hasCachedNextOverlappedPage = cachedBatchData.hasCurrent();
              return hasCachedNextOverlappedPage;
            } else {
              VersionPageReader pageReader = seqPageReaders.remove(0);
              mergeReader.addReader(pageReader.getAllSatisfiedPageData(orderUtils.getAscending())
                      .getBatchDataIterator(), pageReader.version,
                  orderUtils.getOverlapCheckTime(pageReader.getStatistics()));
              currentPageEndPointTime = updateEndPointTime(currentPageEndPointTime, pageReader);
            }
          }

          /*
           * get the latest first point in mergeReader
           */
          timeValuePair = mergeReader.nextTimeValuePair();

          if (valueFilter == null || valueFilter
              .satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
            cachedBatchData.putAnObject(
                timeValuePair.getTimestamp(), timeValuePair.getValue().getValue());
          }
        }
        cachedBatchData.flip();
        hasCachedNextOverlappedPage = cachedBatchData.hasCurrent();
        /*
         * if current overlapped page has valid data, return, otherwise read next overlapped page
         */
        if (hasCachedNextOverlappedPage) {
          return true;
          // condition: seqPage.endTime < mergeReader.currentTime
        } else if (mergeReader.hasNextTimeValuePair()) {
          return false;
        }
      } else {
        return false;
      }
    }
  }

  private long updateEndPointTime(long currentPageEndPointTime, VersionPageReader pageReader) {
    if (orderUtils.getAscending()) {
      return Math.max(currentPageEndPointTime, pageReader.getStatistics().getEndTime());
    } else {
      return Math.min(currentPageEndPointTime, pageReader.getStatistics().getStartTime());
    }
  }

  private void tryToPutAllDirectlyOverlappedUnseqPageReadersIntoMergeReader() throws IOException {

    /*
     * no cached page readers
     */
    if (firstPageReader == null && unSeqPageReaders.isEmpty() && seqPageReaders.isEmpty()) {
      return;
    }

    /*
     * init firstPageReader
     */
    if (firstPageReader == null) {
      initFirstPageReader();
    }

    long currentPageEndpointTime;
    if (mergeReader.hasNextTimeValuePair()) {
      currentPageEndpointTime = mergeReader.getCurrentReadStopTime();
    } else {
      currentPageEndpointTime = orderUtils.getOverlapCheckTime(firstPageReader.getStatistics());
    }

    /*
     * put all currently directly overlapped unseq page reader to mergeUnseq reader
     */
    unpackAllOverlappedUnseqPageReadersToMergeReader(currentPageEndpointTime);
  }

  private void initFirstPageReader() {
    if (!seqPageReaders.isEmpty() && !unSeqPageReaders.isEmpty()) {
      if (orderUtils.isTakeSeqAsFirst(seqPageReaders.get(0).getStatistics(), unSeqPageReaders.peek()
          .getStatistics())) {
        firstPageReader = seqPageReaders.remove(0);
      } else {
        firstPageReader = unSeqPageReaders.poll();
      }
    } else if (!seqPageReaders.isEmpty()) {
      firstPageReader = seqPageReaders.remove(0);
    } else if (!unSeqPageReaders.isEmpty()) {
      firstPageReader = unSeqPageReaders.poll();
    }
  }

  private void unpackAllOverlappedUnseqPageReadersToMergeReader(long endpointTime)
      throws IOException {
    while (!unSeqPageReaders.isEmpty()
        && orderUtils.isOverlapped(endpointTime, unSeqPageReaders.peek().data.getStatistics())) {
      putPageReaderToMergeReader(unSeqPageReaders.poll());
    }
    if (firstPageReader != null && !firstPageReader.isSeq() &&
        orderUtils.isOverlapped(endpointTime, firstPageReader.getStatistics())) {
      putPageReaderToMergeReader(firstPageReader);
      firstPageReader = null;
    }
  }

  private void putPageReaderToMergeReader(VersionPageReader pageReader) throws IOException {
    mergeReader.addReader(
        pageReader.getAllSatisfiedPageData(orderUtils.getAscending()).getBatchDataIterator(),
        pageReader.version,
        orderUtils.getOverlapCheckTime(pageReader.getStatistics()));
  }

  private BatchData nextOverlappedPage() throws IOException {
    if (hasCachedNextOverlappedPage || hasNextOverlappedPage()) {
      hasCachedNextOverlappedPage = false;
      return cachedBatchData;
    }
    throw new IOException("No more batch data");
  }

  private LinkedList<TsFileResource> sortUnSeqFileResources(List<TsFileResource> tsFileResources) {
    return tsFileResources.stream()
        .sorted(
            orderUtils.comparingLong(tsFileResource -> orderUtils.getOrderTime(tsFileResource)))
        .collect(Collectors.toCollection(LinkedList::new));
  }

  /**
   * unpack all overlapped seq/unseq files and find the first TimeSeriesMetadata
   *
   * <p>Because there may be too many files in the scenario used by the user, we cannot open all
   * the chunks at once, which may cause OOM, so we can only unpack one file at a time when needed.
   * This approach is likely to be ubiquitous, but it keeps the system running smoothly
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void tryToUnpackAllOverlappedFilesToTimeSeriesMetadata() throws IOException {
    /*
     * Fill sequence TimeSeriesMetadata List until it is not empty
     */
    while (seqTimeSeriesMetadata.isEmpty() && !seqFileResource.isEmpty()) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              orderUtils.getNextSeqFileResource(seqFileResource, true), seriesPath, context,
              getAnyFilter(), allSensors);
      if (timeseriesMetadata != null) {
        timeseriesMetadata.setSeq(true);
        seqTimeSeriesMetadata.add(timeseriesMetadata);
      }
    }

    /*
     * Fill unSequence TimeSeriesMetadata Priority Queue until it is not empty
     */
    while (unSeqTimeSeriesMetadata.isEmpty() && !unseqFileResource.isEmpty()) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              unseqFileResource.remove(0), seriesPath, context, getAnyFilter(), allSensors);
      if (timeseriesMetadata != null) {
        timeseriesMetadata.setModified(true);
        timeseriesMetadata.setSeq(false);
        unSeqTimeSeriesMetadata.add(timeseriesMetadata);
      }
    }

    /*
     * find end time of the first TimeSeriesMetadata
     */
    long endTime = -1L;
    if (!seqTimeSeriesMetadata.isEmpty() && unSeqTimeSeriesMetadata.isEmpty()) {
      // only has seq
      endTime = orderUtils.getOverlapCheckTime(seqTimeSeriesMetadata.get(0).getStatistics());
    } else if (seqTimeSeriesMetadata.isEmpty() && !unSeqTimeSeriesMetadata.isEmpty()) {
      // only has unseq
      endTime = orderUtils.getOverlapCheckTime(unSeqTimeSeriesMetadata.peek().getStatistics());
    } else if (!seqTimeSeriesMetadata.isEmpty()) {
      // has seq and unseq
      endTime = orderUtils.getCurrentEndPoint(seqTimeSeriesMetadata.get(0).getStatistics(),
          unSeqTimeSeriesMetadata.peek().getStatistics());
    }

    /*
     * unpack all directly overlapped seq/unseq files with first TimeSeriesMetadata
     */
    if (endTime != -1) {
      unpackAllOverlappedTsFilesToTimeSeriesMetadata(endTime);
    }

    /*
     * update the first TimeSeriesMetadata
     */
    if (!seqTimeSeriesMetadata.isEmpty() && unSeqTimeSeriesMetadata.isEmpty()) {
      // only has seq
      firstTimeSeriesMetadata = seqTimeSeriesMetadata.remove(0);
    } else if (seqTimeSeriesMetadata.isEmpty() && !unSeqTimeSeriesMetadata.isEmpty()) {
      // only has unseq
      firstTimeSeriesMetadata = unSeqTimeSeriesMetadata.poll();
    } else if (!seqTimeSeriesMetadata.isEmpty()) {
      // has seq and unseq
      if (orderUtils.isTakeSeqAsFirst(seqTimeSeriesMetadata.get(0).getStatistics(),
          unSeqTimeSeriesMetadata.peek().getStatistics())) {
        firstTimeSeriesMetadata = seqTimeSeriesMetadata.remove(0);
      } else {
        firstTimeSeriesMetadata = unSeqTimeSeriesMetadata.poll();
      }
    }
  }

  private void unpackAllOverlappedTsFilesToTimeSeriesMetadata(long endpointTime)
      throws IOException {
    while (!unseqFileResource.isEmpty()
        && orderUtils.isOverlapped(endpointTime, unseqFileResource.get(0))) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              unseqFileResource.remove(0), seriesPath, context, getAnyFilter(), allSensors);
      if (timeseriesMetadata != null) {
        timeseriesMetadata.setModified(true);
        timeseriesMetadata.setSeq(false);
        unSeqTimeSeriesMetadata.add(timeseriesMetadata);
      }
    }
    while (!seqFileResource.isEmpty()
        && orderUtils.isOverlapped(endpointTime,
        orderUtils.getNextSeqFileResource(seqFileResource, false))) {
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(
              orderUtils.getNextSeqFileResource(seqFileResource, true), seriesPath, context,
              getAnyFilter(), allSensors);
      if (timeseriesMetadata != null) {
        timeseriesMetadata.setSeq(true);
        seqTimeSeriesMetadata.add(timeseriesMetadata);
      }
    }
  }

  private Filter getAnyFilter() {
    return timeFilter != null ? timeFilter : valueFilter;
  }

  void setTimeFilter(long timestamp) {
    ((UnaryFilter) timeFilter).setValue(timestamp);
  }

  Filter getTimeFilter() {
    return timeFilter;
  }

  private class VersionPageReader {

    protected PriorityMergeReader.MergeReaderPriority version;
    protected IPageReader data;

    protected boolean isSeq;

    VersionPageReader(long version, long offset, IPageReader data, boolean isSeq) {
      this.version = new MergeReaderPriority(version, offset);
      this.data = data;
      this.isSeq = isSeq;
    }

    Statistics getStatistics() {
      return data.getStatistics();
    }

    BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
      return data.getAllSatisfiedPageData(ascending);
    }

    void setFilter(Filter filter) {
      data.setFilter(filter);
    }

    boolean isModified() {
      return data.isModified();
    }

    public boolean isSeq() {
      return isSeq;
    }
  }


  public interface TimeOrderUtils {

    long getOrderTime(Statistics<? extends Object> statistics);

    long getOrderTime(TsFileResource fileResource);

    long getOverlapCheckTime(Statistics<? extends Object> range);

    boolean isOverlapped(Statistics<? extends Object> left, Statistics<? extends Object> right);

    boolean isOverlapped(long time, Statistics<? extends Object> right);

    boolean isOverlapped(long time, TsFileResource right);

    TsFileResource getNextSeqFileResource(List<TsFileResource> seqResources, boolean isDelete);

    <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor);

    long getCurrentEndPoint(long time, Statistics<? extends Object> statistics);

    long getCurrentEndPoint(Statistics<? extends Object> seqStatistics,
        Statistics<? extends Object> unseqStatistics);

    boolean isExcessEndpoint(long time, long endpointTime);

    /**
     * Return true if taking first page reader from seq readers
     */
    boolean isTakeSeqAsFirst(Statistics<? extends Object> seqStatistics,
        Statistics<? extends Object> unseqStatistics);

    boolean getAscending();
  }


  class DescTimeOrderUtils implements TimeOrderUtils {

    @Override
    public long getOrderTime(Statistics statistics) {
      return statistics.getEndTime();
    }

    @Override
    public long getOrderTime(TsFileResource fileResource) {
      return fileResource.getEndTime(seriesPath.getDevice());
    }

    @Override
    public long getOverlapCheckTime(Statistics range) {
      return range.getStartTime();
    }

    @Override
    public boolean isOverlapped(Statistics left, Statistics right) {
      return left.getStartTime() <= right.getEndTime();
    }

    @Override
    public boolean isOverlapped(long time, Statistics right) {
      return time <= right.getEndTime();
    }

    @Override
    public boolean isOverlapped(long time, TsFileResource right) {
      return time <= right.getEndTime(seriesPath.getDevice());
    }

    @Override
    public TsFileResource getNextSeqFileResource(List<TsFileResource> seqResources,
        boolean isDelete) {
      if (isDelete) {
        return seqResources.remove(seqResources.size() - 1);
      }
      return seqResources.get(seqResources.size() - 1);
    }

    @Override
    public <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor) {
      Objects.requireNonNull(keyExtractor);
      return (Comparator<T> & Serializable)
          (c1, c2) -> Long.compare(keyExtractor.applyAsLong(c2), keyExtractor.applyAsLong(c1));
    }

    @Override
    public long getCurrentEndPoint(long time, Statistics<? extends Object> statistics) {
      return Math.max(time, statistics.getStartTime());
    }

    @Override
    public long getCurrentEndPoint(Statistics<? extends Object> seqStatistics,
        Statistics<? extends Object> unseqStatistics) {
      return Math.max(seqStatistics.getStartTime(), unseqStatistics.getStartTime());
    }

    @Override
    public boolean isExcessEndpoint(long time, long endpointTime) {
      return time < endpointTime;
    }

    @Override
    public boolean isTakeSeqAsFirst(Statistics<? extends Object> seqStatistics,
        Statistics<? extends Object> unseqStatistics) {
      return seqStatistics.getEndTime() > unseqStatistics.getEndTime();
    }

    @Override
    public boolean getAscending() {
      return false;
    }
  }


  class AscTimeOrderUtils implements TimeOrderUtils {

    @Override
    public long getOrderTime(Statistics statistics) {
      return statistics.getStartTime();
    }

    @Override
    public long getOrderTime(TsFileResource fileResource) {
      return fileResource.getStartTime(seriesPath.getDevice());
    }

    @Override
    public long getOverlapCheckTime(Statistics range) {
      return range.getEndTime();
    }

    @Override
    public boolean isOverlapped(Statistics left, Statistics right) {
      return left.getEndTime() >= right.getStartTime();
    }

    @Override
    public boolean isOverlapped(long time, Statistics right) {
      return time >= right.getStartTime();
    }

    @Override
    public boolean isOverlapped(long time, TsFileResource right) {
      return time >= right.getStartTime(seriesPath.getDevice());
    }

    @Override
    public TsFileResource getNextSeqFileResource(List<TsFileResource> seqResources,
        boolean isDelete) {
      if (isDelete) {
        return seqResources.remove(0);
      }
      return seqResources.get(0);
    }

    @Override
    public <T> Comparator<T> comparingLong(ToLongFunction<? super T> keyExtractor) {
      Objects.requireNonNull(keyExtractor);
      return (Comparator<T> & Serializable)
          (c1, c2) -> Long.compare(keyExtractor.applyAsLong(c1), keyExtractor.applyAsLong(c2));
    }

    @Override
    public long getCurrentEndPoint(long time, Statistics<? extends Object> statistics) {
      return Math.min(time, statistics.getEndTime());
    }

    @Override
    public long getCurrentEndPoint(Statistics<? extends Object> seqStatistics,
        Statistics<? extends Object> unseqStatistics) {
      return Math.min(seqStatistics.getEndTime(), unseqStatistics.getEndTime());
    }

    @Override
    public boolean isExcessEndpoint(long time, long endpointTime) {
      return time > endpointTime;
    }

    @Override
    public boolean isTakeSeqAsFirst(Statistics<? extends Object> seqStatistics,
        Statistics<? extends Object> unseqStatistics) {
      return seqStatistics.getStartTime() < unseqStatistics.getStartTime();
    }

    @Override
    public boolean getAscending() {
      return true;
    }
  }

  public TimeOrderUtils getOrderUtils() {
    return orderUtils;
  }
}
