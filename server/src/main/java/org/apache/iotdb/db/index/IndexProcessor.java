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
package org.apache.iotdb.db.index;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.index.IndexManagerException;
import org.apache.iotdb.db.exception.index.IndexRuntimeException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.common.func.IndexNaiveFunc;
import org.apache.iotdb.db.index.feature.IndexFeatureExtractor;
import org.apache.iotdb.db.index.read.optimize.IIndexCandidateOrderOptimize;
import org.apache.iotdb.db.index.router.IIndexRouter;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Each {@code IndexProcessor} manages all index instances under an <b>IndexSeries</b>.
 *
 * <p>An <b>IndexSeries</b> is one series or a set of series that an index instance acts on.
 *
 * <ul>
 *   <li>For whole matching, the IndexSeries is a set of series represented by a path with wildcard
 *       characters. For example, creating index on IndexSeries "root.steel.*.temperature" means all
 *       series belong to this IndexSeries will be inserted, such as root.steel.s1.temperature,
 *       root. steel.s2.temperature, ...
 *   <li>For subsequence matching, the IndexSeries is a full path. For example, creating index on
 *       IndexSeries "root.wind.azq01.speed" means all subsequence of this series will be inserted.
 * </ul>
 *
 * In current design。users are allowed to create more than one type of index on an IndexSeries. e.g.
 * we can create RTree and DS-Tree on "root.steel.*.temperature". New coming data will be inserted
 * into both of these two indexes.
 */
public class IndexProcessor implements Comparable<IndexProcessor> {

  private static final Logger logger = LoggerFactory.getLogger(IndexProcessor.class);

  private PartialPath indexSeries;
  private final String indexSeriesDirPath;
  private TSDataType tsDataType;
  private final IndexBuildTaskPoolManager indexBuildPoolManager;
  private ReadWriteLock lock = new ReentrantReadWriteLock();
  private Map<IndexType, ReadWriteLock> indexLockMap;

  /**
   * How many indexes of this IndexProcessor are currently inserting data. If 0, no indexes are
   * inserting, that is, this IndexProcessor is not in flushing state.
   */
  private AtomicInteger numIndexBuildTasks;

  /** Whether the processor has been closed. */
  private volatile boolean closed;

  /** The map of index instances. */
  private Map<IndexType, IoTDBIndex> allPathsIndexMap;

  /** Some status data saved when index is closed for next open. */
  private final Map<IndexType, ByteBuffer> previousDataBufferMap;

  private final String previousDataBufferFile;

  /**
   * Each index instance corresponds to an {@linkplain IIndexUsable}. All IIndexUsable of a
   * IndexProcessor are stored in one file.
   */
  private Map<IndexType, IIndexUsable> usableMap;

  private final String usableFile;

  /** The optimizer for the post-processing phase (refinement phase). Unused yet. */
  private final IIndexCandidateOrderOptimize refinePhaseOptimizer;

  /**
   * The constructor.
   *
   * @param indexSeries the index series.
   * @param indexSeriesDirPath the specified directory.
   */
  public IndexProcessor(PartialPath indexSeries, String indexSeriesDirPath) {
    this.indexBuildPoolManager = IndexBuildTaskPoolManager.getInstance();

    this.numIndexBuildTasks = new AtomicInteger(0);
    this.indexSeries = indexSeries;
    this.indexSeriesDirPath = indexSeriesDirPath;
    File dir = IndexUtils.getIndexFile(indexSeriesDirPath);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    this.closed = false;
    this.allPathsIndexMap = new EnumMap<>(IndexType.class);
    this.previousDataBufferMap = new EnumMap<>(IndexType.class);
    this.indexLockMap = new EnumMap<>(IndexType.class);
    this.usableMap = new EnumMap<>(IndexType.class);
    this.previousDataBufferFile = indexSeriesDirPath + File.separator + "previousBuffer";
    this.usableFile = indexSeriesDirPath + File.separator + "usableMap";
    this.tsDataType = initSeriesType();
    this.refinePhaseOptimizer = IIndexCandidateOrderOptimize.Factory.getOptimize();
    deserializePreviousBuffer();
    deserializeUsable(indexSeries);
  }

  /**
   * Determines the data type of the index instances. All time series covered by this IndexProcessor
   * should have the same data type.
   *
   * @return tsDataType of this IndexProcessor
   */
  private TSDataType initSeriesType() {
    try {
      if (indexSeries.isFullPath()) {
        return MManager.getInstance().getSeriesType(indexSeries);
      } else {
        List<PartialPath> list =
            IoTDB.metaManager.getAllTimeseriesPathWithAlias(indexSeries, 1, 0).left;
        if (list.isEmpty()) {
          throw new IndexRuntimeException("No series in the wildcard path");
        } else {
          return MManager.getInstance().getSeriesType(list.get(0));
        }
      }
    } catch (MetadataException e) {
      throw new IndexRuntimeException("get type failed. ", e);
    }
  }

  private String getIndexDir(IndexType indexType) {
    return indexSeriesDirPath + File.separator + indexType;
  }

  private void serializeUsable() {
    File file = SystemFileFactory.INSTANCE.getFile(usableFile);
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ReadWriteIOUtils.write(usableMap.size(), outputStream);
      for (Entry<IndexType, IIndexUsable> entry : usableMap.entrySet()) {
        IndexType indexType = entry.getKey();
        ReadWriteIOUtils.write(indexType.serialize(), outputStream);
        IIndexUsable v = entry.getValue();
        v.serialize(outputStream);
      }
    } catch (IOException e) {
      logger.error("Error when serialize usability. Given up.", e);
    }
  }

  private void serializePreviousBuffer() {
    File file = SystemFileFactory.INSTANCE.getFile(previousDataBufferFile);
    try (OutputStream outputStream = new FileOutputStream(file)) {
      ReadWriteIOUtils.write(previousDataBufferMap.size(), outputStream);
      for (Entry<IndexType, ByteBuffer> entry : previousDataBufferMap.entrySet()) {
        IndexType indexType = entry.getKey();
        ByteBuffer buffer = entry.getValue();
        ReadWriteIOUtils.write(indexType.serialize(), outputStream);
        ReadWriteIOUtils.write(buffer, outputStream);
      }
    } catch (IOException e) {
      logger.error("Error when serialize previous buffer. Given up.", e);
    }
  }

  private void deserializePreviousBuffer() {
    File file = SystemFileFactory.INSTANCE.getFile(previousDataBufferFile);
    if (!file.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(file)) {
      int size = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < size; i++) {
        IndexType indexType = IndexType.deserialize(ReadWriteIOUtils.readShort(inputStream));
        ByteBuffer byteBuffer =
            ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(inputStream);
        previousDataBufferMap.put(indexType, byteBuffer);
      }
    } catch (IOException e) {
      logger.error("Error when deserialize previous buffer. Given up.", e);
    }
  }

  private void deserializeUsable(PartialPath indexSeries) {
    File file = SystemFileFactory.INSTANCE.getFile(usableFile);
    if (!file.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(file)) {
      int size = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < size; i++) {
        short indexTypeShort = ReadWriteIOUtils.readShort(inputStream);
        IndexType indexType = IndexType.deserialize(indexTypeShort);
        IIndexUsable usable =
            IIndexUsable.Factory.deserializeIndexUsability(indexSeries, inputStream);
        usableMap.put(indexType, usable);
      }
    } catch (IOException | IllegalPathException e) {
      logger.error("Error when deserialize usability. Given up.", e);
    }
  }

  /** Flush all index instances to disk except NoIndex. */
  @SuppressWarnings("squid:S2589")
  public synchronized void close(boolean deleteFiles) throws IOException {
    if (closed) {
      return;
    }
    waitingFlushEndAndDo(
        () -> {
          lock.writeLock().lock();
          try {
            // store Preprocessor
            for (Entry<IndexType, IoTDBIndex> entry : allPathsIndexMap.entrySet()) {
              IndexType indexType = entry.getKey();
              if (indexType == IndexType.NO_INDEX) {
                continue;
              }
              IoTDBIndex index = entry.getValue();
              previousDataBufferMap.put(entry.getKey(), index.closeAndRelease());
            }
            logger.info("close and release index processor: {}", indexSeries);
            allPathsIndexMap.clear();
            serializeUsable();
            serializePreviousBuffer();
            closed = true;
            if (deleteFiles) {
              File dir = IndexUtils.getIndexFile(indexSeriesDirPath);
              Files.delete(dir.toPath());
            }
          } catch (IOException e) {
            logger.warn("Remove index files failed", e);
          } finally {
            lock.writeLock().unlock();
          }
        });
  }

  private void waitingFlushEndAndDo(IndexNaiveFunc indexNaiveAction) throws IOException {
    // wait the flushing end.
    long waitingTime;
    long waitingInterval = 100;
    long st = System.currentTimeMillis();
    while (true) {
      if (isFlushing()) {
        try {
          Thread.sleep(waitingInterval);
        } catch (InterruptedException e) {
          logger.error("interrupted, index insert may not complete.", e);
          Thread.currentThread().interrupt();
        }
        waitingTime = System.currentTimeMillis() - st;
        // wait for too long time.
        if (waitingTime > 3000) {
          waitingInterval = 1000;
          if (logger.isWarnEnabled()) {
            logger.warn(
                String.format(
                    "IndexFileProcessor %s: wait-close time %d ms is too long.",
                    indexSeries, waitingTime));
          }
        }
      } else {
        indexNaiveAction.act();
        break;
      }
    }
  }

  public PartialPath getIndexSeries() {
    return indexSeries;
  }

  @Override
  public int hashCode() {
    return indexSeries.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    return compareTo((IndexProcessor) obj) == 0;
  }

  @Override
  public String toString() {
    return indexSeries + ": " + allPathsIndexMap;
  }

  @Override
  public int compareTo(IndexProcessor o) {
    return indexSeries.compareTo(o.indexSeries);
  }

  private boolean isFlushing() {
    return numIndexBuildTasks.get() > 0;
  }

  void startFlushMemTable() {
    lock.writeLock().lock();
    try {
      if (closed) {
        throw new IndexRuntimeException("closed index file !!!!!");
      }
      if (isFlushing()) {
        throw new IndexRuntimeException("There has been a flushing, do you want to wait?");
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Insert sorted series into index instances.
   *
   * @param path the path of time series
   * @param tvList the sorted series
   */
  void buildIndexForOneSeries(PartialPath path, TVList tvList) {
    // for every index of this path, submit a task to pool.
    lock.writeLock().lock();
    numIndexBuildTasks.incrementAndGet();
    try {
      if (tvList.getDataType() != tsDataType) {
        logger.warn(
            "TsDataType unmatched, ignore: indexSeries {}: {}, given series {}: {}",
            indexSeries,
            tsDataType,
            path,
            tvList.getDataType());
      }
      allPathsIndexMap.forEach(
          (indexType, index) -> {
            // NO_INDEX doesn't involve the phase of building index
            if (indexType == IndexType.NO_INDEX) {
              numIndexBuildTasks.decrementAndGet();
              return;
            }
            Runnable buildTask =
                () -> {
                  try {
                    indexLockMap.get(indexType).writeLock().lock();
                    IndexFeatureExtractor extractor = index.startFlushTask(path, tvList);
                    while (extractor.hasNext()) {
                      extractor.processNext();
                      index.buildNext();
                    }
                    index.endFlushTask();
                  } catch (IndexManagerException e) {
                    // Give up the following data, but the previously serialized chunk will not be
                    // affected.
                    logger.error("build index failed", e);
                  } catch (RuntimeException e) {
                    logger.error("RuntimeException", e);
                  } finally {
                    numIndexBuildTasks.decrementAndGet();
                    indexLockMap.get(indexType).writeLock().unlock();
                  }
                };
            indexBuildPoolManager.submit(buildTask);
          });
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** Not return until all index insertion have finished. */
  void endFlushMemTable() {
    // wait until all flushing tasks end.
    try {
      waitingFlushEndAndDo(() -> {});
    } catch (IOException ignored) {
      // the exception is ignored
    }
  }

  /**
   * According to the passed-in {@code indexInfoMap}, refresh the index instances in the current
   * IndexProcessor. When someone index is created or dropped, the IndexProcessor is out of date, so
   * it needs to be refreshed.
   *
   * @param indexInfoMap passed from {@link IIndexRouter}
   */
  public void refreshSeriesIndexMapFromMManager(Map<IndexType, IndexInfo> indexInfoMap) {
    lock.writeLock().lock();
    try {
      // Add indexes that are not in the previous map
      for (Entry<IndexType, IndexInfo> entry : indexInfoMap.entrySet()) {
        IndexType indexType = entry.getKey();
        IndexInfo indexInfo = entry.getValue();
        if (!allPathsIndexMap.containsKey(indexType)) {
          IoTDBIndex index =
              IndexType.constructIndex(
                  indexSeries,
                  tsDataType,
                  getIndexDir(indexType),
                  indexType,
                  indexInfo,
                  previousDataBufferMap.get(indexType));
          allPathsIndexMap.putIfAbsent(indexType, index);
          indexLockMap.putIfAbsent(indexType, new ReentrantReadWriteLock());
          usableMap.putIfAbsent(
              indexType, IIndexUsable.Factory.createEmptyIndexUsability(indexSeries));
        }
      }

      // remove indexes that are removed from the previous map
      for (IndexType indexType : new ArrayList<>(allPathsIndexMap.keySet())) {
        if (!indexInfoMap.containsKey(indexType)) {
          try {
            allPathsIndexMap.get(indexType).closeAndRelease();
          } catch (IOException e) {
            logger.error("Meet error when close {} before removing index", indexType, e);
          }
          // remove index file directories
          File dir = IndexUtils.getIndexFile(getIndexDir(indexType));
          try {
            Files.delete(dir.toPath());
          } catch (IOException e) {
            logger.warn("Remove index dir {} failed.", dir, e);
          }
          allPathsIndexMap.remove(indexType);
          usableMap.remove(indexType);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** For unsequence data, mark them as "index-unusable" in corresponding IIndexUsable. */
  void updateUnsequenceData(PartialPath path, TVList tvList) {
    this.usableMap.forEach(
        (indexType, usable) ->
            usable.minusUsableRange(path, tvList.getMinTime(), tvList.getLastTime()));
  }

  /**
   * index query.
   *
   * @param indexType the type of index to be queried
   * @param queryProps the query conditions
   * @param context the query context
   * @param alignedByTime true if result series need to be aligned by the timestamp.
   * @return index query result
   */
  public QueryDataSet query(
      IndexType indexType,
      Map<String, Object> queryProps,
      QueryContext context,
      boolean alignedByTime)
      throws QueryIndexException {
    try {
      lock.readLock().lock();
      try {
        if (!indexLockMap.containsKey(indexType)) {
          throw new QueryIndexException(
              String.format(
                  "%s hasn't been built on %s", indexType.toString(), indexSeries.getFullPath()));
        } else {
          indexLockMap.get(indexType).readLock().lock();
        }
      } finally {
        lock.readLock().unlock();
      }
      IoTDBIndex index = allPathsIndexMap.get(indexType);
      return index.query(
          queryProps, this.usableMap.get(indexType), context, refinePhaseOptimizer, alignedByTime);

    } finally {
      indexLockMap.get(indexType).readLock().unlock();
    }
  }
}
