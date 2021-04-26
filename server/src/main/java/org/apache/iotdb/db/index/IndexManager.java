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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexProcessorStruct;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.index.router.IIndexRouter;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.FileUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.iotdb.db.index.common.IndexConstant.INDEX_DATA_DIR_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.META_DIR_NAME;
import static org.apache.iotdb.db.index.common.IndexConstant.ROUTER_DIR;

/**
 * IndexManager is the global manager of index framework, which will be called by IoTDB when index
 * creation, deletion, query and insertion. IndexManager will pass the index operations to the
 * corresponding IndexProcessor.
 */
public class IndexManager implements IndexManagerMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(IndexManager.class);
  /**
   * The index root directory. All index metadata files, index data files are stored in this
   * directory.
   */
  private final String indexRootDirPath;

  private final String indexMetaDirPath;
  private final String indexRouterDir;
  private final String indexDataDirPath;
  private final IIndexRouter router;

  /** A function interface to construct an index processor. */
  private CreateIndexProcessorFunc createIndexProcessorFunc;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private IndexManager() {
    indexRootDirPath = DirectoryManager.getInstance().getIndexRootFolder();
    indexMetaDirPath = indexRootDirPath + File.separator + META_DIR_NAME;
    indexRouterDir = indexMetaDirPath + File.separator + ROUTER_DIR;
    indexDataDirPath = indexRootDirPath + File.separator + INDEX_DATA_DIR_NAME;
    createIndexProcessorFunc =
        (indexSeries, indexInfoMap) ->
            new IndexProcessor(
                indexSeries,
                IndexUtils.removeIllegalStarInDir(indexDataDirPath + File.separator + indexSeries));
    router = IIndexRouter.Factory.getIndexRouter(indexRouterDir);
  }

  /**
   * Given an IndexSeries, return its feature file path (unused currently).
   *
   * @param path the path on which the index is created, e.g. Root.ery.*.Glu or Root.Wind.d1.Speed.
   * @param indexType the type of index
   * @return the feature directory path for this index.
   */
  private String getFeatureFileDirectory(PartialPath path, IndexType indexType) {
    return IndexUtils.removeIllegalStarInDir(
        indexDataDirPath + File.separator + path.getFullPath() + File.separator + indexType);
  }

  /**
   * Given an IndexSeries, return its data file path (unused currently).
   *
   * @param path the path on which the index is created, e.g. Root.ery.*.Glu or Root.Wind.d1.Speed.
   * @param indexType the type of index
   * @return the feature directory path for this index.
   */
  private String getIndexDataDirectory(PartialPath path, IndexType indexType) {
    return getFeatureFileDirectory(path, indexType);
  }

  /**
   * Execute the index creation. Due to the complex mapping relationship between the time series and
   * the index instances, we encapsulate the index metadata management into the router {@link
   * IIndexRouter} for stability.
   *
   * @param indexSeriesList a singleton list up to now.
   * @param indexInfo the index information.
   */
  public void createIndex(List<PartialPath> indexSeriesList, IndexInfo indexInfo)
      throws MetadataException {
    if (!indexSeriesList.isEmpty()) {
      router.addIndexIntoRouter(indexSeriesList.get(0), indexInfo, createIndexProcessorFunc, true);
    }
  }

  /**
   * Execute the index deletion.
   *
   * @param indexSeriesList a singleton list up to now.
   * @param indexType the index type to be dropped.
   */
  public void dropIndex(List<PartialPath> indexSeriesList, IndexType indexType)
      throws MetadataException, IOException {
    if (!indexSeriesList.isEmpty()) {
      router.removeIndexFromRouter(indexSeriesList.get(0), indexType);
    }
  }

  /**
   * When the storage group flushes，we construct {@link IndexMemTableFlushTask} for index insertion。
   *
   * <p>So far, the index insertion is triggered only when Memtables flush. A storage group contains
   * several series and each of these series may create several indexes. In other words, one storage
   * group may correspond to several {@linkplain IndexProcessor}.
   *
   * <p>This method return a router to find all {@linkplain IndexProcessor} related to this storage
   * group.
   *
   * @param storageGroupPath the path of the storage group
   * @param sequence true if it's sequence data, otherwise it's unsequence data
   * @return a router to find all {@linkplain IndexProcessor} related to this storage group, and
   *     other informations
   * @see IndexMemTableFlushTask
   */
  public IndexMemTableFlushTask getIndexMemFlushTask(String storageGroupPath, boolean sequence) {
    // StorageGroupPath may contain file separator, we put a temp patch here.
    storageGroupPath = storageGroupPath.replace(File.separatorChar, '/');
    String realStorageGroupPath = storageGroupPath.split("/")[0];
    IIndexRouter sgRouter = router.getRouterByStorageGroup(realStorageGroupPath);
    return new IndexMemTableFlushTask(sgRouter, sequence);
  }

  /**
   * Index query.
   *
   * <p>The initial idea is that index instances only process the "pruning phase" to prune some
   * negative items and return a candidate list, the framework finishes the rest (so-called
   * "post-processing phase" or "refinement phase", to query the raw time series by the candidate
   * list and then to verified which series in candidate list are real positive results).
   *
   * <p>The above design is common enough for all of similarity index methods. However, index
   * technology has various optimizations, and enforcing the above strategy will affect the freedom
   * of index integration. The two implemented indexes (ELB index and RTree index) have their own
   * optimizations which combine the pruning phase and post-processing phase. Therefore, in current
   * version, the query process is entirely passed to the index instance.
   *
   * @param paths the series to be queried.
   * @param indexType the index type to be queried.
   * @param queryProps the properties of this query.
   * @param context the query context.
   * @param alignedByTime whether aligned index result by timestamps.
   * @return the index query result.
   */
  public QueryDataSet queryIndex(
      List<PartialPath> paths,
      IndexType indexType,
      Map<String, Object> queryProps,
      QueryContext context,
      boolean alignedByTime)
      throws QueryIndexException, StorageEngineException {
    if (paths.size() != 1) {
      throw new QueryIndexException("Index allows to query only one path");
    }
    PartialPath queryIndexSeries = paths.get(0);
    IndexProcessorStruct indexProcessorStruct =
        router.startQueryAndCheck(queryIndexSeries, indexType, context);
    List<StorageGroupProcessor> list = indexProcessorStruct.addMergeLock();
    try {
      return indexProcessorStruct.processor.query(indexType, queryProps, context, alignedByTime);
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
      router.endQuery(indexProcessorStruct.processor.getIndexSeries(), indexType, context);
    }
  }

  private void prepareDirectory() {
    File rootDir = IndexUtils.getIndexFile(indexRootDirPath);
    if (!rootDir.exists()) {
      rootDir.mkdirs();
    }
    File routerDir = IndexUtils.getIndexFile(indexRouterDir);
    if (!routerDir.exists()) {
      routerDir.mkdirs();
    }
    File metaDir = IndexUtils.getIndexFile(indexMetaDirPath);
    if (!metaDir.exists()) {
      metaDir.mkdirs();
    }
    File dataDir = IndexUtils.getIndexFile(indexDataDirPath);
    if (!dataDir.exists()) {
      dataDir.mkdirs();
    }
  }

  private void deleteDroppedIndexData() throws IllegalPathException {
    for (File processorDataDir :
        Objects.requireNonNull(IndexUtils.getIndexFile(indexDataDirPath).listFiles())) {
      String processorName = processorDataDir.getName();
      Map<IndexType, IndexInfo> infos =
          router.getIndexInfosByIndexSeries(new PartialPath(processorName));
      if (infos.isEmpty()) {
        FileUtils.deleteDirectory(processorDataDir);
      } else {
        for (File indexDataDir : Objects.requireNonNull(processorDataDir.listFiles())) {
          if (indexDataDir.isDirectory()
              && !infos.containsKey(IndexType.valueOf(indexDataDir.getName()))) {
            FileUtils.deleteDirectory(indexDataDir);
          }
        }
      }
    }
  }

  /** close the index manager. */
  private synchronized void close() {
    router.serialize(true);
  }

  @Override
  public void start() throws StartupException {
    if (!config.isEnableIndex()) {
      return;
    }
    IndexBuildTaskPoolManager.getInstance().start();
    try {
      JMXService.registerMBean(this, ServiceType.INDEX_SERVICE.getJmxName());
      prepareDirectory();
      router.deserializeAndReload(createIndexProcessorFunc);
      deleteDroppedIndexData();
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  /**
   * As IoTDB has no normal shutdown mechanism, this function will not be called. To ensure the
   * information safety, The router needs to serialize index metadata every time createIndex or
   * dropIndex is called.
   */
  @Override
  public void stop() {
    if (!config.isEnableIndex()) {
      return;
    }
    close();
    IndexBuildTaskPoolManager.getInstance().stop();
    JMXService.deregisterMBean(ServiceType.INDEX_SERVICE.getJmxName());
  }

  public static IndexManager getInstance() {
    return InstanceHolder.instance;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.INDEX_SERVICE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static IndexManager instance = new IndexManager();
  }

  /** Delete all index directories. */
  @TestOnly
  public synchronized void deleteAll() {
    logger.info("Start deleting all storage groups' timeseries");
    close();

    File indexMetaDir = IndexUtils.getIndexFile(this.indexMetaDirPath);
    if (indexMetaDir.exists()) {
      FileUtils.deleteDirectory(indexMetaDir);
    }

    File indexDataDir = IndexUtils.getIndexFile(this.indexDataDirPath);
    if (indexDataDir.exists()) {
      FileUtils.deleteDirectory(indexDataDir);
    }
    File indexRootDir =
        IndexUtils.getIndexFile(DirectoryManager.getInstance().getIndexRootFolder());
    if (indexRootDir.exists()) {
      FileUtils.deleteDirectory(indexRootDir);
    }
  }

  @TestOnly
  public IIndexRouter getRouter() {
    return router;
  }
}
