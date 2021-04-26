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
package org.apache.iotdb.db.index.router;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexProcessorStruct;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.func.CreateIndexProcessorFunc;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A proto implementation.
 *
 * <p>The subsequence-matching index is created on a single time series, while the whole-matching
 * index is created on a group of time series with wildcards, so {@code ProtoIndexRouter} manages
 * the two cases with different Map structures.
 *
 * <p>A key function of {@code IIndexRouter } is to quickly route the IndexProcessor for a given
 * series path. If the path is full-path, it can be found as O(1) in {@code fullPathProcessorMap};
 * Otherwise, you must traverse every key in {@code wildCardProcessorMap}.
 */
public class ProtoIndexRouter implements IIndexRouter {

  private static final Logger logger = LoggerFactory.getLogger(ProtoIndexRouter.class);

  /** for subsequence matching indexes. */
  private Map<String, IndexProcessorStruct> fullPathProcessorMap;
  /** for whole matching indexes. */
  private Map<PartialPath, IndexProcessorStruct> wildCardProcessorMap;

  private Map<String, Set<String>> sgToFullPathMap;
  private Map<String, Set<PartialPath>> sgToWildCardPathMap;
  private MManager mManager;
  private File routerFile;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private boolean unmodifiable;

  ProtoIndexRouter(String routerFileDir) {
    this(false);
    this.routerFile = SystemFileFactory.INSTANCE.getFile(routerFileDir + File.separator + "router");
    mManager = MManager.getInstance();
  }

  private ProtoIndexRouter(boolean unmodifiable) {
    this.unmodifiable = unmodifiable;
    fullPathProcessorMap = new ConcurrentHashMap<>();
    sgToFullPathMap = new ConcurrentHashMap<>();
    sgToWildCardPathMap = new ConcurrentHashMap<>();
    wildCardProcessorMap = new ConcurrentHashMap<>();
  }

  @Override
  public void serialize(boolean closeProcessor) {
    try (OutputStream outputStream = new FileOutputStream(routerFile)) {
      ReadWriteIOUtils.write(fullPathProcessorMap.size(), outputStream);
      for (Entry<String, IndexProcessorStruct> entry : fullPathProcessorMap.entrySet()) {
        String indexSeries = entry.getKey();
        IndexProcessorStruct v = entry.getValue();
        ReadWriteIOUtils.write(indexSeries, outputStream);
        ReadWriteIOUtils.write(v.infos.size(), outputStream);
        for (Entry<IndexType, IndexInfo> e : v.infos.entrySet()) {
          IndexInfo indexInfo = e.getValue();
          indexInfo.serialize(outputStream);
        }
        if (closeProcessor && v.processor != null) {
          v.processor.close(false);
        }
      }

      ReadWriteIOUtils.write(wildCardProcessorMap.size(), outputStream);
      for (Entry<PartialPath, IndexProcessorStruct> entry : wildCardProcessorMap.entrySet()) {
        PartialPath indexSeries = entry.getKey();
        IndexProcessorStruct v = entry.getValue();
        ReadWriteIOUtils.write(indexSeries.getFullPath(), outputStream);
        ReadWriteIOUtils.write(v.infos.size(), outputStream);
        for (Entry<IndexType, IndexInfo> e : v.infos.entrySet()) {
          IndexInfo indexInfo = e.getValue();
          indexInfo.serialize(outputStream);
        }
        if (closeProcessor && v.processor != null) {
          v.processor.close(false);
        }
      }
    } catch (IOException e) {
      logger.error("Error when serialize router. Given up.", e);
    }
    if (closeProcessor) {
      fullPathProcessorMap.clear();
      sgToFullPathMap.clear();
      sgToWildCardPathMap.clear();
      wildCardProcessorMap.clear();
    }
  }

  @Override
  public void deserializeAndReload(CreateIndexProcessorFunc func) {
    if (!routerFile.exists()) {
      return;
    }
    try (InputStream inputStream = new FileInputStream(routerFile)) {
      int fullSize = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < fullSize; i++) {
        String indexSeries = ReadWriteIOUtils.readString(inputStream);
        int indexTypeSize = ReadWriteIOUtils.readInt(inputStream);
        for (int j = 0; j < indexTypeSize; j++) {
          IndexInfo indexInfo = IndexInfo.deserialize(inputStream);
          addIndexIntoRouter(new PartialPath(indexSeries), indexInfo, func, false);
        }
      }

      int wildcardSize = ReadWriteIOUtils.readInt(inputStream);
      for (int i = 0; i < wildcardSize; i++) {
        String indexSeries = ReadWriteIOUtils.readString(inputStream);
        int indexTypeSize = ReadWriteIOUtils.readInt(inputStream);
        for (int j = 0; j < indexTypeSize; j++) {
          IndexInfo indexInfo = IndexInfo.deserialize(inputStream);
          addIndexIntoRouter(new PartialPath(indexSeries), indexInfo, func, false);
        }
      }
    } catch (MetadataException | IOException e) {
      logger.error("Error when deserialize router. Given up.", e);
    }
  }

  @Override
  public IIndexRouter getRouterByStorageGroup(String storageGroupPath) {
    lock.readLock().lock();
    try {
      ProtoIndexRouter res = new ProtoIndexRouter(true);
      if (sgToWildCardPathMap.containsKey(storageGroupPath)) {
        for (PartialPath partialPath : sgToWildCardPathMap.get(storageGroupPath)) {
          res.wildCardProcessorMap.put(partialPath, this.wildCardProcessorMap.get(partialPath));
        }
      }
      if (sgToFullPathMap.containsKey(storageGroupPath)) {
        for (String fullPath : sgToFullPathMap.get(storageGroupPath)) {
          res.fullPathProcessorMap.put(fullPath, this.fullPathProcessorMap.get(fullPath));
        }
      }
      return res;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int getIndexNum() {
    return fullPathProcessorMap.size() + wildCardProcessorMap.size();
  }

  @Override
  public IndexProcessorStruct startQueryAndCheck(
      PartialPath partialPath, IndexType indexType, QueryContext context)
      throws QueryIndexException {
    lock.readLock().lock();
    try {
      IndexProcessorStruct struct;
      if (partialPath.isFullPath()) {
        String fullPath = partialPath.getFullPath();
        if (!fullPathProcessorMap.containsKey(fullPath)) {
          throw new QueryIndexException("haven't create index on " + fullPath);
        }
        struct = fullPathProcessorMap.get(fullPath);
      } else {
        if (!wildCardProcessorMap.containsKey(partialPath)) {
          throw new QueryIndexException("haven't create index on " + partialPath);
        }
        struct = wildCardProcessorMap.get(partialPath);
      }
      if (struct.processor == null) {
        throw new QueryIndexException("Index hasn't insert any data");
      }
      // we don't add lock to index processor here. It doesn't matter.
      return struct;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void endQuery(PartialPath indexSeries, IndexType indexType, QueryContext context) {
    // do nothing.
  }

  @Override
  public boolean addIndexIntoRouter(
      PartialPath partialPath,
      IndexInfo indexInfo,
      CreateIndexProcessorFunc func,
      boolean doSerialize)
      throws MetadataException {
    if (unmodifiable) {
      throw new MetadataException("cannot add index to unmodifiable router");
    }
    partialPath.toLowerCase();
    // only the pair.left (indexType map) will be updated.
    lock.writeLock().lock();
    IndexType indexType = indexInfo.getIndexType();
    // record the relationship between storage group and the
    StorageGroupMNode storageGroupMNode = mManager.getStorageGroupNodeByPath(partialPath);
    String storageGroupPath = storageGroupMNode.getPartialPath().getFullPath();
    // add to pathMap
    try {
      if (partialPath.isFullPath()) {
        String fullPath = partialPath.getFullPath();
        if (!fullPathProcessorMap.containsKey(fullPath)) {
          Map<IndexType, IndexInfo> infoMap = new EnumMap<>(IndexType.class);
          infoMap.put(indexType, indexInfo);
          IndexProcessor processor = func.act(partialPath, infoMap);
          fullPathProcessorMap.put(
              fullPath, new IndexProcessorStruct(processor, partialPath, infoMap));
        } else {
          fullPathProcessorMap.get(fullPath).infos.put(indexType, indexInfo);
        }
        IndexProcessorStruct pair = fullPathProcessorMap.get(fullPath);
        pair.processor.refreshSeriesIndexMapFromMManager(pair.infos);

        // add to sg
        Set<String> indexSeriesSet = new HashSet<>();
        Set<String> preSet = sgToFullPathMap.putIfAbsent(storageGroupPath, indexSeriesSet);
        if (preSet != null) {
          indexSeriesSet = preSet;
        }
        indexSeriesSet.add(fullPath);
      } else {
        if (!wildCardProcessorMap.containsKey(partialPath)) {
          Map<IndexType, IndexInfo> infoMap = new EnumMap<>(IndexType.class);
          infoMap.put(indexType, indexInfo);
          IndexProcessor processor = func.act(partialPath, infoMap);
          PartialPath representativePath = getRepresentativePath(partialPath);
          wildCardProcessorMap.put(
              partialPath, new IndexProcessorStruct(processor, representativePath, infoMap));
        } else {
          wildCardProcessorMap.get(partialPath).infos.put(indexType, indexInfo);
        }
        IndexProcessorStruct pair = wildCardProcessorMap.get(partialPath);
        pair.processor.refreshSeriesIndexMapFromMManager(pair.infos);

        // add to sg
        Set<PartialPath> indexSeriesSet = new HashSet<>();
        Set<PartialPath> preSet = sgToWildCardPathMap.putIfAbsent(storageGroupPath, indexSeriesSet);
        if (preSet != null) {
          indexSeriesSet = preSet;
        }
        indexSeriesSet.add(partialPath);
      }
      if (doSerialize) {
        serialize(false);
      }
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  private PartialPath getRepresentativePath(PartialPath wildcardPath) throws MetadataException {
    Pair<List<PartialPath>, Integer> paths =
        mManager.getAllTimeseriesPathWithAlias(wildcardPath, 1, 0);
    if (paths.left.isEmpty()) {
      throw new MetadataException("Please create at least one series before create index");
    } else {
      return paths.left.get(0);
    }
  }

  @Override
  public boolean removeIndexFromRouter(PartialPath partialPath, IndexType indexType)
      throws MetadataException, IOException {
    if (unmodifiable) {
      throw new MetadataException("cannot remove index from unmodifiable router");
    }
    partialPath.toLowerCase();
    // only the pair.left (indexType map) will be updated.
    lock.writeLock().lock();
    // record the relationship between storage group and the index processors
    StorageGroupMNode storageGroupMNode = mManager.getStorageGroupNodeByPath(partialPath);
    String storageGroupPath = storageGroupMNode.getPartialPath().getFullPath();

    // remove from pathMap
    try {
      if (partialPath.isFullPath()) {
        String fullPath = partialPath.getFullPath();
        if (fullPathProcessorMap.containsKey(fullPath)) {
          IndexProcessorStruct pair = fullPathProcessorMap.get(fullPath);
          pair.infos.remove(indexType);
          pair.processor.refreshSeriesIndexMapFromMManager(pair.infos);
          if (pair.infos.isEmpty()) {
            sgToFullPathMap.get(storageGroupPath).remove(fullPath);
            fullPathProcessorMap.remove(fullPath);
            pair.representativePath = null;
            pair.processor.close(true);
            pair.processor = null;
          }
        }
      } else {
        if (wildCardProcessorMap.containsKey(partialPath)) {
          IndexProcessorStruct pair = wildCardProcessorMap.get(partialPath);
          pair.infos.remove(indexType);
          pair.processor.refreshSeriesIndexMapFromMManager(pair.infos);
          if (pair.infos.isEmpty()) {
            sgToWildCardPathMap.get(storageGroupPath).remove(partialPath);
            wildCardProcessorMap.remove(partialPath);
            pair.representativePath = null;
            pair.processor.close(true);
            pair.processor = null;
          }
        }
      }
      serialize(false);
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  @Override
  public Map<IndexType, IndexInfo> getIndexInfosByIndexSeries(PartialPath indexSeries) {
    lock.readLock().lock();
    try {
      if (wildCardProcessorMap.containsKey(indexSeries)) {
        return Collections.unmodifiableMap(wildCardProcessorMap.get(indexSeries).infos);
      }
      if (fullPathProcessorMap.containsKey(indexSeries.getFullPath())) {
        return Collections.unmodifiableMap(
            fullPathProcessorMap.get(indexSeries.getFullPath()).infos);
      }
    } finally {
      lock.readLock().unlock();
    }
    return new EnumMap<>(IndexType.class);
  }

  @Override
  public Iterable<IndexProcessorStruct> getAllIndexProcessorsAndInfo() {
    lock.readLock().lock();
    List<IndexProcessorStruct> res =
        new ArrayList<>(wildCardProcessorMap.size() + fullPathProcessorMap.size());
    try {
      wildCardProcessorMap.forEach((k, v) -> res.add(v));
      fullPathProcessorMap.forEach((k, v) -> res.add(v));
    } finally {
      lock.readLock().unlock();
    }
    return res;
  }

  @Override
  public Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath timeSeries) {
    lock.readLock().lock();
    List<IndexProcessor> res = new ArrayList<>();
    try {
      if (fullPathProcessorMap.containsKey(timeSeries.getFullPath())) {
        res.add(fullPathProcessorMap.get(timeSeries.getFullPath()).processor);
      } else {
        wildCardProcessorMap.forEach(
            (k, v) -> {
              if (k.matchFullPath(timeSeries)) {
                res.add(v.processor);
              }
            });
      }
    } finally {
      lock.readLock().unlock();
    }
    return res;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterable<IndexProcessorStruct> all = getAllIndexProcessorsAndInfo();
    for (IndexProcessorStruct struct : all) {
      sb.append(struct.toString()).append("\n");
    }
    return sb.toString();
  }
}
