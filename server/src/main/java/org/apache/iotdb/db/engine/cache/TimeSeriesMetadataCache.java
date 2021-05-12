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

package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.common.cache.Accountable;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is used to cache <code>TimeSeriesMetadata</code> in IoTDB. The caching strategy is
 * LRU.
 */
public class TimeSeriesMetadataCache {

  private static final Logger logger = LoggerFactory.getLogger(TimeSeriesMetadataCache.class);
  private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("QUERY_DEBUG");
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE =
      config.getAllocateMemoryForTimeSeriesMetaDataCache();
  private static final boolean CACHE_ENABLE = config.isMetaDataCacheEnable();

  private final LRULinkedHashMap<TimeSeriesMetadataCacheKey, TimeseriesMetadata> lruCache;

  private final AtomicLong cacheHitNum = new AtomicLong();
  private final AtomicLong cacheRequestNum = new AtomicLong();

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private final Map<String, WeakReference<String>> devices =
      Collections.synchronizedMap(new WeakHashMap<>());
  private static final String SEPARATOR = "$";

  private TimeSeriesMetadataCache() {
    if (CACHE_ENABLE) {
      logger.info(
          "TimeseriesMetadataCache size = " + MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE);
    }
    lruCache =
        new LRULinkedHashMap<TimeSeriesMetadataCacheKey, TimeseriesMetadata>(
            MEMORY_THRESHOLD_IN_TIME_SERIES_METADATA_CACHE) {

          @Override
          protected long calEntrySize(TimeSeriesMetadataCacheKey key, TimeseriesMetadata value) {
            long currentSize;
            if (count < 10) {
              currentSize =
                  RamUsageEstimator.shallowSizeOf(key)
                      + RamUsageEstimator.sizeOf(key.device)
                      + RamUsageEstimator.sizeOf(key.measurement)
                      + RamUsageEstimator.shallowSizeOf(value)
                      + RamUsageEstimator.sizeOf(value.getMeasurementId())
                      + RamUsageEstimator.shallowSizeOf(value.getStatistics())
                      + (((ChunkMetadata) value.getChunkMetadataList().get(0)).calculateRamSize()
                              + RamUsageEstimator.NUM_BYTES_OBJECT_REF)
                          * value.getChunkMetadataList().size()
                      + RamUsageEstimator.shallowSizeOf(value.getChunkMetadataList());
              averageSize = ((averageSize * count) + currentSize) / (++count);
            } else if (count < 100000) {
              count++;
              currentSize = averageSize;
            } else {
              averageSize =
                  RamUsageEstimator.shallowSizeOf(key)
                      + RamUsageEstimator.sizeOf(key.device)
                      + RamUsageEstimator.sizeOf(key.measurement)
                      + RamUsageEstimator.shallowSizeOf(value)
                      + RamUsageEstimator.sizeOf(value.getMeasurementId())
                      + RamUsageEstimator.shallowSizeOf(value.getStatistics())
                      + (((ChunkMetadata) value.getChunkMetadataList().get(0)).calculateRamSize()
                              + RamUsageEstimator.NUM_BYTES_OBJECT_REF)
                          * value.getChunkMetadataList().size()
                      + RamUsageEstimator.shallowSizeOf(value.getChunkMetadataList());
              count = 1;
              currentSize = averageSize;
            }
            return currentSize;
          }
        };
  }

  public static TimeSeriesMetadataCache getInstance() {
    return TimeSeriesMetadataCache.TimeSeriesMetadataCacheHolder.INSTANCE;
  }

  public TimeseriesMetadata get(TimeSeriesMetadataCacheKey key, Set<String> allSensors)
      throws IOException {
    return get(key, allSensors, false);
  }

  @SuppressWarnings("squid:S1860") // Suppress synchronize warning
  public TimeseriesMetadata get(
      TimeSeriesMetadataCacheKey key, Set<String> allSensors, boolean debug) throws IOException {
    if (!CACHE_ENABLE) {
      // bloom filter part
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.file, true);
      BloomFilter bloomFilter = reader.readBloomFilter();
      if (bloomFilter != null
          && !bloomFilter.contains(key.device + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
        return null;
      }
      return reader.readTimeseriesMetadata(new Path(key.device, key.measurement), false);
    }

    cacheRequestNum.incrementAndGet();

    TimeseriesMetadata timeseriesMetadata;
    lock.readLock().lock();
    try {
      timeseriesMetadata = lruCache.get(key);
    } finally {
      lock.readLock().unlock();
    }

    if (timeseriesMetadata != null) {
      cacheHitNum.incrementAndGet();
      printCacheLog(true);
    } else {
      if (debug) {
        DEBUG_LOGGER.info("Cache miss: {}.{} in file: {}", key.device, key.measurement, key.file);
        DEBUG_LOGGER.info("Device: {}, all sensors: {}", key.device, allSensors);
      }
      // allow for the parallelism of different devices
      synchronized (
          devices.computeIfAbsent(key.device + SEPARATOR + key.file, WeakReference::new)) {
        // double check
        lock.readLock().lock();
        try {
          timeseriesMetadata = lruCache.get(key);
        } finally {
          lock.readLock().unlock();
        }
        if (timeseriesMetadata != null) {
          cacheHitNum.incrementAndGet();
          printCacheLog(true);
        } else {
          Path path = new Path(key.device, key.measurement);
          // bloom filter part
          TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.file, true);
          BloomFilter bloomFilter = reader.readBloomFilter();
          if (bloomFilter != null && !bloomFilter.contains(path.getFullPath())) {
            if (debug) {
              DEBUG_LOGGER.info("TimeSeries meta data {} is filter by bloomFilter!", key);
            }
            return null;
          }
          printCacheLog(false);
          List<TimeseriesMetadata> timeSeriesMetadataList =
              reader.readTimeseriesMetadata(path, allSensors);
          // put TimeSeriesMetadata of all sensors used in this query into cache
          lock.writeLock().lock();
          try {
            timeSeriesMetadataList.forEach(
                metadata -> {
                  TimeSeriesMetadataCacheKey k =
                      new TimeSeriesMetadataCacheKey(
                          key.file, key.device, metadata.getMeasurementId());
                  if (!lruCache.containsKey(k)) {
                    lruCache.put(k, metadata);
                  }
                });
            timeseriesMetadata = lruCache.get(key);
          } finally {
            lock.writeLock().unlock();
          }
        }
      }
    }
    if (timeseriesMetadata == null) {
      if (debug) {
        DEBUG_LOGGER.info("The file doesn't have this time series {}.", key);
      }
      return null;
    } else {
      if (debug) {
        DEBUG_LOGGER.info(
            "Get timeseries: {}.{}  metadata in file: {}  from cache: {}.",
            key.device,
            key.measurement,
            key.file,
            timeseriesMetadata);
      }
      return new TimeseriesMetadata(timeseriesMetadata);
    }
  }

  // Suppress synchronize warning
  // Suppress high Cognitive Complexity warning
  @SuppressWarnings({"squid:S1860", "squid:S3776"})
  public List<TimeseriesMetadata> get(
      TimeSeriesMetadataCacheKey key,
      List<String> subSensorList,
      Set<String> allSensors,
      boolean debug)
      throws IOException {
    if (!CACHE_ENABLE) {
      // bloom filter part
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.filePath, true);
      BloomFilter bloomFilter = reader.readBloomFilter();
      if (bloomFilter != null
          && !bloomFilter.contains(key.device + IoTDBConstant.PATH_SEPARATOR + key.measurement)) {
        return Collections.emptyList();
      }
      return reader.readTimeseriesMetadata(new Path(key.device, key.measurement), subSensorList);
    }

    cacheRequestNum.incrementAndGet();

    List<TimeseriesMetadata> res = new ArrayList<>();

    getVectorTimeSeriesMetadataListFromCache(key, subSensorList, res);

    if (!res.isEmpty()) {
      cacheHitNum.incrementAndGet();
      printCacheLog(true);
    } else {
      if (debug) {
        DEBUG_LOGGER.info(
            "Cache miss: {}.{} in file: {}", key.device, key.measurement, key.filePath);
        DEBUG_LOGGER.info("Device: {}, all sensors: {}", key.device, allSensors);
      }
      // allow for the parallelism of different devices
      synchronized (
          devices.computeIfAbsent(key.device + SEPARATOR + key.filePath, WeakReference::new)) {
        // double check
        getVectorTimeSeriesMetadataListFromCache(key, subSensorList, res);
        if (!res.isEmpty()) {
          cacheHitNum.incrementAndGet();
          printCacheLog(true);
        } else {
          Path path = new Path(key.device, key.measurement);
          // bloom filter part
          TsFileSequenceReader reader = FileReaderManager.getInstance().get(key.filePath, true);
          BloomFilter bloomFilter = reader.readBloomFilter();
          if (bloomFilter != null && !bloomFilter.contains(path.getFullPath())) {
            if (debug) {
              DEBUG_LOGGER.info("TimeSeries meta data {} is filter by bloomFilter!", key);
            }
            return Collections.emptyList();
          }
          printCacheLog(false);
          List<TimeseriesMetadata> timeSeriesMetadataList =
              reader.readTimeseriesMetadata(path, allSensors);
          // put TimeSeriesMetadata of all sensors used in this query into cache
          lock.writeLock().lock();
          try {
            timeSeriesMetadataList.forEach(
                metadata -> {
                  TimeSeriesMetadataCacheKey k =
                      new TimeSeriesMetadataCacheKey(
                          key.filePath, key.device, metadata.getMeasurementId());
                  if (!lruCache.containsKey(k)) {
                    lruCache.put(k, metadata);
                  }
                });
            getVectorTimeSeriesMetadataListFromCache(key, subSensorList, res);
          } finally {
            lock.writeLock().unlock();
          }
        }
      }
    }
    if (res.isEmpty()) {
      if (debug) {
        DEBUG_LOGGER.info("The file doesn't have this time series {}.", key);
      }
      return Collections.emptyList();
    } else {
      if (debug) {
        DEBUG_LOGGER.info(
            "Get timeseries: {}.{}  metadata in file: {}  from cache: {}.",
            key.device,
            key.measurement,
            key.filePath,
            res);
      }
      for (int i = 0; i < res.size(); i++) {
        res.set(i, new TimeseriesMetadata(res.get(i)));
      }
      return res;
    }
  }

  private void getVectorTimeSeriesMetadataListFromCache(
      TimeSeriesMetadataCacheKey key, List<String> subSensorList, List<TimeseriesMetadata> res) {
    lock.readLock().lock();
    try {
      TimeseriesMetadata timeseriesMetadata = lruCache.get(key);
      if (timeseriesMetadata != null) {
        res.add(timeseriesMetadata);
        for (String subSensor : subSensorList) {
          timeseriesMetadata =
              lruCache.get(new TimeSeriesMetadataCacheKey(key.filePath, key.device, subSensor));
          if (timeseriesMetadata != null) {
            res.add(timeseriesMetadata);
          } else {
            res.clear();
            break;
          }
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  private void printCacheLog(boolean isHit) {
    if (!logger.isDebugEnabled()) {
      return;
    }
    logger.debug(
        "[TimeSeriesMetadata cache {}hit] The number of requests for cache is {}, hit rate is {}.",
        isHit ? "" : "didn't ",
        cacheRequestNum.get(),
        cacheHitNum.get() * 1.0 / cacheRequestNum.get());
  }

  public double calculateTimeSeriesMetadataHitRatio() {
    if (cacheRequestNum.get() != 0) {
      return cacheHitNum.get() * 1.0 / cacheRequestNum.get();
    } else {
      return 0;
    }
  }

  public long getUsedMemory() {
    return lruCache.getUsedMemory();
  }

  public long getMaxMemory() {
    return lruCache.getMaxMemory();
  }

  public double getUsedMemoryProportion() {
    return lruCache.getUsedMemoryProportion();
  }

  public long getAverageSize() {
    return lruCache.getAverageSize();
  }

  /** clear LRUCache. */
  public void clear() {
    lock.writeLock().lock();
    if (lruCache != null) {
      lruCache.clear();
    }
    lock.writeLock().unlock();
  }

  public void remove(TimeSeriesMetadataCacheKey key) {
    lock.writeLock().lock();
    if (key != null) {
      lruCache.remove(key);
    }
    lock.writeLock().unlock();
  }

  @TestOnly
  public boolean isEmpty() {
    return lruCache.isEmpty();
  }

  public static class TimeSeriesMetadataCacheKey implements Accountable {

    private final File file;
    private final String device;
    private final String measurement;

    private long ramSize;

    public TimeSeriesMetadataCacheKey(File file, String device, String measurement) {
      this.file = file;
      this.device = device;
      this.measurement = measurement;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TimeSeriesMetadataCacheKey that = (TimeSeriesMetadataCacheKey) o;
      return Objects.equals(file, that.file)
          && Objects.equals(device, that.device)
          && Objects.equals(measurement, that.measurement);
    }

    @Override
    public int hashCode() {
      return Objects.hash(file, device, measurement);
    }

    @Override
    public void setRamSize(long size) {
      this.ramSize = size;
    }

    @Override
    public long getRamSize() {
      return ramSize;
    }
  }

  /** singleton pattern. */
  private static class TimeSeriesMetadataCacheHolder {

    private static final TimeSeriesMetadataCache INSTANCE = new TimeSeriesMetadataCache();
  }
}
