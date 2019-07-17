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
package org.apache.iotdb.db.engine.cache;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to cache <code>TsFileMetaData</code> of tsfile in IoTDB.
 */
public class TsFileMetaDataCache {

  private static final Logger logger = LoggerFactory.getLogger(TsFileMetaDataCache.class);


  private static final int MEMORY_THRESHOLD_IN_B = (int) (50 * 0.3 * 1024 * 1024 * 1024);
  /**
   * key: The file seriesPath of tsfile. value: TsFileMetaData
   */
  private LruLinkedHashMap<String, TsFileMetaData> cache;
  private AtomicLong cacheHintNum = new AtomicLong();
  private AtomicLong cacheRequestNum = new AtomicLong();

  /**
   * estimate size of a deviceIndexMap entry in TsFileMetaData.
   */
  private long deviceIndexMapEntrySize = 0;
  /**
   * estimate size of measurementSchema entry in TsFileMetaData.
   */
  private long measurementSchemaEntrySize = 0;
  /**
   * estimate size of version and CreateBy in TsFileMetaData.
   */
  private long versionAndCreatebySize = 10;

  private TsFileMetaDataCache() {
    cache = new LruLinkedHashMap<String, TsFileMetaData>(MEMORY_THRESHOLD_IN_B, true) {
      @Override
      protected long calEntrySize(String key, TsFileMetaData value) {
        if (deviceIndexMapEntrySize == 0 && value.getDeviceMap().size() > 0) {
          deviceIndexMapEntrySize = RamUsageEstimator
              .sizeOf(value.getDeviceMap().entrySet().iterator().next());
        }
        if (measurementSchemaEntrySize == 0 && value.getMeasurementSchema().size() > 0) {
          measurementSchemaEntrySize = RamUsageEstimator
              .sizeOf(value.getMeasurementSchema().entrySet().iterator().next());
        }
        long valueSize = value.getDeviceMap().size() * deviceIndexMapEntrySize
            + measurementSchemaEntrySize * value.getMeasurementSchema().size()
            + versionAndCreatebySize;
        return key.length() + valueSize;
      }
    };
  }

  public static TsFileMetaDataCache getInstance() {
    return TsFileMetaDataCacheHolder.INSTANCE;
  }

  /**
   * get the TsFileMetaData for given path.
   *
   * @param path -given path
   */
  public TsFileMetaData get(String path) throws IOException {

    Object internPath = path.intern();
    cacheRequestNum.incrementAndGet();
    synchronized (cache) {
      if (cache.containsKey(path)) {
        cacheHintNum.incrementAndGet();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Cache hint: the number of requests for cache is {}, "
                  + "the number of hints for cache is {}",
              cacheRequestNum.get(), cacheHintNum.get());
        }
        return cache.get(path);
      }
    }
    synchronized (internPath) {
      synchronized (cache) {
        if (cache.containsKey(path)) {
          cacheHintNum.incrementAndGet();
          return cache.get(path);
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Cache didn't hint: the number of requests for cache is {}",
            cacheRequestNum.get());
      }
      TsFileMetaData fileMetaData = TsFileMetadataUtils.getTsFileMetaData(path);
      synchronized (cache) {
        cache.put(path, fileMetaData);
        return fileMetaData;
      }
    }
  }

  public void remove(String path) {
    synchronized (cache) {
      cache.remove(path);
    }
  }

  public void clear() {
    synchronized (cache) {
      cache.clear();
    }
  }

  /**
   * Singleton pattern
   */
  private static class TsFileMetaDataCacheHolder {

    private TsFileMetaDataCacheHolder() {
    }

    private static final TsFileMetaDataCache INSTANCE = new TsFileMetaDataCache();
  }
}
