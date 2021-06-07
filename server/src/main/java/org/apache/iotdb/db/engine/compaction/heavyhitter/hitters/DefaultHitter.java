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

package org.apache.iotdb.db.engine.compaction.heavyhitter.hitters;

import org.apache.iotdb.db.engine.compaction.heavyhitter.QueryHeavyHitters;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.MergeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** default query hitter, return first device's time series for hitter compaction */
public class DefaultHitter implements QueryHeavyHitters {

  private static final Logger logger = LoggerFactory.getLogger(DefaultHitter.class);
  protected final ReadWriteLock hitterLock = new ReentrantReadWriteLock();
  protected final int maxHitterNum;

  public DefaultHitter(int maxHitterNum) {
    this.maxHitterNum = maxHitterNum;
  }

  @Override
  public void acceptQuerySeriesList(List<PartialPath> queryPaths) {
    hitterLock.writeLock().lock();
    try {
      for (PartialPath path : queryPaths) {
        acceptQuerySeries(path);
      }
    } finally {
      hitterLock.writeLock().unlock();
    }
  }

  @Override
  public void acceptQuerySeries(PartialPath queryPath) {
    // do nothing
  }

  @Override
  public List<PartialPath> getTopCompactionSeries(PartialPath sgName) throws MetadataException {
    hitterLock.writeLock().lock();
    try {
      List<PartialPath> unmergedSeries = MManager.getInstance().getAllTimeseriesPath(sgName);
      List<List<PartialPath>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
      if (devicePaths.size() > 0) {
        String deviceName = devicePaths.get(0).get(0).getDevice();
        logger.info("default hitter, top compaction device:{}", deviceName);
        return devicePaths.get(0);
      }
      return null;
    } finally {
      hitterLock.writeLock().unlock();
    }
  }

  @Override
  public void clear() {
    // do nothing
  }
}
