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

package org.apache.iotdb.db.engine.compaction.heavyhitter;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.heavyhitter.hitters.DefaultHitter;
import org.apache.iotdb.db.engine.compaction.heavyhitter.hitters.HashMapHitter;
import org.apache.iotdb.db.engine.compaction.heavyhitter.hitters.SpaceSavingHitter;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

public class QueryHitterManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(QueryHitterManager.class);
  private static final QueryHitterManager INSTANCE = new QueryHitterManager();
  private ExecutorService pool;
  private QueryHeavyHitters queryHeavyHitters;

  public static QueryHitterManager getInstance() {
    return INSTANCE;
  }

  public QueryHeavyHitters getQueryHitter() {
    return queryHeavyHitters;
  }

  private QueryHeavyHitters loadQueryHitters() {
    switch (IoTDBDescriptor.getInstance().getConfig().getQueryHitterStrategy()) {
      case HASH_STRATEGY:
        return new HashMapHitter(IoTDBDescriptor.getInstance().getConfig().getMaxHitterNum());
      case SPACE_SAVING_STRATEGY:
        return new SpaceSavingHitter(IoTDBDescriptor.getInstance().getConfig().getMaxHitterNum());
      case DEFAULT_STRATEGY:
      default:
        return new DefaultHitter(IoTDBDescriptor.getInstance().getConfig().getMaxHitterNum());
    }
  }

  @Override
  public void start() throws StartupException {
    if (pool == null) {
      pool = IoTDBThreadPoolFactory.newScheduledThreadPool(1, ThreadName.HITTER_SERVICE.getName());
    }
    if (queryHeavyHitters == null) {
      queryHeavyHitters = loadQueryHitters();
    }
    logger.info("QueryHitterManager started.");
  }

  @Override
  public void stop() {
    if (pool != null) {
      long startTime = System.currentTimeMillis();
      while (!pool.isTerminated()) {
        int timeMillis = 0;
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          logger.error("QueryHitterManager {} shutdown", ThreadName.HITTER_SERVICE.getName(), e);
          Thread.currentThread().interrupt();
        }
        timeMillis += 200;
        long time = System.currentTimeMillis() - startTime;
        if (timeMillis % 60_000 == 0) {
          logger.warn("QueryHitterManager has wait for {} seconds to stop", time / 1000);
        }
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.HITTER_SERVICE;
  }

  public void submitTask(HitterTask hitterTask) {
    if (pool != null && !pool.isTerminated()) {
      pool.submit(hitterTask);
    }
  }

  public class HitterTask implements Runnable {

    private List<PartialPath> queryPaths;

    public HitterTask(List<PartialPath> queryPaths) {
      this.queryPaths = queryPaths;
    }

    @Override
    public void run() {
      getQueryHitter().acceptQuerySeriesList(queryPaths);
    }
  }
}
