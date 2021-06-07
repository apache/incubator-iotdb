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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.heavyhitter.QueryHeavyHitters;
import org.apache.iotdb.db.engine.compaction.heavyhitter.hitters.space.saving.Counter;
import org.apache.iotdb.db.engine.compaction.heavyhitter.hitters.space.saving.StreamSummary;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SpaceSavingHitter extends DefaultHitter implements QueryHeavyHitters {

  private static final Logger logger = LoggerFactory.getLogger(SpaceSavingHitter.class);
  private StreamSummary<PartialPath> streamSummary;
  private int counterRatio = IoTDBDescriptor.getInstance().getConfig().getCounterRatio();

  public SpaceSavingHitter(int maxHitterNum) {
    super(maxHitterNum);
    double error = 1.0 / (maxHitterNum * counterRatio);
    streamSummary = new StreamSummary<>(error);
  }

  @Override
  public void acceptQuerySeries(PartialPath queryPath) {
    streamSummary.offer(queryPath);
  }

  @Override
  public List<PartialPath> getTopCompactionSeries(PartialPath sgName) throws MetadataException {
    hitterLock.writeLock().lock();
    try {
      List<PartialPath> ret = new ArrayList<>();
      List<PartialPath> topSeries =
          streamSummary.getTopK(maxHitterNum).stream()
              .map(Counter::getItem)
              .collect(Collectors.toList());
      Set<PartialPath> sgPaths = new HashSet<>(MManager.getInstance().getAllTimeseriesPath(sgName));
      for (PartialPath series : topSeries) {
        if (sgPaths.contains(series)) {
          ret.add(series);
        }
      }
      return ret;
    } finally {
      hitterLock.writeLock().unlock();
    }
  }

  @Override
  public void clear() {
    hitterLock.writeLock().lock();
    try {
      double error = 1.0 / (maxHitterNum * counterRatio);
      streamSummary = new StreamSummary<>(error);
    } finally {
      hitterLock.writeLock().unlock();
    }
  }
}
