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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

public class HashMapHitter extends DefaultHitter implements QueryHeavyHitters {

  private static final Logger logger = LoggerFactory.getLogger(HashMapHitter.class);
  private Map<PartialPath, Integer> counter = new HashMap<>();
  private PriorityQueue<Entry<PartialPath, Integer>> topHeap =
      new PriorityQueue<>((o1, o2) -> o1.getValue() - o2.getValue());

  public HashMapHitter(int maxHitterNum) {
    super(maxHitterNum);
  }

  @Override
  public void acceptQuerySeries(PartialPath queryPath) {
    if (queryPath == null) {
      return;
    }
    counter.put(queryPath, counter.getOrDefault(queryPath, 0) + 1);
  }

  @Override
  public List<PartialPath> getTopCompactionSeries(PartialPath sgName) throws MetadataException {
    hitterLock.writeLock().lock();
    try {
      List<PartialPath> ret = new ArrayList<>();
      for (Entry<PartialPath, Integer> entry : counter.entrySet()) {
        if (topHeap.size() < maxHitterNum) {
          topHeap.add(entry);
          continue;
        }
        int min = topHeap.peek().getValue();
        if (entry.getValue() > min) {
          topHeap.poll();
          topHeap.add(entry);
        }
      }
      Set<PartialPath> sgPaths = new HashSet<>(MManager.getInstance().getAllTimeseriesPath(sgName));
      while (!topHeap.isEmpty()) {
        PartialPath path = topHeap.poll().getKey();
        if (sgPaths.contains(path)) {
          ret.add(path);
        }
      }
      topHeap.clear();
      return ret;
    } finally {
      hitterLock.writeLock().unlock();
    }
  }

  @Override
  public void clear() {
    hitterLock.writeLock().lock();
    try {
      counter.clear();
    } finally {
      hitterLock.writeLock().unlock();
    }
  }

  /**
   * only for test, used to persist query frequency
   *
   * @param outputPath dump file name
   */
  private void dumpMap(File outputPath) {
    try (BufferedWriter csvWriter =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8),
            1024)) {
      File parent = outputPath.getParentFile();
      if (parent != null && !parent.exists()) {
        parent.mkdirs();
      }
      outputPath.createNewFile();
      for (Map.Entry<PartialPath, Integer> entry : counter.entrySet()) {
        String line = entry.getKey().getFullPath() + "," + entry.getValue();
        csvWriter.write(line);
        csvWriter.newLine();
      }
      csvWriter.flush();
    } catch (IOException e) {
      logger.error("dump map failed, error: {}", e.getMessage(), e);
    }
  }
}
