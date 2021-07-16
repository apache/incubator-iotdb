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

package org.apache.iotdb.cluster.query.manage;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.reader.series.IAggregateReader;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ClusterQueryManagerTest {

  private ClusterQueryManager queryManager;

  @Before
  public void setUp() {
    queryManager = new ClusterQueryManager();
  }

  @Test
  public void testContext() {
    RemoteQueryContext queryContext1 = queryManager.getQueryContext(TestUtils.getNode(0), 1);
    RemoteQueryContext queryContext2 = queryManager.getQueryContext(TestUtils.getNode(0), 1);
    RemoteQueryContext queryContext3 = queryManager.getQueryContext(TestUtils.getNode(1), 1);
    assertSame(queryContext1, queryContext2);
    assertNotEquals(queryContext2, queryContext3);
  }

  @Test
  public void testRegisterReader() {
    IBatchReader reader =
        new IBatchReader() {
          @Override
          public boolean hasNextBatch() {
            return false;
          }

          @Override
          public BatchData nextBatch() {
            return null;
          }

          @Override
          public void close() {}
        };
    long id = queryManager.registerReader(reader);
    assertSame(reader, queryManager.getReader(id));
  }

  @Test
  public void testRegisterReaderByTime() {
    IReaderByTimestamp reader = (timestamp, length) -> null;
    long id = queryManager.registerReaderByTime(reader);
    assertSame(reader, queryManager.getReaderByTimestamp(id));
  }

  @Test
  public void testRegisterAggregateReader() {
    IAggregateReader reader =
        new IAggregateReader() {
          @Override
          public boolean hasNextFile() {
            return false;
          }

          @Override
          public boolean canUseCurrentFileStatistics() {
            return false;
          }

          @Override
          public Statistics currentFileStatistics() {
            return null;
          }

          @Override
          public void skipCurrentFile() {}

          @Override
          public boolean hasNextChunk() {
            return false;
          }

          @Override
          public boolean canUseCurrentChunkStatistics() {
            return false;
          }

          @Override
          public Statistics currentChunkStatistics() {
            return null;
          }

          @Override
          public void skipCurrentChunk() {}

          @Override
          public boolean hasNextPage() {
            return false;
          }

          @Override
          public boolean canUseCurrentPageStatistics() {
            return false;
          }

          @Override
          public Statistics currentPageStatistics() {
            return null;
          }

          @Override
          public void skipCurrentPage() {}

          @Override
          public BatchData nextPage() {
            return null;
          }

          @Override
          public boolean isAscending() {
            return false;
          }
        };
    long id = queryManager.registerAggrReader(reader);
    assertSame(reader, queryManager.getAggrReader(id));
  }

  @Test
  public void testEndQuery() throws StorageEngineException {
    RemoteQueryContext queryContext = queryManager.getQueryContext(TestUtils.getNode(0), 1);
    for (int i = 0; i < 10; i++) {
      IBatchReader reader =
          new IBatchReader() {
            @Override
            public boolean hasNextBatch() {
              return false;
            }

            @Override
            public BatchData nextBatch() {
              return null;
            }

            @Override
            public void close() {}
          };
      queryContext.registerLocalReader(queryManager.registerReader(reader));
    }
    queryManager.endQuery(TestUtils.getNode(0), 1);
    for (int i = 0; i < 10; i++) {
      assertNull(queryManager.getReader(i));
    }
  }
}
