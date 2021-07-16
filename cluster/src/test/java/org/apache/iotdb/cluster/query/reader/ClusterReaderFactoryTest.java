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

package org.apache.iotdb.cluster.query.reader;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;

import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ClusterReaderFactoryTest extends BaseQueryTest {

  @Test
  public void testTTL()
      throws StorageEngineException, MetadataException, QueryProcessException, IOException {

    ClusterReaderFactory readerFactory = new ClusterReaderFactory(testMetaMember);
    RemoteQueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));

    try {
      SeriesRawDataBatchReader seriesReader =
          (SeriesRawDataBatchReader)
              readerFactory.getSeriesBatchReader(
                  pathList.get(0),
                  new HashSet<>(),
                  dataTypes.get(0),
                  null,
                  null,
                  context,
                  dataGroupMemberMap.get(TestUtils.getRaftNode(10, 0)),
                  true,
                  null);
      assertNotNull(seriesReader);
      StorageEngine.getInstance().setTTL(new PartialPath(TestUtils.getTestSg(0)), 100);
      seriesReader =
          (SeriesRawDataBatchReader)
              readerFactory.getSeriesBatchReader(
                  pathList.get(0),
                  new HashSet<>(),
                  dataTypes.get(0),
                  null,
                  null,
                  context,
                  dataGroupMemberMap.get(TestUtils.getRaftNode(10, 0)),
                  true,
                  null);
      assertNull(seriesReader);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
      StorageEngine.getInstance().setTTL(new PartialPath(TestUtils.getTestSg(0)), Long.MAX_VALUE);
    }
  }
}
