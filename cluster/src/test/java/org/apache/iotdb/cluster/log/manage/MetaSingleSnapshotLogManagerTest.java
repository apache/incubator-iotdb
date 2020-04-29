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

package org.apache.iotdb.cluster.log.manage;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestLogApplier;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetaSingleSnapshotLogManagerTest extends IoTDBTest {

  private MetaSingleSnapshotLogManager logManager;

  @Override
  @Before
  public void setUp() throws QueryProcessException, StartupException {
    super.setUp();
    logManager =
        new MetaSingleSnapshotLogManager(new TestLogApplier());
  }

  @Override
  @After
  public void tearDown()
      throws java.io.IOException, org.apache.iotdb.db.exception.StorageEngineException {
    logManager.close();
    super.tearDown();
  }

  @Test
  public void testTakeSnapshot() {
    List<Log> testLogs = TestUtils.prepareTestLogs(10);
    logManager.append(testLogs);
    logManager.commitTo(4);

    logManager.takeSnapshot();
    MetaSimpleSnapshot snapshot = (MetaSimpleSnapshot) logManager.getSnapshot();
    List<String> storageGroups = snapshot.getStorageGroups();
    assertEquals(10, storageGroups.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(TestUtils.getTestSg(i), storageGroups.get(i));
    }
    assertEquals(testLogs.subList(0, 5), snapshot.getSnapshot());
    assertEquals(4, snapshot.getLastLogIndex());
    assertEquals(4, snapshot.getLastLogTerm());
  }
}