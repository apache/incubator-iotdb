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
package org.apache.iotdb.session.pool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

//this test is not for testing the correctness of Session API. So we just implement one of the API.
public class SessionPoolIT {

  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void insert() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(() -> {
        try {
          pool.insertRecord("root.sg1.d1", 1, Collections.singletonList("s" + no),
              Collections.singletonList(TSDataType.INT64), Collections.singletonList(3L));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          fail();
        }
      });
    }
    service.shutdown();
    try {
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    assertTrue(pool.currentAvailableSize() <= 3);
    assertEquals(0, pool.currentOccupiedSize());
    pool.close();
  }

  @Test
  public void incorrectSQL() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    assertEquals(0, pool.currentAvailableSize());
    try {
      pool.insertRecord(".root.sg1.d1", 1, Collections.singletonList("s"),
          Collections.singletonList(TSDataType.INT64),
          Collections.singletonList(3L));
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      //do nothing
    }
    assertEquals(1, pool.currentAvailableSize());
    pool.close();
  }


  @Test
  public void incorrectExecuteQueryStatement() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      try {
        pool.insertRecord("root.sg1.d1", i, Collections.singletonList("s" + i),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList((long) i));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        fail();
      }
    }
    //now let's query
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(() -> {
        try {
          SessionDataSetWrapper wrapper = pool
              .executeQueryStatement("select * from root.sg1.d1 where time = " + no);
          //this is incorrect becasue wrapper is not closed.
          //so all other 7 queries will be blocked
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          fail();
        }
      });
    }
    service.shutdown();
    try {
      assertFalse(service.awaitTermination(3, TimeUnit.SECONDS));
      assertEquals(0, pool.currentAvailableSize());
      assertTrue(pool.currentOccupiedSize() <= 3);
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    pool.close();
  }

  @Test
  public void executeQueryStatement() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    correctQuery(pool);
    pool.close();
  }

  private void correctQuery(SessionPool pool) {
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      try {
        pool.insertRecord("root.sg1.d1", i, Collections.singletonList("s" + i),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList((long) i));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        fail();
      }
    }
    //now let's query
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(() -> {
        try {
          SessionDataSetWrapper wrapper = pool
              .executeQueryStatement("select * from root.sg1.d1 where time = " + no);
          pool.closeResultSet(wrapper);
        } catch (Exception e) {
          e.printStackTrace();
          fail();
        }
      });
    }
    service.shutdown();
    try {
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.currentAvailableSize() <= 3);
      assertEquals(0, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void tryIfTheServerIsRestart() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1, 6000, false);
    for (int i = 0; i < 10; i++) {
      try {
        pool.insertRecord("root.sg1.d1", i, Collections.singletonList("s" + i),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList((long) i));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        fail();
      }
    }
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time > 1");
      EnvironmentUtils.stopDaemon();
      //user does not know what happens.
      while (wrapper.hasNext()) {
        wrapper.next();
      }
    } catch (IoTDBConnectionException e) {
      pool.closeResultSet(wrapper);
      EnvironmentUtils.reactiveDaemon();
      correctQuery(pool);
      pool.close();
      return;
    } catch (StatementExecutionException e) {
      fail("should be TTransportException but get an exception: " + e.getMessage());
    }
    fail("should throw exception but not");
  }

  @Test
  public void tryIfTheServerIsRestartButDataIsGotten() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1, 60000, false);
    for (int i = 0; i < 10; i++) {
      try {
        pool.insertRecord("root.sg1.d1", i, Collections.singletonList("s" + i),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList((long) i));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        fail();
      }
    }
    assertEquals(1, pool.currentAvailableSize());
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time > 1");
      //user does not know what happens.
      assertEquals(0, pool.currentAvailableSize());
      assertEquals(1, pool.currentOccupiedSize());
      while (wrapper.hasNext()) {
        wrapper.next();
      }
      assertEquals(1, pool.currentAvailableSize());
      assertEquals(0, pool.currentOccupiedSize());
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail();
    }
    pool.close();
  }

}