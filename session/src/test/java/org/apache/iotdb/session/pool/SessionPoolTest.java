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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// this test is not for testing the correctness of Session API. So we just implement one of the API.
public class SessionPoolTest {

  private static final Logger logger = LoggerFactory.getLogger(SessionPoolTest.class);

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
      service.submit(
          () -> {
            try {
              pool.insertRecord(
                  "root.sg1.d1",
                  1,
                  Collections.singletonList("s" + no),
                  Collections.singletonList(TSDataType.INT64),
                  Collections.singletonList(3L));
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              fail(e.getMessage());
            }
          });
    }
    service.shutdown();
    try {
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.currentAvailableSize() <= 3);
      assertEquals(0, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      logger.error("insert failed", e);
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void incorrectSQL() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    assertEquals(0, pool.currentAvailableSize());
    try {
      pool.insertRecord(
          ".root.sg1.d1",
          1,
          Collections.singletonList("s"),
          Collections.singletonList(TSDataType.INT64),
          Collections.singletonList(3L));
      assertEquals(1, pool.currentAvailableSize());
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      // do nothing
    } finally {
      pool.close();
    }
  }

  @Test
  public void incorrectExecuteQueryStatement() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    write10Data(pool, true);
    // now let's query
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(
          () -> {
            try {
              SessionDataSetWrapper wrapper =
                  pool.executeQueryStatement("select * from root.sg1.d1 where time = " + no);
              // this is incorrect becasue wrapper is not closed.
              // so all other 7 queries will be blocked
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              fail(e.getMessage());
            }
          });
    }
    service.shutdown();
    try {
      assertFalse(service.awaitTermination(3, TimeUnit.SECONDS));
      assertEquals(0, pool.currentAvailableSize());
      assertTrue(pool.currentOccupiedSize() <= 3);
    } catch (InterruptedException e) {
      logger.error("incorrectExecuteQueryStatement failed,", e);
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void executeQueryStatement() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    correctQuery(pool);
    pool.close();
  }

  private void correctQuery(SessionPool pool) {
    ExecutorService service = Executors.newFixedThreadPool(10);
    write10Data(pool, true);
    // now let's query
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(
          () -> {
            try {
              SessionDataSetWrapper wrapper =
                  pool.executeQueryStatement("select * from root.sg1.d1 where time = " + no);
              pool.closeResultSet(wrapper);
            } catch (Exception e) {
              logger.error("correctQuery failed", e);
              fail(e.getMessage());
            }
          });
    }
    service.shutdown();
    try {
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.currentAvailableSize() <= 3);
      assertEquals(0, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      logger.error("correctQuery failed", e);
      fail(e.getMessage());
    }
  }

  @Test
  public void executeRawDataQuery() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    write10Data(pool, true);
    List<String> pathList = new ArrayList<>();
    pathList.add("root.sg1.d1.s1");
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(
          () -> {
            try {
              SessionDataSetWrapper wrapper = pool.executeRawDataQuery(pathList, no, no + 1);
              if (wrapper.hasNext()) {
                Assert.assertEquals(no, wrapper.sessionDataSet.next().getTimestamp());
              }
              pool.closeResultSet(wrapper);
            } catch (Exception e) {
              logger.error("executeRawDataQuery", e);
              fail(e.getMessage());
            }
          });
    }
    try {
      service.shutdown();
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.currentAvailableSize() <= 3);
      assertEquals(0, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void tryIfTheServerIsRestart() {
    SessionPool pool =
        new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1, 6000, false, null, false);
    write10Data(pool, true);
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time > 1");
      EnvironmentUtils.stopDaemon();
      // user does not know what happens.
      while (wrapper.hasNext()) {
        wrapper.next();
      }
    } catch (IoTDBConnectionException e) {
      pool.closeResultSet(wrapper);
      pool.close();
      EnvironmentUtils.stopDaemon();

      EnvironmentUtils.reactiveDaemon();
      pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1, 6000, false, null, false);
      correctQuery(pool);
      pool.close();
      return;
    } catch (StatementExecutionException e) {
      // After receiving the stop request, thrift calls shutdownNow() to process the executing task.
      // However, when the executing task is blocked, it will report InterruptedException error.
      // And IoTDB warps it as one StatementExecutionException.
      // If the thrift task thread is running, the thread will not be affected and will continue to
      // run, only if the interrupt flag of the thread is set to true. So here, we call the close
      // function on the client and wait for some time before the thrift server can exit normally.
      try {
        while (wrapper.hasNext()) {
          wrapper.next();
        }
      } catch (IoTDBConnectionException ec) {
        pool.closeResultSet(wrapper);
        pool.close();
        EnvironmentUtils.stopDaemon();
        EnvironmentUtils.reactiveDaemon();
        pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1, 6000, false, null, false);
        correctQuery(pool);
        pool.close();
      } catch (StatementExecutionException es) {
        fail("should be TTransportException but get an exception: " + e.getMessage());
      }
      return;
    } finally {
      if (wrapper != null) {
        pool.closeResultSet(wrapper);
      }
      pool.close();
    }
    fail("should throw exception but not");
  }

  @Test
  public void tryIfTheServerIsRestartButDataIsGotten() {
    SessionPool pool =
        new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1, 60000, false, null, false);
    write10Data(pool, true);
    assertEquals(1, pool.currentAvailableSize());
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time > 1");
      // user does not know what happens.
      assertEquals(0, pool.currentAvailableSize());
      assertEquals(1, pool.currentOccupiedSize());
      while (wrapper.hasNext()) {
        wrapper.next();
      }
      pool.closeResultSet(wrapper);
      assertEquals(1, pool.currentAvailableSize());
      assertEquals(0, pool.currentOccupiedSize());
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error("tryIfTheServerIsRestartButDataIsGotten", e);
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void restart() {
    SessionPool pool =
        new SessionPool("127.0.0.1", 6667, "root", "root", 1, 1, 1000, false, null, false);
    write10Data(pool, true);
    // stop the server.
    pool.close();
    EnvironmentUtils.stopDaemon();
    pool = new SessionPool("127.0.0.1", 6667, "root", "root", 1, 1, 1000, false, null, false);
    // all this ten data will fail.
    write10Data(pool, false);
    // restart the server
    EnvironmentUtils.reactiveDaemon();
    write10Data(pool, true);
    pool.close();
  }

  private void write10Data(SessionPool pool, boolean failWhenThrowException) {
    for (int i = 0; i < 10; i++) {
      try {
        pool.insertRecord(
            "root.sg1.d1",
            i,
            Collections.singletonList("s" + i),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList((long) i));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // will fail this 10 times.
        if (failWhenThrowException) {
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void testClose() {
    SessionPool pool =
        new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1, 60000, false, null, false);
    pool.close();
    try {
      pool.insertRecord(
          "root.sg1.d1",
          1,
          Collections.singletonList("s1"),
          Collections.singletonList(TSDataType.INT64),
          Collections.singletonList(1L));
    } catch (IoTDBConnectionException e) {
      Assert.assertEquals("Session pool is closed", e.getMessage());
    } catch (StatementExecutionException e) {
      fail(e.getMessage());
    }
    // some other test cases are not covered:
    // e.g., thread A created a new session, but not returned; thread B close the pool; A get the
    // session.
  }
}
