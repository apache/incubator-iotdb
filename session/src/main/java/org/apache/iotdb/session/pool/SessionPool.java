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

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.async.AsyncSession;
import org.apache.iotdb.session.Config;
import org.apache.iotdb.session.IInsertSession;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.async.AsyncThreadPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SessionPool is a wrapper of a Session Set. Using SessionPool, the user do not need to consider
 * how to reuse a session connection. Even if the session is disconnected, the session pool can
 * recognize it and remove the broken session connection and create a new one.
 * <p>
 * If there is no available connections and the pool reaches its max size, the all methods will hang
 * until there is a available connection.
 * <p>
 * If a user has waited for a session for more than 60 seconds, a warn log will be printed.
 * <p>
 * The only thing you have to remember is that:
 * <p>
 * For a query, if you have get all data, i.e., SessionDataSetWrapper.hasNext() == false, it is ok.
 * Otherwise, i.e., you want to stop the query before you get all data
 * (SessionDataSetWrapper.hasNext() == true), then you have to call closeResultSet(SessionDataSetWrapper
 * wrapper) manually. Otherwise the connection is occupied by the query.
 * <p>
 * Another case that you have to manually call closeResultSet() is that when there is exception when
 * you call SessionDataSetWrapper.hasNext() or next()
 */
public class SessionPool implements IInsertSession {

  private static final Logger logger = LoggerFactory.getLogger(SessionPool.class);
  public static final String SESSION_POOL_IS_CLOSED = "Session pool is closed";
  public static final String CLOSE_THE_SESSION_FAILED = "close the session failed.";
  private static int RETRY = 3;
  private ConcurrentLinkedDeque<Session> queue = new ConcurrentLinkedDeque<>();
  //for session whose resultSet is not released.
  private ConcurrentMap<Session, Session> occupied = new ConcurrentHashMap<>();
  private int size = 0;
  private int maxSize = 0;
  private String ip;
  private int port;
  private String user;
  private String password;
  private int fetchSize;
  private long timeout; //ms
  private static int FINAL_RETRY = RETRY - 1;
  private boolean enableCompression;
  private AsyncSession asyncHandler;
  private boolean enableCacheLeader;
  private ZoneId zoneId;

  private boolean closed;//whether the queue is closed.

  public SessionPool(String ip, int port, String user, String password, int maxSize) {
    this(ip, port, user, password, maxSize, Config.DEFAULT_FETCH_SIZE, 60_000, false, null,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public SessionPool(String ip, int port, String user, String password, int maxSize,
      boolean enableCompression) {
    this(ip, port, user, password, maxSize, Config.DEFAULT_FETCH_SIZE, 60_000, enableCompression,
        null, Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public SessionPool(String ip, int port, String user, String password, int maxSize,
      boolean enableCompression, boolean enableCacheLeader) {
    this(ip, port, user, password, maxSize, Config.DEFAULT_FETCH_SIZE, 60_000, enableCompression,
        null, enableCacheLeader);
  }

  public SessionPool(String ip, int port, String user, String password, int maxSize,
      ZoneId zoneId) {
    this(ip, port, user, password, maxSize, Config.DEFAULT_FETCH_SIZE, 60_000, false, zoneId,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  @SuppressWarnings("squid:S107")
  public SessionPool(String ip, int port, String user, String password, int maxSize, int fetchSize,
      long timeout, boolean enableCompression, ZoneId zoneId, boolean enableCacheLeader) {
    this.maxSize = maxSize;
    this.ip = ip;
    this.port = port;
    this.user = user;
    this.password = password;
    this.fetchSize = fetchSize;
    this.timeout = timeout;
    this.enableCompression = enableCompression;
    this.zoneId = zoneId;
    this.enableCacheLeader = enableCacheLeader;
  }

  //if this method throws an exception, either the server is broken, or the ip/port/user/password is incorrect.

  @SuppressWarnings({"squid:S3776", "squid:S2446"}) // Suppress high Cognitive Complexity warning
  private Session getSession() throws IoTDBConnectionException {
    Session session = queue.poll();
    if (closed) {
      throw new IoTDBConnectionException(SESSION_POOL_IS_CLOSED);
    }
    if (session != null) {
      return session;
    } else {
      long start = System.currentTimeMillis();
      boolean canCreate = false;
      synchronized (this) {
        if (size < maxSize) {
          //we can create more session
          size++;
          canCreate = true;
          //but we do it after skip synchronized block because connection a session is time consuming.
        }
      }
      if (canCreate) {
        //create a new one.
        if (logger.isDebugEnabled()) {
          logger.debug("Create a new Session {}, {}, {}, {}", ip, port, user, password);
        }
        session = new Session(ip, port, user, password, fetchSize, zoneId, enableCacheLeader);
        try {
          session.open(enableCompression);
          //avoid someone has called close() the session pool
          synchronized (this) {
            if (closed) {
              //have to release the connection...
              session.close();
              throw new IoTDBConnectionException(SESSION_POOL_IS_CLOSED);
            } else {
              return session;
            }
          }
        } catch (IoTDBConnectionException e) {
          //if exception, we will throw the exception.
          //Meanwhile, we have to set size--
          synchronized (this) {
            size--;
            //we do not need to notifyAll as any waited thread can continue to work after waked up.
            this.notify();
            if (logger.isDebugEnabled()) {
              logger.debug("open session failed, reduce the count and notify others...");
            }
          }
          throw e;
        }
      } else {
        while (session == null) {
          synchronized (this) {
            if (closed) {
              throw new IoTDBConnectionException(SESSION_POOL_IS_CLOSED);
            }
            //we have to wait for someone returns a session.
            try {
              if (logger.isDebugEnabled()) {
                logger
                    .debug("no more sessions can be created, wait... queue.size={}", queue.size());
              }
              this.wait(1000);
              long time = timeout < 60_000 ? timeout : 60_000;
              if (System.currentTimeMillis() - start > time) {
                logger.warn(
                    "the SessionPool has wait for {} seconds to get a new connection: {}:{} with {}, {}",
                    (System.currentTimeMillis() - start) / 1000, ip, port, user, password);
                logger.warn("current occupied size {}, queue size {}, considered size {} ",
                    occupied.size(), queue.size(), size);
                if (System.currentTimeMillis() - start > timeout) {
                  throw new IoTDBConnectionException(
                      String.format("timeout to get a connection from %s:%s", ip, port));
                }
              }
            } catch (InterruptedException e) {
              logger.error("the SessionPool is damaged", e);
              Thread.currentThread().interrupt();
            }
            session = queue.poll();
          }
        }
        return session;
      }
    }
  }

  public int currentAvailableSize() {
    return queue.size();
  }

  public int currentOccupiedSize() {
    return occupied.size();
  }

  @SuppressWarnings({"squid:S2446"})
  private void putBack(Session session) {
    queue.push(session);
    synchronized (this) {
      //we do not need to notifyAll as any waited thread can continue to work after waked up.
      this.notify();
      //comment the following codes as putBack is too frequently called.
//      if (logger.isTraceEnabled()) {
//        logger.trace("put a session back and notify others..., queue.size = {}", queue.size());
//      }
    }
  }

  private void occupy(Session session) {
    occupied.put(session, session);
  }

  /**
   * close all connections in the pool
   */
  public synchronized void close() {
    for (Session session : queue) {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        //do nothing
        logger.warn(CLOSE_THE_SESSION_FAILED, e);
      }
    }
    for (Session session : occupied.keySet()) {
      try {
        session.close();
      } catch (IoTDBConnectionException e) {
        //do nothing
        logger.warn(CLOSE_THE_SESSION_FAILED, e);
      }
    }
    logger.info("closing the session pool, cleaning queues...");
    this.closed = true;
    queue.clear();
    occupied.clear();
  }

  public void closeResultSet(SessionDataSetWrapper wrapper) {
    boolean putback = true;
    try {
      wrapper.sessionDataSet.closeOperationHandle();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      removeSession();
      putback = false;
    } finally {
      Session session = occupied.remove(wrapper.session);
      if (putback && session != null) {
        putBack(wrapper.session);
      }
    }
  }

  @SuppressWarnings({"squid:S2446"})
  private synchronized void removeSession() {
    logger.warn("Remove a broken Session {}, {}, {}", ip, port, user);
    size--;
    //we do not need to notifyAll as any waited thread can continue to work after waked up.
    this.notify();
    if (logger.isDebugEnabled()) {
      logger.debug("remove a broken session and notify others..., queue.size = {}", queue.size());
    }
  }

  private void closeSession(Session session) {
    if (session != null) {
      try {
        session.close();
      } catch (Exception e2) {
        //do nothing. We just want to guarantee the session is closed.
        logger.warn(CLOSE_THE_SESSION_FAILED, e2);
      }
    }
  }

  private void cleanSessionAndMayThrowConnectionException(Session session, int times,
      IoTDBConnectionException e) throws IoTDBConnectionException {
    closeSession(session);
    removeSession();
    if (times == FINAL_RETRY) {
      throw new IoTDBConnectionException(
          String.format("retry to execute statement on %s:%s failed %d times: %s", ip, port,
              RETRY, e.getMessage()), e);
    }
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   *  a Tablet example:
   *
   *        device1
   *     time s1, s2, s3
   *     1,   1,  1,  1
   *     2,   2,  2,  2
   *     3,   3,  3,  3
   *
   * times in Tablet may be not in ascending order
   *
   * @param tablet data batch
   */
  public void insertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    insertTablet(tablet, false);
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   * <p>
   * a Tablet example:
   *
   *      device1
   * time s1, s2, s3
   * 1,   1,  1,  1
   * 2,   2,  2,  2
   * 3,   3,  3,  3
   *
   * Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   *
   * @param tablet a tablet data of one device
   * @param sorted whether times in Tablet are in ascending order
   */
  public void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertTablet(tablet, sorted);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("insertTablet failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * insert a Tablet asynchronously
   * @param tablet data batch
   * @param sorted whether times in Tablet are in ascending order
   * @param timeout asynchronous call timeout in millisecond
   * @param callback user provided callback if failed, set to null if user does not specify.
   * @return async CompletableFuture
   */
  public CompletableFuture<Integer> asyncInsertTablet(Tablet tablet, boolean sorted, long timeout,
                                                      BiConsumer<Tablet, Throwable> callback) {

    if (this.asyncHandler == null) {
      AsyncThreadPool threadPool = new AsyncThreadPool();
      asyncHandler.setThreadPool(threadPool.getThreadPool());
    }
    return asyncHandler.doAsyncInsertTablet(tablet, sorted, timeout, this, callback);
  }

  /**
   * use batch interface to insert data
   *
   * @param tablets multiple batch
   */
  public void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    insertTablets(tablets, false);
  }

  /**
   * use batch interface to insert data
   *
   * @param tablets multiple batch
   */
  public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertTablets(tablets, sorted);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("insertTablets failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * insert the data of several devices asynchronously. Given a device, for each timestamp,
   * the number of measurements is the same.
   *
   * @param tablets  data batch in multiple device
   * @param sorted   whether times in each Tablet are in ascending order
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided callback if failed, set to null if user does not specify.
   */
  public CompletableFuture<Integer> asyncInsertTablets(
      Map<String, Tablet> tablets, boolean sorted, long timeout,
      BiConsumer<Map<String, Tablet>, Throwable> callback) {
    if (this.asyncHandler == null) {
      AsyncThreadPool threadPool = new AsyncThreadPool();
      asyncHandler.setThreadPool(threadPool.getThreadPool());
    }
    return asyncHandler.doAsyncInsertTablets(tablets, sorted, timeout, this, callback);
  }

  /**
   * Insert data in batch format, which can reduce the overhead of network. This method is just like
   * jdbc batch insert, we pack some insert request in batch and send them to server If you want
   * improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList) throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("insertRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * Insert multiple rows in asynchronous way. This method is just like jdbc executeBatch,
          * we pack some insert request in batch and send them to server. If you want improve your
   * performance, please see insertTablet method.
          * <p>
   * Each row is independent, which could have different deviceId, time, number of measurements
   *
           * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided callback if failed, set to null if user does not specify.
          * @see Session#insertTablet(Tablet)
   */
  public CompletableFuture<Integer> asyncInsertRecords(
          List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,
          List<List<TSDataType>> typesList, List<List<Object>> valuesList, long timeout,
          SixInputConsumer<List<String>, List<Long>, List<List<String>>, List<List<TSDataType>>, List<List<Object>>, Throwable> callback) {
    if (this.asyncHandler == null) {
      AsyncThreadPool threadPool = new AsyncThreadPool();
      asyncHandler.setThreadPool(threadPool.getThreadPool());
    }
    return asyncHandler.doAsyncInsertRecords(deviceIds, times, measurementsList,
            typesList, valuesList, timeout, this, callback);
  }

  /**
   * Insert data that belong to the same device in batch format, which can reduce the overhead of
   * network. This method is just like jdbc batch insert, we pack some insert request in batch and
   * send them to server If you want improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertOneDeviceRecords(String deviceId, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList) throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList,
            false);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("insertRecordsOfOneDevice failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * Insert data that belong to the same device in batch format, which can reduce the overhead of
   * network. This method is just like jdbc batch insert, we pack some insert request in batch and
   * send them to server If you want improve your performance, please see insertTablet method
   *
   * @param haveSorted whether the times list has been ordered.
   * @see Session#insertTablet(Tablet)
   */
  public void insertOneDeviceRecords(String deviceId, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList, boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList,
            haveSorted);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("insertRecordsOfOneDevice failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * Insert data in batch format, which can reduce the overhead of network. This method is just like
   * jdbc batch insert, we pack some insert request in batch and send them to server If you want
   * improve your performance, please see insertTablet method
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecords(deviceIds, times, measurementsList, valuesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("insertRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * Insert multiple rows in asynchronous way. This method is just like jdbc executeBatch,
   * we pack some insert request in batch and send them to server. If you want improve your
   * performance, please see insertTablet method.
   * <p>
   * Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided callback if failed, set to null if user does not specify.
   * @see Session#insertTablet(Tablet)
   */
  public CompletableFuture<Integer> asyncInsertRecords(
      List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,
      List<List<String>> valuesList, long timeout,
      FiveInputConsumer<List<String>, List<Long>, List<List<String>>, List<List<String>>, Throwable> callback) {
    if (this.asyncHandler == null) {
      AsyncThreadPool threadPool = new AsyncThreadPool();
      asyncHandler.setThreadPool(threadPool.getThreadPool());
    }
    return asyncHandler.doAsyncInsertRecords(deviceIds, times, measurementsList,
        valuesList, timeout, this, callback);
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecord(deviceId, time, measurements, types, values);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("insertRecord failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * insert data in one row asynchronously. if you want improve your performance,
   * please use insertRecords method or insertTablet method
   *
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided callback if failed, set to null if user does not specify.
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public CompletableFuture<Integer> asyncInsertRecord(
      String deviceId, long time, List<String> measurements, List<TSDataType> types,
      List<Object> values, long timeout,
      SixInputConsumer<String, Long, List<String>, List<TSDataType>, List<Object>, Throwable> callback) {
    if (this.asyncHandler == null) {
      AsyncThreadPool threadPool = new AsyncThreadPool();
      asyncHandler.setThreadPool(threadPool.getThreadPool());
    }
    return asyncHandler.doAsyncInsertRecord(deviceId, time, measurements, types,
        values, timeout, this, callback);
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(String deviceId, long time, List<String> measurements,
      List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.insertRecord(deviceId, time, measurements, values);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("insertRecord failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * insert data in one row asynchronously. if you want improve your performance,
   * please use insertRecords method or insertTablet method
   *
   * @param timeout  asynchronous call timeout in millisecond
   * @param callback user provided callback if failed, set to null if user does not specify.
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public CompletableFuture<Integer> asyncInsertRecord(
      String deviceId, long time, List<String> measurements, List<String> values, long timeout,
      FiveInputConsumer<String, Long, List<String>, List<String>, Throwable> callback) {
    if (this.asyncHandler == null) {
      AsyncThreadPool threadPool = new AsyncThreadPool();
      asyncHandler.setThreadPool(threadPool.getThreadPool());
    }
    return asyncHandler.doAsyncInsertRecord(deviceId, time, measurements, values,
        timeout, this, callback);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertTablet(tablet);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("testInsertTablet failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertTablets(tablets);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("testInsertTablets failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertRecords(deviceIds, times, measurementsList, valuesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("testInsertRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertRecords(deviceIds, times, measurementsList, typesList, valuesList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("testInsertRecords failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<String> values) throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertRecord(deviceId, time, measurements, values);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("testInsertRecord failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.testInsertRecord(deviceId, time, measurements, types, values);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("testInsertRecord failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param path timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteTimeseries(path);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("deleteTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param paths timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteTimeseries(paths);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("deleteTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete data <= time in one timeseries
   *
   * @param path data in which time series to delete
   * @param time data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(String path, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteData(path, time);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("deleteData failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete data <= time in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param time  data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(List<String> paths, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteData(paths, time);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("deleteData failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  /**
   * delete data >= startTime and data <= endTime in multiple timeseries
   *
   * @param paths     data in which time series to delete
   * @param startTime delete range start time
   * @param endTime   delete range end time
   */
  public void deleteData(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteData(paths, startTime, endTime);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("deleteData failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void setStorageGroup(String storageGroupId)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.setStorageGroup(storageGroupId);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("setStorageGroup failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteStorageGroup(storageGroup);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("deleteStorageGroup failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void deleteStorageGroups(List<String> storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.deleteStorageGroups(storageGroup);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("deleteStorageGroups failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void createTimeseries(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor) throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.createTimeseries(path, dataType, encoding, compressor);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("createTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void createTimeseries(String path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props, Map<String, String> tags,
      Map<String, String> attributes, String measurementAlias)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.createTimeseries(path, dataType, encoding, compressor, props, tags, attributes,
            measurementAlias);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("createTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
      List<TSEncoding> encodings, List<CompressionType> compressors,
      List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList, List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.createMultiTimeseries(paths, dataTypes, encodings, compressors, propsList, tagsList,
            attributesList, measurementAliasList);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("createMultiTimeseries failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  public boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        boolean resp = session.checkTimeseriesExists(path);
        putBack(session);
        return resp;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("checkTimeseriesExists failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    //never go here.
    return false;
  }

  /**
   * execure query sql users must call closeResultSet(SessionDataSetWrapper) if they do not use the
   * SessionDataSet any more. users do not need to call sessionDataSet.closeOpeationHandler() any
   * more.
   *
   * @param sql query statement
   * @return result set Notice that you must get the result instance. Otherwise a data leakage will
   * happen
   */
  @SuppressWarnings("squid:S2095") // Suppress wrapper not closed warning
  public SessionDataSetWrapper executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        SessionDataSet resp = session.executeQueryStatement(sql);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("executeQueryStatement failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    // never go here
    return null;
  }

  /**
   * execute non query statement
   *
   * @param sql non query statement
   */
  public void executeNonQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        session.executeNonQueryStatement(sql);
        putBack(session);
        return;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("executeNonQueryStatement failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
  }

  @SuppressWarnings("squid:S2095") // Suppress wrapper not closed warning
  public SessionDataSetWrapper executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < RETRY; i++) {
      Session session = getSession();
      try {
        SessionDataSet resp = session.executeRawDataQuery(paths, startTime, endTime);
        SessionDataSetWrapper wrapper = new SessionDataSetWrapper(resp, session, this);
        occupy(session);
        return wrapper;
      } catch (IoTDBConnectionException e) {
        // TException means the connection is broken, remove it and get a new one.
        logger.warn("executeRawDataQuery failed", e);
        cleanSessionAndMayThrowConnectionException(session, i, e);
      } catch (StatementExecutionException | RuntimeException e) {
        putBack(session);
        throw e;
      }
    }
    // never go here
    return null;
  }
}
