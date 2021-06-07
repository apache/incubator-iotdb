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
package org.apache.iotdb.session;

import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertSinglePointReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringSinglePointReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@SuppressWarnings({"java:S107", "java:S1135"}) // need enough parameters, ignore todos
public class Session {

  private static final Logger logger = LoggerFactory.getLogger(Session.class);
  protected static final TSProtocolVersion protocolVersion =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
  public static final String MSG_UNSUPPORTED_DATA_TYPE = "Unsupported data type:";
  public static final String MSG_DONOT_ENABLE_REDIRECT =
      "Query do not enable redirect," + " please confirm the session and server conf.";
  protected String username;
  protected String password;
  protected int fetchSize;
  private static final byte TYPE_NULL = -2;
  /**
   * Timeout of query can be set by users. If not set, default value 0 will be used, which will use
   * server configuration.
   */
  private long timeout = 0;

  protected boolean enableRPCCompression;
  protected int connectionTimeoutInMs;
  protected ZoneId zoneId;

  protected int thriftDefaultBufferSize;
  protected int thriftMaxFrameSize;

  protected EndPoint defaultEndPoint;
  protected SessionConnection defaultSessionConnection;
  private boolean isClosed = true;

  // Cluster version cache
  protected boolean enableCacheLeader;
  protected SessionConnection metaSessionConnection;
  protected Map<String, EndPoint> deviceIdToEndpoint;
  protected Map<EndPoint, SessionConnection> endPointToSessionConnection;
  private AtomicReference<IoTDBConnectionException> tmp = new AtomicReference<>();

  protected boolean enableQueryRedirection = false;

  public Session(String host, int rpcPort) {
    this(
        host,
        rpcPort,
        Config.DEFAULT_USER,
        Config.DEFAULT_PASSWORD,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(String host, String rpcPort, String username, String password) {
    this(
        host,
        Integer.parseInt(rpcPort),
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(String host, int rpcPort, String username, String password) {
    this(
        host,
        rpcPort,
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(String host, int rpcPort, String username, String password, int fetchSize) {
    this(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(
      String host, int rpcPort, String username, String password, int fetchSize, long timeoutInMs) {
    this(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
    this.timeout = timeoutInMs;
  }

  public Session(String host, int rpcPort, String username, String password, ZoneId zoneId) {
    this(
        host,
        rpcPort,
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        zoneId,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(
      String host, int rpcPort, String username, String password, boolean enableCacheLeader) {
    this(
        host,
        rpcPort,
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        enableCacheLeader);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      boolean enableCacheLeader) {
    this(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        zoneId,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        enableCacheLeader);
  }

  @SuppressWarnings("squid:S107")
  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableCacheLeader) {
    this.defaultEndPoint = new EndPoint(host, rpcPort);
    this.username = username;
    this.password = password;
    this.fetchSize = fetchSize;
    this.zoneId = zoneId;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.enableCacheLeader = enableCacheLeader;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public int getFetchSize() {
    return this.fetchSize;
  }

  public synchronized void open() throws IoTDBConnectionException {
    open(false, Config.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  public synchronized void open(boolean enableRPCCompression) throws IoTDBConnectionException {
    open(enableRPCCompression, Config.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  private synchronized void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException {
    if (!isClosed) {
      return;
    }

    this.enableRPCCompression = enableRPCCompression;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    defaultSessionConnection = constructSessionConnection(this, defaultEndPoint, zoneId);
    defaultSessionConnection.setEnableRedirect(enableQueryRedirection);
    metaSessionConnection = defaultSessionConnection;
    isClosed = false;
    if (enableCacheLeader || enableQueryRedirection) {
      deviceIdToEndpoint = new HashMap<>();
      endPointToSessionConnection = new HashMap<>();
      endPointToSessionConnection.put(defaultEndPoint, defaultSessionConnection);
    }
  }

  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed) {
      return;
    }
    try {
      if (enableCacheLeader) {
        for (SessionConnection sessionConnection : endPointToSessionConnection.values()) {
          sessionConnection.close();
        }
      } else {
        defaultSessionConnection.close();
      }
    } finally {
      isClosed = true;
    }
  }

  public SessionConnection constructSessionConnection(
      Session session, EndPoint endpoint, ZoneId zoneId) throws IoTDBConnectionException {
    return new SessionConnection(session, endpoint, zoneId);
  }

  public synchronized String getTimeZone() {
    return defaultSessionConnection.getTimeZone();
  }

  public synchronized void setTimeZone(String zoneId)
      throws StatementExecutionException, IoTDBConnectionException {
    defaultSessionConnection.setTimeZone(zoneId);
  }

  public void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      metaSessionConnection.setStorageGroup(storageGroup);
    } catch (RedirectException e) {
      handleMetaRedirection(storageGroup, e);
    }
  }

  public void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      metaSessionConnection.deleteStorageGroups(Collections.singletonList(storageGroup));
    } catch (RedirectException e) {
      handleMetaRedirection(storageGroup, e);
    }
  }

  public void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      metaSessionConnection.deleteStorageGroups(storageGroups);
    } catch (RedirectException e) {
      handleMetaRedirection(storageGroups.toString(), e);
    }
  }

  public void createTimeseries(
      String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateTimeseriesReq request =
        genTSCreateTimeseriesReq(path, dataType, encoding, compressor, null, null, null, null);
    defaultSessionConnection.createTimeseries(request);
  }

  public void createTimeseries(
      String path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String measurementAlias)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateTimeseriesReq request =
        genTSCreateTimeseriesReq(
            path, dataType, encoding, compressor, props, tags, attributes, measurementAlias);
    defaultSessionConnection.createTimeseries(request);
  }

  private TSCreateTimeseriesReq genTSCreateTimeseriesReq(
      String path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String measurementAlias) {
    TSCreateTimeseriesReq request = new TSCreateTimeseriesReq();
    request.setPath(path);
    request.setDataType(dataType.ordinal());
    request.setEncoding(encoding.ordinal());
    request.setCompressor(compressor.ordinal());
    request.setProps(props);
    request.setTags(tags);
    request.setAttributes(attributes);
    request.setMeasurementAlias(measurementAlias);
    return request;
  }

  public void createAlignedTimeseries(
      String prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      CompressionType compressor,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateAlignedTimeseriesReq request =
        getTSCreateAlignedTimeseriesReq(
            prefixPath, measurements, dataTypes, encodings, compressor, measurementAliasList);
    defaultSessionConnection.createAlignedTimeseries(request);
  }

  private TSCreateAlignedTimeseriesReq getTSCreateAlignedTimeseriesReq(
      String prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      CompressionType compressor,
      List<String> measurementAliasList) {
    TSCreateAlignedTimeseriesReq request = new TSCreateAlignedTimeseriesReq();
    request.setPrefixPath(prefixPath);
    request.setMeasurements(measurements);
    request.setDataTypes(dataTypes.stream().map(TSDataType::ordinal).collect(Collectors.toList()));
    request.setEncodings(encodings.stream().map(TSEncoding::ordinal).collect(Collectors.toList()));
    request.setCompressor(compressor.ordinal());
    request.setMeasurementAlias(measurementAliasList);
    return request;
  }

  public void createMultiTimeseries(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateMultiTimeseriesReq request =
        genTSCreateMultiTimeseriesReq(
            paths,
            dataTypes,
            encodings,
            compressors,
            propsList,
            tagsList,
            attributesList,
            measurementAliasList);
    defaultSessionConnection.createMultiTimeseries(request);
  }

  private TSCreateMultiTimeseriesReq genTSCreateMultiTimeseriesReq(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList) {
    TSCreateMultiTimeseriesReq request = new TSCreateMultiTimeseriesReq();

    request.setPaths(paths);

    List<Integer> dataTypeOrdinals = new ArrayList<>(dataTypes.size());
    for (TSDataType dataType : dataTypes) {
      dataTypeOrdinals.add(dataType.ordinal());
    }
    request.setDataTypes(dataTypeOrdinals);

    List<Integer> encodingOrdinals = new ArrayList<>(dataTypes.size());
    for (TSEncoding encoding : encodings) {
      encodingOrdinals.add(encoding.ordinal());
    }
    request.setEncodings(encodingOrdinals);

    List<Integer> compressionOrdinals = new ArrayList<>(paths.size());
    for (CompressionType compression : compressors) {
      compressionOrdinals.add(compression.ordinal());
    }
    request.setCompressors(compressionOrdinals);

    request.setPropsList(propsList);
    request.setTagsList(tagsList);
    request.setAttributesList(attributesList);
    request.setMeasurementAliasList(measurementAliasList);

    return request;
  }

  public boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    return defaultSessionConnection.checkTimeseriesExists(path, timeout);
  }

  public void setTimeout(long timeoutInMs) throws StatementExecutionException {
    if (timeoutInMs < 0) {
      throw new StatementExecutionException("Timeout must be >= 0, please check and try again.");
    }
    this.timeout = timeoutInMs;
  }

  public long getTimeout() {
    return timeout;
  }

  /**
   * execute query sql
   *
   * @param sql query statement
   * @return result set
   */
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    return executeStatementMayRedirect(sql, timeout);
  }

  /**
   * execute query sql with explicit timeout
   *
   * @param sql query statement
   * @param timeoutInMs the timeout of this query, in milliseconds
   * @return result set
   */
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    if (timeoutInMs < 0) {
      throw new StatementExecutionException("Timeout must be >= 0, please check and try again.");
    }
    return executeStatementMayRedirect(sql, timeoutInMs);
  }

  /**
   * execute the query, may redirect query to other node.
   *
   * @param sql the query statement
   * @param timeoutInMs time in ms
   * @return data set
   * @throws StatementExecutionException statement is not right
   * @throws IoTDBConnectionException the network is not good
   */
  private SessionDataSet executeStatementMayRedirect(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      logger.info("{} execute sql {}", defaultSessionConnection.getEndPoint(), sql);
      return defaultSessionConnection.executeQueryStatement(sql, timeoutInMs);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        logger.debug(
            "{} redirect query {} to {}",
            defaultSessionConnection.getEndPoint(),
            sql,
            e.getEndPoint());
        // retry
        try {
          return defaultSessionConnection.executeQueryStatement(sql, timeout);
        } catch (RedirectException redirectException) {
          logger.error("{} redirect twice", sql, redirectException);
          throw new StatementExecutionException(sql + " redirect twice, please try again.");
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  /**
   * execute non query statement
   *
   * @param sql non query statement
   */
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.executeNonQueryStatement(sql);
  }

  /**
   * query eg. select * from paths where time >= startTime and time < endTime time interval include
   * startTime and exclude endTime
   *
   * @param paths series path
   * @param startTime included
   * @param endTime excluded
   * @return data set
   * @throws StatementExecutionException statement is not right
   * @throws IoTDBConnectionException the network is not good
   */
  public SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeRawDataQuery(paths, startTime, endTime);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        logger.debug("redirect query {} to {}", paths, e.getEndPoint());
        // retry
        try {
          return defaultSessionConnection.executeRawDataQuery(paths, startTime, endTime);
        } catch (RedirectException redirectException) {
          logger.error("Redirect twice", redirectException);
          throw new StatementExecutionException("Redirect twice, please try again.");
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  /**
   * only: select last status from root.ln.d1.s1 where time >= 1621326244168;
   *
   * @param paths timeSeries eg. root.ln.d1.s1,root.ln.d1.s2
   * @param LastTime get the last data, whose timestamp greater than or equal LastTime eg.
   *     1621326244168
   */
  public SessionDataSet executeLastDataQuery(List<String> paths, long LastTime)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeLastDataQuery(paths, LastTime);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeLastDataQuery(paths, LastTime);
        } catch (RedirectException redirectException) {
          logger.error("redirect twice", redirectException);
          throw new StatementExecutionException("redirect twice, please try again.");
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  /**
   * query eg. select last status from root.ln.wf01.wt01; <PrefixPath> + <suffixPath> = <TimeSeries>
   *
   * @param paths timeSeries. eg.root.ln.d1.s1,root.ln.d1.s2
   */
  public SessionDataSet executeLastDataQuery(List<String> paths)
      throws StatementExecutionException, IoTDBConnectionException {
    long time = 0L;
    return executeLastDataQuery(paths, time);
  }

  /**
   * insert data in one row, if you want to improve your performance, please use insertRecords
   * method or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      boolean isAligned,
      Object... values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request =
        genTSInsertRecordReq(deviceId, time, measurements, types, Arrays.asList(values), isAligned);
    insertRecord(deviceId, request);
  }

  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      Object... values)
      throws IoTDBConnectionException, StatementExecutionException {
    // not vector by default
    insertRecord(deviceId, time, measurements, types, false, values);
  }

  private void insertRecord(String deviceId, TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      getSessionConnection(deviceId).insertRecord(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    }
  }

  private void insertRecord(String deviceId, TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      getSessionConnection(deviceId).insertRecord(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    }
  }

  private SessionConnection getSessionConnection(String deviceId) {
    EndPoint endPoint;
    if (enableCacheLeader && (endPoint = deviceIdToEndpoint.get(deviceId)) != null) {
      return endPointToSessionConnection.get(endPoint);
    } else {
      return defaultSessionConnection;
    }
  }

  private void handleMetaRedirection(String storageGroup, RedirectException e)
      throws IoTDBConnectionException {
    if (enableCacheLeader) {
      logger.debug("storageGroup[{}]:{}", storageGroup, e.getMessage());
      SessionConnection connection =
          endPointToSessionConnection.computeIfAbsent(
              e.getEndPoint(),
              k -> {
                try {
                  return constructSessionConnection(this, e.getEndPoint(), zoneId);
                } catch (IoTDBConnectionException ex) {
                  tmp.set(ex);
                  return null;
                }
              });
      if (connection == null) {
        throw new IoTDBConnectionException(tmp.get());
      }
      metaSessionConnection = connection;
    }
  }

  private void handleRedirection(String deviceId, EndPoint endpoint)
      throws IoTDBConnectionException {
    if (enableCacheLeader) {
      deviceIdToEndpoint.put(deviceId, endpoint);
      SessionConnection connection =
          endPointToSessionConnection.computeIfAbsent(
              endpoint,
              k -> {
                try {
                  return constructSessionConnection(this, endpoint, zoneId);
                } catch (IoTDBConnectionException ex) {
                  tmp.set(ex);
                  return null;
                }
              });
      if (connection == null) {
        throw new IoTDBConnectionException(tmp.get());
      }
    }
  }

  private void handleQueryRedirection(EndPoint endPoint) throws IoTDBConnectionException {
    if (enableQueryRedirection) {
      SessionConnection connection =
          endPointToSessionConnection.computeIfAbsent(
              endPoint,
              k -> {
                try {
                  SessionConnection sessionConnection =
                      constructSessionConnection(this, endPoint, zoneId);
                  sessionConnection.setEnableRedirect(enableQueryRedirection);
                  return sessionConnection;
                } catch (IoTDBConnectionException ex) {
                  tmp.set(ex);
                  return null;
                }
              });
      if (connection == null) {
        throw new IoTDBConnectionException(tmp.get());
      }
      defaultSessionConnection = connection;
    }
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertInBatch method
   * or insertBatch method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values,
      boolean isAligned)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request =
        genTSInsertRecordReq(deviceId, time, measurements, types, values, isAligned);
    insertRecord(deviceId, request);
  }

  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    // not vector by default
    TSInsertRecordReq request =
        genTSInsertRecordReq(deviceId, time, measurements, types, values, false);
    insertRecord(deviceId, request);
  }

  private TSInsertRecordReq genTSInsertRecordReq(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values,
      boolean isAligned)
      throws IoTDBConnectionException {
    TSInsertRecordReq request = new TSInsertRecordReq();
    request.setPrefixPath(deviceId);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    ByteBuffer buffer = ByteBuffer.allocate(calculateLength(types, values));
    putValues(types, values, buffer);
    request.setValues(buffer);
    request.setIsAligned(isAligned);
    return request;
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertInBatch method
   * or insertBatch method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request =
        genTSInsertStringRecordReq(deviceId, time, measurements, values);
    insertRecord(deviceId, request);
  }

  private TSInsertStringRecordReq genTSInsertStringRecordReq(
      String deviceId, long time, List<String> measurements, List<String> values) {
    TSInsertStringRecordReq request = new TSInsertStringRecordReq();
    request.setDeviceId(deviceId);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    request.setValues(values);
    return request;
  }

  public void insertStringSinglePoint(String deviceId, long time, String measurement, String value)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringSinglePointReq request =
        genTSInsertStringSinglePointReq(deviceId, time, measurement, value);
    insertStringSinglePoint(deviceId, request);
  }

  private TSInsertStringSinglePointReq genTSInsertStringSinglePointReq(
      String deviceId, long time, String measurement, String value) {
    TSInsertStringSinglePointReq request = new TSInsertStringSinglePointReq();
    request.setDeviceId(deviceId);
    request.setTimestamp(time);
    request.setMeasurement(measurement);
    request.setValue(value);
    return request;
  }

  public void insertSinglePoint(
      String deviceId, long time, String measurement, TSDataType type, Object value)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertSinglePointReq request =
        genTSInsertSinglePointReq(deviceId, time, measurement, type, value);
    insertSinglePoint(deviceId, request);
  }

  private void insertSinglePoint(String deviceId, TSInsertSinglePointReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      getSessionConnection(deviceId).insertSinglePoint(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    }
  }

  private void insertStringSinglePoint(String deviceId, TSInsertStringSinglePointReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      getSessionConnection(deviceId).insertStringSinglePoint(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    }
  }

  private TSInsertSinglePointReq genTSInsertSinglePointReq(
      String deviceId, long time, String measurement, TSDataType type, Object value)
      throws IoTDBConnectionException {
    TSInsertSinglePointReq request = new TSInsertSinglePointReq();
    request.setDeviceId(deviceId);
    request.setTimestamp(time);
    request.setMeasurement(measurement);
    ByteBuffer buffer = ByteBuffer.allocate(calculateLength(type, value));
    putValue(type, value, buffer);
    request.setValue(buffer);
    return request;
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   *
   * <p>Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }
    if (enableCacheLeader) {
      insertStringRecordsWithLeaderCache(deviceIds, times, measurementsList, valuesList);
    } else {
      TSInsertStringRecordsReq request =
          genTSInsertStringRecordsReq(deviceIds, times, measurementsList, valuesList);
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        // ignore
      }
    }
  }

  private void insertStringRecordsWithLeaderCache(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    Map<String, TSInsertStringRecordsReq> deviceGroup = new HashMap<>();
    for (int i = 0; i < deviceIds.size(); i++) {
      TSInsertStringRecordsReq request =
          deviceGroup.computeIfAbsent(deviceIds.get(i), k -> new TSInsertStringRecordsReq());
      updateTSInsertStringRecordsReq(
          request, deviceIds.get(i), times.get(i), measurementsList.get(i), valuesList.get(i));
    }
    // TODO parallel
    StringBuilder errMsgBuilder = new StringBuilder();
    for (Entry<String, TSInsertStringRecordsReq> entry : deviceGroup.entrySet()) {
      try {
        getSessionConnection(entry.getKey()).insertRecords(entry.getValue());
      } catch (RedirectException e) {
        handleRedirection(entry.getKey(), e.getEndPoint());
      } catch (StatementExecutionException e) {
        errMsgBuilder.append(e.getMessage());
      }
    }
    String errMsg = errMsgBuilder.toString();
    if (!errMsg.isEmpty()) {
      throw new StatementExecutionException(errMsg);
    }
  }

  private TSInsertStringRecordsReq genTSInsertStringRecordsReq(
      List<String> deviceId,
      List<Long> time,
      List<List<String>> measurements,
      List<List<String>> values) {
    TSInsertStringRecordsReq request = new TSInsertStringRecordsReq();
    request.setDeviceIds(deviceId);
    request.setTimestamps(time);
    request.setMeasurementsList(measurements);
    request.setValuesList(values);
    return request;
  }

  private void updateTSInsertStringRecordsReq(
      TSInsertStringRecordsReq request,
      String deviceId,
      long time,
      List<String> measurements,
      List<String> values) {
    request.addToDeviceIds(deviceId);
    request.addToTimestamps(time);
    request.addToMeasurementsList(measurements);
    request.addToValuesList(values);
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   *
   * <p>Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }
    if (enableCacheLeader) {
      insertRecordsWithLeaderCache(deviceIds, times, measurementsList, typesList, valuesList);
    } else {
      TSInsertRecordsReq request =
          genTSInsertRecordsReq(deviceIds, times, measurementsList, typesList, valuesList);
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        // ignore
      }
    }
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   *
   * <p>Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList, false);
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   *
   * <p>Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @param haveSorted whether the times have been sorted
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }
    TSInsertRecordsOfOneDeviceReq request =
        genTSInsertRecordsOfOneDeviceReq(
            deviceId, times, measurementsList, typesList, valuesList, haveSorted);
    try {
      getSessionConnection(deviceId).insertRecordsOfOneDevice(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    }
  }

  private TSInsertRecordsOfOneDeviceReq genTSInsertRecordsOfOneDeviceReq(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, BatchExecutionException {
    // check params size
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "times, measurementsList and valuesList's size should be equal");
    }

    if (haveSorted) {
      if (!checkSorted(times)) {
        throw new BatchExecutionException(
            "Times in InsertOneDeviceRecords are not in ascending order");
      }
    } else {
      // sort
      Integer[] index = new Integer[times.size()];
      for (int i = 0; i < times.size(); i++) {
        index[i] = i;
      }
      Arrays.sort(index, Comparator.comparingLong(times::get));
      times.sort(Long::compareTo);
      // sort measurementList
      measurementsList = sortList(measurementsList, index);
      // sort typesList
      typesList = sortList(typesList, index);
      // sort values
      valuesList = sortList(valuesList, index);
    }

    TSInsertRecordsOfOneDeviceReq request = new TSInsertRecordsOfOneDeviceReq();
    request.setDeviceId(deviceId);
    request.setTimestamps(times);
    request.setMeasurementsList(measurementsList);
    List<ByteBuffer> buffersList = objectValuesListToByteBufferList(valuesList, typesList);
    request.setValuesList(buffersList);
    return request;
  }

  @SuppressWarnings("squid:S3740")
  private List sortList(List source, Integer[] index) {
    Object[] result = new Object[source.size()];
    for (int i = 0; i < index.length; i++) {
      result[i] = source.get(index[i]);
    }
    return Arrays.asList(result);
  }

  private List<ByteBuffer> objectValuesListToByteBufferList(
      List<List<Object>> valuesList, List<List<TSDataType>> typesList)
      throws IoTDBConnectionException {
    List<ByteBuffer> buffersList = new ArrayList<>();
    for (int i = 0; i < valuesList.size(); i++) {
      ByteBuffer buffer = ByteBuffer.allocate(calculateLength(typesList.get(i), valuesList.get(i)));
      putValues(typesList.get(i), valuesList.get(i), buffer);
      buffersList.add(buffer);
    }
    return buffersList;
  }

  private void insertRecordsWithLeaderCache(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    Map<String, TSInsertRecordsReq> deviceGroup = new HashMap<>();
    for (int i = 0; i < deviceIds.size(); i++) {
      TSInsertRecordsReq request =
          deviceGroup.computeIfAbsent(deviceIds.get(i), k -> new TSInsertRecordsReq());
      updateTSInsertRecordsReq(
          request,
          deviceIds.get(i),
          times.get(i),
          measurementsList.get(i),
          typesList.get(i),
          valuesList.get(i));
    }
    // TODO parallel
    StringBuilder errMsgBuilder = new StringBuilder();
    for (Entry<String, TSInsertRecordsReq> entry : deviceGroup.entrySet()) {
      try {
        getSessionConnection(entry.getKey()).insertRecords(entry.getValue());
      } catch (RedirectException e) {
        handleRedirection(entry.getKey(), e.getEndPoint());
      } catch (StatementExecutionException e) {
        errMsgBuilder.append(e.getMessage());
      }
    }
    String errMsg = errMsgBuilder.toString();
    if (!errMsg.isEmpty()) {
      throw new StatementExecutionException(errMsg);
    }
  }

  private TSInsertRecordsReq genTSInsertRecordsReq(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException {
    TSInsertRecordsReq request = new TSInsertRecordsReq();
    request.setDeviceIds(deviceIds);
    request.setTimestamps(times);
    request.setMeasurementsList(measurementsList);
    List<ByteBuffer> buffersList = objectValuesListToByteBufferList(valuesList, typesList);
    request.setValuesList(buffersList);
    return request;
  }

  private void updateTSInsertRecordsReq(
      TSInsertRecordsReq request,
      String deviceId,
      Long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException {
    request.addToDeviceIds(deviceId);
    request.addToTimestamps(time);
    request.addToMeasurementsList(measurements);
    ByteBuffer buffer = ByteBuffer.allocate(calculateLength(types, values));
    putValues(types, values, buffer);
    request.addToValuesList(buffer);
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>a Tablet example: device1 time s1, s2, s3 1, 1, 1, 1 2, 2, 2, 2 3, 3, 3, 3
   *
   * <p>times in Tablet may be not in ascending order
   *
   * @param tablet data batch
   */
  public void insertTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, false);
    EndPoint endPoint;
    try {
      if (enableCacheLeader && (endPoint = deviceIdToEndpoint.get(tablet.prefixPath)) != null) {
        endPointToSessionConnection.get(endPoint).insertTablet(request);
      } else {
        defaultSessionConnection.insertTablet(request);
      }
    } catch (RedirectException e) {
      handleRedirection(tablet.prefixPath, e.getEndPoint());
    }
  }

  /**
   * insert a Tablet
   *
   * @param tablet data batch
   * @param sorted whether times in Tablet are in ascending order
   */
  public void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted);
    EndPoint endPoint;
    try {
      if (enableCacheLeader && (endPoint = deviceIdToEndpoint.get(tablet.prefixPath)) != null) {
        endPointToSessionConnection.get(endPoint).insertTablet(request);
      } else {
        defaultSessionConnection.insertTablet(request);
      }
    } catch (RedirectException e) {
      handleRedirection(tablet.prefixPath, e.getEndPoint());
    }
  }

  private TSInsertTabletReq genTSInsertTabletReq(Tablet tablet, boolean sorted)
      throws BatchExecutionException {
    if (sorted) {
      checkSortedThrowable(tablet);
    } else {
      sortTablet(tablet);
    }

    TSInsertTabletReq request = new TSInsertTabletReq();

    if (tablet.isAligned()) {
      if (tablet.getSchemas().size() > 1) {
        throw new BatchExecutionException("One tablet should only contain one aligned timeseries!");
      }
      request.setIsAligned(true);
      IMeasurementSchema measurementSchema = tablet.getSchemas().get(0);
      request.setPrefixPath(tablet.prefixPath);
      int measurementsSize = measurementSchema.getValueMeasurementIdList().size();
      for (int i = 0; i < measurementsSize; i++) {
        request.addToMeasurements(measurementSchema.getValueMeasurementIdList().get(i));
        request.addToTypes(measurementSchema.getValueTSDataTypeList().get(i).ordinal());
      }
      request.setIsAligned(true);
    } else {
      for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
        request.setPrefixPath(tablet.prefixPath);
        request.addToMeasurements(measurementSchema.getMeasurementId());
        request.addToTypes(measurementSchema.getType().ordinal());
        request.setIsAligned(tablet.isAligned());
      }
    }
    request.setTimestamps(SessionUtils.getTimeBuffer(tablet));
    request.setValues(SessionUtils.getValueBuffer(tablet));
    request.setSize(tablet.rowSize);
    return request;
  }

  /**
   * insert the data of several deivces. Given a deivce, for each timestamp, the number of
   * measurements is the same.
   *
   * <p>Times in each Tablet may not be in ascending order
   *
   * @param tablets data batch in multiple device
   */
  public void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    insertTablets(tablets, false);
  }

  /**
   * insert the data of several devices. Given a device, for each timestamp, the number of
   * measurements is the same.
   *
   * @param tablets data batch in multiple device
   * @param sorted whether times in each Tablet are in ascending order
   */
  public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    if (enableCacheLeader) {
      insertTabletsWithLeaderCache(tablets, sorted);
    } else {
      TSInsertTabletsReq request = genTSInsertTabletsReq(new ArrayList<>(tablets.values()), sorted);
      try {
        defaultSessionConnection.insertTablets(request);
      } catch (RedirectException ignored) {
        // ignored
      }
    }
  }

  private void insertTabletsWithLeaderCache(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    EndPoint endPoint;
    SessionConnection connection;
    Map<SessionConnection, TSInsertTabletsReq> tabletGroup = new HashMap<>();
    for (Entry<String, Tablet> entry : tablets.entrySet()) {
      endPoint = deviceIdToEndpoint.get(entry.getKey());
      if (endPoint != null) {
        connection = endPointToSessionConnection.get(endPoint);
      } else {
        connection = defaultSessionConnection;
      }
      TSInsertTabletsReq request =
          tabletGroup.computeIfAbsent(connection, k -> new TSInsertTabletsReq());
      updateTSInsertTabletsReq(request, entry.getValue(), sorted);
    }

    // TODO parallel
    StringBuilder errMsgBuilder = new StringBuilder();
    for (Entry<SessionConnection, TSInsertTabletsReq> entry : tabletGroup.entrySet()) {
      try {
        entry.getKey().insertTablets(entry.getValue());
      } catch (RedirectException e) {
        for (Entry<String, EndPoint> deviceEndPointEntry : e.getDeviceEndPointMap().entrySet()) {
          handleRedirection(deviceEndPointEntry.getKey(), deviceEndPointEntry.getValue());
        }
      } catch (StatementExecutionException e) {
        errMsgBuilder.append(e.getMessage());
      }
    }
    String errMsg = errMsgBuilder.toString();
    if (!errMsg.isEmpty()) {
      throw new StatementExecutionException(errMsg);
    }
  }

  private TSInsertTabletsReq genTSInsertTabletsReq(List<Tablet> tablets, boolean sorted)
      throws BatchExecutionException {
    TSInsertTabletsReq request = new TSInsertTabletsReq();

    for (Tablet tablet : tablets) {
      updateTSInsertTabletsReq(request, tablet, sorted);
    }
    return request;
  }

  private void updateTSInsertTabletsReq(TSInsertTabletsReq request, Tablet tablet, boolean sorted)
      throws BatchExecutionException {
    if (sorted) {
      checkSortedThrowable(tablet);
    } else {
      sortTablet(tablet);
    }

    request.addToDeviceIds(tablet.prefixPath);
    List<String> measurements = new ArrayList<>();
    List<Integer> dataTypes = new ArrayList<>();
    for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
      measurements.add(measurementSchema.getMeasurementId());
      dataTypes.add(measurementSchema.getType().ordinal());
    }
    request.addToMeasurementsList(measurements);
    request.addToTypesList(dataTypes);
    request.addToTimestampsList(SessionUtils.getTimeBuffer(tablet));
    request.addToValuesList(SessionUtils.getValueBuffer(tablet));
    request.addToSizeList(tablet.rowSize);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    testInsertTablet(tablet, false);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted);
    defaultSessionConnection.testInsertTablet(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    testInsertTablets(tablets, false);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletsReq request = genTSInsertTabletsReq(new ArrayList<>(tablets.values()), sorted);
    defaultSessionConnection.testInsertTablets(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordsReq request =
        genTSInsertStringRecordsReq(deviceIds, times, measurementsList, valuesList);
    defaultSessionConnection.testInsertRecords(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordsReq request =
        genTSInsertRecordsReq(deviceIds, times, measurementsList, typesList, valuesList);
    defaultSessionConnection.testInsertRecords(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request =
        genTSInsertStringRecordReq(deviceId, time, measurements, values);
    defaultSessionConnection.testInsertRecord(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request =
        genTSInsertRecordReq(deviceId, time, measurements, types, values, false);
    defaultSessionConnection.testInsertRecord(request);
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param path timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteTimeseries(Collections.singletonList(path));
  }

  /**
   * delete some timeseries, including data and schema
   *
   * @param paths timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteTimeseries(paths);
  }

  /**
   * delete data <= time in one timeseries
   *
   * @param path data in which time series to delete
   * @param endTime data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(String path, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    deleteData(Collections.singletonList(path), Long.MIN_VALUE, endTime);
  }

  /**
   * delete data <= time in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param endTime data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(List<String> paths, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    deleteData(paths, Long.MIN_VALUE, endTime);
  }

  /**
   * delete data >= startTime and data <= endTime in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param startTime delete range start time
   * @param endTime delete range end time
   */
  public void deleteData(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    TSDeleteDataReq request = genTSDeleteDataReq(paths, startTime, endTime);
    defaultSessionConnection.deleteData(request);
  }

  private TSDeleteDataReq genTSDeleteDataReq(List<String> paths, long startTime, long endTime) {
    TSDeleteDataReq request = new TSDeleteDataReq();
    request.setPaths(paths);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    return request;
  }

  private int calculateLength(List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException {
    int res = 0;
    for (int i = 0; i < types.size(); i++) {
      // types
      res += Byte.BYTES;
      switch (types.get(i)) {
        case BOOLEAN:
          res += 1;
          break;
        case INT32:
          res += Integer.BYTES;
          break;
        case INT64:
          res += Long.BYTES;
          break;
        case FLOAT:
          res += Float.BYTES;
          break;
        case DOUBLE:
          res += Double.BYTES;
          break;
        case TEXT:
          res += Integer.BYTES;
          res += ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET).length;
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    return res;
  }

  private int calculateLength(TSDataType type, Object value) throws IoTDBConnectionException {
    int res = 0;
    // types
    res += Byte.BYTES;
    switch (type) {
      case BOOLEAN:
        res += 1;
        break;
      case INT32:
        res += Integer.BYTES;
        break;
      case INT64:
        res += Long.BYTES;
        break;
      case FLOAT:
        res += Float.BYTES;
        break;
      case DOUBLE:
        res += Double.BYTES;
        break;
      case TEXT:
        res += Integer.BYTES;
        res += ((String) value).getBytes(TSFileConfig.STRING_CHARSET).length;
        break;
      default:
        throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + type);
    }

    return res;
  }

  /**
   * put value in buffer
   *
   * <p>// * @param types types list // * @param values values list
   *
   * @param buffer buffer to insert
   * @throws IoTDBConnectionException
   */
  private void putValue(TSDataType type, Object value, ByteBuffer buffer)
      throws IoTDBConnectionException {
    ReadWriteIOUtils.write(type, buffer);
    switch (type) {
      case BOOLEAN:
        ReadWriteIOUtils.write((Boolean) value, buffer);
        break;
      case INT32:
        ReadWriteIOUtils.write((Integer) value, buffer);
        break;
      case INT64:
        ReadWriteIOUtils.write((Long) value, buffer);
        break;
      case FLOAT:
        ReadWriteIOUtils.write((Float) value, buffer);
        break;
      case DOUBLE:
        ReadWriteIOUtils.write((Double) value, buffer);
        break;
      case TEXT:
        byte[] bytes = ((String) value).getBytes(TSFileConfig.STRING_CHARSET);
        ReadWriteIOUtils.write(bytes.length, buffer);
        buffer.put(bytes);
        break;
      default:
        throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + type);
    }

    buffer.flip();
  }

  /**
   * put value in buffer
   *
   * @param types types list
   * @param values values list
   * @param buffer buffer to insert
   * @throws IoTDBConnectionException
   */
  private void putValues(List<TSDataType> types, List<Object> values, ByteBuffer buffer)
      throws IoTDBConnectionException {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        ReadWriteIOUtils.write(TYPE_NULL, buffer);
        continue;
      }
      ReadWriteIOUtils.write(types.get(i), buffer);
      switch (types.get(i)) {
        case BOOLEAN:
          ReadWriteIOUtils.write((Boolean) values.get(i), buffer);
          break;
        case INT32:
          ReadWriteIOUtils.write((Integer) values.get(i), buffer);
          break;
        case INT64:
          ReadWriteIOUtils.write((Long) values.get(i), buffer);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((Float) values.get(i), buffer);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((Double) values.get(i), buffer);
          break;
        case TEXT:
          byte[] bytes = ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET);
          ReadWriteIOUtils.write(bytes.length, buffer);
          buffer.put(bytes);
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    buffer.flip();
  }

  /**
   * check whether the batch has been sorted
   *
   * @return whether the batch has been sorted
   */
  private boolean checkSorted(Tablet tablet) {
    for (int i = 1; i < tablet.rowSize; i++) {
      if (tablet.timestamps[i] < tablet.timestamps[i - 1]) {
        return false;
      }
    }
    return true;
  }

  private boolean checkSorted(List<Long> times) {
    for (int i = 1; i < times.size(); i++) {
      if (times.get(i) < times.get(i - 1)) {
        return false;
      }
    }
    return true;
  }

  private void checkSortedThrowable(Tablet tablet) throws BatchExecutionException {
    if (!checkSorted(tablet)) {
      throw new BatchExecutionException("Times in Tablet are not in ascending order");
    }
  }

  protected void sortTablet(Tablet tablet) {
    /*
     * following part of code sort the batch data by time,
     * so we can insert continuous data in value list to get a better performance
     */
    // sort to get index, and use index to sort value list
    Integer[] index = new Integer[tablet.rowSize];
    for (int i = 0; i < tablet.rowSize; i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparingLong(o -> tablet.timestamps[o]));
    Arrays.sort(tablet.timestamps, 0, tablet.rowSize);
    int columnIndex = 0;
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema instanceof MeasurementSchema) {
        tablet.values[columnIndex] = sortList(tablet.values[columnIndex], schema.getType(), index);
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] = sortBitMap(tablet.bitMaps[columnIndex], index);
        }
        columnIndex++;
      } else {
        int measurementSize = schema.getValueMeasurementIdList().size();
        for (int j = 0; j < measurementSize; j++) {
          tablet.values[columnIndex] =
              sortList(tablet.values[columnIndex], schema.getValueTSDataTypeList().get(j), index);
          if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
            tablet.bitMaps[columnIndex] = sortBitMap(tablet.bitMaps[columnIndex], index);
          }
          columnIndex++;
        }
      }
    }
  }

  /**
   * sort value list by index
   *
   * @param valueList value list
   * @param dataType data type
   * @param index index
   * @return sorted list
   */
  private Object sortList(Object valueList, TSDataType dataType, Integer[] index) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        boolean[] sortedValues = new boolean[boolValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedValues[i] = boolValues[index[i]];
        }
        return sortedValues;
      case INT32:
        int[] intValues = (int[]) valueList;
        int[] sortedIntValues = new int[intValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedIntValues[i] = intValues[index[i]];
        }
        return sortedIntValues;
      case INT64:
        long[] longValues = (long[]) valueList;
        long[] sortedLongValues = new long[longValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedLongValues[i] = longValues[index[i]];
        }
        return sortedLongValues;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        float[] sortedFloatValues = new float[floatValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedFloatValues[i] = floatValues[index[i]];
        }
        return sortedFloatValues;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        double[] sortedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedDoubleValues[i] = doubleValues[index[i]];
        }
        return sortedDoubleValues;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        Binary[] sortedBinaryValues = new Binary[binaryValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedBinaryValues[i] = binaryValues[index[i]];
        }
        return sortedBinaryValues;
      default:
        throw new UnSupportedDataTypeException(MSG_UNSUPPORTED_DATA_TYPE + dataType);
    }
  }

  /**
   * sort BitMap by index
   *
   * @param bitMap BitMap to be sorted
   * @param index index
   * @return sorted bitMap
   */
  private BitMap sortBitMap(BitMap bitMap, Integer[] index) {
    BitMap sortedBitMap = new BitMap(bitMap.getSize());
    for (int i = 0; i < index.length; i++) {
      if (bitMap.isMarked(index[i])) {
        sortedBitMap.mark(i);
      }
    }
    return sortedBitMap;
  }

  public void setSchemaTemplate(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    TSSetSchemaTemplateReq request = getTSSetSchemaTemplateReq(templateName, prefixPath);
    defaultSessionConnection.setSchemaTemplate(request);
  }

  /**
   * @param name template name
   * @param schemaNames list of schema names, if this measurement is vector, name it. if this
   *     measurement is not vector, keep this name as same as measurement's name
   * @param measurements List of measurements, if it is a single measurement, just put it's name
   *     into a list and add to measurements if it is a vector measurement, put all measurements of
   *     the vector into a list and add to measurements
   * @param dataTypes List of datatypes, if it is a single measurement, just put it's type into a
   *     list and add to dataTypes if it is a vector measurement, put all types of the vector into a
   *     list and add to dataTypes
   * @param encodings List of encodings, if it is a single measurement, just put it's encoding into
   *     a list and add to encodings if it is a vector measurement, put all encodings of the vector
   *     into a list and add to encodings
   * @param compressors List of compressors
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   */
  public void createSchemaTemplate(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateSchemaTemplateReq request =
        getTSCreateSchemaTemplateReq(
            name, schemaNames, measurements, dataTypes, encodings, compressors);
    defaultSessionConnection.createSchemaTemplate(request);
  }

  private TSSetSchemaTemplateReq getTSSetSchemaTemplateReq(String templateName, String prefixPath) {
    TSSetSchemaTemplateReq request = new TSSetSchemaTemplateReq();
    request.setTemplateName(templateName);
    request.setPrefixPath(prefixPath);
    return request;
  }

  private TSCreateSchemaTemplateReq getTSCreateSchemaTemplateReq(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors) {
    TSCreateSchemaTemplateReq request = new TSCreateSchemaTemplateReq();
    request.setName(name);
    request.setSchemaNames(schemaNames);
    request.setMeasurements(measurements);

    List<List<Integer>> requestType = new ArrayList<>();
    for (List<TSDataType> typesList : dataTypes) {
      requestType.add(typesList.stream().map(TSDataType::ordinal).collect(Collectors.toList()));
    }
    request.setDataTypes(requestType);

    List<List<Integer>> requestEncoding = new ArrayList<>();
    for (List<TSEncoding> encodingList : encodings) {
      requestEncoding.add(
          encodingList.stream().map(TSEncoding::ordinal).collect(Collectors.toList()));
    }
    request.setEncodings(requestEncoding);
    request.setCompressors(
        compressors.stream().map(CompressionType::ordinal).collect(Collectors.toList()));
    return request;
  }

  public boolean isEnableQueryRedirection() {
    return enableQueryRedirection;
  }

  public void setEnableQueryRedirection(boolean enableQueryRedirection) {
    this.enableQueryRedirection = enableQueryRedirection;
  }
}
