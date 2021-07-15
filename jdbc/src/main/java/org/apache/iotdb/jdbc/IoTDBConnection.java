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
package org.apache.iotdb.jdbc;

import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class IoTDBConnection implements Connection {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBConnection.class);
  private static final TSProtocolVersion protocolVersion =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
  private static final String NOT_SUPPORT_PREPARE_CALL = "Does not support prepareCall";
  private static final String NOT_SUPPORT_PREPARE_STATEMENT = "Does not support prepareStatement";
  private TSIService.Iface client = null;
  private long sessionId = -1;
  private IoTDBConnectionParams params;
  private boolean isClosed = true;
  private SQLWarning warningChain = null;
  private TTransport transport;
  private TConfiguration tConfiguration = TConfigurationConst.defaultTConfiguration;
  /**
   * Timeout of query can be set by users. Unit: s If not set, default value 0 will be used, which
   * will use server configuration.
   */
  private int queryTimeout = 0;

  private ZoneId zoneId;
  private boolean autoCommit;
  private String url;

  public String getUserName() {
    return userName;
  }

  private String userName;

  public IoTDBConnection() {
    // allowed to create an instance without parameter input.
  }

  public IoTDBConnection(String url, Properties info) throws SQLException, TTransportException {
    if (url == null) {
      throw new IoTDBURLException("Input url cannot be null");
    }
    params = Utils.parseUrl(url, info);
    this.url = url;
    this.userName = info.get("user").toString();
    openTransport();
    if (Config.rpcThriftCompressionEnable) {
      setClient(new TSIService.Client(new TCompactProtocol(transport)));
    } else {
      setClient(new TSIService.Client(new TBinaryProtocol(transport)));
    }
    // open client session
    openSession();
    // Wrap the client with a thread-safe proxy to serialize the RPC calls
    setClient(RpcUtils.newSynchronizedClient(getClient()));
    autoCommit = false;
  }

  public String getUrl() {
    return url;
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new SQLException("Does not support isWrapperFor");
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new SQLException("Does not support unwrap");
  }

  @Override
  public void abort(Executor arg0) throws SQLException {
    throw new SQLException("Does not support abort");
  }

  @Override
  public void clearWarnings() {
    warningChain = null;
  }

  @Override
  public void close() throws SQLException {
    if (isClosed) {
      return;
    }
    TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
    try {
      getClient().closeSession(req);
    } catch (TException e) {
      throw new SQLException(
          "Error occurs when closing session at server. Maybe server is down.", e);
    } finally {
      isClosed = true;
      if (transport != null) {
        transport.close();
      }
    }
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLException("Does not support commit");
  }

  @Override
  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    throw new SQLException("Does not support createArrayOf");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLException("Does not support createBlob");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLException("Does not support createClob");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLException("Does not suppport createNClob");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Does not support createSQLXML");
  }

  @Override
  public Statement createStatement() throws SQLException {
    if (isClosed) {
      throw new SQLException("Cannot create statement because connection is closed");
    }
    return new IoTDBStatement(this, getClient(), sessionId, zoneId, queryTimeout);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLException(
          String.format(
              "Statements with result set concurrency %d are not supported", resultSetConcurrency));
    }
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
      throw new SQLException(
          String.format("Statements with ResultSet type %d are not supported", resultSetType));
    }
    return new IoTDBStatement(this, getClient(), sessionId, zoneId, queryTimeout);
  }

  @Override
  public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
    throw new SQLException("Does not support createStatement");
  }

  @Override
  public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
    throw new SQLException("Does not support createStruct");
  }

  @Override
  public boolean getAutoCommit() {
    return autoCommit;
  }

  @Override
  public void setAutoCommit(boolean arg0) {
    autoCommit = arg0;
  }

  @Override
  public String getCatalog() {
    return "Apache IoTDB";
  }

  @Override
  public void setCatalog(String arg0) throws SQLException {
    throw new SQLException("Does not support setCatalog");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLException("Does not support getClientInfo");
  }

  @Override
  public void setClientInfo(Properties arg0) throws SQLClientInfoException {
    throw new SQLClientInfoException("Does not support setClientInfo", null);
  }

  @Override
  public String getClientInfo(String arg0) throws SQLException {
    throw new SQLException("Does not support getClientInfo");
  }

  @Override
  public int getHoldability() {
    // throw new SQLException("Method not supported");
    return 0;
  }

  @Override
  public void setHoldability(int arg0) throws SQLException {
    throw new SQLException("Does not support setHoldability");
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    if (isClosed) {
      throw new SQLException("Cannot create statement because connection is closed");
    }
    return new IoTDBDatabaseMetadata(this, getClient(), sessionId);
  }

  @Override
  public int getNetworkTimeout() {
    return Config.DEFAULT_CONNECTION_TIMEOUT_MS;
  }

  @Override
  public String getSchema() throws SQLException {
    throw new SQLException("Does not support getSchema");
  }

  @Override
  public void setSchema(String arg0) throws SQLException {
    throw new SQLException("Does not support setSchema");
  }

  @Override
  public int getTransactionIsolation() {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public void setTransactionIsolation(int arg0) throws SQLException {
    throw new SQLException("Does not support setTransactionIsolation");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLException("Does not support getTypeMap");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
    throw new SQLException("Does not support setTypeMap");
  }

  @Override
  public SQLWarning getWarnings() {
    return warningChain;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public void setReadOnly(boolean arg0) throws SQLException {
    throw new SQLException("Does not support setReadOnly");
  }

  @Override
  public boolean isValid(int arg0) {
    return !isClosed;
  }

  @Override
  public String nativeSQL(String arg0) throws SQLException {
    throw new SQLException("Does not support nativeSQL");
  }

  @Override
  public CallableStatement prepareCall(String arg0) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3)
      throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_CALL);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new IoTDBPreparedStatement(this, getClient(), sessionId, sql, zoneId);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLException(NOT_SUPPORT_PREPARE_STATEMENT);
  }

  @Override
  public void releaseSavepoint(Savepoint arg0) throws SQLException {
    throw new SQLException("Does not support releaseSavepoint");
  }

  @Override
  public void rollback() {
    // do nothing in rollback
  }

  @Override
  public void rollback(Savepoint arg0) {
    // do nothing in rollback
  }

  @Override
  public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
    throw new SQLClientInfoException("Does not support setClientInfo", null);
  }

  @Override
  public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
    throw new SQLException("Does not support setNetworkTimeout");
  }

  public int getQueryTimeout() {
    return this.queryTimeout;
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    if (seconds < 0) {
      throw new SQLException(String.format("queryTimeout %d must be >= 0!", seconds));
    }
    this.queryTimeout = seconds;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Does not support setSavepoint");
  }

  @Override
  public Savepoint setSavepoint(String arg0) throws SQLException {
    throw new SQLException("Does not support setSavepoint");
  }

  public TSIService.Iface getClient() {
    return client;
  }

  public long getSessionId() {
    return sessionId;
  }

  public void setClient(TSIService.Iface client) {
    this.client = client;
  }

  private void openTransport() throws TTransportException {
    RpcTransportFactory.setDefaultBufferCapacity(params.getThriftDefaultBufferSize());
    RpcTransportFactory.setThriftMaxFrameSize(params.getThriftMaxFrameSize());
    transport =
        RpcTransportFactory.INSTANCE.getTransport(
            new TSocket(
                tConfiguration,
                params.getHost(),
                params.getPort(),
                Config.DEFAULT_CONNECTION_TIMEOUT_MS));
    if (!transport.isOpen()) {
      transport.open();
    }
  }

  private void openSession() throws SQLException {
    TSOpenSessionReq openReq = new TSOpenSessionReq();

    openReq.setUsername(params.getUsername());
    openReq.setPassword(params.getPassword());
    openReq.setZoneId(getTimeZone());

    TSOpenSessionResp openResp = null;
    try {
      openResp = client.openSession(openReq);
      sessionId = openResp.getSessionId();
      // validate connection
      RpcUtils.verifySuccess(openResp.getStatus());

      if (protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        logger.warn(
            "Protocol differ, Client version is {}}, but Server version is {}",
            protocolVersion.getValue(),
            openResp.getServerProtocolVersion().getValue());
        if (openResp.getServerProtocolVersion().getValue() == 0) { // less than 0.10
          throw new TException(
              String.format(
                  "Protocol not supported, Client version is %s, but Server version is %s",
                  protocolVersion.getValue(), openResp.getServerProtocolVersion().getValue()));
        }
      }

    } catch (TException e) {
      transport.close();
      if (e.getMessage().contains("Required field 'client_protocol' was not present!")) {
        // the server is an old version (less than 0.10)
        throw new SQLException(
            String.format(
                "Can not establish connection with %s : You may try to connect an old version IoTDB instance using a client with new version: %s. ",
                params.getJdbcUriString(), e.getMessage()),
            e);
      }
      throw new SQLException(
          String.format(
              "Can not establish connection with %s : %s. ",
              params.getJdbcUriString(), e.getMessage()),
          e);
    } catch (StatementExecutionException e) {
      // failed to connect, disconnect from the server
      transport.close();
      throw new IoTDBSQLException(e.getMessage(), openResp.getStatus());
    }
    isClosed = false;
  }

  boolean reconnect() {
    boolean flag = false;
    for (int i = 1; i <= Config.RETRY_NUM; i++) {
      try {
        if (transport != null) {
          transport.close();
          openTransport();
          if (Config.rpcThriftCompressionEnable) {
            setClient(new TSIService.Client(new TCompactProtocol(transport)));
          } else {
            setClient(new TSIService.Client(new TBinaryProtocol(transport)));
          }
          openSession();
          setClient(RpcUtils.newSynchronizedClient(getClient()));
          flag = true;
          break;
        }
      } catch (Exception e) {
        try {
          Thread.sleep(Config.RETRY_INTERVAL_MS);
        } catch (InterruptedException e1) {
          logger.error("reconnect is interrupted.", e1);
          Thread.currentThread().interrupt();
        }
      }
    }
    return flag;
  }

  public String getTimeZone() {
    if (zoneId == null) {
      zoneId = ZoneId.systemDefault();
    }
    return zoneId.toString();
  }

  public void setTimeZone(String zoneId) throws TException, IoTDBSQLException {
    TSSetTimeZoneReq req = new TSSetTimeZoneReq(sessionId, zoneId);
    TSStatus resp = getClient().setTimeZone(req);
    try {
      RpcUtils.verifySuccess(resp);
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), resp);
    }
    this.zoneId = ZoneId.of(zoneId);
  }

  public ServerProperties getServerProperties() throws TException {
    return getClient().getProperties();
  }
}
