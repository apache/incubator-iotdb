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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class IoTDBJDBCResultSet extends AbstractIoTDBJDBCResultSet {

  public IoTDBJDBCResultSet(
      Statement statement,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      boolean ignoreTimeStamp,
      TSIService.Iface client,
      String sql,
      long queryId,
      long sessionId,
      TSQueryDataSet dataset,
      long timeout)
      throws SQLException {
    super(
        statement,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        ignoreTimeStamp,
        client,
        sql,
        queryId,
        sessionId,
        timeout);
    ioTDBRpcDataSet.setTsQueryDataSet(dataset);
  }

  public IoTDBJDBCResultSet(
      Statement statement,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      boolean ignoreTimeStamp,
      TSIService.Iface client,
      String sql,
      long queryId,
      long sessionId,
      TSQueryDataSet dataset,
      long timeout,
      boolean isRpcFetchResult)
      throws SQLException {
    super(
        statement,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        ignoreTimeStamp,
        client,
        sql,
        queryId,
        sessionId,
        timeout,
        isRpcFetchResult);
    ioTDBRpcDataSet.setTsQueryDataSet(dataset);
  }

  @Override
  public long getLong(String columnName) throws SQLException {
    try {
      return ioTDBRpcDataSet.getLong(columnName);
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  protected boolean fetchResults() throws SQLException {
    try {
      return ioTDBRpcDataSet.fetchResults();
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  protected boolean hasCachedResults() {
    return ioTDBRpcDataSet.hasCachedResults();
  }

  @Override
  protected void constructOneRow() {
    ioTDBRpcDataSet.constructOneRow();
  }

  @Override
  protected void checkRecord() throws SQLException {
    try {
      ioTDBRpcDataSet.checkRecord();
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  protected String getValueByName(String columnName) throws SQLException {
    try {
      return ioTDBRpcDataSet.getValueByName(columnName);
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  protected Object getObjectByName(String columnName) throws SQLException {
    try {
      return ioTDBRpcDataSet.getObjectByName(columnName);
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  public boolean isIgnoreTimeStamp() {
    return ioTDBRpcDataSet.ignoreTimeStamp;
  }
}
