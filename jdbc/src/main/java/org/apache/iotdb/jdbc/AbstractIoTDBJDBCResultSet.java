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

import org.apache.iotdb.rpc.IoTDBRpcDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSIService;

import org.apache.thrift.TException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.MathContext;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public abstract class AbstractIoTDBJDBCResultSet implements ResultSet {

  protected Statement statement;
  protected SQLWarning warningChain = null;
  protected List<String> columnTypeList;
  protected IoTDBRpcDataSet ioTDBRpcDataSet;
  private boolean isRpcFetchResult = true;

  public AbstractIoTDBJDBCResultSet(
      Statement statement,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      boolean ignoreTimeStamp,
      TSIService.Iface client,
      String sql,
      long queryId,
      long sessionId,
      long timeout)
      throws SQLException {
    this.ioTDBRpcDataSet =
        new IoTDBRpcDataSet(
            sql,
            columnNameList,
            columnTypeList,
            columnNameIndex,
            ignoreTimeStamp,
            queryId,
            ((IoTDBStatement) statement).getStmtId(),
            client,
            sessionId,
            null,
            statement.getFetchSize(),
            timeout);
    this.statement = statement;
    this.columnTypeList = columnTypeList;
  }

  public AbstractIoTDBJDBCResultSet(
      Statement statement,
      List<String> columnNameList,
      List<String> columnTypeList,
      Map<String, Integer> columnNameIndex,
      boolean ignoreTimeStamp,
      TSIService.Iface client,
      String sql,
      long queryId,
      long sessionId,
      long timeout,
      boolean isRpcFetchResult)
      throws SQLException {
    this.ioTDBRpcDataSet =
        new IoTDBRpcDataSet(
            sql,
            columnNameList,
            columnTypeList,
            columnNameIndex,
            ignoreTimeStamp,
            queryId,
            ((IoTDBStatement) statement).getStmtId(),
            client,
            sessionId,
            null,
            statement.getFetchSize(),
            timeout);
    this.statement = statement;
    this.columnTypeList = columnTypeList;
    this.isRpcFetchResult = isRpcFetchResult;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean absolute(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void afterLast() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void close() throws SQLException {
    try {
      ioTDBRpcDataSet.close();
    } catch (StatementExecutionException e) {
      throw new SQLException("Error occurs for close operation in server side because ", e);
    } catch (TException e) {
      throw new SQLException("Error occurs when connecting to server for close operation ", e);
    }
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int findColumn(String columnName) {
    return ioTDBRpcDataSet.findColumn(columnName);
  }

  @Override
  public boolean first() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Array getArray(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Array getArray(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getAsciiStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getAsciiStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    try {
      return getBigDecimal(ioTDBRpcDataSet.findColumnNameByIndex(columnIndex));
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    String value = getValueByName(columnName);
    if (value != null) {
      return new BigDecimal(value);
    } else {
      return null;
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    MathContext mc = new MathContext(scale);
    return getBigDecimal(columnIndex).round(mc);
  }

  @Override
  public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
    return getBigDecimal(findColumn(columnName), scale);
  }

  @Override
  public InputStream getBinaryStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getBinaryStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Blob getBlob(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Blob getBlob(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    try {
      return getBoolean(ioTDBRpcDataSet.findColumnNameByIndex(columnIndex));
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public boolean getBoolean(String columnName) throws SQLException {
    try {
      return ioTDBRpcDataSet.getBoolean(columnName);
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte getByte(String columnName) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public byte[] getBytes(String columnName) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Reader getCharacterStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Reader getCharacterStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Clob getClob(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Clob getClob(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getConcurrency() {
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    return new Date(getLong(columnIndex));
  }

  @Override
  public Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName));
  }

  @Override
  public Date getDate(int arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Date getDate(String arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    try {
      return getDouble(ioTDBRpcDataSet.findColumnNameByIndex(columnIndex));
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public double getDouble(String columnName) throws SQLException {
    try {
      return ioTDBRpcDataSet.getDouble(columnName);
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public int getFetchDirection() {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void setFetchSize(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    try {
      return getFloat(ioTDBRpcDataSet.findColumnNameByIndex(columnIndex));
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public float getFloat(String columnName) throws SQLException {
    try {
      return ioTDBRpcDataSet.getFloat(columnName);
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    try {
      return getInt(ioTDBRpcDataSet.findColumnNameByIndex(columnIndex));
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public int getInt(String columnName) throws SQLException {
    try {
      return ioTDBRpcDataSet.getInt(columnName);
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    try {
      return getLong(ioTDBRpcDataSet.findColumnNameByIndex(columnIndex));
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public abstract long getLong(String columnName) throws SQLException;

  @Override
  public ResultSetMetaData getMetaData() {
    return new IoTDBResultMetadata(
        ioTDBRpcDataSet.columnNameList,
        ioTDBRpcDataSet.columnTypeList,
        ioTDBRpcDataSet.ignoreTimeStamp);
  }

  @Override
  public Reader getNCharacterStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Reader getNCharacterStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public NClob getNClob(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public NClob getNClob(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public String getNString(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public String getNString(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    try {
      return getObject(ioTDBRpcDataSet.findColumnNameByIndex(columnIndex));
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public Object getObject(String columnName) throws SQLException {
    return getObjectByName(columnName);
  }

  @Override
  public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Ref getRef(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Ref getRef(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public RowId getRowId(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public RowId getRowId(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public SQLXML getSQLXML(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public SQLXML getSQLXML(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public short getShort(String columnName) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Statement getStatement() {
    return this.statement;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    try {
      return getString(ioTDBRpcDataSet.findColumnNameByIndex(columnIndex));
    } catch (StatementExecutionException e) {
      throw new SQLException(e.getMessage());
    }
  }

  @Override
  public String getString(String columnName) throws SQLException {
    return getValueByName(columnName);
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    return new Time(getLong(columnIndex));
  }

  @Override
  public Time getTime(String columnName) throws SQLException {
    return getTime(findColumn(columnName));
  }

  @Override
  public Time getTime(int arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Time getTime(String arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return new Timestamp(getLong(columnIndex));
  }

  @Override
  public Timestamp getTimestamp(String columnName) throws SQLException {
    return getTimestamp(findColumn(columnName));
  }

  @Override
  public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getType() {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public URL getURL(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public URL getURL(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getUnicodeStream(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InputStream getUnicodeStream(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public SQLWarning getWarnings() {
    return warningChain;
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isClosed() {
    return ioTDBRpcDataSet.isClosed;
  }

  @Override
  public boolean isFirst() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean last() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean next() throws SQLException {
    if (hasCachedResults()) {
      constructOneRow();
      return true;
    }
    if (ioTDBRpcDataSet.emptyResultSet) {
      return false;
    }
    if (isRpcFetchResult && fetchResults()) {
      constructOneRow();
      return true;
    }
    return false;
  }

  /** @return true means has results */
  abstract boolean fetchResults() throws SQLException;

  abstract boolean hasCachedResults();

  abstract void constructOneRow();

  @Override
  public boolean previous() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean relative(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateArray(int arg0, Array arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateArray(String arg0, Array arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(int arg0, Blob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(String arg0, Blob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(int arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(String arg0, InputStream arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBoolean(int arg0, boolean arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBoolean(String arg0, boolean arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateByte(int arg0, byte arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateByte(String arg0, byte arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBytes(int arg0, byte[] arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateBytes(String arg0, byte[] arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(int arg0, Clob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(String arg0, Clob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(int arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(String arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateDate(int arg0, Date arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateDate(String arg0, Date arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateDouble(int arg0, double arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateDouble(String arg0, double arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateFloat(int arg0, float arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateFloat(String arg0, float arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateInt(int arg0, int arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateInt(String arg0, int arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateLong(int arg0, long arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateLong(String arg0, long arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(int arg0, NClob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(String arg0, NClob arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(int arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(String arg0, Reader arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNString(int arg0, String arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNString(String arg0, String arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNull(int arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateNull(String arg0) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateObject(int arg0, Object arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateObject(String arg0, Object arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRef(int arg0, Ref arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRef(String arg0, Ref arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRowId(int arg0, RowId arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateRowId(String arg0, RowId arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateShort(int arg0, short arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateShort(String arg0, short arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateString(int arg0, String arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateString(String arg0, String arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateTime(int arg0, Time arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateTime(String arg0, Time arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean wasNull() {
    return ioTDBRpcDataSet.lastReadWasNull;
  }

  abstract void checkRecord() throws SQLException;

  abstract String getValueByName(String columnName) throws SQLException;

  abstract Object getObjectByName(String columnName) throws SQLException;
}
