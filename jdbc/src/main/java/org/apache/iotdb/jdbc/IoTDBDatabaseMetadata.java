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

import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchMetadataResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IoTDBDatabaseMetadata implements DatabaseMetaData {

  private IoTDBConnection connection;
  private TSIService.Iface client;
  private static final Logger logger = LoggerFactory.getLogger(IoTDBDatabaseMetadata.class);
  private static final String METHOD_NOT_SUPPORTED_STRING = "Method not supported";
  // when running the program in IDE, we can not get the version info using
  // getImplementationVersion()
  private static final String DATABASE_VERSION =
      IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBDatabaseMetadata.class.getPackage().getImplementationVersion()
          : "UNKNOWN";
  private long sessionId;
  private WatermarkEncoder groupedLSBWatermarkEncoder;
  private static String sqlKeywordsThatArentSQL92;

  IoTDBDatabaseMetadata(IoTDBConnection connection, TSIService.Iface client, long sessionId) {
    this.connection = connection;
    this.client = client;
    this.sessionId = sessionId;
  }

  static {
    String[] allIotdbSQLKeywords = {
      "ALTER",
      "ADD",
      "ALIAS",
      "ALL",
      "AVG",
      "ALIGN",
      "ATTRIBUTES",
      "AS",
      "ASC",
      "BY",
      "BOOLEAN",
      "BITMAP",
      "CREATE",
      "CONFIGURATION",
      "COMPRESSOR",
      "CHILD",
      "COUNT",
      "COMPRESSION",
      "CLEAR",
      "CACHE",
      "CONTAIN",
      "CONCAT",
      "DELETE",
      "DEVICE",
      "DESCRIBE",
      "DATATYPE",
      "DOUBLE",
      "DIFF",
      "DROP",
      "DEVICES",
      "DISABLE",
      "DESC",
      "ENCODING",
      "FROM",
      "FILL",
      "FLOAT",
      "FLUSH",
      "FIRST_VALUE",
      "FULL",
      "FALSE",
      "FOR",
      "FUNCTION",
      "FUNCTIONS",
      "GRANT",
      "GROUP",
      "GORILLA",
      "GLOBAL",
      "GZIP",
      "INSERT",
      "INTO",
      "INT32",
      "INT64",
      "INDEX",
      "INFO",
      "KILL",
      "LIMIT",
      "LINEAR",
      "LABEL",
      "LINK",
      "LIST",
      "LOAD",
      "LEVEL",
      "LAST_VALUE",
      "LAST",
      "LZO",
      "LZ4",
      "LATEST",
      "LIKE",
      "METADATA",
      "MERGE",
      "MOVE",
      "MIN_TIME",
      "MAX_TIME",
      "MIN_VALUE",
      "MAX_VALUE",
      "NOW",
      "NODES",
      "ORDER",
      "OFFSET",
      "ON",
      "OFF",
      "OF",
      "PROCESSLIST",
      "PREVIOUS",
      "PREVIOUSUNTILLAST",
      "PROPERTY",
      "PLAIN",
      "PLAIN_DICTIONARY",
      "PRIVILEGES",
      "PASSWORD",
      "PATHS",
      "PAA",
      "PLA",
      "PARTITION",
      "QUERY",
      "ROOT",
      "RLE",
      "REGULAR",
      "ROLE",
      "REVOKE",
      "REMOVE",
      "RENAME",
      "SELECT",
      "SHOW",
      "SET",
      "SLIMIT",
      "SOFFSET",
      "STORAGE",
      "SUM",
      "SNAPPY",
      "SNAPSHOT",
      "SCHEMA",
      "TO",
      "TIMESERIES",
      "TIMESTAMP",
      "TEXT",
      "TS_2DIFF",
      "TRACING",
      "TTL",
      "TASK",
      "TIME",
      "TAGS",
      "TRUE",
      "TEMPORARY",
      "TOP",
      "TOLERANCE",
      "UPDATE",
      "UNLINK",
      "UPSERT",
      "USING",
      "USER",
      "UNSET",
      "UNCOMPRESSED",
      "VALUES",
      "VERSION",
      "WHERE",
      "WITH",
      "WATERMARK_EMBEDDING"
    };
    String[] sql92Keywords = {
      "ABSOLUTE", "EXEC", "OVERLAPS", "ACTION", "EXECUTE", "PAD", "ADA", "EXISTS", "PARTIAL", "ADD",
      "EXTERNAL", "PASCAL", "ALL", "EXTRACT", "POSITION", "ALLOCATE", "FALSE", "PRECISION", "ALTER",
          "FETCH",
      "PREPARE", "AND", "FIRST", "PRESERVE", "ANY", "FLOAT", "PRIMARY", "ARE", "FOR", "PRIOR",
      "AS", "FOREIGN", "PRIVILEGES", "ASC", "FORTRAN", "PROCEDURE", "ASSERTION", "FOUND", "PUBLIC",
          "AT",
      "FROM", "READ", "AUTHORIZATION", "FULL", "REAL", "AVG", "GET", "REFERENCES", "BEGIN",
          "GLOBAL",
      "RELATIVE", "BETWEEN", "GO", "RESTRICT", "BIT", "GOTO", "REVOKE", "BIT_LENGTH", "GRANT",
          "RIGHT",
      "BOTH", "GROUP", "ROLLBACK", "BY", "HAVING", "ROWS", "CASCADE", "HOUR", "SCHEMA", "CASCADED",
      "IDENTITY", "SCROLL", "CASE", "IMMEDIATE", "SECOND", "CAST", "IN", "SECTION", "CATALOG",
          "INCLUDE",
      "SELECT", "CHAR", "INDEX", "SESSION", "CHAR_LENGTH", "INDICATOR", "SESSION_USER", "CHARACTER",
          "INITIALLY", "SET",
      "CHARACTER_LENGTH", "INNER", "SIZE", "CHECK", "INPUT", "SMALLINT", "CLOSE", "INSENSITIVE",
          "SOME", "COALESCE",
      "INSERT", "SPACE", "COLLATE", "INT", "SQL", "COLLATION", "INTEGER", "SQLCA", "COLUMN",
          "INTERSECT",
      "SQLCODE", "COMMIT", "INTERVAL", "SQLERROR", "CONNECT", "INTO", "SQLSTATE", "CONNECTION",
          "IS", "SQLWARNING",
      "CONSTRAINT", "ISOLATION", "SUBSTRING", "CONSTRAINTS", "JOIN", "SUM", "CONTINUE", "KEY",
          "SYSTEM_USER", "CONVERT",
      "LANGUAGE", "TABLE", "CORRESPONDING", "LAST", "TEMPORARY", "COUNT", "LEADING", "THEN",
          "CREATE", "LEFT",
      "TIME", "CROSS", "LEVEL", "TIMESTAMP", "CURRENT", "LIKE", "TIMEZONE_HOUR", "CURRENT_DATE",
          "LOCAL", "TIMEZONE_MINUTE",
      "CURRENT_TIME", "LOWER", "TO", "CURRENT_TIMESTAMP", "MATCH", "TRAILING", "CURRENT_USER",
          "MAX", "TRANSACTION", "CURSOR",
      "MIN", "TRANSLATE", "DATE", "MINUTE", "TRANSLATION", "DAY", "MODULE", "TRIM", "DEALLOCATE",
          "MONTH",
      "TRUE", "DEC", "NAMES", "UNION", "DECIMAL", "NATIONAL", "UNIQUE", "DECLARE", "NATURAL",
          "UNKNOWN",
      "DEFAULT", "NCHAR", "UPDATE", "DEFERRABLE", "NEXT", "UPPER", "DEFERRED", "NO", "USAGE",
          "DELETE",
      "NONE", "USER", "DESC", "NOT", "USING", "DESCRIBE", "NULL", "VALUE", "DESCRIPTOR", "NULLIF",
      "VALUES", "DIAGNOSTICS", "NUMERIC", "VARCHAR", "DISCONNECT", "OCTET_LENGTH", "VARYING",
          "DISTINCT", "OF", "VIEW",
      "DOMAIN", "ON", "WHEN", "DOUBLE", "ONLY", "WHENEVER", "DROP", "OPEN", "WHERE", "ELSE",
      "OPTION", "WITH", "END", "OR", "WORK", "END-EXEC", "ORDER", "WRITE", "ESCAPE", "OUTER",
      "YEAR", "EXCEPT", "OUTPUT", "ZONE", "EXCEPTION"
    };
    TreeMap myKeywordMap = new TreeMap();
    for (int i = 0; i < allIotdbSQLKeywords.length; i++)
      myKeywordMap.put(allIotdbSQLKeywords[i], null);
    HashMap sql92KeywordMap = new HashMap(sql92Keywords.length);
    for (int j = 0; j < sql92Keywords.length; j++) sql92KeywordMap.put(sql92Keywords[j], null);
    Iterator it = sql92KeywordMap.keySet().iterator();
    while (it.hasNext()) myKeywordMap.remove(it.next());
    StringBuffer keywordBuf = new StringBuffer();
    it = myKeywordMap.keySet().iterator();
    if (it.hasNext()) keywordBuf.append(it.next().toString());
    while (it.hasNext()) {
      keywordBuf.append(",");
      keywordBuf.append(it.next().toString());
    }
    sqlKeywordsThatArentSQL92 = keywordBuf.toString();
  }

  private WatermarkEncoder getWatermarkEncoder() {
    try {
      groupedLSBWatermarkEncoder =
          new GroupedLSBWatermarkEncoder(
              client.getProperties().getWatermarkSecretKey(),
              client.getProperties().getWatermarkBitString(),
              client.getProperties().getWatermarkParamMarkRate(),
              client.getProperties().getWatermarkParamMaxRightBit());
    } catch (TException e) {
      e.printStackTrace();
    }
    return groupedLSBWatermarkEncoder;
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public boolean allProceduresAreCallable() {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() {
    return true;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int arg0) {
    return true;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() {
    return false; // The return value is tentatively FALSE and may be adjusted later
  }

  @Override
  public boolean generatedKeyAlwaysReturned() {
    return true;
  }

  @Override
  public long getMaxLogicalLobSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public boolean supportsRefCursors() {
    return false;
  }

  @Override
  public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[21];
      fields[0] = new Field("", "TYPE_CAT", "TEXT");
      fields[1] = new Field("", "TYPE_SCHEM", "TEXT");
      fields[2] = new Field("", "TYPE_NAME", "TEXT");
      fields[3] = new Field("", "ATTR_NAME", "TEXT");
      fields[4] = new Field("", "DATA_TYPE", "INT32");
      fields[5] = new Field("", "ATTR_TYPE_NAME", "TEXT");
      fields[6] = new Field("", "ATTR_SIZE", "INT32");
      fields[7] = new Field("", "DECIMAL_DIGITS", "INT32");
      fields[8] = new Field("", "NUM_PREC_RADIX", "INT32");
      fields[9] = new Field("", "NULLABLE ", "INT32");
      fields[10] = new Field("", "REMARKS", "TEXT");
      fields[11] = new Field("", "ATTR_DEF", "TEXT");
      fields[12] = new Field("", "SQL_DATA_TYPE", "INT32");
      fields[13] = new Field("", "SQL_DATETIME_SUB", "INT32");
      fields[14] = new Field("", "CHAR_OCTET_LENGTH", "INT32");
      fields[15] = new Field("", "ORDINAL_POSITION", "INT32");
      fields[16] = new Field("", "IS_NULLABLE", "TEXT");
      fields[17] = new Field("", "SCOPE_CATALOG", "TEXT");
      fields[18] = new Field("", "SCOPE_SCHEMA", "TEXT");
      fields[19] = new Field("", "SCOPE_TABLE", "TEXT");
      fields[20] = new Field("", "SOURCE_DATA_TYPE", "INT32");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public ResultSet getBestRowIdentifier(
      String arg0, String arg1, String arg2, int arg3, boolean arg4) throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[8];
      fields[0] = new Field("", "SCOPE", "INT32");
      fields[1] = new Field("", "COLUMN_NAME", "TEXT");
      fields[2] = new Field("", "DATA_TYPE", "INT32");
      fields[3] = new Field("", "TYPE_NAME", "TEXT");
      fields[4] = new Field("", "COLUMN_SIZE", "INT32");
      fields[5] = new Field("", "BUFFER_LENGTH", "INT32");
      fields[6] = new Field("", "DECIMAL_DIGITS", "INT32");
      fields[7] = new Field("", "PSEUDO_COLUMN", "INT32");

      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }

    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public String getCatalogSeparator() {
    return ".";
  }

  @Override
  public String getCatalogTerm() {
    return "storage group";
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    Statement stmt = this.connection.createStatement();
    ResultSet rs = stmt.executeQuery("SHOW STORAGE GROUP ");
    Field[] fields = new Field[1];
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigpaths = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    while (rs.next()) {
      List<Map> paths = new ArrayList<Map>();
      Map<String, Object> m = new HashMap<>();
      m.put("type", TSDataType.TEXT);
      m.put("val", rs.getString(1));
      paths.add(m);
      bigpaths.add(paths);
    }
    addToDataSet(bigpaths, dataSet);
    columnNameList.add("TYPE_CAT");
    columnTypeList.add("TEXT");
    columnNameIndex.put("TYPE_CAT", 0);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(rs, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  public static TSQueryDataSet convertQueryDataSetByFetchSize(
      QueryDataSet queryDataSet, int fetchSize, WatermarkEncoder watermarkEncoder)
      throws IOException {
    List<TSDataType> dataTypes = queryDataSet.getDataTypes();
    int columnNum = dataTypes.size();
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
    // one time column and each value column has a actual value buffer and a bitmap value to
    // indicate whether it is a null
    int columnNumWithTime = columnNum * 2 + 1;
    DataOutputStream[] dataOutputStreams = new DataOutputStream[columnNumWithTime];
    ByteArrayOutputStream[] byteArrayOutputStreams = new ByteArrayOutputStream[columnNumWithTime];
    for (int i = 0; i < columnNumWithTime; i++) {
      byteArrayOutputStreams[i] = new ByteArrayOutputStream();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
    }
    int rowCount = 0;
    int[] valueOccupation = new int[columnNum];
    // used to record a bitmap for every 8 row record
    int[] bitmap = new int[columnNum];
    for (int i = 0; i < fetchSize; i++) {
      if (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        if (watermarkEncoder != null) {
          rowRecord = watermarkEncoder.encodeRecord(rowRecord);
        }
        // use columnOutput to write byte array
        dataOutputStreams[0].writeLong(rowRecord.getTimestamp());
        List<org.apache.iotdb.tsfile.read.common.Field> fields = rowRecord.getFields();
        for (int k = 0; k < fields.size(); k++) {
          org.apache.iotdb.tsfile.read.common.Field field = fields.get(k);
          DataOutputStream dataOutputStream = dataOutputStreams[2 * k + 1]; // DO NOT FORGET +1
          if (field == null || field.getDataType() == null) {
            bitmap[k] = (bitmap[k] << 1);
          } else {
            bitmap[k] = (bitmap[k] << 1) | 0x01;
            TSDataType type = field.getDataType();
            switch (type) {
              case INT32:
                dataOutputStream.writeInt(field.getIntV());
                valueOccupation[k] += 4;
                break;
              case INT64:
                dataOutputStream.writeLong(field.getLongV());
                valueOccupation[k] += 8;
                break;
              case FLOAT:
                dataOutputStream.writeFloat(field.getFloatV());
                valueOccupation[k] += 4;
                break;
              case DOUBLE:
                dataOutputStream.writeDouble(field.getDoubleV());
                valueOccupation[k] += 8;
                break;
              case BOOLEAN:
                dataOutputStream.writeBoolean(field.getBoolV());
                valueOccupation[k] += 1;
                break;
              case TEXT:
                dataOutputStream.writeInt(field.getBinaryV().getLength());
                dataOutputStream.write(field.getBinaryV().getValues());
                valueOccupation[k] = valueOccupation[k] + 4 + field.getBinaryV().getLength();
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }
          }
        }
        rowCount++;
        if (rowCount % 8 == 0) {
          for (int j = 0; j < bitmap.length; j++) {
            DataOutputStream dataBitmapOutputStream = dataOutputStreams[2 * (j + 1)];
            dataBitmapOutputStream.writeByte(bitmap[j]);
            // we should clear the bitmap every 8 row record
            bitmap[j] = 0;
          }
        }
      } else {
        break;
      }
    }

    // feed the remaining bitmap
    int remaining = rowCount % 8;
    if (remaining != 0) {
      for (int j = 0; j < bitmap.length; j++) {
        DataOutputStream dataBitmapOutputStream = dataOutputStreams[2 * (j + 1)];
        dataBitmapOutputStream.writeByte(bitmap[j] << (8 - remaining));
      }
    }
    // calculate the time buffer size
    int timeOccupation = rowCount * 8;
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeOccupation);
    timeBuffer.put(byteArrayOutputStreams[0].toByteArray());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);

    // calculate the bitmap buffer size
    int bitmapOccupation = rowCount / 8 + (rowCount % 8 == 0 ? 0 : 1);

    List<ByteBuffer> bitmapList = new LinkedList<>();
    List<ByteBuffer> valueList = new LinkedList<>();
    for (int i = 1; i < byteArrayOutputStreams.length; i += 2) {
      ByteBuffer valueBuffer = ByteBuffer.allocate(valueOccupation[(i - 1) / 2]);
      valueBuffer.put(byteArrayOutputStreams[i].toByteArray());
      valueBuffer.flip();
      valueList.add(valueBuffer);

      ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapOccupation);
      bitmapBuffer.put(byteArrayOutputStreams[i + 1].toByteArray());
      bitmapBuffer.flip();
      bitmapList.add(bitmapBuffer);
    }
    tsQueryDataSet.setBitmapList(bitmapList);
    tsQueryDataSet.setValueList(valueList);
    return tsQueryDataSet;
  }

  private void addToDataSet(List<List<Map>> listbigPaths, ListDataSet dataSet) {
    List<TSDataType> listType = new ArrayList<>();
    int i = 0;
    for (List<Map> listPaths : listbigPaths) {
      RowRecord record = new RowRecord(0);
      for (Map<String, Object> map : listPaths) {
        TSDataType columnType = (TSDataType) map.get("type");
        Object val = map.get("val");
        org.apache.iotdb.tsfile.read.common.Field field =
            new org.apache.iotdb.tsfile.read.common.Field(columnType);
        switch (columnType) {
          case TEXT:
            field.setBinaryV(new Binary(val.toString()));
            break;
          case FLOAT:
            field.setFloatV(((float) val));
            break;
          case INT32:
            field.setIntV(((int) val));
            break;
          case INT64:
            field.setLongV(((long) val));
            break;
          case DOUBLE:
            field.setDoubleV(((double) val));
            break;
          case BOOLEAN:
            field.setBoolV(((boolean) val));
            break;
        }
        record.addField(field);
        if (i == 0) {
          listType.add(columnType);
        }
      }
      i++;
      dataSet.putRecord(record);
    }
    dataSet.setDataTypes(listType);
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    Statement stmt = this.connection.createStatement();
    ResultSet rs = stmt.executeQuery("SHOW STORAGE GROUP ");
    Field[] fields = new Field[4];
    fields[0] = new Field("", "NAME", "TEXT");
    fields[1] = new Field("", "MAX_LEN", "INT32");
    fields[2] = new Field("", "DEFAULT_VALUE", "INT32");
    fields[3] = new Field("", "DESCRIPTION", "TEXT");
    List<TSDataType> listType =
        Arrays.asList(TSDataType.TEXT, TSDataType.INT32, TSDataType.INT32, TSDataType.TEXT);
    List<Object> listVal = Arrays.asList("fetch_size", 10, 10, "");
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    List<Map> properties = new ArrayList<Map>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
      Map<String, Object> m = new HashMap<>();
      m.put("type", listType.get(i));
      m.put("val", listVal.get(i));
      properties.add(m);
    }
    bigproperties.add(properties);
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(rs, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getColumnPrivileges(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW STORAGE GROUP";
    if (catalog != null && catalog.length() > 0) {
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && schemaPattern.length() > 0) {
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0) {
      sql = sql + "." + tableNamePattern;
    }

    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0
        && columnNamePattern != null
        && columnNamePattern.length() > 0) {
      sql = sql + "." + columnNamePattern;
    }
    ResultSet rs = stmt.executeQuery(sql);
    Field[] fields = new Field[8];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "GRANTOR", "TEXT");
    fields[5] = new Field("", "GRANTEE", "TEXT");
    fields[6] = new Field("", "PRIVILEGE", "TEXT");
    fields[7] = new Field("", "IS_GRANTABLE", "TEXT");
    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        if (i < 4) {
          m.put("val", rs.getString(1));
        } else if (i == 5) {
          m.put("val", getUserName());
        } else if (i == 6) {
          m.put("val", "");
        } else if (i == 7) {
          m.put("val", "NO");
        } else {
          m.put("val", "");
        }

        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(rs, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public ResultSet getCrossReference(
      String arg0, String arg1, String arg2, String arg3, String arg4, String arg5)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", "PKTABLE_CAT", "TEXT");
      fields[1] = new Field("", "PKTABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "PKTABLE_NAME", "TEXT");
      fields[3] = new Field("", "PKCOLUMN_NAME", "TEXT");
      fields[4] = new Field("", "FKTABLE_CAT", "TEXT");
      fields[5] = new Field("", "FKTABLE_SCHEM", "TEXT");
      fields[6] = new Field("", "FKTABLE_NAME", "TEXT");
      fields[7] = new Field("", "FKCOLUMN_NAME", "TEXT");
      fields[8] = new Field("", "KEY_SEQ", "TEXT");
      fields[9] = new Field("", "UPDATE_RULE ", "TEXT");
      fields[10] = new Field("", "DELETE_RULE", "TEXT");
      fields[11] = new Field("", "FK_NAME", "TEXT");
      fields[12] = new Field("", "PK_NAME", "TEXT");
      fields[13] = new Field("", "DEFERRABILITY", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public int getDatabaseMajorVersion() {
    int major_version = 0;
    try {
      String version = client.getProperties().getVersion();
      String[] versions = version.split(".");
      if (versions.length >= 2) {
        major_version = Integer.valueOf(versions[0]);
      }
    } catch (TException e) {
      e.printStackTrace();
    }
    return major_version;
  }

  @Override
  public int getDatabaseMinorVersion() {
    int minor_version = 0;
    try {
      String version = client.getProperties().getVersion();
      String[] versions = version.split(".");
      if (versions.length >= 2) {
        minor_version = Integer.valueOf(versions[1]);
      }
    } catch (TException e) {
      e.printStackTrace();
    }
    return minor_version;
  }

  @Override
  public String getDatabaseProductName() {
    return Constant.GLOBAL_DB_NAME;
  }

  @Override
  public String getDatabaseProductVersion() {
    return DATABASE_VERSION;
  }

  @Override
  public int getDefaultTransactionIsolation() {
    return 0;
  }

  @Override
  public int getDriverMajorVersion() {
    return 4;
  }

  @Override
  public int getDriverMinorVersion() {
    return 3;
  }

  @Override
  public String getDriverName() {
    return org.apache.iotdb.jdbc.IoTDBDriver.class.getName();
  }

  @Override
  public String getDriverVersion() {
    return DATABASE_VERSION;
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, final String table)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", "PKTABLE_CAT", "TEXT");
      fields[1] = new Field("", "PKTABLE_SCHEM", "INT32");
      fields[2] = new Field("", "PKTABLE_NAME", "TEXT");
      fields[3] = new Field("", "PKCOLUMN_NAME", "TEXT");
      fields[4] = new Field("", "FKTABLE_CAT", "TEXT");
      fields[5] = new Field("", "FKTABLE_SCHEM", "TEXT");
      fields[6] = new Field("", "FKTABLE_NAME", "TEXT");
      fields[7] = new Field("", "FKCOLUMN_NAME", "TEXT");
      fields[8] = new Field("", "KEY_SEQ", "INT32");
      fields[9] = new Field("", "UPDATE_RULE", "INT32");
      fields[10] = new Field("", "DELETE_RULE", "INT32");
      fields[11] = new Field("", "FK_NAME", "TEXT");
      fields[12] = new Field("", "PK_NAME", "TEXT");
      fields[13] = new Field("", "DEFERRABILITY", "INT32");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public String getExtraNameCharacters() {
    return "";
  }

  @Override
  public ResultSet getFunctionColumns(
      String catalog,
      String schemaPattern,
      java.lang.String functionNamePattern,
      java.lang.String columnNamePattern)
      throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("show functions");
    Field[] fields = new Field[17];
    fields[0] = new Field("", "FUNCTION_CAT ", "TEXT");
    fields[1] = new Field("", "FUNCTION_SCHEM", "TEXT");
    fields[2] = new Field("", "FUNCTION_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "COLUMN_TYPE", "INT32");
    fields[5] = new Field("", "DATA_TYPE", "INT32");
    fields[6] = new Field("", "TYPE_NAME", "TEXT");
    fields[7] = new Field("", "PRECISION", "INT32");
    fields[8] = new Field("", "LENGTH", "INT32");
    fields[9] = new Field("", "SCALE", "INT32");
    fields[10] = new Field("", "RADIX", "INT32");
    fields[11] = new Field("", "NULLABLE", "INT32");
    fields[12] = new Field("", "REMARKS", "TEXT");
    fields[13] = new Field("", "CHAR_OCTET_LENGTH", "INT32");
    fields[14] = new Field("", "ORDINAL_POSITION", "INT32");
    fields[15] = new Field("", "IS_NULLABLE", "TEXT");
    fields[16] = new Field("", "SPECIFIC_NAME", "TEXT");
    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        if (i == 2) {
          m.put("val", rs.getString(1));
        } else if (fields[i].getSqlType().equals("INT32")) {
          m.put("val", 0);
        } else {
          m.put("val", "");
        }
        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(rs, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery("show functions");
    Field[] fields = new Field[6];
    fields[0] = new Field("", "FUNCTION_CAT ", "TEXT");
    fields[1] = new Field("", "FUNCTION_SCHEM", "TEXT");
    fields[2] = new Field("", "FUNCTION_NAME", "TEXT");
    fields[3] = new Field("", "REMARKS", "TEXT");
    fields[4] = new Field("", "FUNCTION_TYPE", "INT32");
    fields[5] = new Field("", "SPECIFIC_NAME", "TEXT");
    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        if (i == 2) {
          m.put("val", rs.getString(1));
        } else if (i == 4) {
          m.put("val", 0);
        } else {
          m.put("val", "");
        }

        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(rs, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public String getIdentifierQuoteString() {
    return "\' or \"";
  }

  @Override
  public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", "PKTABLE_CAT", "TEXT");
      fields[1] = new Field("", "PKTABLE_SCHEM", "INT32");
      fields[2] = new Field("", "PKTABLE_NAME", "TEXT");
      fields[3] = new Field("", "PKCOLUMN_NAME", "TEXT");
      fields[4] = new Field("", "FKTABLE_CAT", "TEXT");
      fields[5] = new Field("", "FKTABLE_SCHEM", "TEXT");
      fields[6] = new Field("", "FKTABLE_NAME", "TEXT");
      fields[7] = new Field("", "FKCOLUMN_NAME", "TEXT");
      fields[8] = new Field("", "KEY_SEQ", "INT32");
      fields[9] = new Field("", "UPDATE_RULE", "INT32");
      fields[10] = new Field("", "DELETE_RULE", "INT32");
      fields[11] = new Field("", "FK_NAME", "TEXT");
      fields[12] = new Field("", "PK_NAME", "TEXT");
      fields[13] = new Field("", "DEFERRABILITY", "INT32");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[14];
      fields[0] = new Field("", "TABLE_CAT", "TEXT");
      fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "TABLE_NAME", "TEXT");
      fields[3] = new Field("", "NON_UNIQUE", "TEXT");
      fields[4] = new Field("", "INDEX_QUALIFIER", "TEXT");
      fields[5] = new Field("", "INDEX_NAME", "TEXT");
      fields[6] = new Field("", "TYPE", "TEXT");
      fields[7] = new Field("", "ORDINAL_POSITION", "TEXT");
      fields[8] = new Field("", "COLUMN_NAME", "TEXT");
      fields[9] = new Field("", "ASC_OR_DESC", "TEXT");
      fields[10] = new Field("", "CARDINALITY", "TEXT");
      fields[11] = new Field("", "PAGES", "TEXT");
      fields[12] = new Field("", "PK_NAME", "TEXT");
      fields[13] = new Field("", "FILTER_CONDITION", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public int getJDBCMajorVersion() {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() {
    return 3;
  }

  @Override
  public int getMaxBinaryLiteralLength() {
    return Integer.MAX_VALUE;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxCatalogNameLength() {
    return 1024;
  }

  @Override
  public int getMaxCharLiteralLength() {
    return Integer.MAX_VALUE;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxColumnNameLength() {
    return 1024;
  }

  @Override
  public int getMaxColumnsInGroupBy() {
    return 1;
  }

  @Override
  public int getMaxColumnsInIndex() {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() {
    return 1;
  }

  @Override
  public int getMaxColumnsInSelect() {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxConnections() {
    int maxcount = 0;
    try {
      maxcount = client.getProperties().getMaxConcurrentClientNum();
    } catch (TException e) {
      e.printStackTrace();
    }
    return maxcount;
  }

  @Override
  public int getMaxCursorNameLength() {
    return 0;
  }

  @Override
  public int getMaxIndexLength() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxProcedureNameLength() {
    return 0;
  }
  /** maxrowsize unlimited */
  @Override
  public int getMaxRowSize() {
    return 2147483639;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxSchemaNameLength() {
    return 1024;
  }

  @Override
  public int getMaxStatementLength() {
    try {
      return client.getProperties().getThriftMaxFrameSize();
    } catch (TException e) {
      e.printStackTrace();
    }
    return 0;
  }

  @Override
  public int getMaxStatements() {
    return 0;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxTableNameLength() {
    return 1024;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxTablesInSelect() {
    return 1024;
  }
  /** Although there is no limit, it is not recommended */
  @Override
  public int getMaxUserNameLength() {
    return 1024;
  }

  @Override
  public String getNumericFunctions() {
    ResultSet resultSet = null;
    Statement statement = null;
    String result = "";
    try {
      statement = connection.createStatement();
      StringBuilder str = new StringBuilder("");
      resultSet = statement.executeQuery("show functions");
      List<String> listfunction = Arrays.asList("MAX_TIME", "MIN_TIME", "TIME_DIFFERENCE", "NOW");
      while (resultSet.next()) {
        if (listfunction.contains(resultSet.getString(1))) {
          continue;
        }
        str.append(resultSet.getString(1)).append(",");
      }
      result = str.toString();
      if (result.length() > 0) {
        result = result.substring(0, result.length() - 1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(resultSet, statement);
    }
    return result;
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    Statement stmt = connection.createStatement();
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT);
    List<Object> listValSub_1 = Arrays.asList(catalog, "", table, "time", 1, "PRIMARY");
    List<Object> listValSub_2 = Arrays.asList(catalog, "", table, "deivce", 2, "PRIMARY");
    List<List<Object>> listVal = Arrays.asList(listValSub_1, listValSub_2);
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    Field[] fields = new Field[6];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "KEY_SEQ", "INT32");
    fields[5] = new Field("", "PK_NAME", "TEXT");
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    for (List<Object> listob : listVal) {
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        m.put("val", listob.get(i));
        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {

      Field[] fields = new Field[20];
      fields[0] = new Field("", "PROCEDURE_CAT", "TEXT");
      fields[1] = new Field("", "PROCEDURE_SCHEM", "TEXT");
      fields[2] = new Field("", "PROCEDURE_NAME", "TEXT");
      fields[3] = new Field("", "COLUMN_NAME", "TEXT");
      fields[4] = new Field("", "COLUMN_TYPE", "TEXT");
      fields[5] = new Field("", "DATA_TYPE", "INT32");
      fields[6] = new Field("", "TYPE_NAME", "TEXT");
      fields[7] = new Field("", "PRECISION", "TEXT");
      fields[8] = new Field("", "LENGTH", "TEXT");
      fields[9] = new Field("", "SCALE", "TEXT");
      fields[10] = new Field("", "RADIX", "TEXT");
      fields[11] = new Field("", "NULLABLE", "TEXT");
      fields[12] = new Field("", "REMARKS", "TEXT");
      fields[13] = new Field("", "COLUMN_DEF", "TEXT");
      fields[14] = new Field("", "SQL_DATA_TYPE", "INT32");
      fields[15] = new Field("", "SQL_DATETIME_SUB", "TEXT");
      fields[16] = new Field("", "CHAR_OCTET_LENGTH", "TEXT");
      fields[17] = new Field("", "ORDINAL_POSITION", "TEXT");
      fields[18] = new Field("", "IS_NULLABLE", "TEXT");
      fields[19] = new Field("", "SPECIFIC_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public String getProcedureTerm() {
    return "";
  }

  @Override
  public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[6];
      fields[0] = new Field("", "PROCEDURE_CAT", "TEXT");
      fields[1] = new Field("", "PROCEDURE_SCHEM", "TEXT");
      fields[2] = new Field("", "PROCEDURE_NAME", "TEXT");
      fields[3] = new Field("", "REMARKS", "TEXT");
      fields[4] = new Field("", "PROCEDURE_TYPE", "TEXT");
      fields[5] = new Field("", "SPECIFIC_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    Statement stmt = connection.createStatement();
    Field[] fields = new Field[12];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "DATA_TYPE", "INT32");
    fields[5] = new Field("", "COLUMN_SIZE", "INT32");
    fields[6] = new Field("", "DECIMAL_DIGITS", "INT32");
    fields[7] = new Field("", "NUM_PREC_RADIX", "INT32");
    fields[8] = new Field("", "COLUMN_USAGE", "TEXT");
    fields[9] = new Field("", "REMARKS", "TEXT");
    fields[10] = new Field("", "CHAR_OCTET_LENGTH", "INT32");
    fields[11] = new Field("", "IS_NULLABLE", "TEXT");
    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT);
    List<Object> listVal =
        Arrays.asList(
            catalog, catalog, tableNamePattern, "times", Types.BIGINT, 1, 0, 2, "", "", 13, "NO");
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    List<Map> properties = new ArrayList<Map>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
      Map<String, Object> m = new HashMap<>();
      m.put("type", listType.get(i));
      m.put("val", listVal.get(i));
      properties.add(m);
    }
    bigproperties.add(properties);
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public int getResultSetHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public String getSQLKeywords() {
    return sqlKeywordsThatArentSQL92;
  }

  @Override
  public int getSQLStateType() {
    return 0;
  }

  @Override
  public String getSchemaTerm() {
    return "stroge group";
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    Statement stmt = this.connection.createStatement();
    ResultSet rs = stmt.executeQuery("SHOW STORAGE GROUP ");
    Field[] fields = new Field[2];
    fields[0] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[1] = new Field("", "TABLE_CATALOG", "TEXT");
    List<TSDataType> listType = Arrays.asList(TSDataType.TEXT, TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        m.put("val", rs.getString(1));
        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(rs, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return getSchemas();
  }

  @Override
  public String getSearchStringEscape() {
    return "\\";
  }

  @Override
  public String getStringFunctions() {
    return getSystemFunctions();
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[4];
      fields[0] = new Field("", "TABLE_CAT", "TEXT");
      fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "TABLE_NAME", "TEXT");
      fields[3] = new Field("", "SUPERTABLE_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[6];
      fields[0] = new Field("", "TABLE_CAT", "TEXT");
      fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "TABLE_NAME", "TEXT");
      fields[3] = new Field("", "SUPERTYPE_CAT", "TEXT");
      fields[4] = new Field("", "SUPERTYPE_SCHEM", "TEXT");
      fields[5] = new Field("", "SUPERTYPE_NAME", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public String getSystemFunctions() {
    String result = "";
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      statement = connection.createStatement();
      StringBuilder str = new StringBuilder("");
      resultSet = statement.executeQuery("show functions");
      while (resultSet.next()) {
        str.append(resultSet.getString(1)).append(",");
      }
      result = str.toString();
      if (result.length() > 0) {
        result = result.substring(0, result.length() - 1);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      colse(resultSet, statement);
    }
    return result;
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW STORAGE GROUP";
    if (catalog != null && catalog.length() > 0) {
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && schemaPattern.length() > 0) {
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0) {
      sql = sql + "." + tableNamePattern;
    }

    ResultSet rs = stmt.executeQuery(sql);
    Field[] fields = new Field[8];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "GRANTOR", "TEXT");
    fields[5] = new Field("", "GRANTEE", "TEXT");
    fields[6] = new Field("", "PRIVILEGE", "TEXT");
    fields[7] = new Field("", "IS_GRANTABLE", "TEXT");
    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        if (i < 4) {
          m.put("val", rs.getString(1));
        } else if (i == 5) {
          m.put("val", getUserName());
        } else if (i == 6) {
          m.put("val", "");
        } else if (i == 7) {
          m.put("val", "NO");
        } else {
          m.put("val", "");
        }

        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(rs, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    Statement stmt = this.connection.createStatement();
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigpaths = new ArrayList<List<Map>>();
    List<Map> paths = new ArrayList<Map>();
    ListDataSet dataSet = new ListDataSet();
    Map<String, Object> m = new HashMap<>();
    m.put("type", TSDataType.TEXT);
    m.put("val", "table");
    paths.add(m);
    bigpaths.add(paths);
    addToDataSet(bigpaths, dataSet);
    columnNameList.add("TABLE_TYPE");
    columnTypeList.add("TEXT");
    columnNameIndex.put("TABLE_TYPE", 0);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW STORAGE GROUP";
    if (catalog != null && catalog.length() > 0) {
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && schemaPattern.length() > 0) {
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0) {
      sql = sql + "." + tableNamePattern;
    }

    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0
        && columnNamePattern != null
        && columnNamePattern.length() > 0) {
      sql = sql + "." + columnNamePattern;
    }
    ResultSet rs = stmt.executeQuery(sql);
    Field[] fields = new Field[24];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "COLUMN_NAME", "TEXT");
    fields[4] = new Field("", "DATA_TYPE", "INT32");
    fields[5] = new Field("", "TYPE_NAME", "TEXT");
    fields[6] = new Field("", "COLUMN_SIZE", "INT32");
    fields[7] = new Field("", "BUFFER_LENGTH", "INT32");
    fields[8] = new Field("", "DECIMAL_DIGITS", "INT32");
    fields[9] = new Field("", "NUM_PREC_RADIX", "INT32");
    fields[10] = new Field("", "NULLABLE", "INT32");
    fields[11] = new Field("", "REMARKS", "TEXT");
    fields[12] = new Field("", "COLUMN_DEF", "TEXT");
    fields[13] = new Field("", "SQL_DATA_TYPE", "INT32");
    fields[14] = new Field("", "SQL_DATETIME_SUB", "INT32");
    fields[15] = new Field("", "CHAR_OCTET_LENGTH", "INT32");
    fields[16] = new Field("", "ORDINAL_POSITION", "INT32");
    fields[17] = new Field("", "IS_NULLABLE", "TEXT");
    fields[18] = new Field("", "SCOPE_CATALOG", "TEXT");
    fields[19] = new Field("", "SCOPE_SCHEMA", "TEXT");
    fields[20] = new Field("", "SCOPE_TABLE", "TEXT");
    fields[21] = new Field("", "SOURCE_DATA_TYPE", "INT32");
    fields[22] = new Field("", "IS_AUTOINCREMENT", "TEXT");
    fields[23] = new Field("", "IS_GENERATEDCOLUMN", "TEXT");

    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        if (i < 4) {
          m.put("val", rs.getString(1));
        } else if (i == 4) {
          m.put("val", getSQLType(fields[i].getSqlType()));
        } else if (i == 6) {
          m.put("val", getTypePrecision(fields[i].getSqlType()));
        } else if (i == 7) {
          m.put("val", 0);
        } else if (i == 8) {
          m.put("val", getTypeScale(fields[i].getSqlType()));
        } else if (i == 9) {
          m.put("val", 10);
        } else if (i == 10) {
          m.put("val", 0);
        } else if (i == 11) {
          m.put("val", "");
        } else if (i == 12) {
          m.put("val", "");
        } else if (i == 13) {
          m.put("val", 0);
        } else if (i == 14) {
          m.put("val", 0);
        } else if (i == 15) {
          m.put("val", getTypePrecision(fields[i].getSqlType()));
        } else if (i == 16) {
          m.put("val", 1);
        } else if (i == 17) {
          m.put("val", "NO");
        } else if (i == 18) {
          m.put("val", "");
        } else if (i == 19) {
          m.put("val", "");
        } else if (i == 20) {
          m.put("val", "");
        } else if (i == 21) {
          m.put("val", 0);
        } else if (i == 22) {
          m.put("val", "NO");
        } else if (i == 23) {
          m.put("val", "NO");
        } else {
          m.put("val", "");
        }

        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(rs, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  private void colse(ResultSet rs, Statement stmt) {

    try {
      if (rs != null) {
        rs.close();
      }
    } catch (Exception ex) {
      rs = null;
    }
    try {
      if (stmt != null) {
        stmt.close();
      }
    } catch (Exception ex) {
      stmt = null;
    }
  }

  public int getTypeScale(String columnType) {
    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
      case "INT32":
      case "INT64":
      case "TEXT":
        return 0;
      case "FLOAT":
        return 6;
      case "DOUBLE":
        return 15;
      default:
        break;
    }
    return 0;
  }

  private int getSQLType(String columnType) {
    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
        return Types.BOOLEAN;
      case "INT32":
        return Types.INTEGER;
      case "INT64":
        return Types.BIGINT;
      case "FLOAT":
        return Types.FLOAT;
      case "DOUBLE":
        return Types.DOUBLE;
      case "TEXT":
        return Types.LONGVARCHAR;
      default:
        break;
    }
    return 0;
  }

  private int getTypePrecision(String columnType) {
    // BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT,
    switch (columnType.toUpperCase()) {
      case "BOOLEAN":
        return 1;
      case "INT32":
        return 10;
      case "INT64":
        return 19;
      case "FLOAT":
        return 38;
      case "DOUBLE":
        return 308;
      case "TEXT":
        return Integer.MAX_VALUE;
      default:
        break;
    }
    return 0;
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    Statement stmt = this.connection.createStatement();

    String sql = "SHOW timeseries";
    if (catalog != null && catalog.length() > 0) {
      sql = sql + " " + catalog;
    } else if (schemaPattern != null && schemaPattern.length() > 0) {
      sql = sql + " " + schemaPattern;
    }
    if (((catalog != null && catalog.length() > 0)
            || schemaPattern != null && schemaPattern.length() > 0)
        && tableNamePattern != null
        && tableNamePattern.length() > 0) {
      sql = sql + "." + tableNamePattern;
    }
    ResultSet rs = stmt.executeQuery(sql);
    Field[] fields = new Field[10];
    fields[0] = new Field("", "TABLE_CAT", "TEXT");
    fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
    fields[2] = new Field("", "TABLE_NAME", "TEXT");
    fields[3] = new Field("", "TABLE_TYPE", "TEXT");
    fields[4] = new Field("", "REMARKS", "TEXT");
    fields[5] = new Field("", "TYPE_CAT", "TEXT");
    fields[6] = new Field("", "TYPE_SCHEM", "TEXT");
    fields[7] = new Field("", "TYPE_NAME", "TEXT");
    fields[8] = new Field("", "SELF_REFERENCING_COL_NAME", "TEXT");
    fields[9] = new Field("", "REF_GENERATION", "TEXT");

    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }
    while (rs.next()) {
      System.out.println(rs.getString(1) + "--" + rs.getString(3));
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        if (i < 2) {
          m.put("val", rs.getString(3));
        } else if (i == 2) {
          m.put("val", rs.getString(1));
        } else if (i == 3) {
          m.put("val", "TABLE");
        } else {
          m.put("val", "");
        }
        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public String getTimeDateFunctions() {
    return "MAX_TIME,MIN_TIME,TIME_DIFFERENCE,NOW";
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    Statement stmt = connection.createStatement();
    Field[] fields = new Field[18];
    fields[0] = new Field("", "TYPE_NAME", "TEXT");
    fields[1] = new Field("", "DATA_TYPE", "INT32");
    fields[2] = new Field("", "PRECISION", "INT32");
    fields[3] = new Field("", "LITERAL_PREFIX", "TEXT");
    fields[4] = new Field("", "LITERAL_SUFFIX", "TEXT");
    fields[5] = new Field("", "CREATE_PARAMS", "TEXT");
    fields[6] = new Field("", "NULLABLE", "INT32");
    fields[7] = new Field("", "CASE_SENSITIVE", "BOOLEAN");
    fields[8] = new Field("", "SEARCHABLE", "TEXT");
    fields[9] = new Field("", "UNSIGNED_ATTRIBUTE", "BOOLEAN");
    fields[10] = new Field("", "FIXED_PREC_SCALE", "BOOLEAN");
    fields[11] = new Field("", "AUTO_INCREMENT", "BOOLEAN");
    fields[12] = new Field("", "LOCAL_TYPE_NAME", "TEXT");
    fields[13] = new Field("", "MINIMUM_SCALE", "INT32");
    fields[14] = new Field("", "MAXIMUM_SCALE", "INT32");
    fields[15] = new Field("", "SQL_DATA_TYPE", "INT32");
    fields[16] = new Field("", "SQL_DATETIME_SUB", "INT32");
    fields[17] = new Field("", "NUM_PREC_RADIX", "INT32");
    List<TSDataType> listType =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.BOOLEAN,
            TSDataType.TEXT,
            TSDataType.BOOLEAN,
            TSDataType.BOOLEAN,
            TSDataType.BOOLEAN,
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.INT32);
    List<Object> listValSub_1 =
        Arrays.asList(
            "INT32",
            Types.INTEGER,
            10,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_2 =
        Arrays.asList(
            "INT64",
            Types.BIGINT,
            19,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_3 =
        Arrays.asList(
            "BOOLEAN",
            Types.BOOLEAN,
            1,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_4 =
        Arrays.asList(
            "FLOAT",
            Types.FLOAT,
            38,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_5 =
        Arrays.asList(
            "DOUBLE",
            Types.DOUBLE,
            308,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<Object> listValSub_6 =
        Arrays.asList(
            "TEXT",
            Types.LONGVARCHAR,
            64,
            "",
            "",
            "",
            1,
            true,
            "",
            false,
            true,
            false,
            "",
            0,
            10,
            0,
            0,
            10);
    List<List<Object>> listVal =
        Arrays.asList(
            listValSub_1, listValSub_2, listValSub_3, listValSub_4, listValSub_5, listValSub_6);
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    List<List<Map>> bigproperties = new ArrayList<List<Map>>();
    ListDataSet dataSet = new ListDataSet();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
      Map<String, Object> m = new HashMap<>();
    }
    for (List<Object> listob : listVal) {
      List<Map> properties = new ArrayList<Map>();
      for (int i = 0; i < fields.length; i++) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", listType.get(i));
        m.put("val", listob.get(i));
        properties.add(m);
      }
      bigproperties.add(properties);
    }
    addToDataSet(bigproperties, dataSet);
    TSQueryDataSet tsdataset = null;
    try {
      tsdataset =
          convertQueryDataSetByFetchSize(dataSet, stmt.getFetchSize(), getWatermarkEncoder());
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        0,
        sessionId,
        tsdataset,
        (long) 60 * 1000,
        false);
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[7];
      fields[0] = new Field("", "TABLE_CAT", "TEXT");
      fields[1] = new Field("", "TABLE_SCHEM", "TEXT");
      fields[2] = new Field("", "TABLE_NAME", "TEXT");
      fields[3] = new Field("", "CLASS_NAME", "TEXT");
      fields[4] = new Field("", "DATA_TYPE", "INT32");
      fields[5] = new Field("", "REMARKS", "TEXT");
      fields[6] = new Field("", "BASE_TYPE", "TEXT");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public String getURL() {
    // TODO: Return the URL for this DBMS or null if it cannot be generated
    return this.connection.getUrl();
  }

  @Override
  public String getUserName() throws SQLException {
    return connection.getUserName();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    List<String> columnNameList = new ArrayList<String>();
    List<String> columnTypeList = new ArrayList<String>();
    Map<String, Integer> columnNameIndex = new HashMap<String, Integer>();
    Statement stmt = connection.createStatement();
    try {
      Field[] fields = new Field[8];
      fields[0] = new Field("", "SCOPE", "INT32");
      fields[1] = new Field("", "COLUMN_NAME", "TEXT");
      fields[2] = new Field("", "DATA_TYPE", "INT32");
      fields[3] = new Field("", "TYPE_NAME", "TEXT");
      fields[4] = new Field("", "COLUMN_SIZE", "INT32");
      fields[5] = new Field("", "BUFFER_LENGTH", "INT32");
      fields[6] = new Field("", "DECIMAL_DIGITS", "INT32");
      fields[7] = new Field("", "PSEUDO_COLUMN", "INT32");
      for (int i = 0; i < fields.length; i++) {
        columnNameList.add(fields[i].getName());
        columnTypeList.add(fields[i].getSqlType());
        columnNameIndex.put(fields[i].getName(), i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      colse(null, stmt);
    }
    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        false,
        client,
        null,
        0,
        sessionId,
        null,
        (long) 60 * 1000);
  }

  @Override
  public boolean insertsAreDetected(int type) {
    return false;
  }

  @Override
  public boolean isCatalogAtStart() {
    return false;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    try {
      return client.getProperties().isReadOnly;
    } catch (TException e) {
      e.printStackTrace();
    }
    throw new SQLException("Can not get the read-only mode");
  }

  @Override
  public boolean locatorsUpdateCopy() {
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() {
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) {
    return true;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) {
    return true;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) {
    return true;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) {
    return true;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) {
    return true;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) {
    return true;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() {
    return true;
  }

  @Override
  public boolean supportsBatchUpdates() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() {
    return true;
  }

  @Override
  public boolean supportsConvert() {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() {
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() {
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() {
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsGetGeneratedKeys() {
    return false;
  }

  @Override
  public boolean supportsGroupBy() {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() {
    return true;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() {
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults() {
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() {
    return true;
  }

  @Override
  public boolean supportsNamedParameters() {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() {
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() {
    return true;
  }

  @Override
  public boolean supportsOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() {
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) {
    return false;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) {
    if (ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability) {
      return true;
    }
    return false;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    if (ResultSet.FETCH_FORWARD == type || ResultSet.TYPE_FORWARD_ONLY == type) {
      return true;
    }
    return false;
  }

  @Override
  public boolean supportsSavepoints() {
    return false;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() {
    return false;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) {
    return false;
  }

  @Override
  public boolean supportsTransactions() {
    return false;
  }

  @Override
  public boolean supportsUnion() {
    return false;
  }

  @Override
  public boolean supportsUnionAll() {
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() {
    return false;
  }

  @Override
  public boolean usesLocalFiles() {
    return false;
  }

  /** @deprecated recommend using getMetadataInJson() instead of toString() */
  @Deprecated
  @Override
  public String toString() {
    try {
      return getMetadataInJsonFunc();
    } catch (IoTDBSQLException e) {
      logger.error("Failed to fetch metadata in json because: ", e);
    } catch (TException e) {
      boolean flag = connection.reconnect();
      this.client = connection.getClient();
      if (flag) {
        try {
          return getMetadataInJsonFunc();
        } catch (TException e2) {
          logger.error(
              "Fail to get all timeseries "
                  + "info after reconnecting."
                  + " please check server status",
              e2);
        } catch (IoTDBSQLException e1) {
          // ignored
        }
      } else {
        logger.error(
            "Fail to reconnect to server "
                + "when getting all timeseries info. please check server status");
      }
    }
    return "";
  }

  /*
   * recommend using getMetadataInJson() instead of toString()
   */
  public String getMetadataInJson() throws SQLException {
    try {
      return getMetadataInJsonFunc();
    } catch (TException e) {
      boolean flag = connection.reconnect();
      this.client = connection.getClient();
      if (flag) {
        try {
          return getMetadataInJsonFunc();
        } catch (TException e2) {
          throw new SQLException(
              "Failed to fetch all metadata in json "
                  + "after reconnecting. Please check the server status.");
        }
      } else {
        throw new SQLException(
            "Failed to reconnect to the server "
                + "when fetching all metadata in json. Please check the server status.");
      }
    }
  }

  private String getMetadataInJsonFunc() throws TException, IoTDBSQLException {
    TSFetchMetadataReq req = new TSFetchMetadataReq(sessionId, "METADATA_IN_JSON");
    TSFetchMetadataResp resp = client.fetchMetadata(req);
    try {
      RpcUtils.verifySuccess(resp.getStatus());
    } catch (StatementExecutionException e) {
      throw new IoTDBSQLException(e.getMessage(), resp.getStatus());
    }
    return resp.getMetadataInJson();
  }
}
