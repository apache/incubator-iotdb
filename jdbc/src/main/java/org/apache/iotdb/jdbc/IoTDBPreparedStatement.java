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

import org.apache.iotdb.service.rpc.thrift.TSIService.Iface;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IoTDBPreparedStatement extends IoTDBStatement implements PreparedStatement {

  private String sql;
  private static final String METHOD_NOT_SUPPORTED_STRING = "Method not supported";
  private static final Logger logger = LoggerFactory.getLogger(IoTDBPreparedStatement.class);

  /** save the SQL parameters as (paramLoc,paramValue) pairs. */
  private final Map<Integer, String> parameters = new LinkedHashMap<>();

  IoTDBPreparedStatement(
      IoTDBConnection connection, Iface client, Long sessionId, String sql, ZoneId zoneId)
      throws SQLException {
    super(connection, client, sessionId, zoneId);
    this.sql = sql;
  }

  @Override
  public void addBatch() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void clearParameters() {
    this.parameters.clear();
  }

  @Override
  public boolean execute() throws SQLException {
    return super.execute(createCompleteSql(sql, parameters));
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return super.executeQuery(createCompleteSql(sql, parameters));
  }

  @Override
  public int executeUpdate() throws SQLException {
    return super.executeUpdate(createCompleteSql(sql, parameters));
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public ParameterMetaData getParameterMetaData() {
    return new ParameterMetaData() {
      @Override
      public int getParameterCount() {
        return parameters.size();
      }

      @Override
      public int isNullable(int param) {
        return ParameterMetaData.parameterNullableUnknown;
      }

      @Override
      public boolean isSigned(int param) {
        try {
          return Integer.parseInt(parameters.get(param)) < 0;
        } catch (Exception e) {
          return false;
        }
      }

      @Override
      public int getPrecision(int param) {
        return parameters.get(param).length();
      }

      @Override
      public int getScale(int param) {
        try {
          double d = Double.parseDouble(parameters.get(param));
          if (d >= 1) { // we only need the fraction digits
            d = d - (long) d;
          }
          if (d == 0) { // nothing to count
            return 0;
          }
          d *= 10; // shifts 1 digit to left
          int count = 1;
          while (d - (long) d != 0) { // keeps shifting until there are no more fractions
            d *= 10;
            count++;
          }
          return count;
        } catch (Exception e) {
          return 0;
        }
      }

      @Override
      public int getParameterType(int param) {
        return 0;
      }

      @Override
      public String getParameterTypeName(int param) {
        return null;
      }

      @Override
      public String getParameterClassName(int param) {
        return null;
      }

      @Override
      public int getParameterMode(int param) {
        return 0;
      }

      @Override
      public <T> T unwrap(Class<T> iface) {
        return null;
      }

      @Override
      public boolean isWrapperFor(Class<?> iface) {
        return false;
      }
    };
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) {
    this.parameters.put(parameterIndex, Boolean.toString(x));
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    Binary binary = new Binary(x);
    this.parameters.put(parameterIndex, binary.getStringValue());
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setDouble(int parameterIndex, double x) {
    this.parameters.put(parameterIndex, Double.toString(x));
  }

  @Override
  public void setFloat(int parameterIndex, float x) {
    this.parameters.put(parameterIndex, Float.toString(x));
  }

  @Override
  public void setInt(int parameterIndex, int x) {
    this.parameters.put(parameterIndex, Integer.toString(x));
  }

  @Override
  public void setLong(int parameterIndex, long x) {
    this.parameters.put(parameterIndex, Long.toString(x));
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    throw new SQLException(Constant.PARAMETER_NOT_NULL);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    throw new SQLException(Constant.PARAMETER_NOT_NULL);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof Integer) {
      setInt(parameterIndex, (Integer) x);
    } else if (x instanceof Long) {
      setLong(parameterIndex, (Long) x);
    } else if (x instanceof Float) {
      setFloat(parameterIndex, (Float) x);
    } else if (x instanceof Double) {
      setDouble(parameterIndex, (Double) x);
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, (Boolean) x);
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof Time) {
      setTime(parameterIndex, (Time) x);
    } else {
      // Can't infer a type.
      throw new SQLException(
          String.format(
              "Can''t infer the SQL type to use for an instance of %s. Use setObject() with"
                  + " an explicit Types value to specify the type to use.",
              x.getClass().getName()));
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    if (!(x instanceof BigDecimal)) {
      setObject(parameterIndex, x, targetSqlType, 0);
    } else {
      setObject(parameterIndex, x, targetSqlType, ((BigDecimal) x).scale());
    }
  }

  @Override
  public void setObject(int parameterIndex, Object parameterObj, int targetSqlType, int scale)
      throws SQLException {
    if (parameterObj == null) {
      setNull(parameterIndex, java.sql.Types.OTHER);
    } else {
      try {
        switch (targetSqlType) {
          case Types.BOOLEAN:
            if (parameterObj instanceof Boolean) {
              setBoolean(parameterIndex, ((Boolean) parameterObj).booleanValue());
              break;
            } else if (parameterObj instanceof String) {
              if ("true".equalsIgnoreCase((String) parameterObj)
                  || "Y".equalsIgnoreCase((String) parameterObj)) {
                setBoolean(parameterIndex, true);
              } else if ("false".equalsIgnoreCase((String) parameterObj)
                  || "N".equalsIgnoreCase((String) parameterObj)) {
                setBoolean(parameterIndex, false);
              } else if (((String) parameterObj).matches("-?\\d+\\.?\\d*")) {
                setBoolean(parameterIndex, !((String) parameterObj).matches("-?[0]+[.]*[0]*"));
              } else {
                throw new SQLException(
                    "No conversion from " + parameterObj + " to Types.BOOLEAN possible.");
              }
              break;
            } else if (parameterObj instanceof Number) {
              int intValue = ((Number) parameterObj).intValue();

              setBoolean(parameterIndex, intValue != 0);

              break;
            } else {
              throw new SQLException(
                  "No conversion from " + parameterObj + " to Types.BOOLEAN possible.");
            }

          case Types.BIT:
          case Types.TINYINT:
          case Types.SMALLINT:
          case Types.INTEGER:
          case Types.BIGINT:
          case Types.REAL:
          case Types.FLOAT:
          case Types.DOUBLE:
          case Types.DECIMAL:
          case Types.NUMERIC:
            setNumericObject(parameterIndex, parameterObj, targetSqlType, scale);
            break;
          case Types.CHAR:
          case Types.VARCHAR:
          case Types.LONGVARCHAR:
            if (parameterObj instanceof BigDecimal) {
              setString(
                  parameterIndex,
                  StringUtils.fixDecimalExponent(
                      StringUtils.consistentToString((BigDecimal) parameterObj)));
            } else {
              setString(parameterIndex, parameterObj.toString());
            }

            break;

          case Types.CLOB:
            if (parameterObj instanceof java.sql.Clob) {
              setClob(parameterIndex, (java.sql.Clob) parameterObj);
            } else {
              setString(parameterIndex, parameterObj.toString());
            }

            break;

          case Types.BINARY:
          case Types.VARBINARY:
          case Types.LONGVARBINARY:
          case Types.BLOB:
            throw new SQLException(Constant.PARAMETER_SUPPORTED);
          case Types.DATE:
          case Types.TIMESTAMP:
            java.util.Date parameterAsDate;

            if (parameterObj instanceof String) {
              ParsePosition pp = new ParsePosition(0);
              DateFormat sdf =
                  new SimpleDateFormat(getDateTimePattern((String) parameterObj, false), Locale.US);
              parameterAsDate = sdf.parse((String) parameterObj, pp);
            } else {
              parameterAsDate = (Date) parameterObj;
            }

            switch (targetSqlType) {
              case Types.DATE:
                if (parameterAsDate instanceof java.sql.Date) {
                  setDate(parameterIndex, (java.sql.Date) parameterAsDate);
                } else {
                  setDate(parameterIndex, new java.sql.Date(parameterAsDate.getTime()));
                }

                break;

              case Types.TIMESTAMP:
                if (parameterAsDate instanceof java.sql.Timestamp) {
                  setTimestamp(parameterIndex, (java.sql.Timestamp) parameterAsDate);
                } else {
                  setTimestamp(parameterIndex, new java.sql.Timestamp(parameterAsDate.getTime()));
                }

                break;
            }

            break;

          case Types.TIME:
            if (parameterObj instanceof String) {
              DateFormat sdf =
                  new SimpleDateFormat(getDateTimePattern((String) parameterObj, true), Locale.US);
              setTime(parameterIndex, new Time(sdf.parse((String) parameterObj).getTime()));
            } else if (parameterObj instanceof Timestamp) {
              Timestamp xT = (Timestamp) parameterObj;
              setTime(parameterIndex, new Time(xT.getTime()));
            } else {
              setTime(parameterIndex, (Time) parameterObj);
            }

            break;

          case Types.OTHER:
            throw new SQLException(Constant.PARAMETER_SUPPORTED); //
          default:
            throw new SQLException(Constant.PARAMETER_SUPPORTED); //
        }
      } catch (Exception ex) {
        if (ex instanceof SQLException) {
          throw (SQLException) ex;
        }

        throw new SQLException(Constant.PARAMETER_SUPPORTED); //
      }
    }
  }

  private final String getDateTimePattern(String dt, boolean toTime) throws Exception {
    //
    // Special case
    //
    int dtLength = (dt != null) ? dt.length() : 0;

    if ((dtLength >= 8) && (dtLength <= 10)) {
      int dashCount = 0;
      boolean isDateOnly = true;

      for (int i = 0; i < dtLength; i++) {
        char c = dt.charAt(i);

        if (!Character.isDigit(c) && (c != '-')) {
          isDateOnly = false;

          break;
        }

        if (c == '-') {
          dashCount++;
        }
      }

      if (isDateOnly && (dashCount == 2)) {
        return "yyyy-MM-dd";
      }
    }
    boolean colonsOnly = true;

    for (int i = 0; i < dtLength; i++) {
      char c = dt.charAt(i);

      if (!Character.isDigit(c) && (c != ':')) {
        colonsOnly = false;

        break;
      }
    }

    if (colonsOnly) {
      return "HH:mm:ss";
    }

    int n;
    int z;
    int count;
    int maxvecs;
    char c;
    char separator;
    StringReader reader = new StringReader(dt + " ");
    ArrayList<Object[]> vec = new ArrayList<Object[]>();
    ArrayList<Object[]> vecRemovelist = new ArrayList<Object[]>();
    Object[] nv = new Object[3];
    Object[] v;
    nv[0] = Character.valueOf('y');
    nv[1] = new StringBuilder();
    nv[2] = Integer.valueOf(0);
    vec.add(nv);

    if (toTime) {
      nv = new Object[3];
      nv[0] = Character.valueOf('h');
      nv[1] = new StringBuilder();
      nv[2] = Integer.valueOf(0);
      vec.add(nv);
    }

    while ((z = reader.read()) != -1) {
      separator = (char) z;
      maxvecs = vec.size();

      for (count = 0; count < maxvecs; count++) {
        v = vec.get(count);
        n = ((Integer) v[2]).intValue();
        c = getSuccessor(((Character) v[0]).charValue(), n);

        if (!Character.isLetterOrDigit(separator)) {
          if ((c == ((Character) v[0]).charValue()) && (c != 'S')) {
            vecRemovelist.add(v);
          } else {
            ((StringBuilder) v[1]).append(separator);

            if ((c == 'X') || (c == 'Y')) {
              v[2] = Integer.valueOf(4);
            }
          }
        } else {
          if (c == 'X') {
            c = 'y';
            nv = new Object[3];
            nv[1] = (new StringBuilder(((StringBuilder) v[1]).toString())).append('M');
            nv[0] = Character.valueOf('M');
            nv[2] = Integer.valueOf(1);
            vec.add(nv);
          } else if (c == 'Y') {
            c = 'M';
            nv = new Object[3];
            nv[1] = (new StringBuilder(((StringBuilder) v[1]).toString())).append('d');
            nv[0] = Character.valueOf('d');
            nv[2] = Integer.valueOf(1);
            vec.add(nv);
          }

          ((StringBuilder) v[1]).append(c);

          if (c == ((Character) v[0]).charValue()) {
            v[2] = Integer.valueOf(n + 1);
          } else {
            v[0] = Character.valueOf(c);
            v[2] = Integer.valueOf(1);
          }
        }
      }

      int size = vecRemovelist.size();

      for (int i = 0; i < size; i++) {
        v = vecRemovelist.get(i);
        vec.remove(v);
      }

      vecRemovelist.clear();
    }

    int size = vec.size();

    for (int i = 0; i < size; i++) {
      v = vec.get(i);
      c = ((Character) v[0]).charValue();
      n = ((Integer) v[2]).intValue();

      boolean bk = getSuccessor(c, n) != c;
      boolean atEnd = (((c == 's') || (c == 'm') || ((c == 'h') && toTime)) && bk);
      boolean finishesAtDate = (bk && (c == 'd') && !toTime);
      boolean containsEnd = (((StringBuilder) v[1]).toString().indexOf('W') != -1);

      if ((!atEnd && !finishesAtDate) || (containsEnd)) {
        vecRemovelist.add(v);
      }
    }

    size = vecRemovelist.size();

    for (int i = 0; i < size; i++) {
      vec.remove(vecRemovelist.get(i));
    }

    vecRemovelist.clear();
    v = vec.get(0); // might throw exception

    StringBuilder format = (StringBuilder) v[1];
    format.setLength(format.length() - 1);

    return format.toString();
  }

  private final char getSuccessor(char c, int n) {
    return ((c == 'y') && (n == 2))
        ? 'X'
        : (((c == 'y') && (n < 4))
            ? 'y'
            : ((c == 'y')
                ? 'M'
                : (((c == 'M') && (n == 2))
                    ? 'Y'
                    : (((c == 'M') && (n < 3))
                        ? 'M'
                        : ((c == 'M')
                            ? 'd'
                            : (((c == 'd') && (n < 2))
                                ? 'd'
                                : ((c == 'd')
                                    ? 'H'
                                    : (((c == 'H') && (n < 2))
                                        ? 'H'
                                        : ((c == 'H')
                                            ? 'm'
                                            : (((c == 'm') && (n < 2))
                                                ? 'm'
                                                : ((c == 'm')
                                                    ? 's'
                                                    : (((c == 's') && (n < 2))
                                                        ? 's'
                                                        : 'W'))))))))))));
  }

  private void setNumericObject(
      int parameterIndex, Object parameterObj, int targetSqlType, int scale) throws SQLException {
    Number parameterAsNum;

    if (parameterObj instanceof Boolean) {
      parameterAsNum =
          ((Boolean) parameterObj).booleanValue() ? Integer.valueOf(1) : Integer.valueOf(0);
    } else if (parameterObj instanceof String) {
      switch (targetSqlType) {
        case Types.BIT:
          if ("1".equals(parameterObj) || "0".equals(parameterObj)) {
            parameterAsNum = Integer.valueOf((String) parameterObj);
          } else {
            boolean parameterAsBoolean = "true".equalsIgnoreCase((String) parameterObj);

            parameterAsNum = parameterAsBoolean ? Integer.valueOf(1) : Integer.valueOf(0);
          }

          break;

        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
          parameterAsNum = Integer.valueOf((String) parameterObj);

          break;

        case Types.BIGINT:
          parameterAsNum = Long.valueOf((String) parameterObj);

          break;

        case Types.REAL:
          parameterAsNum = Float.valueOf((String) parameterObj);

          break;

        case Types.FLOAT:
        case Types.DOUBLE:
          parameterAsNum = Double.valueOf((String) parameterObj);

          break;

        case Types.DECIMAL:
        case Types.NUMERIC:
        default:
          parameterAsNum = new java.math.BigDecimal((String) parameterObj);
      }
    } else {
      parameterAsNum = (Number) parameterObj;
    }

    switch (targetSqlType) {
      case Types.BIT:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        setInt(parameterIndex, parameterAsNum.intValue());
        break;

      case Types.BIGINT:
        setLong(parameterIndex, parameterAsNum.longValue());
        break;

      case Types.REAL:
        setFloat(parameterIndex, parameterAsNum.floatValue());
        break;

      case Types.FLOAT:
        setFloat(parameterIndex, parameterAsNum.floatValue());
        break;
      case Types.DOUBLE:
        setDouble(parameterIndex, parameterAsNum.doubleValue());

        break;

      case Types.DECIMAL:
      case Types.NUMERIC:
        if (parameterAsNum instanceof java.math.BigDecimal) {
          BigDecimal scaledBigDecimal = null;

          try {
            scaledBigDecimal = ((java.math.BigDecimal) parameterAsNum).setScale(scale);
          } catch (ArithmeticException ex) {
            try {
              scaledBigDecimal =
                  ((java.math.BigDecimal) parameterAsNum).setScale(scale, BigDecimal.ROUND_HALF_UP);
            } catch (ArithmeticException arEx) {
              throw new SQLException(
                  "Can't set scale of '"
                      + scale
                      + "' for DECIMAL argument '"
                      + parameterAsNum
                      + "'");
            }
          }

          setBigDecimal(parameterIndex, scaledBigDecimal);
        } else if (parameterAsNum instanceof java.math.BigInteger) {
          setBigDecimal(
              parameterIndex,
              new java.math.BigDecimal((java.math.BigInteger) parameterAsNum, scale));
        } else {
          setBigDecimal(parameterIndex, BigDecimal.valueOf(parameterAsNum.doubleValue()));
        }

        break;
    }
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLException(METHOD_NOT_SUPPORTED_STRING);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setString(int parameterIndex, String x) {
    this.parameters.put(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    try {
      long time = x.getTime();
      String timeprecision = client.getProperties().getTimestampPrecision();
      switch (timeprecision.toLowerCase()) {
        case "ms":
          break;
        case "us":
          time = time * 1000;
          break;
        case "ns":
          time = time * 1000000;
          break;
        default:
          break;
      }
      setLong(parameterIndex, time);
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    try {
      ZonedDateTime zonedDateTime = null;
      long time = x.getTime();
      String timeprecision = client.getProperties().getTimestampPrecision();
      switch (timeprecision.toLowerCase()) {
        case "ms":
          break;
        case "us":
          time = time * 1000;
          break;
        case "ns":
          time = time * 1000000;
          break;
        default:
          break;
      }
      if (cal != null) {
        zonedDateTime =
            ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(time), ZoneId.of(cal.getTimeZone().getID()));
      } else {
        zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), super.zoneId);
      }
      this.parameters.put(
          parameterIndex, zonedDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) {
    ZonedDateTime zonedDateTime =
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(x.getTime()), super.zoneId);
    this.parameters.put(
        parameterIndex, zonedDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    ZonedDateTime zonedDateTime = null;
    if (cal != null) {
      zonedDateTime =
          ZonedDateTime.ofInstant(
              Instant.ofEpochMilli(x.getTime()), ZoneId.of(cal.getTimeZone().getID()));
    } else {
      zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(x.getTime()), super.zoneId);
    }
    this.parameters.put(
        parameterIndex, zonedDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SQLException(Constant.PARAMETER_SUPPORTED);
  }

  private String createCompleteSql(final String sql, Map<Integer, String> parameters)
      throws SQLException {
    List<String> parts = splitSqlStatement(sql);

    StringBuilder newSql = new StringBuilder(parts.get(0));
    for (int i = 1; i < parts.size(); i++) {
      if (logger.isDebugEnabled()) {
        logger.debug("SQL {}", sql);
        logger.debug("parameters {}", parameters.size());
      }
      if (!parameters.containsKey(i)) {
        throw new SQLException("Parameter #" + i + " is unset");
      }
      newSql.append(parameters.get(i));
      newSql.append(parts.get(i));
    }
    return newSql.toString();
  }

  private List<String> splitSqlStatement(final String sql) {
    List<String> parts = new ArrayList<>();
    int apCount = 0;
    int off = 0;
    boolean skip = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (skip) {
        skip = false;
        continue;
      }
      switch (c) {
        case '\'':
          // skip something like 'xxxxx'
          apCount++;
          break;
        case '\\':
          // skip something like \r\n
          skip = true;
          break;
        case '?':
          // for input like: select a from 'bc' where d, 'bc' will be skipped
          if ((apCount & 1) == 0) {
            parts.add(sql.substring(off, i));
            off = i + 1;
          }
          break;
        default:
          break;
      }
    }
    parts.add(sql.substring(off));
    return parts;
  }
}
