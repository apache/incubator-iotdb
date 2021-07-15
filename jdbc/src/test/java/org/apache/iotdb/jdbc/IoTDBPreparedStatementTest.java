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

import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.*;
import org.apache.iotdb.service.rpc.thrift.TSIService.Iface;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IoTDBPreparedStatementTest {

  @Mock TSExecuteStatementResp execStatementResp;
  @Mock TSStatus getOperationStatusResp;
  private ZoneId zoneId = ZoneId.systemDefault();
  @Mock private IoTDBConnection connection;
  @Mock private Iface client;
  @Mock private TSStatus successStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  private TSStatus Status_SUCCESS = new TSStatus(successStatus);
  private long queryId;
  private long sessionId;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(execStatementResp.getStatus()).thenReturn(Status_SUCCESS);
    when(execStatementResp.getQueryId()).thenReturn(queryId);

    when(client.executeStatement(any(TSExecuteStatementReq.class))).thenReturn(execStatementResp);
  }

  @SuppressWarnings("resource")
  @Test
  public void testNonParameterized() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void unusedArgument() throws SQLException {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "123");
    ps.execute();
  }

  @SuppressWarnings("resource")
  @Test(expected = SQLException.class)
  public void unsetArgument() throws SQLException {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.execute();
  }

  @SuppressWarnings("resource")
  @Test
  public void oneIntArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setInt(1, 123);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneLongArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 123);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneFloatArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setFloat(1, 123.133f);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123.133 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneDoubleArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setDouble(1, 123.456);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 123.456 and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneBooleanArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setBoolean(1, false);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < false and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneStringArgument1() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "'abcde'");
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 'abcde' and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneStringArgument2() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < ? and time > 2017-11-1 0:13:00";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "\"abcde\"");
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < \"abcde\" and time > 2017-11-1 0:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneStringArgument3() throws Exception {
    String sql = "SELECT status, ? FROM root.ln.wf01.wt01";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setString(1, "temperature");
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01", argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneTimeLongArgument() throws Exception {
    String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 1233);
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > 1233",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void oneTimeTimestampArgument() throws Exception {
    String sql = "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:13:00"));
    ps.execute();
    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE time > 2017-11-01T00:13:00",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void escapingOfStringArgument() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '134' and temperature = ?";
    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 1333);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '134' and temperature = 1333",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void pastingIntoEscapedQuery() throws Exception {
    String sql =
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '\\044e' || temperature = ?";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setDouble(1, -1323.0);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "SELECT status, temperature FROM root.ln.wf01.wt01 WHERE status = '\\044e' || temperature = -1323.0",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void testInsertStatement1() throws Exception {
    String sql = "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f) VALUES(?,?,?,?,?,?,?)";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setLong(1, 12324);
    ps.setBoolean(2, false);
    ps.setInt(3, 123);
    ps.setLong(4, 123234345);
    ps.setFloat(5, 123.423f);
    ps.setDouble(6, -1323.0);
    ps.setString(7, "'abc'");
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f) VALUES(12324,false,123,123234345,123.423,-1323.0,'abc')",
        argument.getValue().getStatement());
  }

  @SuppressWarnings("resource")
  @Test
  public void testInsertStatement2() throws Exception {
    String sql = "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f) VALUES(?,?,?,?,?,?,?)";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setTimestamp(1, Timestamp.valueOf("2017-11-01 00:13:00"));
    ps.setBoolean(2, false);
    ps.setInt(3, 123);
    ps.setLong(4, 123234345);
    ps.setFloat(5, 123.423f);
    ps.setDouble(6, -1323.0);
    ps.setString(7, "\"abc\"");
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "INSERT INTO root.ln.wf01.wt01(time,a,b,c,d,e,f) VALUES(2017-11-01T00:13:00,false,123,123234345,123.423,-1323.0,\"abc\")",
        argument.getValue().getStatement());
  }

  @Test
  public void testInsertStatement3() throws Exception {
    String sql = "INSERT INTO root.ln.wf01.wt02(time,a,b,c,d,e,f) VALUES(?,?,?,?,?,?,?)";

    IoTDBPreparedStatement ps =
        new IoTDBPreparedStatement(connection, client, sessionId, sql, zoneId);
    ps.setObject(1, "2020-01-01 10:10:10", Types.TIMESTAMP, -1);
    ps.setObject(2, false, Types.BOOLEAN, -1);
    ps.setObject(3, 123, Types.INTEGER, -1);
    ps.setObject(4, 123234345, Types.BIGINT);
    ps.setObject(5, 123.423f, Types.FLOAT);
    ps.setObject(6, -1323.0, Types.DOUBLE);
    ps.setObject(7, "\"abc\"", Types.VARCHAR);
    ps.execute();

    ArgumentCaptor<TSExecuteStatementReq> argument =
        ArgumentCaptor.forClass(TSExecuteStatementReq.class);
    verify(client).executeStatement(argument.capture());
    assertEquals(
        "INSERT INTO root.ln.wf01.wt02(time,a,b,c,d,e,f) VALUES(2020-01-01T10:10:10,false,123,123234345,123.423,-1323.0,\"abc\")",
        argument.getValue().getStatement());
  }
}
