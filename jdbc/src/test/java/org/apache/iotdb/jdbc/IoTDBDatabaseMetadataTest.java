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
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSExecuteBatchStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class IoTDBDatabaseMetadataTest {
  @Mock TSExecuteStatementResp execStatementResp;
  private long queryId;
  private long sessionId;
  private TSStatus resp;
  @Mock private IoTDBConnection connection;
  @Mock private TSIService.Iface client;
  @Mock private Statement statement;
  @Mock private DatabaseMetaData databaseMetaData;
  @Mock private TSStatus successStatus = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  @Mock private ServerProperties properties;

  private ZoneId zoneID = ZoneId.systemDefault();

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(connection.createStatement())
        .thenReturn(new IoTDBStatement(connection, client, sessionId, zoneID, 0, 1L));
    databaseMetaData = new IoTDBDatabaseMetadata(connection, client, sessionId);
    when(client.executeStatement(any(TSExecuteStatementReq.class))).thenReturn(execStatementResp);
    when(client.getProperties()).thenReturn(properties);
    when(execStatementResp.getStatus()).thenReturn(successStatus);
    when(execStatementResp.getQueryId()).thenReturn(queryId);
  }

  @Test
  public void testGetAttributes() throws SQLException {
    ResultSet resultSet = databaseMetaData.getExportedKeys(null, null, null);
    Assert.assertEquals("Time", resultSet.getMetaData().getColumnName(1));
    Assert.assertEquals("PKTABLE_CAT", resultSet.getMetaData().getColumnName(2));
  }

  @Test
  public void testGetBestRowIdentifier() throws SQLException {
    ResultSet resultSet = databaseMetaData.getBestRowIdentifier(null, null, null, 0, true);
    Assert.assertEquals("Time", resultSet.getMetaData().getColumnName(1));
    Assert.assertEquals("SCOPE", resultSet.getMetaData().getColumnName(2));
    Assert.assertEquals("COLUMN_NAME", resultSet.getMetaData().getColumnName(3));
    Assert.assertEquals("DATA_TYPE", resultSet.getMetaData().getColumnName(4));
    Assert.assertEquals("TYPE_NAME", resultSet.getMetaData().getColumnName(5));
    Assert.assertEquals("COLUMN_SIZE", resultSet.getMetaData().getColumnName(6));
    Assert.assertEquals("BUFFER_LENGTH", resultSet.getMetaData().getColumnName(7));
    Assert.assertEquals("DECIMAL_DIGITS", resultSet.getMetaData().getColumnName(8));
    Assert.assertEquals("PSEUDO_COLUMN", resultSet.getMetaData().getColumnName(9));
  }

  @Test
  public void testGetCatalogs() throws SQLException, TException {
    Statement statement = connection.createStatement();
    resp = new TSStatus();
    resp =
        RpcUtils.getStatus(
            Collections.singletonList(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS)));
    when(client.executeBatchStatement(any(TSExecuteBatchStatementReq.class))).thenReturn(resp);
    List<TSStatus> resExpected =
        new ArrayList<TSStatus>() {
          {
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
            add(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
          }
        };
    resp.setSubStatus(resExpected);

    statement.clearBatch();
    statement.addBatch("SET STORAGE GROUP TO root.ln.wf01.wt01");
    statement.addBatch(
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
    int[] result = statement.executeBatch();
    assertEquals(resp.getSubStatus().size(), result.length);
    for (int i = 0; i < resp.getSubStatus().size(); i++) {
      assertEquals(resExpected.get(i).code, result[i]);
    }
    List<String> dataTypeList = new ArrayList<String>();
    dataTypeList.add("TEXT");
    List<String> columnsList = new ArrayList<String>();
    columnsList.add("storage group");
    Map<String, Integer> columnNameIndexMap = new HashMap<String, Integer>();
    columnNameIndexMap.put("storage group", 0);
    when(client.executeQueryStatement(any(TSExecuteStatementReq.class)))
        .thenReturn(execStatementResp);
    when(execStatementResp.getStatus()).thenReturn(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
    when(execStatementResp.getQueryId()).thenReturn(queryId);
    when(execStatementResp.getDataTypeList()).thenReturn(dataTypeList);
    when(execStatementResp.getColumns()).thenReturn(columnsList);
    execStatementResp.columnNameIndexMap = columnNameIndexMap;
    when(client.getProperties().getWatermarkSecretKey()).thenReturn("IoTDB*2019@Beijing");
    when(client.getProperties().getWatermarkBitString()).thenReturn("100101110100");
    when(client.getProperties().getWatermarkParamMarkRate()).thenReturn(5);
    when(client.getProperties().getWatermarkParamMaxRightBit()).thenReturn(5);
    ResultSet rs = databaseMetaData.getCatalogs();
    assertEquals(2, rs.findColumn("TYPE_CAT"));
  }

  @Test
  public void testGetImportedKeys() throws SQLException {
    ResultSet resultSet = databaseMetaData.getImportedKeys(null, null, null);
    Assert.assertEquals("Time", resultSet.getMetaData().getColumnName(1));
    Assert.assertEquals("PKTABLE_CAT", resultSet.getMetaData().getColumnName(2));
    Assert.assertEquals("PKTABLE_SCHEM", resultSet.getMetaData().getColumnName(3));
  }

  @Test
  public void testGetIndexInfo() throws SQLException {
    ResultSet resultSet = databaseMetaData.getIndexInfo(null, null, null, false, false);
    Assert.assertEquals("Time", resultSet.getMetaData().getColumnName(1));
    Assert.assertEquals("TABLE_CAT", resultSet.getMetaData().getColumnName(2));
    Assert.assertEquals("TABLE_SCHEM", resultSet.getMetaData().getColumnName(3));
  }
}
