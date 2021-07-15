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
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class IoTDBConnectionTest {

  @Mock private TSIService.Iface client;

  private IoTDBConnection connection = new IoTDBConnection();
  private TSStatus successStatus = RpcUtils.SUCCESS_STATUS;
  private long sessionId;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() {}

  @Test
  public void testSetTimeZone() throws IoTDBSQLException, TException {
    String timeZone = "Asia/Shanghai";
    when(client.setTimeZone(any(TSSetTimeZoneReq.class))).thenReturn(new TSStatus(successStatus));
    connection.setClient(client);
    connection.setTimeZone(timeZone);
    assertEquals(connection.getTimeZone(), timeZone);
  }

  @Test
  public void testGetTimeZone() throws TException {
    String timeZone = ZoneId.systemDefault().toString();
    sessionId = connection.getSessionId();
    when(client.getTimeZone(sessionId)).thenReturn(new TSGetTimeZoneResp(successStatus, timeZone));
    connection.setClient(client);
    assertEquals(connection.getTimeZone(), timeZone);
  }

  @Test
  public void testGetServerProperties() throws TException {
    final String version = "v0.1";
    @SuppressWarnings("serial")
    final List<String> supportedAggregationTime =
        new ArrayList<String>() {
          {
            add("max_time");
            add("min_time");
          }
        };
    final String timestampPrecision = "ms";
    new ServerProperties();
    when(client.getProperties())
        .thenReturn(new ServerProperties(version, supportedAggregationTime, timestampPrecision, 1));
    connection.setClient(client);
    assertEquals(connection.getServerProperties().getVersion(), version);
    for (int i = 0; i < supportedAggregationTime.size(); i++) {
      assertEquals(
          connection.getServerProperties().getSupportedTimeAggregationOperations().get(i),
          supportedAggregationTime.get(i));
    }
    assertEquals(connection.getServerProperties().getTimestampPrecision(), timestampPrecision);
  }

  @Test
  public void setTimeoutTest() throws SQLException {
    connection.setQueryTimeout(60);
    Assert.assertEquals(60, connection.getQueryTimeout());
  }
}
