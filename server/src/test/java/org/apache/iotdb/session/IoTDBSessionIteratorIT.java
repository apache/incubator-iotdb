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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBSessionIteratorIT {

  private Session session;

  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test1() {
    String[] retArray = new String[]{
        "0,1,2.0,null",
        "1,1,2.0,null",
        "2,1,2.0,null",
        "3,1,2.0,null",
        "4,1,2.0,null",
        "5,1,2.0,4.0",
        "6,1,2.0,4.0",
        "7,1,2.0,4.0",
        "8,1,2.0,4.0",
        "9,1,2.0,4.0",
    };

    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement("select * from root.sg1");
      sessionDataSet.setFetchSize(1024);
      DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        String ans = String.format("%s,%s,%s,%s", iterator.getLong(1), iterator.getInt("root.sg1.d1.s1"),
            iterator.getFloat(3), iterator.getString("root.sg1.d2.s1"));
        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void test2() {
    String[] retArray = new String[]{
        "9,root.sg1.d1.s1,1",
        "9,root.sg1.d1.s2,2.0",
        "9,root.sg1.d2.s1,4.0"
    };

    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement("select last * from root.sg1");
      sessionDataSet.setFetchSize(1024);
      DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        String ans = String.format("%s,%s,%s", iterator.getLong(1), iterator.getString(2),
            iterator.getString(3));
        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void test3() {
    String[] retArray = new String[]{
        "root.sg1.d1",
        "root.sg1.d2"
    };

    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement("show devices");
      sessionDataSet.setFetchSize(1024);
      assertEquals(1, sessionDataSet.getColumnNames().size());
      DataIterator iterator = sessionDataSet.iterator();
      int count = 0;
      while (iterator.next()) {
        String ans = iterator.getString(1);
        assertEquals(retArray[count], ans);
        count++;
      }
      assertEquals(retArray.length, count);
      sessionDataSet.closeOperationHandle();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private void prepareData() throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    session.setStorageGroup("root.sg1");
    session.createTimeseries("root.sg1.d1.s1", TSDataType.INT32, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d1.s2", TSDataType.FLOAT, TSEncoding.RLE,
        CompressionType.SNAPPY);
    session.createTimeseries("root.sg1.d2.s1", TSDataType.DOUBLE, TSEncoding.RLE,
        CompressionType.SNAPPY);
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    types.add(TSDataType.INT32);
    types.add(TSDataType.FLOAT);

    for (long time = 0; time < 10; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1);
      values.add(2f);
      session.insertRecord(deviceId, time, measurements, types, values);
    }

    deviceId = "root.sg1.d2";
    measurements = new ArrayList<>();
    types = new ArrayList<>();
    measurements.add("s1");
    types.add(TSDataType.DOUBLE);

    for (long time = 5; time < 10; time++) {
      List<Object> values = new ArrayList<>();
      values.add(4d);
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }

}
