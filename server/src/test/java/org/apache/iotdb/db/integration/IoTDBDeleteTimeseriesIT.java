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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.junit.Assert.fail;

public class IoTDBDeleteTimeseriesIT {

  private long memtableSizeThreshold;

  @Before
  public void setUp() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    memtableSizeThreshold = IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(16);
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(memtableSizeThreshold);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void deleteTimeseriesAndCreateDifferentTypeTest() throws Exception {
    String[] retArray = new String[] {"1,1,", "2,1.1,"};
    int cnt = 0;

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) VALUES(1,1,2)");
      boolean hasResult = statement.execute("SELECT s1 FROM root.turbine1.d1");
      Assert.assertTrue(hasResult);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
      statement.execute("DELETE timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=DOUBLE, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) VALUES(2,1.1)");
      statement.execute("FLUSH");

      hasResult = statement.execute("SELECT s1 FROM root.turbine1.d1");
      Assert.assertTrue(hasResult);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
    }

    EnvironmentUtils.restartDaemon();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResult = statement.execute("SELECT * FROM root");
      Assert.assertTrue(hasResult);
    }
  }

  @Test
  public void deleteTimeseriesAndCreateSameTypeTest() throws Exception {
    String[] retArray = new String[] {"1,1,", "2,5,"};
    int cnt = 0;

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute(
          "create timeseries root.turbine1.d1.s2 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1,s2) VALUES(1,1,2)");
      boolean hasResult = statement.execute("SELECT s1 FROM root.turbine1.d1");
      Assert.assertTrue(hasResult);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
      statement.execute("DELETE timeseries root.turbine1.d1.s1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=INT64, encoding=PLAIN, compression=SNAPPY");
      statement.execute("INSERT INTO root.turbine1.d1(timestamp,s1) VALUES(2,5)");
      statement.execute("FLUSH");

      hasResult = statement.execute("SELECT s1 FROM root.turbine1.d1");
      Assert.assertTrue(hasResult);
      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        while (resultSet.next()) {
          StringBuilder builder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            builder.append(resultSet.getString(i)).append(",");
          }
          Assert.assertEquals(retArray[cnt], builder.toString());
          cnt++;
        }
      }
    }

    EnvironmentUtils.restartDaemon();

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResult = statement.execute("SELECT * FROM root");
      Assert.assertTrue(hasResult);
    }
  }

  @Test
  public void deleteTimeSeriesMultiIntervalTest() {
    String[] retArray1 = new String[] {"0,0"};

    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);
      String insertSql = "insert into root.sg.d1(time, s1) values(%d, %d)";
      for (int i = 1; i <= 4; i++) {
        statement.execute(String.format(insertSql, i, i));
      }
      statement.execute("flush");

      statement.execute("delete from root.sg.d1.s1 where time >= 1 and time <= 2");
      statement.execute("delete from root.sg.d1.s1 where time >= 3 and time <= 4");

      boolean hasResultSet =
          statement.execute("select count(s1) from root.sg.d1 where time >= 3 and time <= 4");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg.d1.s1"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }
}
