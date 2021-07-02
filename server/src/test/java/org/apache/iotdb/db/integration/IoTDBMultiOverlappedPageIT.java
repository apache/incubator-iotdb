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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.first_value;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBMultiOverlappedPageIT {

  private static int beforeMaxNumberOfPointsInPage;
  private static long beforeMemtableSizeThreshold;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    beforeMemtableSizeThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1024 * 16);
    // max_number_of_points_in_page = 10
    beforeMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // recovery value
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxNumberOfPointsInPage(beforeMaxNumberOfPointsInPage);
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(beforeMemtableSizeThreshold);
  }

  @Test
  public void selectOverlappedPageTest() {
    String[] res = {
      "11,111", "12,112", "13,113", "14,114", "15,215", "16,216", "17,217", "18,218", "19,219",
      "20,220", "21,221", "22,222", "23,223", "24,224"
    };

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String sql =
          "select s0 from root.vehicle.d0 where time >= 1 and time <= 50 AND root.vehicle.d0.s0 >= 111";
      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString("root.vehicle.d0.s0");
          assertEquals(res[cnt], ans);
          cnt++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectOverlappedPageTest2() {

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String sql = "select first_value(s0) from root.vehicle.d0 where time > 18";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          String ans = resultSet.getString(first_value("root.vehicle.d0.s0"));
          assertEquals("219", ans);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void insertData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");

      for (long time = 1; time <= 10; time++) {
        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
        statement.execute(sql);
      }
      for (long time = 11; time <= 20; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 100 + time);
        statement.execute(sql);
      }

      for (long time = 21; time <= 30; time++) {
        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
        statement.execute(sql);
      }
      statement.execute("flush");
      for (long time = 101; time <= 110; time++) {
        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
        statement.execute(sql);
      }
      statement.execute("flush");
      for (long time = 1; time <= 10; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time + 100);
        statement.execute(sql);
      }
      for (long time = 15; time <= 24; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time + 200);
        statement.execute(sql);
      }
      statement.execute("flush");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
