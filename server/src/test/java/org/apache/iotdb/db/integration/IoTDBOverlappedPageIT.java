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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBOverlappedPageIT {

  private static int beforeMaxNumberOfPointsInPage;

  private static String[] dataSet1 =
      new String[] {
        "SET STORAGE GROUP TO root.sg1",
        "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(time,s1) values(1, 1)",
        "INSERT INTO root.sg1.d1(time,s1) values(10, 10)",
        "flush",
        "INSERT INTO root.sg1.d1(time,s1) values(20, 20)",
        "INSERT INTO root.sg1.d1(time,s1) values(30, 30)",
        "flush",
        "INSERT INTO root.sg1.d1(time,s1) values(110, 110)",
        "flush",
        "INSERT INTO root.sg1.d1(time,s1) values(5, 5)",
        "INSERT INTO root.sg1.d1(time,s1) values(50, 50)",
        "INSERT INTO root.sg1.d1(time,s1) values(100, 100)",
        "flush",
        "INSERT INTO root.sg1.d1(time,s1) values(15, 15)",
        "INSERT INTO root.sg1.d1(time,s1) values(25, 25)",
        "flush",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    EnvironmentUtils.closeStatMonitor();

    // max_number_of_points_in_page = 10
    beforeMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(10);
    EnvironmentUtils.envSetUp();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // recovery value
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMemtableSizeThreshold(beforeMaxNumberOfPointsInPage);
  }

  @Test
  public void selectOverlappedPageTest() {
    String[] res = {
      "11,111", "12,112", "13,113", "14,114", "15,115", "16,116", "17,117", "18,118", "19,119",
      "20,120",
    };

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      insertData();

      String sql =
          "select s0 from root.vehicle.d0 where time >= 1 and time <= 110 AND root.vehicle.d0.s0 > 110";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString("root.vehicle.d0.s0");
          Assert.assertEquals(res[cnt], ans);
          cnt++;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test to check if timeValuePair is updated when there are unSeqPageReaders are unpacked in
   * method hasNextOverlappedPage() in SeriesReader.java
   */
  @Test
  public void selectOverlappedPageTest2() {
    String[] res = {"0,10"};

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String insertSql : dataSet1) {
        statement.execute(insertSql);
      }

      boolean hasResultSet = statement.execute("select count(s1) from root.sg1.d1");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString("count(root.sg1.d1.s1)");
          Assert.assertEquals(res[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(res.length, cnt);
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
      statement.execute("flush");

      for (long time = 11; time <= 20; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time + 100);
        statement.execute(sql);
      }
      for (long time = 100; time <= 120; time++) {
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
      for (long time = 11; time <= 20; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time + 100);
        statement.execute(sql);
      }
      statement.execute("flush");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
