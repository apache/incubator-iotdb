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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.junit.Assert.fail;

public class IoTDBGroupByMonthIT {

  private static final String TIMESTAMP_STR = "Time";
  private final DateFormat df = new SimpleDateFormat("MM/dd/yyyy:HH:mm:ss");

  @Before
  public void setUp() throws Exception {
    df.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  /**
   * Test when interval = slidingStep = 1 month. StartTime: 2020-10-31 00:00:00, EndTime: 2021-03-01
   * 00:00:00
   */
  @Test
  public void groupByNaturalMonth1() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] retArray1 = {
        "10/31/2020:00:00:00", "30.0",
        "11/30/2020:00:00:00", "31.0",
        "12/31/2020:00:00:00", "31.0",
        "01/31/2021:00:00:00", "28.0",
        "02/28/2021:00:00:00", "1.0"
      };

      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 "
                  + "GROUP BY ([1604102400000, 1614556800000), 1mo, 1mo)");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String time = resultSet.getString(TIMESTAMP_STR);
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          Assert.assertEquals(retArray1[cnt++], df.format(Long.parseLong(time)));
          Assert.assertEquals(retArray1[cnt++], ans);
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test when interval = 10 days < slidingStep = 1 month. StartTime: 2020-10-31 00:00:00, EndTime:
   * 2021-03-01 00:00:00
   */
  @Test
  public void groupByNaturalMonth2() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] retArray1 = {
        "10/31/2020:00:00:00", "10.0",
        "11/30/2020:00:00:00", "10.0",
        "12/31/2020:00:00:00", "10.0",
        "01/31/2021:00:00:00", "10.0",
        "02/28/2021:00:00:00", "1.0"
      };

      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 "
                  + "GROUP BY ([1604102400000, 1614556800000), 10d, 1mo)");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String time = resultSet.getString(TIMESTAMP_STR);
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          Assert.assertEquals(retArray1[cnt++], df.format(Long.parseLong(time)));
          Assert.assertEquals(retArray1[cnt++], ans);
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test when endTime - startTime = interval StartTime: 2020-10-31 00:00:00, EndTime: 2020-11-30
   * 00:00:00
   */
  @Test
  public void groupByNaturalMonth3() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 "
                  + "GROUP BY ([1604102400000, 1606694400000), 1mo)");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(1, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * StartTime: 2021-01-31 00:00:00, EndTime: 2021-03-31 00:00:00. First Month with 28 days, Second
   * month with 31 days
   */
  @Test
  public void groupByNaturalMonth4() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] retArray1 = {
        "01/31/2021:00:00:00", "28.0",
        "02/28/2021:00:00:00", "31.0"
      };

      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 GROUP BY ([1612051200000, 1617148800000), 1mo)");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String time = resultSet.getString(TIMESTAMP_STR);
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          Assert.assertEquals(retArray1[cnt++], df.format(Long.parseLong(time)));
          Assert.assertEquals(retArray1[cnt++], ans);
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** Test group by month with order by time desc. */
  @Test
  public void groupByNaturalMonth5() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute(
          "select sum(temperature) from root.sg1.d1 "
              + "GROUP BY ([1612051200000, 1617148800000), 1mo) order by time desc");

      fail("No Exception thrown");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("doesn't support order by time desc now."));
    }
  }

  /** StartTime: now() - 1mo, EndTime: now(). */
  @Test
  public void groupByNaturalMonth6() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select sum(temperature) from root.sg1.d1 GROUP BY ([now() - 1mo, now()), 1d)");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans = resultSet.getString(sum("root.sg1.d1.temperature"));
          if (ans.equals("0.0")) {
            cnt++;
          }
        }
        Assert.assertTrue(cnt >= 28);
        Assert.assertTrue(cnt <= 31);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // 2020-10-31 08:00:00
      long startTime = 1604102400000L;
      // 2021-03-31 08:00:00
      long endTime = 1617148800000L;

      for (long i = startTime; i <= endTime; i += 86400_000L) {
        statement.execute("insert into root.sg1.d1(timestamp, temperature) values (" + i + ", 1)");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
