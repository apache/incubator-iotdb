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

package org.apache.iotdb.db.integration.aggregation;

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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import static org.apache.iotdb.db.constant.TestConstant.avg;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.apache.iotdb.db.constant.TestConstant.extreme;
import static org.apache.iotdb.db.constant.TestConstant.first_value;
import static org.apache.iotdb.db.constant.TestConstant.last_value;
import static org.apache.iotdb.db.constant.TestConstant.max_time;
import static org.apache.iotdb.db.constant.TestConstant.max_value;
import static org.apache.iotdb.db.constant.TestConstant.min_time;
import static org.apache.iotdb.db.constant.TestConstant.min_value;
import static org.apache.iotdb.db.constant.TestConstant.sum;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class IoTDBAggregationIT {

  private static final double DETLA = 1e-6;
  private static final String TIMESTAMP_STR = "Time";
  private static final String TEMPERATURE_STR = "root.ln.wf01.wt01.temperature";

  private static String[] creationSqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle.d0",
        "SET STORAGE GROUP TO root.vehicle.d1",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"
      };
  private static String[] dataSet2 =
      new String[] {
        "SET STORAGE GROUP TO root.ln.wf01.wt01",
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.hardware WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(1, 1.1, false, 11)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(2, 2.2, true, 22)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(3, 3.3, false, 33 )",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(4, 4.4, false, 44)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, hardware) "
            + "values(5, 5.5, false, 55)"
      };
  private static String[] dataSet3 =
      new String[] {
        "SET STORAGE GROUP TO root.sg",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=RLE",
        "insert into root.sg.d1(timestamp,s1) values(5,5)",
        "insert into root.sg.d1(timestamp,s1) values(12,12)",
        "flush",
        "insert into root.sg.d1(timestamp,s1) values(15,15)",
        "insert into root.sg.d1(timestamp,s1) values(25,25)",
        "flush",
        "insert into root.sg.d1(timestamp,s1) values(1,111)",
        "insert into root.sg.d1(timestamp,s1) values(20,200)",
        "flush"
      };
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";
  private String insertTemplate =
      "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)" + " VALUES(%d,%d,%d,%f,%s,%s)";
  private long prevPartitionInterval;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(1000);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
  }

  // add test for part of points in page don't satisfy filter
  // details in: https://issues.apache.org/jira/projects/IOTDB/issues/IOTDB-54
  @Test
  public void test() {
    String[] retArray = new String[] {"0,2", "0,4", "0,3"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("SELECT count(temperature) FROM root.ln.wf01.wt01 WHERE time > 3");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT count(temperature) FROM root.ln.wf01.wt01 WHERE time > 3 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute("SELECT min_time(temperature) FROM root.ln.wf01.wt01 WHERE time > 3");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_time(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT min_time(temperature) FROM root.ln.wf01.wt01 WHERE temperature > 3");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_time(TEMPERATURE_STR));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void countTest() {
    String[] retArray = new String[] {"0,2001,2001,2001,2001", "0,7500,7500,7500,7500"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT count(s0),count(s1),count(s2),count(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT count(s0),count(s1),count(s2),count(s3) " + "FROM root.vehicle.d0");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "SELECT count(s0),count(s1),count(s2),count(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT count(s0),count(s1),count(s2),count(s3) "
                  + "FROM root.vehicle.d0 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d0s1))
                  + ","
                  + resultSet.getString(count(d0s2))
                  + ","
                  + resultSet.getString(count(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void firstTest() {
    String[] retArray = new String[] {"0,2000,2000,2000.0,2000", "0,500,500,500.0,500"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT first_value(s0),first_value(s1),first_value(s2),first_value(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 1500 AND time <= 9000");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(first_value(d0s0))
                  + ","
                  + resultSet.getString(first_value(d0s1))
                  + ","
                  + resultSet.getString(first_value(d0s2))
                  + ","
                  + resultSet.getString(first_value(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT first_value(s0),first_value(s1),first_value(s2),first_value(s3) "
                  + "FROM root.vehicle.d0");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(first_value(d0s0))
                  + ","
                  + resultSet.getString(first_value(d0s1))
                  + ","
                  + resultSet.getString(first_value(d0s2))
                  + ","
                  + resultSet.getString(first_value(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "SELECT first_value(s0),first_value(s1),first_value(s2),first_value(s3) "
                  + "FROM root.vehicle.d0 WHERE time >= 1500 AND time <= 9000 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(first_value(d0s0))
                  + ","
                  + resultSet.getString(first_value(d0s1))
                  + ","
                  + resultSet.getString(first_value(d0s2))
                  + ","
                  + resultSet.getString(first_value(d0s3));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void lastTest() {
    String[] retArray = new String[] {"0,8499,8499.0", "0,1499,1499.0", "0,2200,2200.0"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT last_value(s0),last_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 1500 AND time < 9000");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT last_value(s0),last_value(s2) " + "FROM root.vehicle.d0 WHERE time <= 1600");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT last_value(s0),last_value(s2) " + "FROM root.vehicle.d0 WHERE time <= 2200");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT last_value(s0),last_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time <= 2200 order by time desc");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d0s2));
          Assert.assertEquals(retArray[retArray.length - cnt - 1], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "SELECT last_value(s0),last_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 1500 AND time < 9000 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void maxminTimeTest() {
    String[] retArray = new String[] {"0,8499,500", "0,2499,2000"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT max_time(s0),min_time(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_time(d0s0))
                  + ","
                  + resultSet.getString(min_time(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT max_time(s0),min_time(s2) "
                  + "FROM root.vehicle.d0 WHERE time <= 2500 AND time > 1800");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_time(d0s0))
                  + ","
                  + resultSet.getString(min_time(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "SELECT max_time(s0),min_time(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_time(d0s0))
                  + ","
                  + resultSet.getString(min_time(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void firstLastValueTest() throws SQLException {
    String[] retArray =
        new String[] {
          "0,2.2,4.4",
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT first_value(temperature),last_value(temperature) "
                  + "FROM root.ln.wf01.wt01 WHERE time > 1 AND time < 5");
      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(1)
                  + ","
                  + resultSet.getString(2);
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    }
  }

  @Test
  public void maxminValueTest() {
    String[] retArray = new String[] {"0,8499,500.0", "0,2499,500.0"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT max_value(s0),min_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_value(d0s0))
                  + ","
                  + resultSet.getString(min_value(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT max_value(s0),min_value(s2) " + "FROM root.vehicle.d0 WHERE time < 2500");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_value(d0s0))
                  + ","
                  + resultSet.getString(min_value(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "SELECT max_value(s0),min_value(s2) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000 order by time desc");

      Assert.assertTrue(hasResultSet);
      cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_value(d0s0))
                  + ","
                  + resultSet.getString(min_value(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void extremeTest() {
    String[] retArray = new String[] {"0,8499", "0,2499"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT extreme(s0) FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000");

      Assert.assertTrue(hasResultSet);
      int cnt;
      try (ResultSet resultSet = statement.getResultSet()) {
        cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(extreme(d0s0));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet = statement.execute("SELECT extreme(s0) FROM root.vehicle.d0 WHERE time < 2500");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(extreme(d0s0));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "SELECT extreme(s0) "
                  + "FROM root.vehicle.d0 WHERE time >= 100 AND time < 9000 order by time desc");

      Assert.assertTrue(hasResultSet);
      cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(extreme(d0s0));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void avgSumTest() {
    double[][] retArray = {
      {0.0, 1.4508E7, 7250.374812593702},
      {0.0, 626750.0, 1250.9980039920158}
    };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT sum(s0),avg(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          double[] ans = new double[3];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(sum(d0s0)));
          ans[2] = Double.valueOf(resultSet.getString(avg(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT sum(s0),avg(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 1000 AND time <= 2000");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          double[] ans = new double[3];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(sum(d0s0)));
          ans[2] = Double.valueOf(resultSet.getString(avg(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "SELECT sum(s0),avg(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000 order by time desc");

      Assert.assertTrue(hasResultSet);
      cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          double[] ans = new double[3];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(sum(d0s0)));
          ans[2] = Double.valueOf(resultSet.getString(avg(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void avgSumErrorTest() {
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "SELECT avg(s3)" + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");
        try (ResultSet resultSet = statement.getResultSet()) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("Unsupported data type in aggregation AVG : TEXT"));
      }
      try {
        statement.execute(
            "SELECT sum(s3)" + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");
        try (ResultSet resultSet = statement.getResultSet()) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("Unsupported data type in aggregation SUM : TEXT"));
      }
      try {
        statement.execute(
            "SELECT avg(s4)" + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");
        try (ResultSet resultSet = statement.getResultSet()) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("Unsupported data type in aggregation AVG : BOOLEAN"));
      }
      try {
        statement.execute(
            "SELECT sum(s4)" + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");
        try (ResultSet resultSet = statement.getResultSet()) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(
            e.getMessage().contains("Unsupported data type in aggregation SUM : BOOLEAN"));
      }
      try {
        statement.execute("SELECT avg(status) FROM root.ln.wf01.wt01");
        try (ResultSet resultSet = statement.getResultSet()) {
          resultSet.next();
          fail();
        }
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("Boolean statistics does not support: avg"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /** test aggregation query with more than one functions on one series */
  @Test
  public void mergeAggrOnOneSeriesTest() {
    double[][] retArray = {
      {0.0, 1.4508E7, 7250.374812593702, 7250.374812593702, 1.4508E7},
      {0.0, 626750.0, 1250.9980039920158, 1250.9980039920158, 626750.0},
      {0.0, 1.4508E7, 2001, 7250.374812593702, 7250.374812593702},
      {0.0, 1.4508E7, 2001, 7250.374812593702, 7250.374812593702, 2001, 1.4508E7}
    };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT sum(s0), avg(s2), avg(s0), sum(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          double[] ans = new double[5];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(sum(d0s0)));
          ans[2] = Double.valueOf(resultSet.getString(avg(d0s2)));
          ans[3] = Double.valueOf(resultSet.getString(avg(d0s0)));
          ans[4] = Double.valueOf(resultSet.getString(sum(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT sum(s0), avg(s2), avg(s0), sum(s2)"
                  + "FROM root.vehicle.d0 WHERE time >= 1000 AND time <= 2000");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          double[] ans = new double[5];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(sum(d0s0)));
          ans[2] = Double.valueOf(resultSet.getString(avg(d0s2)));
          ans[3] = Double.valueOf(resultSet.getString(avg(d0s0)));
          ans[4] = Double.valueOf(resultSet.getString(sum(d0s2)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(2, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT sum(s0), count(s0), avg(s2), avg(s0)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          double[] ans = new double[5];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(sum(d0s0)));
          ans[2] = Double.valueOf(resultSet.getString(count(d0s0)));
          ans[3] = Double.valueOf(resultSet.getString(avg(d0s2)));
          ans[4] = Double.valueOf(resultSet.getString(avg(d0s0)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(3, cnt);
      }

      hasResultSet =
          statement.execute(
              "SELECT sum(s2), count(s0), avg(s2), avg(s1), count(s2),sum(s0)"
                  + "FROM root.vehicle.d0 WHERE time >= 6000 AND time <= 9000");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          double[] ans = new double[7];
          ans[0] = Double.valueOf(resultSet.getString(TIMESTAMP_STR));
          ans[1] = Double.valueOf(resultSet.getString(sum(d0s2)));
          ans[2] = Double.valueOf(resultSet.getString(count(d0s0)));
          ans[3] = Double.valueOf(resultSet.getString(avg(d0s2)));
          ans[4] = Double.valueOf(resultSet.getString(avg(d0s1)));
          ans[5] = Double.valueOf(resultSet.getString(count(d0s2)));
          ans[6] = Double.valueOf(resultSet.getString(sum(d0s0)));
          assertArrayEquals(retArray[cnt], ans, DETLA);
          cnt++;
        }
        Assert.assertEquals(4, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void descAggregationWithUnseqData() {
    String[] retArray =
        new String[] {
          "0,12",
        };
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("SELECT max_time(s1) FROM root.sg.d1 where time < 15");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_time("root.sg.d1.s1"));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : creationSqls) {
        statement.execute(sql);
      }

      // prepare BufferWrite file
      for (int i = 5000; i < 7000; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.execute("FLUSH");
      for (int i = 7500; i < 8500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.execute("FLUSH");
      // prepare Unseq-File
      for (int i = 500; i < 1500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      statement.execute("FLUSH");
      for (int i = 3000; i < 6500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }
      statement.execute("merge");

      // prepare BufferWrite cache
      for (int i = 9000; i < 10000; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
      }
      // prepare Overflow cache
      for (int i = 2000; i < 2500; i++) {
        statement.execute(
            String.format(
                Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
      }

      for (String sql : dataSet3) {
        statement.execute(sql);
      }

      for (String sql : dataSet2) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
