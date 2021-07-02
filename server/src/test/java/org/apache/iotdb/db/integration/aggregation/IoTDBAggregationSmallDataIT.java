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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

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
import static org.junit.Assert.fail;

/** Multiple aggregation with filter test. */
public class IoTDBAggregationSmallDataIT {

  private static final String TIMESTAMP_STR = "Time";
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";
  private final String d0s4 = "root.vehicle.d0.s4";
  private final String d1s0 = "root.vehicle.d1.s0";
  private final String d1s1 = "root.vehicle.d1.s1";

  private static String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "insert into root.vehicle.d0(timestamp,s0) values(1,101)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s0) values(100,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(101,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(102,80)",
        "insert into root.vehicle.d0(timestamp,s0) values(103,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(104,90)",
        "insert into root.vehicle.d0(timestamp,s0) values(105,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,99)",
        "insert into root.vehicle.d0(timestamp,s0) values(2,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(50,10000)",
        "insert into root.vehicle.d0(timestamp,s0) values(1000,22222)",
        "insert into root.vehicle.d0(timestamp,s0) values(106,199)",
        "DELETE FROM root.vehicle.d0.s0 WHERE time < 104",
        "insert into root.vehicle.d0(timestamp,s1) values(1,1101)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,198)",
        "insert into root.vehicle.d0(timestamp,s1) values(100,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(101,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(102,180)",
        "insert into root.vehicle.d0(timestamp,s1) values(103,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(104,190)",
        "insert into root.vehicle.d0(timestamp,s1) values(105,199)",
        "insert into root.vehicle.d0(timestamp,s1) values(2,40000)",
        "insert into root.vehicle.d0(timestamp,s1) values(50,50000)",
        "insert into root.vehicle.d0(timestamp,s1) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,55555)",
        "insert into root.vehicle.d0(timestamp,s2) values(2,2.22)",
        "insert into root.vehicle.d0(timestamp,s2) values(3,3.33)",
        "insert into root.vehicle.d0(timestamp,s2) values(4,4.44)",
        "insert into root.vehicle.d0(timestamp,s2) values(102,10.00)",
        "insert into root.vehicle.d0(timestamp,s2) values(105,11.11)",
        "insert into root.vehicle.d0(timestamp,s2) values(1000,1000.11)",
        "insert into root.vehicle.d0(timestamp,s3) values(60,'aaaaa')",
        "insert into root.vehicle.d0(timestamp,s3) values(70,'bbbbb')",
        "insert into root.vehicle.d0(timestamp,s3) values(80,'ccccc')",
        "insert into root.vehicle.d0(timestamp,s3) values(101,'ddddd')",
        "insert into root.vehicle.d0(timestamp,s3) values(102,'fffff')",
        "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
        "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
        "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",
        "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
        "insert into root.vehicle.d0(timestamp,s4) values(100, true)"
      };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    insertSQL();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void countWithTimeFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,3,7,4,5,1"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT count(s0),count(s1),count(s2),count(s3),count(s4) "
                  + "FROM root.vehicle.d0 WHERE time >= 3 AND time <= 106");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
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
                  + resultSet.getString(count(d0s3))
                  + ","
                  + resultSet.getString(count(d0s4));
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
  public void countWithoutFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,4,0,6,1"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // select count(d0.s0),count(d1.s1),count(d0.s3),count(d0.s4) from root.vehicle
      boolean hasResultSet =
          statement.execute(
              "SELECT count(d0.s0),count(d1.s1),count(d0.s3),count(d0.s4) FROM root.vehicle");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + resultSet.getString(count(d1s1))
                  + ","
                  + resultSet.getString(count(d0s3))
                  + ","
                  + resultSet.getString(count(d0s4));
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
  public void maxValueWithoutFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,22222,null"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      try {
        statement.execute(
            "SELECT max_value(d0.s0),max_value(d1.s1),max_value(d0.s3) FROM root.vehicle");
        fail();
      } catch (IoTDBSQLException e) {
        Assert.assertTrue(
            e.toString()
                .contains(
                    "500: [INTERNAL_SERVER_ERROR] Exception occurred while executing executeStatement. Binary statistics does not support: max"));
      }

      boolean hasResultSet =
          statement.execute("SELECT max_value(d0.s0),max_value(d1.s1) FROM root.vehicle");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_value(d0s0))
                  + ","
                  + resultSet.getString(max_value(d1s1));
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
  public void extremeWithoutFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,22222,null"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      try {
        statement.execute("SELECT extreme(d0.s0),extreme(d1.s1),extreme(d0.s3) FROM root.vehicle");
        fail();
      } catch (IoTDBSQLException e) {
        Assert.assertTrue(
            e.toString()
                .contains(
                    "500: [INTERNAL_SERVER_ERROR] Exception occurred while executing executeStatement. Binary statistics does not support: max"));
      }

      boolean hasResultSet =
          statement.execute("SELECT extreme(d0.s0),extreme(d1.s1) FROM root.vehicle");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(extreme(d0s0))
                  + ","
                  + resultSet.getString(extreme(d1s1));
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
  public void firstValueWithoutFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,90,null,aaaaa"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // select first_value(d0.s0),first_value(d1.s1),first_value(d0.s3) from root.vehicle
      boolean hasResultSet =
          statement.execute(
              "SELECT first_value(d0.s0),first_value(d1.s1),first_value(d0.s3) FROM root.vehicle");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(first_value(d0s0))
                  + ","
                  + resultSet.getString(first_value(d1s1))
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
  public void lastValueWithoutFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,22222,null,good"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // select last_value(d0.s0),last_value(d1.s1),last_value(d0.s3) from root.vehicle
      boolean hasResultSet =
          statement.execute(
              "SELECT last_value(d0.s0),last_value(d1.s1),last_value(d0.s3) FROM root.vehicle");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d1s1))
                  + ","
                  + resultSet.getString(last_value(d0s3));
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
  public void sumWithoutFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,22610.0,0.0"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // select sum(d0.s0),sum(d1.s1),sum(d0.s3) from root.vehicle
      boolean hasResultSet = statement.execute("SELECT sum(d0.s0),sum(d1.s1) FROM root.vehicle");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(sum(d0s0))
                  + ","
                  + resultSet.getString(sum(d1s1));
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
  public void lastValueWithSingleValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,22222,55555"};
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT last_value(s0),last_value(s1) FROM root.vehicle.d0 WHERE s2 >= 3.33");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d0s1));
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
  public void firstValueWithSingleValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,99,180"};
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "SELECT first_value(s0),first_value(s1) FROM root.vehicle.d0 WHERE s2 >= 3.33");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(first_value(d0s0))
                  + ","
                  + resultSet.getString(first_value(d0s1));
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
  public void sumWithSingleValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,22321.0,55934.0,1029"};
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("SELECT sum(s0),sum(s1),sum(s2) FROM root.vehicle.d0 WHERE s2 >= 3.33");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(sum(d0s0))
                  + ","
                  + resultSet.getString(sum(d0s1))
                  + ","
                  + Math.round(resultSet.getDouble(sum(d0s2)));
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
  public void avgWithSingleValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,11160.5,18645,206"};
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("SELECT avg(s0),avg(s1),avg(s2) FROM root.vehicle.d0 WHERE s2 >= 3.33");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(avg(d0s0))
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s1)))
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s2)));
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
  public void countWithSingleValueFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,2,3,5,1,0"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT count(s0),count(s1),count(s2),count(s3),"
                  + "count(s4) FROM root.vehicle.d0 WHERE s2 >= 3.33");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
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
                  + resultSet.getString(count(d0s3))
                  + ","
                  + resultSet.getString(count(d0s4));
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
  public void minTimeWithMultiValueFiltersTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,104,1,2,101,100"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_time(s0),min_time(s1),min_time(s2)"
                  + ",min_time(s3),min_time(s4) FROM root.vehicle.d0 WHERE s1 < 50000 AND s1 != 100");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_time(d0s0))
                  + ","
                  + resultSet.getString(min_time(d0s1))
                  + ","
                  + resultSet.getString(min_time(d0s2))
                  + ","
                  + resultSet.getString(min_time(d0s3))
                  + ","
                  + resultSet.getString(min_time(d0s4));
          Assert.assertEquals(ans, retArray[cnt]);
          cnt++;
          Assert.assertEquals(1, cnt);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void maxTimeWithMultiValueFiltersTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,105,105,105,102,100"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT max_time(s0),max_time(s1),max_time(s2)"
                  + ",max_time(s3),max_time(s4) FROM root.vehicle.d0 WHERE s1 < 50000 AND s1 != 100");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_time(d0s0))
                  + ","
                  + resultSet.getString(max_time(d0s1))
                  + ","
                  + resultSet.getString(max_time(d0s2))
                  + ","
                  + resultSet.getString(max_time(d0s3))
                  + ","
                  + resultSet.getString(max_time(d0s4));
          Assert.assertEquals(ans, retArray[cnt]);
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
  public void minValueWithMultiValueFiltersTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,90,180,2.22,ddddd,true"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT min_value(s0),min_value(s1),min_value(s2),"
                  + "min_value(s3),min_value(s4) FROM root.vehicle.d0 WHERE s1 < 50000 AND s1 != 100");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(min_value(d0s0))
                  + ","
                  + resultSet.getString(min_value(d0s1))
                  + ","
                  + resultSet.getString(min_value(d0s2))
                  + ","
                  + resultSet.getString(min_value(d0s3))
                  + ","
                  + resultSet.getString(min_value(d0s4));
          Assert.assertEquals(ans, retArray[cnt]);
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
  public void maxValueWithMultiValueFiltersTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,99,40000,11.11,fffff,true"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT max_value(s0),max_value(s1),max_value(s2),"
                  + "max_value(s3),max_value(s4) FROM root.vehicle.d0 "
                  + "WHERE s1 < 50000 AND s1 != 100");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(max_value(d0s0))
                  + ","
                  + resultSet.getString(max_value(d0s1))
                  + ","
                  + resultSet.getString(max_value(d0s2))
                  + ","
                  + resultSet.getString(max_value(d0s3))
                  + ","
                  + resultSet.getString(max_value(d0s4));
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
  public void extremeWithMultiValueFiltersTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,99,40000,11.11"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "SELECT extreme(s0),extreme(s1),extreme(s2)"
                  + " FROM root.vehicle.d0 "
                  + "WHERE s1 < 50000 AND s1 != 100");

      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(extreme(d0s0))
                  + ","
                  + resultSet.getString(extreme(d0s1))
                  + ","
                  + resultSet.getString(extreme(d0s2));
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
  public void selectAllSQLTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,null,1101,null,null,999",
          "2,null,40000,2.22,null,null",
          "3,null,null,3.33,null,null",
          "4,null,null,4.44,null,null",
          "50,null,50000,null,null,null",
          "60,null,null,null,aaaaa,null",
          "70,null,null,null,bbbbb,null",
          "80,null,null,null,ccccc,null",
          "100,null,199,null,null,null",
          "101,null,199,null,ddddd,null",
          "102,null,180,10.0,fffff,null",
          "103,null,199,null,null,null",
          "104,90,190,null,null,null",
          "105,99,199,11.11,null,null",
          "106,199,null,null,null,null",
          "1000,22222,55555,1000.11,null,888",
          "946684800000,null,100,null,good,null"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("SELECT * FROM root");
      if (hasResultSet) {
        try (ResultSet resultSet = statement.getResultSet()) {
          int cnt = 0;
          while (resultSet.next()) {
            String ans =
                resultSet.getString(TIMESTAMP_STR)
                    + ","
                    + resultSet.getString(d0s0)
                    + ","
                    + resultSet.getString(d0s1)
                    + ","
                    + resultSet.getString(d0s2)
                    + ","
                    + resultSet.getString(d0s3)
                    + ","
                    + resultSet.getString(d1s0);
            Assert.assertEquals(ans, retArray[cnt]);
            cnt++;
          }
          Assert.assertEquals(17, cnt);
        }
      }

      retArray = new String[] {"100,true"};
      hasResultSet = statement.execute("SELECT s4 FROM root.vehicle.d0");
      if (hasResultSet) {
        try (ResultSet resultSet = statement.getResultSet()) {
          int cnt = 0;
          while (resultSet.next()) {
            String ans = resultSet.getString(TIMESTAMP_STR) + "," + resultSet.getString(d0s4);
            Assert.assertEquals(ans, retArray[cnt]);
            cnt++;
          }
          Assert.assertEquals(1, cnt);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public static void insertSQL() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
