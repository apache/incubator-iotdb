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

public class IoTDBAggregationLargeDataIT {

  private static final String TIMESTAMP_STR = "Time";
  private final String d0s0 = "root.vehicle.d0.s0";
  private final String d0s1 = "root.vehicle.d0.s1";
  private final String d0s2 = "root.vehicle.d0.s2";
  private final String d0s3 = "root.vehicle.d0.s3";
  private final String d0s4 = "root.vehicle.d0.s4";

  private static String[] createSql =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      };

  private static String[] insertSql =
      new String[] {
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
        "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
        "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
      };

  private long prevPartitionInterval;

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
  }

  @Test
  public void test() throws ClassNotFoundException {
    insertSQL();

    lastValueAggreWithSingleFilterTest();
    avgAggreWithSingleFilterTest();
    sumAggreWithSingleFilterTest();
    firstAggreWithSingleFilterTest();
    countAggreWithSingleFilterTest();
    minTimeAggreWithSingleFilterTest();
    minValueAggreWithSingleFilterTest();
    maxValueAggreWithSingleFilterTest();
    extremeAggreWithSingleFilterTest();

    countAggreWithMultiFilterTest();
    maxTimeAggreWithMultiFilterTest();
    avgAggreWithMultiFilterTest();
    sumAggreWithMultiFilterTest();
    firstAggreWithMultiFilterTest();
  }

  private void lastValueAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,9,39,63.0,E,true"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select last_value(s0),last_value(s1),last_value(s2),last_value(s3),last_value(s4)"
                  + " from root.vehicle.d0 where s1 >= 0");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d0s1))
                  + ","
                  + resultSet.getString(last_value(d0s2))
                  + ","
                  + resultSet.getString(last_value(d0s3))
                  + ","
                  + resultSet.getString(last_value(d0s4));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "select last_value(s0),last_value(s1),last_value(s2),last_value(s3),last_value(s4)"
                  + " from root.vehicle.d0 where s1 >= 0 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(last_value(d0s0))
                  + ","
                  + resultSet.getString(last_value(d0s1))
                  + ","
                  + resultSet.getString(last_value(d0s2))
                  + ","
                  + resultSet.getString(last_value(d0s3))
                  + ","
                  + resultSet.getString(last_value(d0s4));
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

  private void sumAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,55061.0,156752.0,20254"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select sum(s0),sum(s1),sum(s2)" + " from root.vehicle.d0 where s1 >= 0");
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
          Assert.assertEquals(ans, retArray[cnt]);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "select sum(s0),sum(s1),sum(s2)"
                  + " from root.vehicle.d0 where s1 >= 0 order by time desc");
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
          Assert.assertEquals(ans, retArray[retArray.length - cnt - 1]);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void firstAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,90,1101,2.22,ddddd,true"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select first_value(s0),first_value(s1),first_value(s2),first_value(s3),"
                  + "first_value(s4) from root.vehicle.d0 where s1 >= 0");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
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
                  + resultSet.getString(first_value(d0s3))
                  + ","
                  + resultSet.getString(first_value(d0s4));
          Assert.assertEquals(ans, retArray[cnt]);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "select first_value(s0),first_value(s1),first_value(s2),first_value(s3),"
                  + "first_value(s4) from root.vehicle.d0 where s1 >= 0 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
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
                  + resultSet.getString(first_value(d0s3))
                  + ","
                  + resultSet.getString(first_value(d0s4));
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

  private void avgAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,75,212,28"};
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute("select avg(s0),avg(s1),avg(s2) from root.vehicle.d0 where s1 >= 0");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s0)))
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s1)))
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s2)));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "select avg(s0),avg(s1),avg(s2) from root.vehicle.d0 where s1 >= 0 order by time desc");
      Assert.assertTrue(hasResultSet);
      try (ResultSet resultSet = statement.getResultSet()) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s0)))
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

  private void countAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,733,740,734"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select count(s0),count(s1),count(s2) from root.vehicle.d0 where s1 >= 0");
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
                  + resultSet.getString(count(d0s2));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      hasResultSet =
          statement.execute(
              "select count(s0),count(s1),count(s2) from root.vehicle.d0 where s1 >= 0 order by time desc");
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
                  + resultSet.getString(count(d0s2));
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

  private void minTimeAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,104,1,2,101,100"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select min_time(s0),min_time(s1),min_time(s2),min_time(s3),min_time(s4)"
                  + " from root.vehicle.d0 where s1 >= 0");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;

      try (ResultSet resultSet = statement.getResultSet()) {
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
        }
        Assert.assertEquals(1, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "select min_time(s0),min_time(s1),min_time(s2),min_time(s3),min_time(s4)"
                  + " from root.vehicle.d0 where s1 >= 0 order by time desc");
      Assert.assertTrue(hasResultSet);
      cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
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
        }
        Assert.assertEquals(1, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void minValueAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,0,0,0.0,B,true"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select min_value(s0),min_value(s1),min_value(s2),"
                  + "min_value(s3),min_value(s4) from root.vehicle.d0 "
                  + "where s1 < 50000 and s1 != 100");

      if (hasResultSet) {
        int cnt = 0;
        try (ResultSet resultSet = statement.getResultSet()) {
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
      }

      hasResultSet =
          statement.execute(
              "select min_value(s0),min_value(s1),min_value(s2),"
                  + "min_value(s3),min_value(s4) from root.vehicle.d0 "
                  + "where s1 < 50000 and s1 != 100 order by time desc");

      if (hasResultSet) {
        int cnt = 0;
        try (ResultSet resultSet = statement.getResultSet()) {
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
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void maxValueAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,99,40000,122.0,fffff,true"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select max_value(s0),max_value(s1),max_value(s2),"
                  + "max_value(s3),max_value(s4) from root.vehicle.d0 "
                  + "where s1 < 50000 and s1 != 100");

      if (hasResultSet) {
        int cnt = 0;
        try (ResultSet resultSet = statement.getResultSet()) {
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
            Assert.assertEquals(ans, retArray[cnt]);
            cnt++;
          }
          Assert.assertEquals(1, cnt);
        }
      }

      hasResultSet =
          statement.execute(
              "select max_value(s0),max_value(s1),max_value(s2),"
                  + "max_value(s3),max_value(s4) from root.vehicle.d0 "
                  + "where s1 < 50000 and s1 != 100 order by time desc");

      if (hasResultSet) {
        int cnt = 0;
        try (ResultSet resultSet = statement.getResultSet()) {
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

  private void extremeAggreWithSingleFilterTest() {
    String[] retArray = new String[] {"0,99,40000,122.0"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select extreme(s0),extreme(s1),extreme(s2)"
                  + " from root.vehicle.d0 "
                  + "where s1 < 50000 and s1 != 100");

      if (hasResultSet) {
        int cnt = 0;
        try (ResultSet resultSet = statement.getResultSet()) {
          while (resultSet.next()) {
            String ans =
                resultSet.getString(TIMESTAMP_STR)
                    + ","
                    + resultSet.getString(extreme(d0s0))
                    + ","
                    + resultSet.getString(extreme(d0s1))
                    + ","
                    + resultSet.getString(extreme(d0s2));
            Assert.assertEquals(ans, retArray[cnt]);
            cnt++;
          }
          Assert.assertEquals(1, cnt);
        }
      }

      hasResultSet =
          statement.execute(
              "select extreme(s0),extreme(s1),extreme(s2)"
                  + " from root.vehicle.d0 "
                  + "where s1 < 50000 and s1 != 100 order by time desc");

      if (hasResultSet) {
        int cnt = 0;
        try (ResultSet resultSet = statement.getResultSet()) {
          while (resultSet.next()) {
            String ans =
                resultSet.getString(TIMESTAMP_STR)
                    + ","
                    + resultSet.getString(extreme(d0s0))
                    + ","
                    + resultSet.getString(extreme(d0s1))
                    + ","
                    + resultSet.getString(extreme(d0s2));
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

  private void avgAggreWithMultiFilterTest() {
    String[] retArray = new String[] {"0,55061.0,733,75,212,28"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select sum(s0),count(s0),avg(s0),avg(s1),"
                  + "avg(s2) from root.vehicle.d0 where s1 >= 0 or s2 < 10");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(sum(d0s0))
                  + ","
                  + resultSet.getString(count(d0s0))
                  + ","
                  + Math.round(resultSet.getDouble(avg(d0s0)))
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

  private void sumAggreWithMultiFilterTest() {
    String[] retArray = new String[] {"0,55061.0,156752.0,20262"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select sum(s0),sum(s1),sum(s2) from root.vehicle.d0 where s1 >= 0 or s2 < 10");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
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

      hasResultSet =
          statement.execute(
              "select sum(s0),sum(s1),sum(s2) from root.vehicle.d0"
                  + " where s1 >= 0 or s2 < 10 order by time desc ");
      Assert.assertTrue(hasResultSet);
      cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
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

  private void firstAggreWithMultiFilterTest() {
    String[] retArray = new String[] {"0,90,1101,2.22,ddddd,true"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select first_value(s0),first_value(s1),first_value(s2),first_value(s3),"
                  + "first_value(s4) from root.vehicle.d0 where s1 >= 0 or s2 < 10");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
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
                  + resultSet.getString(first_value(d0s3))
                  + ","
                  + resultSet.getString(first_value(d0s4));
          Assert.assertEquals(retArray[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(1, cnt);
      }

      // keep the correctness of `order by time desc`
      hasResultSet =
          statement.execute(
              "select first_value(s0),first_value(s1),first_value(s2),first_value(s3),"
                  + "first_value(s4) from root.vehicle.d0 where s1 >= 0 or s2 < 10 order by time desc");
      Assert.assertTrue(hasResultSet);
      cnt = 0;
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
                  + resultSet.getString(first_value(d0s3))
                  + ","
                  + resultSet.getString(first_value(d0s4));
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

  private void countAggreWithMultiFilterTest() {
    String[] retArray = new String[] {"0,733,740,736,482,1"};

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select count(s0),count(s1),count(s2),count(s3),"
                  + "count(s4) from root.vehicle.d0 where s1 >= 0 or s2 < 10");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
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

  private void maxTimeAggreWithMultiFilterTest() throws ClassNotFoundException {
    String[] retArray = new String[] {"0,3999,3999,3999,3599,100"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet =
          statement.execute(
              "select max_time(s0),max_time(s1),max_time(s2),"
                  + "max_time(s3),max_time(s4) from root.vehicle.d0 "
                  + "where s1 < 50000 and s1 != 100");
      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
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

      hasResultSet =
          statement.execute(
              "select max_time(s0),max_time(s1),max_time(s2),"
                  + "max_time(s3),max_time(s4) from root.vehicle.d0 "
                  + "where s1 < 50000 and s1 != 100 order by time desc");
      Assert.assertTrue(hasResultSet);
      cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
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

  private static String[] stringValue = new String[] {"A", "B", "C", "D", "E"};

  public static void insertSQL() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    double d0s0sum = 0.0, d0s1sum = 0.0, d0s2sum = 0.0;
    int cnt = 0;
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : createSql) {
        statement.execute(sql);
      }

      // insert large amount of data
      for (int time = 3000; time < 3600; time++) {
        if (time % 5 == 0) {
          continue;
        }

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, stringValue[time % 5]);
        statement.execute(sql);
        cnt++;
        d0s0sum += time % 100;
        d0s1sum += time % 17;
        d0s2sum += time % 22;
      }

      statement.execute("flush");

      // insert large amount of data
      for (int time = 3700; time < 4000; time++) {
        if (time % 6 == 0) {
          continue;
        }

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        statement.execute(sql);

        cnt++;
        d0s0sum += time % 70;
        d0s1sum += time % 40;
        d0s2sum += time % 123;
      }

      statement.execute("merge");
      for (String sql : insertSql) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
