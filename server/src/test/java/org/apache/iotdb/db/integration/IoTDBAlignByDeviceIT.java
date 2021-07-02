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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

public class IoTDBAlignByDeviceIT {

  private static String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.vehicle",
        "SET STORAGE GROUP TO root.other",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
        "CREATE TIMESERIES root.other.d1.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
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
        "insert into root.vehicle.d0(timestamp,s1) values(2000-01-01T08:00:00+08:00, 100)",
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
        "insert into root.vehicle.d0(timestamp,s3) values(2000-01-01T08:00:00+08:00, 'good')",
        "insert into root.vehicle.d0(timestamp,s4) values(100, false)",
        "insert into root.vehicle.d0(timestamp,s4) values(100, true)",
        "insert into root.vehicle.d1(timestamp,s0) values(1,999)",
        "insert into root.vehicle.d1(timestamp,s0) values(1000,888)",
        "insert into root.other.d1(timestamp,s0) values(2, 3.14)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
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
    }
  }

  @Test
  public void selectTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,null,null,null,",
          "2,root.vehicle.d0,10000,40000,2.22,null,null,",
          "3,root.vehicle.d0,null,null,3.33,null,null,",
          "4,root.vehicle.d0,null,null,4.44,null,null,",
          "50,root.vehicle.d0,10000,50000,null,null,null,",
          "60,root.vehicle.d0,null,null,null,aaaaa,null,",
          "70,root.vehicle.d0,null,null,null,bbbbb,null,",
          "80,root.vehicle.d0,null,null,null,ccccc,null,",
          "100,root.vehicle.d0,99,199,null,null,true,",
          "101,root.vehicle.d0,99,199,null,ddddd,null,",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,",
          "103,root.vehicle.d0,99,199,null,null,null,",
          "104,root.vehicle.d0,90,190,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,",
          "106,root.vehicle.d0,99,null,null,null,null,",
          "1000,root.vehicle.d0,22222,55555,1000.11,null,null,",
          "946684800000,root.vehicle.d0,null,100,null,good,null,",
          "1,root.vehicle.d1,999,null,null,null,null,",
          "1000,root.vehicle.d1,888,null,null,null,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select * from root.vehicle align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(19, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWithDuplicatedPathsTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,101,1101,",
          "2,root.vehicle.d0,10000,10000,40000,",
          "50,root.vehicle.d0,10000,10000,50000,",
          "100,root.vehicle.d0,99,99,199,",
          "101,root.vehicle.d0,99,99,199,",
          "102,root.vehicle.d0,80,80,180,",
          "103,root.vehicle.d0,99,99,199,",
          "104,root.vehicle.d0,90,90,190,",
          "105,root.vehicle.d0,99,99,199,",
          "106,root.vehicle.d0,99,99,null,",
          "1000,root.vehicle.d0,22222,22222,55555,",
          "946684800000,root.vehicle.d0,null,null,100,",
          "1,root.vehicle.d1,999,999,null,",
          "1000,root.vehicle.d1,888,888,null,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s0,s0,s1 from root.vehicle.d0, root.vehicle.d1 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s0,s1",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.BIGINT
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(14, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectLimitTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "2,root.vehicle.d0,10000,10000,40000,",
          "50,root.vehicle.d0,10000,10000,50000,",
          "100,root.vehicle.d0,99,99,199,",
          "101,root.vehicle.d0,99,99,199,",
          "102,root.vehicle.d0,80,80,180,",
          "103,root.vehicle.d0,99,99,199,",
          "104,root.vehicle.d0,90,90,190,",
          "105,root.vehicle.d0,99,99,199,",
          "106,root.vehicle.d0,99,99,null,",
          "1000,root.vehicle.d0,22222,22222,55555,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s0,s0,s1 from root.vehicle.* limit 10 offset 1 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s0,s1",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.BIGINT
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(10, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSlimitTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,",
          "2,root.vehicle.d0,10000,40000,",
          "50,root.vehicle.d0,10000,50000,",
          "100,root.vehicle.d0,99,199,",
          "101,root.vehicle.d0,99,199,",
          "102,root.vehicle.d0,80,180,",
          "103,root.vehicle.d0,99,199,",
          "104,root.vehicle.d0,90,190,",
          "105,root.vehicle.d0,99,199,",
          "106,root.vehicle.d0,99,null,",
          "1000,root.vehicle.d0,22222,55555,",
          "946684800000,root.vehicle.d0,null,100,",
          "1,root.vehicle.d1,999,null,",
          "1000,root.vehicle.d1,888,null,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s0,s0,s1 from root.vehicle.* slimit 2 soffset 1 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.BIGINT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(14, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "100,root.vehicle.d0,99,199,null,null,true,",
          "101,root.vehicle.d0,99,199,null,ddddd,null,",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,",
          "103,root.vehicle.d0,99,199,null,null,null,",
          "104,root.vehicle.d0,90,190,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // single device
      boolean hasResultSet =
          statement.execute(
              "select * from root.vehicle.d0 where s0 > 0 AND s1 < 200 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(6, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void aggregateTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {"root.vehicle.d1,2,null,null,null,null,", "root.vehicle.d0,11,11,6,6,1,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s0),count(s1),count(s2),count(s3),count(s4) "
                  + "from root.vehicle.d1,root.vehicle.d0 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Device,count(s0),count(s1),count(s2),count(s3),count(s4)",
                new int[] {
                  Types.VARCHAR,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
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
  public void groupByTimeTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "2,root.vehicle.d0,1,1,3,0,0,",
          "22,root.vehicle.d0,0,0,0,0,0,",
          "42,root.vehicle.d0,0,0,0,0,0,",
          "2,root.vehicle.d1,0,null,null,null,null,",
          "22,root.vehicle.d1,0,null,null,null,null,",
          "42,root.vehicle.d1,0,null,null,null,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(*) from root.vehicle GROUP BY ([2,50),20ms) align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,count(s0),count(s1),count(s2),count(s3),count(s4)",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                  Types.BIGINT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(6, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void groupByTimeWithValueFilterTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "2,root.vehicle.d0,2,", "102,root.vehicle.d0,1",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select count(s2) from root.vehicle.d0 where s2 > 3 and s2 <= 10 GROUP BY ([2,200),100ms) align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,count(s2)",
                new int[] {
                  Types.TIMESTAMP, Types.VARCHAR, Types.BIGINT,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
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
  public void fillTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "3,root.vehicle.d0,10000,40000,3.33,null,null,",
          "3,root.vehicle.d1,999,null,null,null,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select * from root.vehicle where time = 3 Fill(int32[previous, 5ms]) align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
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
  public void errorCaseTest1() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select d0.s1, d0.s2, d1.s0 from root.vehicle align by device");
      fail("No exception thrown.");
    } catch (Exception e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "The paths of the SELECT clause can only be single level. In other words, "
                      + "the paths of the SELECT clause can only be measurements or STAR, without DOT."));
    }
  }

  @Test
  public void errorCaseTest3() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select s0 from root.*.* align by device");
      fail("No exception thrown.");
    } catch (Exception e) {
      // root.vehicle.d0.s0 INT32
      // root.vehicle.d1.s0 INT32
      // root.other.d1.s0 FLOAT
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "The data types of the same measurement column should be the same across devices in "
                      + "ALIGN_BY_DEVICE sql."));
    }
  }

  /**
   * root.vehicle.d0.s0 INT32 root.vehicle.d1.s0 INT32 root.other.d1.s0 FLOAT but
   * count(root.vehicle.d0.s0) INT64 count(root.vehicle.d1.s0) INT64 count(root.other.d1.s0) INT64
   */
  @Test
  public void unusualCaseTest1() throws ClassNotFoundException {
    String[] retArray =
        new String[] {"root.other.d1,1,", "root.vehicle.d0,11,", "root.vehicle.d1,2,"};

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select count(s0) from root.*.* align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData, "Device,count(s0)", new int[] {Types.VARCHAR, Types.BIGINT});

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
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
  public void unusualCaseTest2() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,101,1101,101,1101,null,null,null,",
          "2,root.vehicle.d0,10000,10000,40000,10000,40000,2.22,null,null,",
          "3,root.vehicle.d0,null,null,null,null,null,3.33,null,null,",
          "4,root.vehicle.d0,null,null,null,null,null,4.44,null,null,",
          "1,root.vehicle.d1,999,999,null,999,null,null,null,null,"
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // duplicated devices
      boolean hasResultSet =
          statement.execute(
              "select s0,s0,s1,* from root.vehicle.*, root.vehicle.d0, root.vehicle.d1"
                  + " where time < 20 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s0,s1,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(5, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectConstantTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,null,null,null,'11',",
          "2,root.vehicle.d0,10000,40000,2.22,null,null,'11',",
          "3,root.vehicle.d0,null,null,3.33,null,null,'11',",
          "4,root.vehicle.d0,null,null,4.44,null,null,'11',",
          "50,root.vehicle.d0,10000,50000,null,null,null,'11',",
          "60,root.vehicle.d0,null,null,null,aaaaa,null,'11',",
          "70,root.vehicle.d0,null,null,null,bbbbb,null,'11',",
          "80,root.vehicle.d0,null,null,null,ccccc,null,'11',",
          "100,root.vehicle.d0,99,199,null,null,true,'11',",
          "101,root.vehicle.d0,99,199,null,ddddd,null,'11',",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,'11',",
          "103,root.vehicle.d0,99,199,null,null,null,'11',",
          "104,root.vehicle.d0,90,190,null,null,null,'11',",
          "105,root.vehicle.d0,99,199,11.11,null,null,'11',",
          "106,root.vehicle.d0,99,null,null,null,null,'11',",
          "1000,root.vehicle.d0,22222,55555,1000.11,null,null,'11',",
          "946684800000,root.vehicle.d0,null,100,null,good,null,'11',",
          "1,root.vehicle.d1,999,null,null,null,null,'11',",
          "1000,root.vehicle.d1,888,null,null,null,null,'11',",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select *, '11' from root.vehicle align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4,'11'",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN, /* constant column */
                  Types.VARCHAR
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(19, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectNonExistTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,null,null,null,null,",
          "2,root.vehicle.d0,10000,40000,2.22,null,null,null,",
          "3,root.vehicle.d0,null,null,3.33,null,null,null,",
          "4,root.vehicle.d0,null,null,4.44,null,null,null,",
          "50,root.vehicle.d0,10000,50000,null,null,null,null,",
          "60,root.vehicle.d0,null,null,null,aaaaa,null,null,",
          "70,root.vehicle.d0,null,null,null,bbbbb,null,null,",
          "80,root.vehicle.d0,null,null,null,ccccc,null,null,",
          "100,root.vehicle.d0,99,199,null,null,true,null,",
          "101,root.vehicle.d0,99,199,null,ddddd,null,null,",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,null,",
          "103,root.vehicle.d0,99,199,null,null,null,null,",
          "104,root.vehicle.d0,90,190,null,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,null,",
          "106,root.vehicle.d0,99,null,null,null,null,null,",
          "1000,root.vehicle.d0,22222,55555,1000.11,null,null,null,",
          "946684800000,root.vehicle.d0,null,100,null,good,null,null,",
          "1,root.vehicle.d1,999,null,null,null,null,null,",
          "1000,root.vehicle.d1,888,null,null,null,null,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute("select s0, s1, s2, s3, s4, s5 from root.vehicle.*  align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4,s5",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN, /* non exist column */
                  Types.VARCHAR
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(19, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectUnorderedConstantTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,'11',null,'22',null,null,",
          "2,root.vehicle.d0,10000,40000,'11',2.22,'22',null,null,",
          "3,root.vehicle.d0,null,null,'11',3.33,'22',null,null,",
          "4,root.vehicle.d0,null,null,'11',4.44,'22',null,null,",
          "50,root.vehicle.d0,10000,50000,'11',null,'22',null,null,",
          "60,root.vehicle.d0,null,null,'11',null,'22',aaaaa,null,",
          "70,root.vehicle.d0,null,null,'11',null,'22',bbbbb,null,",
          "80,root.vehicle.d0,null,null,'11',null,'22',ccccc,null,",
          "100,root.vehicle.d0,99,199,'11',null,'22',null,true,",
          "101,root.vehicle.d0,99,199,'11',null,'22',ddddd,null,",
          "102,root.vehicle.d0,80,180,'11',10.0,'22',fffff,null,",
          "103,root.vehicle.d0,99,199,'11',null,'22',null,null,",
          "104,root.vehicle.d0,90,190,'11',null,'22',null,null,",
          "105,root.vehicle.d0,99,199,'11',11.11,'22',null,null,",
          "106,root.vehicle.d0,99,null,'11',null,'22',null,null,",
          "1000,root.vehicle.d0,22222,55555,'11',1000.11,'22',null,null,",
          "946684800000,root.vehicle.d0,null,100,'11',null,'22',good,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s0, s1,'11', s2, '22', s3, s4 from root.vehicle.d0 align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,'11',s2,'22',s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  /* constant column */ Types.VARCHAR,
                  Types.FLOAT,
                  /* constant column */ Types.VARCHAR,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(17, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectUnorderedConstantAndNonExistTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,'11',null,'22',null,null,null,",
          "2,root.vehicle.d0,10000,40000,'11',2.22,'22',null,null,null,",
          "3,root.vehicle.d0,null,null,'11',3.33,'22',null,null,null,",
          "4,root.vehicle.d0,null,null,'11',4.44,'22',null,null,null,",
          "50,root.vehicle.d0,10000,50000,'11',null,'22',null,null,null,",
          "60,root.vehicle.d0,null,null,'11',null,'22',null,aaaaa,null,",
          "70,root.vehicle.d0,null,null,'11',null,'22',null,bbbbb,null,",
          "80,root.vehicle.d0,null,null,'11',null,'22',null,ccccc,null,",
          "100,root.vehicle.d0,99,199,'11',null,'22',null,null,true,",
          "101,root.vehicle.d0,99,199,'11',null,'22',null,ddddd,null,",
          "102,root.vehicle.d0,80,180,'11',10.0,'22',null,fffff,null,",
          "103,root.vehicle.d0,99,199,'11',null,'22',null,null,null,",
          "104,root.vehicle.d0,90,190,'11',null,'22',null,null,null,",
          "105,root.vehicle.d0,99,199,'11',11.11,'22',null,null,null,",
          "106,root.vehicle.d0,99,null,'11',null,'22',null,null,null,",
          "1000,root.vehicle.d0,22222,55555,'11',1000.11,'22',null,null,null,",
          "946684800000,root.vehicle.d0,null,100,'11',null,'22',null,good,null,",
          "1,root.vehicle.d1,999,null,'11',null,'22',null,null,null,",
          "1000,root.vehicle.d1,888,null,'11',null,'22',null,null,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s0, s1,'11', s2, '22', s5, s3, s4 from root.vehicle.* align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,'11',s2,'22',s5,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  /* constant column */ Types.VARCHAR,
                  Types.FLOAT, /* constant column */
                  Types.VARCHAR, /* non exist column */
                  Types.VARCHAR,
                  Types.VARCHAR,
                  Types.BOOLEAN,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(19, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectConstantAndNonExistTestWithUnorderedDevice() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d1,null,999,null,'11',null,'11','22',null,null,999,",
          "1000,root.vehicle.d1,null,888,null,'11',null,'11','22',null,null,888,",
          "1,root.vehicle.d0,null,101,null,'11',null,'11','22',null,null,101,",
          "2,root.vehicle.d0,null,10000,null,'11',2.22,'11','22',null,null,10000,",
          "3,root.vehicle.d0,null,null,null,'11',3.33,'11','22',null,null,null,",
          "4,root.vehicle.d0,null,null,null,'11',4.44,'11','22',null,null,null,",
          "50,root.vehicle.d0,null,10000,null,'11',null,'11','22',null,null,10000,",
          "60,root.vehicle.d0,null,null,null,'11',null,'11','22',null,aaaaa,null,",
          "70,root.vehicle.d0,null,null,null,'11',null,'11','22',null,bbbbb,null,",
          "80,root.vehicle.d0,null,null,null,'11',null,'11','22',null,ccccc,null,",
          "100,root.vehicle.d0,null,99,null,'11',null,'11','22',null,null,99,",
          "101,root.vehicle.d0,null,99,null,'11',null,'11','22',null,ddddd,99,",
          "102,root.vehicle.d0,null,80,null,'11',10.0,'11','22',null,fffff,80,",
          "103,root.vehicle.d0,null,99,null,'11',null,'11','22',null,null,99,",
          "104,root.vehicle.d0,null,90,null,'11',null,'11','22',null,null,90,",
          "105,root.vehicle.d0,null,99,null,'11',11.11,'11','22',null,null,99,",
          "106,root.vehicle.d0,null,99,null,'11',null,'11','22',null,null,99,",
          "1000,root.vehicle.d0,null,22222,null,'11',1000.11,'11','22',null,null,22222,",
          "946684800000,root.vehicle.d0,null,null,null,'11',null,'11','22',null,good,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet =
          statement.execute(
              "select s5, s0, s5, '11', s2, '11', '22', s5, s3, s0 from root.vehicle.d1, root.vehicle.d0  align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s5,s0,s5,'11',s2,'11','22',s5,s3,s0",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.VARCHAR,
                  Types.INTEGER,
                  /* non exist column */ Types.VARCHAR, /* constant column */
                  Types.VARCHAR,
                  Types.FLOAT, /* constant column */
                  Types.VARCHAR,
                  /* constant column */ Types.VARCHAR, /* non exist column */
                  Types.VARCHAR,
                  Types.VARCHAR,
                  Types.INTEGER,
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(19, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      Assert.assertNotNull(typeIndex);
      Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
      actualIndexToExpectedIndexList.add(typeIndex);
    }
    return actualIndexToExpectedIndexList;
  }

  @Test
  public void selectWithRegularExpressionTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,root.vehicle.d0,101,1101,null,null,null,",
          "2,root.vehicle.d0,10000,40000,2.22,null,null,",
          "3,root.vehicle.d0,null,null,3.33,null,null,",
          "4,root.vehicle.d0,null,null,4.44,null,null,",
          "50,root.vehicle.d0,10000,50000,null,null,null,",
          "60,root.vehicle.d0,null,null,null,aaaaa,null,",
          "70,root.vehicle.d0,null,null,null,bbbbb,null,",
          "80,root.vehicle.d0,null,null,null,ccccc,null,",
          "100,root.vehicle.d0,99,199,null,null,true,",
          "101,root.vehicle.d0,99,199,null,ddddd,null,",
          "102,root.vehicle.d0,80,180,10.0,fffff,null,",
          "103,root.vehicle.d0,99,199,null,null,null,",
          "104,root.vehicle.d0,90,190,null,null,null,",
          "105,root.vehicle.d0,99,199,11.11,null,null,",
          "106,root.vehicle.d0,99,null,null,null,null,",
          "1000,root.vehicle.d0,22222,55555,1000.11,null,null,",
          "946684800000,root.vehicle.d0,null,100,null,good,null,",
          "1,root.vehicle.d1,999,null,null,null,null,",
          "1000,root.vehicle.d1,888,null,null,null,null,",
        };

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      boolean hasResultSet = statement.execute("select * from root.vehicle.d* align by device");
      Assert.assertTrue(hasResultSet);

      try (ResultSet resultSet = statement.getResultSet()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        List<Integer> actualIndexToExpectedIndexList =
            checkHeader(
                resultSetMetaData,
                "Time,Device,s0,s1,s2,s3,s4",
                new int[] {
                  Types.TIMESTAMP,
                  Types.VARCHAR,
                  Types.INTEGER,
                  Types.BIGINT,
                  Types.FLOAT,
                  Types.VARCHAR,
                  Types.BOOLEAN
                });

        int cnt = 0;
        while (resultSet.next()) {
          String[] expectedStrings = retArray[cnt].split(",");
          StringBuilder expectedBuilder = new StringBuilder();
          StringBuilder actualBuilder = new StringBuilder();
          for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            actualBuilder.append(resultSet.getString(i)).append(",");
            expectedBuilder
                .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
                .append(",");
          }
          Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
          cnt++;
        }
        Assert.assertEquals(19, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
