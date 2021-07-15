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

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBAutoCreateSchemaIT {
  private Statement statement;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

    Class.forName(Config.JDBC_DRIVER_NAME);
    connection = DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    statement.close();
    connection.close();
    EnvironmentUtils.cleanEnv();
  }

  /** create timeseries without setting storage group */
  @Test
  public void createTimeseriesTest() throws ClassNotFoundException {
    String[] sqls = {
      "CREATE TIMESERIES root.sg0.d1.s2 WITH DATATYPE=INT32,ENCODING=RLE",
      "INSERT INTO root.sg0.d1(timestamp,s2) values(1,123)",
    };
    executeSQL(sqls);
  }

  /** insert data when storage group has been set but timeseries hasn't been created */
  @Test
  public void insertTest1() throws ClassNotFoundException {
    String[] sqls = {
      "SET STORAGE GROUP TO root.sg0",
      "INSERT INTO root.sg0.d1(timestamp,s2) values(1,123.123)",
      "INSERT INTO root.sg0.d1(timestamp,s3) values(1,\"abc\")",
    };
    executeSQL(sqls);
  }

  /** insert data when storage group hasn't been set and timeseries hasn't been created */
  @Test
  public void insertTest2() throws ClassNotFoundException {
    String[] sqls = {
      "INSERT INTO root.sg0.d1(timestamp,s2) values(1,\"abc\")",
      "INSERT INTO root.sg0.d2(timestamp,s3) values(1,123.123)",
      "INSERT INTO root.sg0.d2(timestamp,s4) values(1,123456)",
    };
    executeSQL(sqls);
  }

  private void executeSQL(String[] sqls) throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String result = "";
      Long now_start = 0L;
      boolean cmp = false;

      for (String sql : sqls) {
        if (cmp) {
          Assert.assertEquals(sql, result);
          cmp = false;
        } else if (sql.equals("SHOW TIMESERIES")) {
          DatabaseMetaData data = connection.getMetaData();
          result = data.toString();
          cmp = true;
        } else {
          if (sql.contains("NOW()") && now_start == 0L) {
            now_start = System.currentTimeMillis();
          }

          statement.execute(sql);
          if (sql.split(" ")[0].equals("SELECT")) {
            ResultSet resultSet = statement.getResultSet();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            String[] column = new String[count];
            for (int i = 0; i < count; i++) {
              column[i] = metaData.getColumnName(i + 1);
            }
            result = "";
            while (resultSet.next()) {
              for (int i = 1; i <= count; i++) {
                if (now_start > 0L && column[i - 1].equals(TestConstant.TIMESTAMP_STR)) {
                  String timestr = resultSet.getString(i);
                  Long tn = Long.valueOf(timestr);
                  Long now = System.currentTimeMillis();
                  if (tn >= now_start && tn <= now) {
                    timestr = "NOW()";
                  }
                  result += timestr + ',';
                } else {
                  result += resultSet.getString(i) + ',';
                }
              }
              result += '\n';
            }
            cmp = true;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * insert data when the time series that is a prefix path of an existing time series hasn't been
   * created
   */
  @Ignore // nested measurement has been forbidden
  @Test
  public void testInsertAutoCreate1() throws Exception {
    String[] timeSeriesArray = {"root.sg1.a.a", "root.sg1.a", "root.sg1.a.a.a"};

    for (String timeSeries : timeSeriesArray) {
      statement.execute(
          String.format("INSERT INTO %s(timestamp, a) values(123, \"aabb\")", timeSeries));
    }

    // ensure that insert data in cache is right.
    insertAutoCreate1Tool();

    EnvironmentUtils.stopDaemon();
    setUp();

    // ensure that insert data in cache is right after recovering.
    insertAutoCreate1Tool();
  }

  private void insertAutoCreate1Tool() throws SQLException {
    boolean hasResult = statement.execute("select * from root.sg1");
    Assert.assertTrue(hasResult);

    Set<String> strSet = new HashSet<>();
    String[] valueList = {};
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        valueList =
            new String[] {
              resultSet.getString("root.sg1.a.a"),
              resultSet.getString("root.sg1.a.a.a"),
              resultSet.getString("root.sg1.a.a.a.a")
            };
        strSet = new HashSet<>(Arrays.asList(valueList));
      }
    }
    Assert.assertEquals(3, valueList.length);
    Assert.assertEquals(1, strSet.size());
    Assert.assertTrue(strSet.contains("aabb"));
  }

  /**
   * test if automatically creating a time series will cause the storage group with same name to
   * disappear
   */
  @Test
  public void testInsertAutoCreate2() throws Exception {
    String storageGroup = "root.sg2.a.b.c";
    String timeSeriesPrefix = "root.sg2.a.b";

    statement.execute(String.format("SET storage group TO %s", storageGroup));
    try {
      statement.execute(
          String.format("INSERT INTO %s(timestamp, c) values(123, \"aabb\")", timeSeriesPrefix));
    } catch (IoTDBSQLException ignored) {
    }

    // ensure that current storage group in cache is right.
    InsertAutoCreate2Tool(storageGroup, timeSeriesPrefix);

    EnvironmentUtils.stopDaemon();
    setUp();

    // ensure that storage group in cache is right after recovering.
    InsertAutoCreate2Tool(storageGroup, timeSeriesPrefix);
  }

  private void InsertAutoCreate2Tool(String storageGroup, String timeSeriesPrefix)
      throws SQLException {
    statement.execute("show timeseries");
    Set<String> resultList = new HashSet<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String str = resultSet.getString("timeseries");
        resultList.add(str);
      }
    }
    Assert.assertFalse(resultList.contains(timeSeriesPrefix + "c"));

    statement.execute("show storage group");
    resultList.clear();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        resultList.add(resultSet.getString("storage group"));
      }
    }
    Assert.assertTrue(resultList.contains(storageGroup));
  }
}
