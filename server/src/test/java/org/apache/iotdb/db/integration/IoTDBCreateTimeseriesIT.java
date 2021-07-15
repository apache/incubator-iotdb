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
import org.apache.iotdb.jdbc.IoTDBSQLException;

import org.junit.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBCreateTimeseriesIT {
  private Statement statement;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
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

  /** Test creating a time series that is a prefix path of an existing time series */
  @Ignore // nested measurement has been forbidden
  @Test
  public void testCreateTimeseries1() throws Exception {
    String[] timeSeriesArray = {"root.sg1.aa.bb", "root.sg1.aa.bb.cc", "root.sg1.aa"};

    try {
      for (String timeSeries : timeSeriesArray) {
        statement.execute(
            String.format(
                "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
                timeSeries));
      }

      // ensure that current timeseries in cache is right.
      createTimeSeries1Tool(timeSeriesArray);

      EnvironmentUtils.stopDaemon();
      setUp();

      // ensure timeseries in cache is right after recovering.
      createTimeSeries1Tool(timeSeriesArray);
    } catch (IoTDBSQLException e) {
      Assert.assertEquals("300: Path [root.sg1.aa.bb] already exist", e.getMessage());
    }
  }

  private void createTimeSeries1Tool(String[] timeSeriesArray) throws SQLException {
    boolean hasResult = statement.execute("show timeseries");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timeseries = resultSet.getString("timeseries");
        resultList.add(timeseries);
      }
    }
    Assert.assertEquals(3, resultList.size());

    List<String> collect =
        resultList.stream()
            .sorted(Comparator.comparingInt(e -> e.split("\\.").length))
            .collect(Collectors.toList());

    Assert.assertEquals(timeSeriesArray[2], collect.get(0));
    Assert.assertEquals(timeSeriesArray[0], collect.get(1));
    Assert.assertEquals(timeSeriesArray[1], collect.get(2));
  }

  /** Test if creating a time series will cause the storage group with same name to disappear */
  @Test
  public void testCreateTimeseries2() throws Exception {
    String storageGroup = "root.sg1.a.b.c";

    statement.execute(String.format("SET storage group TO %s", storageGroup));
    try {
      statement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              storageGroup));
    } catch (IoTDBSQLException ignored) {
    }

    // ensure that current storage group in cache is right.
    createTimeSeries2Tool(storageGroup);

    EnvironmentUtils.stopDaemon();
    setUp();

    // ensure storage group in cache is right after recovering.
    createTimeSeries2Tool(storageGroup);
  }

  private void createTimeSeries2Tool(String storageGroup) throws SQLException {
    statement.execute("show timeseries");
    Set<String> resultList = new HashSet<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String str = resultSet.getString("timeseries");
        resultList.add(str);
      }
    }
    Assert.assertFalse(resultList.contains(storageGroup));

    statement.execute("show storage group");
    resultList.clear();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String res = resultSet.getString("storage group");
        resultList.add(res);
      }
    }
    Assert.assertTrue(resultList.contains(storageGroup));
  }
}
