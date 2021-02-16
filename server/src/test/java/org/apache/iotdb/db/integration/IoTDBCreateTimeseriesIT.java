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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBCreateTimeseriesIT {
  private Statement statement;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();

    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = DriverManager.
        getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  /**
   * Test creating a time series that is a prefix path of an existing time series
   */
  @Test
  public void testCreateTimeseries1() throws Exception {
    String timeSeries1 = "root.sg1.aa.bb";
    String timeSeries2 = "root.sg1.aa.bb.cc";

    statement.execute(
        String.format("create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY", timeSeries1));
    statement.execute(
        String.format("create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY", timeSeries2));

    EnvironmentUtils.stopDaemon();
    setUp();

    boolean hasResult = statement.execute("show timeseries");
    Assert.assertTrue(hasResult);

    List<String> resultList = new ArrayList<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String timeseries = resultSet.getString("timeseries");
        resultList.add(timeseries);
      }
    }
    Assert.assertEquals(2, resultList.size());

    if (resultList.get(0).split("\\.").length < resultList.get(1).split("\\.").length) {
      Assert.assertEquals(timeSeries1, resultList.get(0));
      Assert.assertEquals(timeSeries2, resultList.get(1));
    } else {
      Assert.assertEquals(timeSeries2, resultList.get(0));
      Assert.assertEquals(timeSeries1, resultList.get(1));
    }

  }

  /**
   * Test if creating a time series will cause the storage group with same name to disappear
   */
  @Test
  public void testCreateTimeseries2() throws Exception {
    String timeSeries = "root.sg1.a.b.c";

    statement.execute(String.format("SET storage group TO %s", timeSeries));
    try {
      statement.execute(
          String.format("create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY", timeSeries));
    } catch (IoTDBSQLException ignored) {
    }

    EnvironmentUtils.stopDaemon();
    setUp();

    statement.execute("show timeseries");
    Set<String> resultList = new HashSet<>();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String str = resultSet.getString("timeseries");
        resultList.add(str);
      }
    }
    Assert.assertFalse(resultList.contains(timeSeries));

    statement.execute("show storage group");
    resultList.clear();
    try (ResultSet resultSet = statement.getResultSet()) {
      while (resultSet.next()) {
        String storageGroup = resultSet.getString("storage group");
        resultList.add(storageGroup);
      }
    }
    Assert.assertTrue(resultList.contains(timeSeries));

  }

}
