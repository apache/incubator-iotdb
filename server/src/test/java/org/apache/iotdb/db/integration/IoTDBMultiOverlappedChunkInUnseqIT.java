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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBMultiOverlappedChunkInUnseqIT {

  private static long previousMemtableSizeThreshold;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    previousMemtableSizeThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1024);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // recovery value
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMemtableSizeThreshold(previousMemtableSizeThreshold);
  }

  @Test
  public void selectOverlappedPageTest() {

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String sql = "select count(s0) from root.vehicle.d0 where time < 1000000";
      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          String ans = resultSet.getString(count("root.vehicle.d0.s0"));
          assertEquals("1000", ans);
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

      String sql =
          String.format(
              "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", 1000000, 1000000);
      statement.execute(sql);

      statement.execute("flush");
      for (long time = 1; time <= 1000; time++) {
        sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
        statement.execute(sql);
      }
      for (long time = 2; time <= 1000; time++) {
        sql = String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 1000);
        statement.execute(sql);
      }
      statement.execute("flush");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
