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
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBMultiStatementsIT {

  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;

  @BeforeClass
  public static void setUp() throws Exception {

    EnvironmentUtils.closeStatMonitor();

    // use small page setting
    // origin value
    maxNumberOfPointsInPage = tsFileConfig.getMaxNumberOfPointsInPage();
    pageSizeInByte = tsFileConfig.getPageSizeInByte();
    groupSizeInByte = tsFileConfig.getGroupSizeInByte();

    // new value
    tsFileConfig.setMaxNumberOfPointsInPage(1000);
    tsFileConfig.setPageSizeInByte(1024 * 150);
    tsFileConfig.setGroupSizeInByte(1024 * 1000);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1024 * 1000);

    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // recovery value
    tsFileConfig.setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    tsFileConfig.setPageSizeInByte(pageSizeInByte);
    tsFileConfig.setGroupSizeInByte(groupSizeInByte);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(groupSizeInByte);
    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : TestConstant.create_sql) {
        statement.execute(sql);
      }

      statement.execute("SET STORAGE GROUP TO root.fans");
      statement.execute("CREATE TIMESERIES root.fans.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE");
      statement.execute("CREATE TIMESERIES root.fans.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE");

      for (int time = 1; time < 10; time++) {

        String sql =
            String.format("insert into root.fans.d0(timestamp,s0) values(%s,%s)", time, time % 10);
        statement.execute(sql);
        sql = String.format("insert into root.fans.d0(timestamp,s1) values(%s,%s)", time, time % 5);
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  // "select * from root.vehicle" : test select wild data
  @Test
  public void selectAllTest() throws ClassNotFoundException {
    String[] retArray =
        new String[] {
          "1,1,1", "2,2,2", "3,3,3", "4,4,4", "5,5,0", "6,6,1", "7,7,2", "8,8,3", "9,9,4"
        };

    String selectSql = "select * from root";

    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement1 = connection.createStatement();
        Statement statement2 = connection.createStatement()) {
      statement1.setFetchSize(10);
      boolean hasResultSet1 = statement1.execute(selectSql);
      Assert.assertTrue(hasResultSet1);
      ResultSet resultSet1 = statement1.getResultSet();
      int cnt1 = 0;
      try {
        while (resultSet1.next() && cnt1 < 5) {
          StringBuilder builder = new StringBuilder();
          builder
              .append(resultSet1.getString(TestConstant.TIMESTAMP_STR))
              .append(",")
              .append(resultSet1.getString("root.fans.d0.s0"))
              .append(",")
              .append(resultSet1.getString("root.fans.d0.s1"));
          Assert.assertEquals(retArray[cnt1], builder.toString());
          cnt1++;
        }

        statement2.setFetchSize(10);
        boolean hasResultSet2 = statement2.execute(selectSql);
        Assert.assertTrue(hasResultSet2);
        ResultSet resultSet2 = statement2.getResultSet();
        int cnt2 = 0;
        try {
          while (resultSet2.next()) {
            StringBuilder builder = new StringBuilder();
            builder
                .append(resultSet2.getString(TestConstant.TIMESTAMP_STR))
                .append(",")
                .append(resultSet2.getString("root.fans.d0.s0"))
                .append(",")
                .append(resultSet2.getString("root.fans.d0.s1"));
            Assert.assertEquals(retArray[cnt2], builder.toString());
            cnt2++;
          }
        } finally {
          resultSet2.close();
        }
        Assert.assertEquals(9, cnt2);

        // use do-while instead of while because in the previous while loop, we have executed the
        // next function,
        // and the cursor has been moved to the next position, so we should fetch that value first.
        do {
          StringBuilder builder = new StringBuilder();
          builder
              .append(resultSet1.getString(TestConstant.TIMESTAMP_STR))
              .append(",")
              .append(resultSet1.getString("root.fans.d0.s0"))
              .append(",")
              .append(resultSet1.getString("root.fans.d0.s1"));
          Assert.assertEquals(retArray[cnt1], builder.toString());
          cnt1++;
        } while (resultSet1.next());
        // Although the statement2 has the same sql as statement1, they shouldn't affect each other.
        // So the statement1's ResultSet should also have 9 rows in total.
        Assert.assertEquals(9, cnt1);
      } finally {
        resultSet1.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
