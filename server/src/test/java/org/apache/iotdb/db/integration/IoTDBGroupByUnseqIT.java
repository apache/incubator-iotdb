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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.count;
import static org.junit.Assert.fail;

public class IoTDBGroupByUnseqIT {

  private static String[] dataSet1 =
      new String[] {
        "SET STORAGE GROUP TO root.sg1",
        "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(time,s1) values(1, 1)",
        "INSERT INTO root.sg1.d1(time,s1) values(2, 2)",
        "INSERT INTO root.sg1.d1(time,s1) values(3, 3)",
        "INSERT INTO root.sg1.d1(time,s1) values(4, 4)",
        "INSERT INTO root.sg1.d1(time,s1) values(8, 8)",
        "INSERT INTO root.sg1.d1(time,s1) values(10, 10)",
        "INSERT INTO root.sg1.d1(time,s1) values(11, 11)",
        "INSERT INTO root.sg1.d1(time,s1) values(12, 12)",
        "flush",
        "INSERT INTO root.sg1.d1(time,s1) values(7, 7)",
        "INSERT INTO root.sg1.d1(time,s1) values(9, 9)",
        "flush"
      };

  private static String[] dataSet2 =
      new String[] {
        "SET STORAGE GROUP TO root.sg2",
        "CREATE TIMESERIES root.sg2.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg2.d1(time,s1) values(1, 1)",
        "INSERT INTO root.sg2.d1(time,s1) values(10, 10)",
        "flush",
        "INSERT INTO root.sg2.d1(time,s1) values(19, 19)",
        "INSERT INTO root.sg2.d1(time,s1) values(30, 30)",
        "flush",
        "INSERT INTO root.sg2.d1(time,s1) values(5, 5)",
        "INSERT INTO root.sg2.d1(time,s1) values(15, 15)",
        "INSERT INTO root.sg2.d1(time,s1) values(26, 26)",
        "INSERT INTO root.sg2.d1(time,s1) values(30, 30)",
        "flush"
      };

  private int maxNumberOfPointsInPage;

  @Before
  public void setUp() throws Exception {
    Class.forName(Config.JDBC_DRIVER_NAME);
    EnvironmentUtils.closeStatMonitor();
    maxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(4);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
  }

  /**
   * This test contains one seq file and one unseq file. In the seq file, it contains two pages:
   * 1,2,3,4 in one page, 8,10,11,12 in another page. In the unseq file, it only contains one page:
   * 7, 9. The unseq page is overlapped with the second seq page.
   */
  @Test
  public void test1() {
    String[] retArray1 =
        new String[] {
          "1,3", "4,1", "7,3", "10,3",
        };

    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }

      boolean hasResultSet =
          statement.execute("select count(s1) from root.sg1.d1 group by ([1, 13), 3ms)");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg1.d1.s1"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * This test contains two seq files and one unseq file. In the first seq file, it contains two
   * points: [1, 10]. In the second seq file, it contains two points: [19, 30]. In the unseq file,
   * it contains two CHUNKS: [5, 15], [26, 30]. The unseq file is overlapped with two seq files.
   * While the chunk [19,30] in the second seq file is unpacked, it should replace [26,30] as the
   * first chunk.
   */
  @Test
  public void test2() {
    String[] retArray1 = new String[] {"5,1", "10,1", "15,2", "20,0", "25,1"};

    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);

      for (String sql : dataSet2) {
        statement.execute(sql);
      }

      boolean hasResultSet =
          statement.execute("select count(s1) from root.sg2.d1 group by ([5, 30), 5ms)");

      Assert.assertTrue(hasResultSet);
      int cnt = 0;
      try (ResultSet resultSet = statement.getResultSet()) {
        while (resultSet.next()) {
          String ans =
              resultSet.getString(TIMESTAMP_STR)
                  + ","
                  + resultSet.getString(count("root.sg2.d1.s1"));
          Assert.assertEquals(retArray1[cnt], ans);
          cnt++;
        }
        Assert.assertEquals(retArray1.length, cnt);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }
}
