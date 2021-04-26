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
package org.apache.iotdb.db.index.it;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.index.IndexManager;
import org.apache.iotdb.db.index.common.math.Randomwalk;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.apache.iotdb.db.index.common.IndexType.NO_INDEX;
import static org.junit.Assert.fail;

public class NoIndexWriteIT {

  private static final String insertPattern = "INSERT INTO %s(timestamp, %s) VALUES (%d, %.3f)";

  private static final String storageGroupSub = "root.wind1";
  private static final String storageGroupWhole = "root.wind2";

  private static final String speed1 = "root.wind1.azq01.speed";
  private static final String speed1Device = "root.wind1.azq01";
  private static final String speed1Sensor = "speed";

  private static final String directionDevicePattern = "root.wind2.%d";
  private static final String directionPattern = "root.wind2.%d.direction";
  private static final String directionSensor = "direction";

  private static final String indexSub = speed1;
  private static final String indexWhole = "root.wind2.*.direction";
  private static final int wholeSize = 5;
  private static final int wholeDim = 100;
  private static final int subLength = 10_000;

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(true);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
  }

  @Test
  public void checkWrite() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    //    IoTDBDescriptor.getInstance().getConfig().setEnableIndex(false);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement(); ) {
      long start = System.currentTimeMillis();
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupSub));
      statement.execute(String.format("SET STORAGE GROUP TO %s", storageGroupWhole));

      System.out.println(
          String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", speed1));
      statement.execute(
          String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", speed1));

      for (int i = 0; i < wholeSize; i++) {
        String wholePath = String.format(directionPattern, i);
        statement.execute(
            String.format("CREATE TIMESERIES %s WITH DATATYPE=FLOAT,ENCODING=PLAIN", wholePath));
      }
      long startCreateIndex = System.currentTimeMillis();
      statement.execute(String.format("CREATE INDEX ON %s WITH INDEX=%s", indexSub, NO_INDEX));
      statement.execute(String.format("CREATE INDEX ON %s WITH INDEX=%s", indexWhole, NO_INDEX));

      System.out.println(IndexManager.getInstance().getRouter());
      Assert.assertEquals(
          "<{NO_INDEX=[type: NO_INDEX, time: 0, props: {}]}\n"
              + "root.wind2.*.direction: {NO_INDEX=NO_INDEX}>\n"
              + "<{NO_INDEX=[type: NO_INDEX, time: 0, props: {}]}\n"
              + "root.wind1.azq01.speed: {NO_INDEX=NO_INDEX}>\n",
          IndexManager.getInstance().getRouter().toString());
      TVList subInput = Randomwalk.generateRanWalkTVList(subLength);
      long startInsertSub = System.currentTimeMillis();
      for (int i = 0; i < subInput.size(); i++) {
        statement.execute(
            String.format(
                insertPattern,
                speed1Device,
                speed1Sensor,
                subInput.getTime(i),
                subInput.getDouble(i)));
      }
      statement.execute("flush");
      System.out.println("insert finish for subsequence case");
      System.out.println(IndexManager.getInstance().getRouter());
      Assert.assertEquals(
          "<{NO_INDEX=[type: NO_INDEX, time: 0, props: {}]}\n"
              + "root.wind2.*.direction: {NO_INDEX=NO_INDEX}>\n"
              + "<{NO_INDEX=[type: NO_INDEX, time: 0, props: {}]}\n"
              + "root.wind1.azq01.speed: {NO_INDEX=NO_INDEX}>\n",
          IndexManager.getInstance().getRouter().toString());

      long startInsertWhole = System.currentTimeMillis();
      TVList wholeInput = Randomwalk.generateRanWalkTVList(wholeDim * wholeSize);
      for (int i = 0; i < wholeSize; i++) {
        String device = String.format(directionDevicePattern, i);
        statement.execute(
            String.format(
                insertPattern,
                device,
                directionSensor,
                wholeInput.getTime(i),
                wholeInput.getDouble(i)));
      }
      statement.execute("flush");
      long end = System.currentTimeMillis();
      System.out.println(IndexManager.getInstance().getRouter());
      Assert.assertEquals(
          "<{NO_INDEX=[type: NO_INDEX, time: 0, props: {}]}\n"
              + "root.wind2.*.direction: {NO_INDEX=NO_INDEX}>\n"
              + "<{NO_INDEX=[type: NO_INDEX, time: 0, props: {}]}\n"
              + "root.wind1.azq01.speed: {NO_INDEX=NO_INDEX}>\n",
          IndexManager.getInstance().getRouter().toString());
      System.out.println("insert finish for subsequence case");
      System.out.println(String.format("create series use %d ms", (startCreateIndex - start)));
      System.out.println(
          String.format("create index  use %d ms", (startInsertSub - startCreateIndex)));
      System.out.println(
          String.format("insert sub    use %d ms", (startInsertWhole - startInsertSub)));
      System.out.println(String.format("insert whole  use %d ms", (end - startInsertWhole)));

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
