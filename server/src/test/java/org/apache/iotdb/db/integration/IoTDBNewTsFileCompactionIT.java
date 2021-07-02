/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBNewTsFileCompactionIT {

  private int prevMergePagePointNumber;
  private int preMaxNumberOfPointsInPage;
  private PartialPath storageGroupPath;
  // the unit is ns
  private static final long MAX_WAIT_TIME_FOR_MERGE = Long.MAX_VALUE;
  private static final float FLOAT_DELTA = 0.00001f;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    prevMergePagePointNumber =
        IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();
    preMaxNumberOfPointsInPage =
        TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();
    storageGroupPath = new PartialPath("root.sg1");
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(1);
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(1);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);

    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      statement.execute("SET STORAGE GROUP TO root.sg1");
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergePagePointNumberThreshold(prevMergePagePointNumber);
    TSFileDescriptor.getInstance()
        .getConfig()
        .setMaxNumberOfPointsInPage(preMaxNumberOfPointsInPage);
  }

  /**
   * first file has only one page for each chunk and only one chunk for each time series second file
   * has only one page for each chunk and only one chunk for each time series
   */
  @Test
  public void test1() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(10000);

      // first file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("FLUSH");

      // second file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has only one page for each chunk and two chunks for each time series second file has
   * only one page for each chunk and only one chunk for each time series
   */
  @Test
  public void test2() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(1);

      // first file
      // two chunks
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      // second file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has two pages for each chunk and only one chunk for each time series second file has
   * only one page for each chunk and only one chunk for each time series
   */
  @Test
  public void test3() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(10000);

      // first file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      // second file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has two pages for each chunk and two chunks for each time series second file has
   * only one page for each chunk and only one chunk for each time series
   */
  @Test
  public void test4() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"},
      {"5", "5"},
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);

      // first file
      // one chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      // another chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      statement.execute("FLUSH");

      // second file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(5, 5)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has only one page for each chunk and only one chunk for each time series second file
   * has two pages for each chunk and only one chunk for each time series
   */
  @Test
  public void test5() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(10000);

      // first file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("FLUSH");

      // second file
      // two pages for one chunk
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has only one page for each chunk and two chunks for each time series second file has
   * two pages for each chunk and only one chunk for each time series
   */
  @Test
  public void test6() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"},
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(1);

      // first file
      // two chunks
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);
      // second file
      // two pages for one chunk
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");

      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has two pages for each chunk and only one chunk for each time series second file has
   * two pages for each chunk and only one chunk for each time series
   */
  @Test
  public void test7() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(10000);

      // first file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      // second file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has two pages for each chunk and two chunks for each time series second file has two
   * pages for each chunk and only one chunk for each time series
   */
  @Test
  public void test8() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"},
      {"5", "5"},
      {"6", "6"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);

      // first file
      // one chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      // another chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      statement.execute("FLUSH");

      // second file
      // two pages for one chunk
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(6, 6)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has only one page for each chunk and only one chunk for each time series second file
   * has only one page for each chunk and two chunks for each time series
   */
  @Test
  public void test9() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(10000);

      // first file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("FLUSH");

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(1);
      // second file
      // two pages for one chunk
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has only one page for each chunk and two chunks for each time series second file has
   * only one page for each chunk and two chunks for each time series
   */
  @Test
  public void test10() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(1);

      // first file
      // two chunks
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      // second file
      // two pages for one chunk
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);

      try (ResultSet resultSet =
          statement.executeQuery("SELECT count(s1) FROM root.sg1.d1 where time < 4")) {
        assertTrue(resultSet.next());
        assertEquals(3L, resultSet.getLong("count(root.sg1.d1.s1)"));
      }
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has two pages for each chunk and only one chunk for each time series second file has
   * only one page for each chunk and two chunks for each time series
   */
  @Test
  public void test11() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(10000);
      // first file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(1);
      // second file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has two pages for each chunk and two chunks for each time series second file has
   * only one page for each chunk and two chunks for each time series
   */
  @Test
  public void test12() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"},
      {"5", "5"},
      {"6", "6"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);
      // first file
      // one chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      // another chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      statement.execute("FLUSH");

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(1);
      // second file
      // two pages for one chunk
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(6, 6)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has only one page for each chunk and only one chunk for each time series second file
   * has two pages for each chunk and two chunks for each time series
   */
  @Test
  public void test13() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"},
      {"5", "5"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(10000);

      // first file
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("FLUSH");

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);
      // second file
      // one chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      // another chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(5, 5)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has only one page for each chunk and two chunks for each time series second file has
   * two pages for each chunk and two chunks for each time series
   */
  @Test
  public void test14() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"},
      {"5", "5"},
      {"6", "6"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(1);

      // first file
      // two chunks
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);
      // second file
      // one chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      // another chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(6, 6)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has two pages for each chunk and only one chunk for each time series second file has
   * two pages for each chunk and two chunks for each time series
   */
  @Test
  public void test15() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"},
      {"5", "5"},
      {"6", "6"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(10000);

      // first file
      // two pages for one chunk
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      statement.execute("FLUSH");

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);
      // second file
      // one chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      // another chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(6, 6)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /**
   * first file has two pages for each chunk and two chunks for each time series second file has two
   * pages for each chunk and two chunks for each time series
   */
  @Test
  public void test16() throws SQLException {
    String[][] retArray = {
      {"1", "1"},
      {"2", "2"},
      {"3", "3"},
      {"4", "4"},
      {"5", "5"},
      {"6", "6"},
      {"7", "7"},
      {"8", "8"}
    };
    int preAvgSeriesPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getAvgSeriesPointNumberThreshold();
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(2);
      // first file
      // one chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(1, 1)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(2, 2)");
      // another chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(3, 3)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(4, 4)");
      statement.execute("FLUSH");

      // second file
      // one chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(5, 5)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(6, 6)");
      // another chunk with two pages
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(7, 7)");
      statement.execute("INSERT INTO root.sg1.d1(time,s1) values(8, 8)");
      statement.execute("FLUSH");

      statement.execute("MERGE");
      assertTrue(waitForMergeFinish());

      int cnt;
      try (ResultSet resultSet = statement.executeQuery("SELECT s1 FROM root.sg1.d1")) {
        cnt = 0;
        while (resultSet.next()) {
          long time = resultSet.getLong("Time");
          float s1 = resultSet.getFloat("root.sg1.d1.s1");
          assertEquals(Long.parseLong(retArray[cnt][0]), time);
          assertEquals(Float.parseFloat(retArray[cnt][1]), s1, FLOAT_DELTA);
          cnt++;
        }
      }
      assertEquals(retArray.length, cnt);
    } catch (StorageEngineException | InterruptedException e) {
      e.printStackTrace();
      fail();

    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setAvgSeriesPointNumberThreshold(preAvgSeriesPointNumberThreshold);
    }
  }

  /** wait until merge is finished */
  private boolean waitForMergeFinish() throws StorageEngineException, InterruptedException {
    StorageGroupProcessor storageGroupProcessor =
        StorageEngine.getInstance().getProcessor(storageGroupPath);
    TsFileResourceManager resourceManager = storageGroupProcessor.getTsFileResourceManager();

    long startTime = System.nanoTime();
    // get the size of level 1's tsfile list to judge whether merge is finished
    while (CompactionScheduler.currentTaskNum.get() != 0) {
      TimeUnit.MILLISECONDS.sleep(100);
      // wait too long, just break
      if ((System.nanoTime() - startTime) >= MAX_WAIT_TIME_FOR_MERGE) {
        break;
      }
    }
    return resourceManager.getTsFileList(true).size() == 1;
  }
}
