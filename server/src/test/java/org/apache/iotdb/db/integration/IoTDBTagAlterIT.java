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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBTagAlterIT {

  @Before
  public void setUp() {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void renameTest() throws ClassNotFoundException {
    String[] ret1 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag2\":\"v2\",\"tagNew1\":\"v1\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT,"
            + " encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)";
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      boolean hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret1[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret1.length, count);

      try {
        statement.execute("ALTER timeseries root.turbine.d1.s1 RENAME tag3 TO tagNew3");
        fail();
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .contains(
                    "TimeSeries [PartialPath{fullPath=root.turbine.d1.s1, majorVersion=0, minorVersion=0}] does not have tag/attribute [tag3]."));
      }

      try {
        statement.execute("ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO tag2");
        fail();
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .contains(
                    "TimeSeries [PartialPath{fullPath=root.turbine.d1.s1, majorVersion=0, minorVersion=0}] already has a tag/attribute named [tag2]."));
      }

      statement.execute("ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO tagNew1");
      hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      resultSet = statement.getResultSet();
      count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void setTest() throws ClassNotFoundException {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"newV1\",\"tag2\":\"v2\"},{\"attr2\":\"newV2\",\"attr1\":\"v1\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)";
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      boolean hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      try {
        statement.execute("ALTER timeseries root.turbine.d1.s1 SET tag3=v3");
        fail();
      } catch (Exception e) {
        assertTrue(
            e.getMessage()
                .contains(
                    "TimeSeries [PartialPath{fullPath=root.turbine.d1.s1, majorVersion=0, minorVersion=0}] does not have tag/attribute [tag3]."));
      }

      statement.execute("ALTER timeseries root.turbine.d1.s1 SET tag1=newV1, attr2=newV2");
      hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      resultSet = statement.getResultSet();
      count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void dropTest() throws ClassNotFoundException {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"tag2\":\"v2\"},{\"attr2\":\"v2\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)";
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      boolean hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute("ALTER timeseries root.turbine.d1.s1 DROP attr1,tag1");
      hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      resultSet = statement.getResultSet();
      count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);

      try {
        statement.execute("show timeseries where tag1=v1");
        fail();
      } catch (Exception e) {
        assertTrue(e.getMessage().contains("The key tag1 is not a tag"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void addTagTest() throws ClassNotFoundException {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag4\":\"v4\",\"tag2\":\"v2\",\"tag3\":\"v3\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)";
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      boolean hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute("ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3=v3, tag4=v4");
      hasResult = statement.execute("show timeseries where tag3=v3");
      assertTrue(hasResult);
      resultSet = statement.getResultSet();
      count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void addAttributeTest() throws ClassNotFoundException {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\",\"attr4\":\"v4\",\"attr3\":\"v3\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)";
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      boolean hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute("ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3=v3, attr4=v4");
      hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      resultSet = statement.getResultSet();
      count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void upsertTest() throws ClassNotFoundException {
    String[] ret = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,"
          + "{\"tag1\":\"v1\",\"tag2\":\"v2\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret2 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"tag1\":\"v1\",\"tag2\":\""
          + "newV2\",\"tag3\":\"v3\"},{\"attr2\":\"v2\",\"attr1\":\"v1\"}"
    };
    String[] ret3 = {
      "root.turbine.d1.s1,temperature,root.turbine,FLOAT,RLE,SNAPPY,{\"tag1\":\"newV1\",\"tag2\":\""
          + "newV2\",\"tag3\":\"newV3\"},{\"attr2\":\"v2\",\"attr1\":\"newA1\",\"attr3\":\"v3\"}"
    };

    String sql =
        "create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY "
            + "tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)";
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
      boolean hasResult = statement.execute("show timeseries");
      assertTrue(hasResult);
      ResultSet resultSet = statement.getResultSet();
      int count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret.length, count);

      statement.execute("ALTER timeseries root.turbine.d1.s1 UPSERT TAGS(tag3=v3, tag2=newV2)");
      hasResult = statement.execute("show timeseries where tag3=v3");
      assertTrue(hasResult);
      resultSet = statement.getResultSet();
      count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret2[count], ans);
          count++;
        }
      } finally {
        resultSet.close();
      }
      assertEquals(ret2.length, count);

      statement.execute(
          "ALTER timeseries root.turbine.d1.s1 UPSERT TAGS(tag1=newV1, tag3=newV3) ATTRIBUTES(attr1=newA1, attr3=v3)");
      hasResult = statement.execute("show timeseries where tag3=newV3");
      assertTrue(hasResult);
      resultSet = statement.getResultSet();
      count = 0;
      try {
        while (resultSet.next()) {
          String ans =
              resultSet.getString("timeseries")
                  + ","
                  + resultSet.getString("alias")
                  + ","
                  + resultSet.getString("storage group")
                  + ","
                  + resultSet.getString("dataType")
                  + ","
                  + resultSet.getString("encoding")
                  + ","
                  + resultSet.getString("compression")
                  + ","
                  + resultSet.getString("tags")
                  + ","
                  + resultSet.getString("attributes");
          assertEquals(ret3[count], ans);
          count++;
        }
        assertEquals(ret3.length, count);

        statement.execute("show timeseries where tag3=v3");
        resultSet = statement.getResultSet();
        assertFalse(resultSet.next());
      } finally {
        resultSet.close();
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
