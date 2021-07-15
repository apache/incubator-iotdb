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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.metadata.MManager.TIME_SERIES_TREE_HEADER;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBMetadataFetchIT {

  private DatabaseMetaData databaseMetaData;
  private static final Logger logger = LoggerFactory.getLogger(IoTDBMetadataFetchIT.class);

  private static void insertSQL() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] insertSqls =
          new String[] {
            "SET STORAGE GROUP TO root.ln.wf01.wt01",
            "SET STORAGE GROUP TO root.ln1.wf01.wt01",
            "SET STORAGE GROUP TO root.ln2.wf01.wt01",
            "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE = BOOLEAN, ENCODING = PLAIN",
            "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE = FLOAT, ENCODING = RLE, "
                + "compressor = SNAPPY, MAX_POINT_NUMBER = 3"
          };

      for (String sql : insertSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      logger.error("insertSQL() failed", e);
      fail(e.getMessage());
    }
  }

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();

    insertSQL();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void showTimeseriesTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      String[] sqls =
          new String[] {
            "show timeseries root.ln.wf01.wt01.status", // full seriesPath
            "show timeseries root.ln", // prefix seriesPath
            "show timeseries root.ln.*.wt01", // seriesPath with stars
            "show timeseries", // the same as root
            "show timeseries root.a.b", // nonexistent timeseries, thus returning ""
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,")),
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01.status,null,root.ln.wf01.wt01,BOOLEAN,PLAIN,SNAPPY,null,null,",
                    "root.ln.wf01.wt01.temperature,null,root.ln.wf01.wt01,FLOAT,RLE,SNAPPY,null,null,")),
            new HashSet<>(Collections.singletonList(""))
          };
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                Assert.assertTrue(standard.contains(builder.toString()));
              }
            }
          }
        } catch (SQLException e) {
          logger.error("showTimeseriesTest() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showStorageGroupTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "show storage group",
            "show storage group root.ln.wf01",
            "show storage group root.ln.wf01.wt01.status"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList("root.ln.wf01.wt01,", "root.ln1.wf01.wt01,", "root.ln2.wf01.wt01,")),
            new HashSet<>(Collections.singletonList("root.ln.wf01.wt01,")),
            new HashSet<>(Collections.singletonList(""))
          };

      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                Assert.assertTrue(standard.contains(builder.toString()));
              }
            }
          }
        } catch (SQLException e) {
          logger.error("showStorageGroupTest() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void databaseMetaDataTest() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection =
          DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      databaseMetaData = connection.getMetaData();
      showTimeseriesInJson();

    } catch (Exception e) {
      logger.error("databaseMetaDataTest() failed", e);
      fail(e.getMessage());
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Test
  public void showVersion() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String sql = "show version";
      try {
        boolean hasResultSet = statement.execute(sql);
        if (hasResultSet) {
          try (ResultSet resultSet = statement.getResultSet()) {
            resultSet.next();
            Assert.assertEquals(IoTDBConstant.VERSION, resultSet.getString(1));
          }
        }
      } catch (Exception e) {
        fail(e.getMessage());
      }
    }
  }

  @Test
  public void showDevicesWithSgTest() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "show devices root.ln with storage group", "show devices root.ln.wf01.wt01.temperature"
          };
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(
                Arrays.asList(
                    "root.ln.wf01.wt01,root.ln.wf01.wt01,",
                    "root.ln.wf01.wt01.status,root.ln.wf01.wt01,")),
            new HashSet<>(Collections.singletonList(""))
          };

      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                Assert.assertTrue(standard.contains(builder.toString()));
              }
            }
          }
        } catch (SQLException e) {
          logger.error("showDevicesTest() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showDevicesTest() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {"show devices root.ln", "show devices root.ln.wf01.wt01.temperature"};
      Set<String>[] standards =
          new Set[] {
            new HashSet<>(Arrays.asList("root.ln.wf01.wt01,", "root.ln.wf01.wt01.status,")),
            new HashSet<>(Collections.singletonList(""))
          };

      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                Assert.assertTrue(standard.contains(builder.toString()));
              }
            }
          }
        } catch (SQLException e) {
          logger.error("showDevicesTest() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showChildPaths() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"show child paths root.ln"};
      String[] standards = new String[] {"root.ln.wf01,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showChildPaths() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showChildNodes() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"show child nodes root.ln"};
      String[] standards = new String[] {"wf01,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showChildNodes() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountTimeSeries() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"COUNT TIMESERIES root.ln", "COUNT TIMESERIES"};
      String[] standards = new String[] {"2,\n", "2,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showCountTimeSeries() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountDevices() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "COUNT DEVICES root.ln", "COUNT DEVICES", "COUNT DEVICES root.ln.wf01.wt01.temperature"
          };
      String[] standards = new String[] {"1,\n", "1,\n", "0,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showCountDevices() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountStorageGroup() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls =
          new String[] {
            "count storage group root.ln",
            "count storage group",
            "count storage group root.ln.wf01.wt01.status"
          };
      String[] standards = new String[] {"1,\n", "3,\n", "0,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showCountStorageGroup() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountTimeSeriesGroupBy() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"COUNT TIMESERIES root group by level=1"};
      Set<String>[] standards =
          new Set[] {new HashSet<>(Arrays.asList("root.ln,2,", "root.ln1,0,", "root.ln2,0,"))};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        Set<String> standard = standards[n];
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              while (resultSet.next()) {
                StringBuilder builder = new StringBuilder();
                builder.append(resultSet.getString(1)).append(",");
                builder.append(resultSet.getInt(2)).append(",");
                Assert.assertTrue(standard.contains(builder.toString()));
              }
            }
          }
        } catch (SQLException e) {
          logger.error("showCountTimeSeriesGroupBy() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void showCountNodes() throws SQLException, ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      String[] sqls = new String[] {"COUNT NODES root level=1"};
      String[] standards = new String[] {"3,\n"};
      for (int n = 0; n < sqls.length; n++) {
        String sql = sqls[n];
        String standard = standards[n];
        StringBuilder builder = new StringBuilder();
        try {
          boolean hasResultSet = statement.execute(sql);
          if (hasResultSet) {
            try (ResultSet resultSet = statement.getResultSet()) {
              ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
              while (resultSet.next()) {
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                  builder.append(resultSet.getString(i)).append(",");
                }
                builder.append("\n");
              }
            }
          }
          Assert.assertEquals(standard, builder.toString());
        } catch (SQLException e) {
          logger.error("showCountNodes() failed", e);
          fail(e.getMessage());
        }
      }
    }
  }

  /** show metadata in json */
  private void showTimeseriesInJson() {
    String metadataInJson = databaseMetaData.toString();
    String standard =
        "===  Timeseries Tree  ===\n"
            + "\n"
            + "{\n"
            + "\t\"root\":{\n"
            + "\t\t\"ln2\":{\n"
            + "\t\t\t\"wf01\":{\n"
            + "\t\t\t\t\"wt01\":{}\n"
            + "\t\t\t}\n"
            + "\t\t},\n"
            + "\t\t\"ln\":{\n"
            + "\t\t\t\"wf01\":{\n"
            + "\t\t\t\t\"wt01\":{\n"
            + "\t\t\t\t\t\"temperature\":{\n"
            + "\t\t\t\t\t\t\"args\":\"{max_point_number=3}\",\n"
            + "\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf01.wt01\",\n"
            + "\t\t\t\t\t\t\"DataType\":\"FLOAT\",\n"
            + "\t\t\t\t\t\t\"Compressor\":\"SNAPPY\",\n"
            + "\t\t\t\t\t\t\"Encoding\":\"RLE\"\n"
            + "\t\t\t\t\t},\n"
            + "\t\t\t\t\t\"status\":{\n"
            + "\t\t\t\t\t\t\t\"StorageGroup\":\"root.ln.wf01.wt01\",\n"
            + "\t\t\t\t\t\t\t\"DataType\":\"BOOLEAN\",\n"
            + "\t\t\t\t\t\t\t\"Compressor\":\"SNAPPY\",\n"
            + "\t\t\t\t\t\t\t\"Encoding\":\"PLAIN\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t}\n"
            + "\t\t\t}\n"
            + "\t\t},\n"
            + "\t\t\"ln1\":{\n"
            + "\t\t\t\"wf01\":{\n"
            + "\t\t\t\t\"wt01\":{}\n"
            + "\t\t\t}\n"
            + "\t\t}\n"
            + "\t}\n"
            + "}";

    // TODO Remove the constant json String.
    // Do not depends on the sequence of property in json string if you do not
    // explictly mark the sequence, when we use jackson, the json result may change again
    String rawJsonString = metadataInJson.substring(TIME_SERIES_TREE_HEADER.length());
    Gson gson = new Gson();
    JsonObject actual = gson.fromJson(rawJsonString, JsonObject.class);
    JsonObject expected =
        gson.fromJson(standard.substring(TIME_SERIES_TREE_HEADER.length()), JsonObject.class);

    Assert.assertEquals(expected, actual);
  }
}
