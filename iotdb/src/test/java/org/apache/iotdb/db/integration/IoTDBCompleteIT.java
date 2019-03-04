/**
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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the IoTDB server should be
 * defined as integration test.
 */
public class IoTDBCompleteIT {

  private IoTDB daemon;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.closeMemControl();
    daemon = IoTDB.getInstance();
    daemon.active();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    daemon.stop();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void test() throws ClassNotFoundException, SQLException {
    String[] sqls = {"SET STORAGE GROUP TO root.vehicle"};
    executeSQL(sqls);
    simpleTest();
    insertTest();
    selectTest();
    deleteTest();
  }

  public void simpleTest() throws ClassNotFoundException, SQLException {
    String[] sqlS = {"CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
        "SHOW TIMESERIES",
        "===  Timeseries Tree  ===\n"
            + "\n"
            + "root:{\n"
            + "    vehicle:{\n"
            + "        d0:{\n"
            + "            s0:{\n"
            + "                 DataType: INT32,\n"
            + "                 Encoding: RLE,\n"
            + "                 Compressor: UNCOMPRESSED,\n"
            + "                 args: {},\n"
            + "                 StorageGroup: root.vehicle\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}",
        "DELETE TIMESERIES root.vehicle.d0.s0",
        "SHOW TIMESERIES",
        "===  Timeseries Tree  ===\n"
            + "\n"
            + "root:{\n"
            + "    vehicle\n"
            + "}",
        "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=BOOLEAN,ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64,ENCODING=TS_2DIFF",
        "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT,ENCODING=GORILLA",
        "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=DOUBLE,ENCODING=RLE",
        "CREATE TIMESERIES root.vehicle.d1.s5 WITH DATATYPE=TEXT,ENCODING=PLAIN",
        "CREATE TIMESERIES root.vehicle.d2.s6 WITH DATATYPE=INT32,ENCODING=TS_2DIFF,compressor=UNCOMPRESSED",
        "CREATE TIMESERIES root.vehicle.d3.s7 WITH DATATYPE=INT32,ENCODING=RLE,compressor=SNAPPY",
        "CREATE TIMESERIES root.vehicle.d4.s8 WITH DATATYPE=INT32,ENCODING=RLE,MAX_POINT_NUMBER=100",
        "CREATE TIMESERIES root.vehicle.d5.s9 WITH DATATYPE=FLOAT,ENCODING=PLAIN,compressor=SNAPPY,MAX_POINT_NUMBER=10",
        "CREATE TIMESERIES root.vehicle.d6.s10 WITH DATATYPE=DOUBLE,ENCODING=RLE,compressor=UNCOMPRESSED,MAX_POINT_NUMBER=10",
        "DELETE TIMESERIES root.vehicle.d0.*",
        "SHOW TIMESERIES",
        "===  Timeseries Tree  ===\n"
            + "\n"
            + "root:{\n"
            + "    vehicle:{\n"
            + "        d1:{\n"
            + "            s5:{\n"
            + "                 DataType: TEXT,\n"
            + "                 Encoding: PLAIN,\n"
            + "                 Compressor: UNCOMPRESSED,\n"
            + "                 args: {},\n"
            + "                 StorageGroup: root.vehicle\n"
            + "            }\n"
            + "        },\n"
            + "        d2:{\n"
            + "            s6:{\n"
            + "                 DataType: INT32,\n"
            + "                 Encoding: TS_2DIFF,\n"
            + "                 Compressor: UNCOMPRESSED,\n"
            + "                 args: {},\n"
            + "                 StorageGroup: root.vehicle\n"
            + "            }\n"
            + "        },\n"
            + "        d3:{\n"
            + "            s7:{\n"
            + "                 DataType: INT32,\n"
            + "                 Encoding: RLE,\n"
            + "                 Compressor: SNAPPY,\n"
            + "                 args: {},\n"
            + "                 StorageGroup: root.vehicle\n"
            + "            }\n"
            + "        },\n"
            + "        d4:{\n"
            + "            s8:{\n"
            + "                 DataType: INT32,\n"
            + "                 Encoding: RLE,\n"
            + "                 Compressor: UNCOMPRESSED,\n"
            + "                 args: {max_point_number=100},\n"
            + "                 StorageGroup: root.vehicle\n"
            + "            }\n"
            + "        },\n"
            + "        d5:{\n"
            + "            s9:{\n"
            + "                 DataType: FLOAT,\n"
            + "                 Encoding: PLAIN,\n"
            + "                 Compressor: SNAPPY,\n"
            + "                 args: {max_point_number=10},\n"
            + "                 StorageGroup: root.vehicle\n"
            + "            }\n"
            + "        },\n"
            + "        d6:{\n"
            + "            s10:{\n"
            + "                 DataType: DOUBLE,\n"
            + "                 Encoding: RLE,\n"
            + "                 Compressor: UNCOMPRESSED,\n"
            + "                 args: {max_point_number=101},\n"
            + "                 StorageGroup: root.vehicle\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}",
        "DELETE TIMESERIES root.vehicle.*",
        "SHOW TIMESERIES",
        "===  Timeseries Tree  ===\n"
            + "\n"
            + "root:{\n"
            + "    vehicle\n"
            + "}"
    };
    executeSQL(sqlS);
  }

  public void insertTest() throws ClassNotFoundException, SQLException {
    String[] sqlS = {"CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,101)",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.vehicle.d0(timestamp,s0,s1) values(2,102,202)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(NOW(),104)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(2000-01-01T08:00:00+08:00,105)",
        "SELECT * FROM root.vehicle.d0",
        "1,101,null,\n" + "2,102,202,\n" + "946684800000,105,null,\n" + "NOW(),104,null,\n",
        "DELETE TIMESERIES root.vehicle.*"};
    executeSQL(sqlS);
  }

  public void deleteTest() throws ClassNotFoundException, SQLException {
    String[] sqlS = {"CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,1)", "SELECT * FROM root.vehicle.d0",
        "1,1,\n" + "2,1,\n" + "3,1,\n" + "4,1,\n" + "5,1,\n" + "6,1,\n" + "7,1,\n" + "8,1,\n"
            + "9,1,\n"
            + "10,1,\n",
        "DELETE FROM root.vehicle.d0.s0 WHERE time < 8", "SELECT * FROM root.vehicle.d0",
        "8,1,\n" + "9,1,\n" + "10,1,\n",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(2000-01-01T08:00:00+08:00,1)",
        "SELECT * FROM root.vehicle.d0", "8,1,\n" + "9,1,\n" + "10,1,\n" + "946684800000,1,\n",
        "DELETE FROM root.vehicle.d0.s0 WHERE time < 2000-01-02T08:00:00+08:00",
        "SELECT * FROM root.vehicle.d0", "",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(NOW(),1)",
        "SELECT * FROM root.vehicle.d0", "NOW(),1,\n",
        "DELETE FROM root.vehicle.d0.s0 WHERE time <= NOW()",
        "SELECT * FROM root.vehicle.d0", "",
        "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,1)",
        "INSERT INTO root.vehicle.d1(timestamp,s1) values(1,1)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,5)",
        "INSERT INTO root.vehicle.d1(timestamp,s1) values(5,5)", "SELECT * FROM root.vehicle",
        "1,1,1,\n" + "5,5,5,\n", "DELETE FROM root.vehicle.d0.s0,root.vehicle.d1.s1 WHERE time < 3",
        "SELECT * FROM root.vehicle", "5,5,5,\n", "DELETE FROM root.vehicle.* WHERE time < 7",
        "SELECT * FROM root.vehicle", "", "DELETE TIMESERIES root.vehicle.*"};
    executeSQL(sqlS);
  }

  public void selectTest() throws ClassNotFoundException, SQLException {
    String[] sqlS = {"CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,101)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,102)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,103)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,104)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,105)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,106)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,107)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,108)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,109)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,110)",
        "SELECT * FROM root.vehicle.d0 WHERE s0 < 104", "1,101,\n" + "2,102,\n" + "3,103,\n",
        "SELECT * FROM root.vehicle.d0 WHERE s0 > 105 and time < 8", "6,106,\n" + "7,107,\n",
        "SELECT * FROM root.vehicle.d0",
        "1,101,\n" + "2,102,\n" + "3,103,\n" + "4,104,\n" + "5,105,\n"
            + "6,106,\n" + "7,107,\n" + "8,108,\n" + "9,109,\n" + "10,110,\n",
        "DELETE TIMESERIES root.vehicle.*"};
    executeSQL(sqlS);
  }

  public void funcTest() throws ClassNotFoundException, SQLException {
    String[] sqlS = {"CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,110)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,109)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,108)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,107)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,106)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,105)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,104)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,103)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,102)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,101)",
        "SELECT COUNT(s0) FROM root.vehicle.d0",
        "0,10,\n", "SELECT COUNT(s0) FROM root.vehicle.d0 WHERE root.vehicle.d0.s0 < 105", "0,4,\n",
        "SELECT MAX_TIME(s0) FROM root.vehicle.d0", "0,10,\n",
        "SELECT MAX_TIME(s0) FROM root.vehicle.d0 WHERE root.vehicle.d0.s0 > 105", "0,5,\n",
        "SELECT MIN_TIME(s0) FROM root.vehicle.d0", "0,1,\n",
        "SELECT MIN_TIME(s0) FROM root.vehicle.d0 WHERE root.vehicle.d0.s0 < 106", "0,6,\n",
        "SELECT MAX_VALUE(s0) FROM root.vehicle.d0", "0,110,\n",
        "SELECT MAX_VALUE(s0) FROM root.vehicle.d0 WHERE time > 4", "0,106,\n",
        "SELECT MIN_VALUE(s0) FROM root.vehicle.d0", "0,101,\n",
        "SELECT MIN_VALUE(s0) FROM root.vehicle.d0 WHERE time < 5", "0,107,\n",
        "DELETE FROM root.vehicle.d0.s0 WHERE time <= 10",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(NOW(),5)",
        "SELECT * FROM root.vehicle.d0",
        "NOW(),5,\n", "UPDATE root.vehicle.d0 SET s0 = 10 WHERE time <= NOW()",
        "SELECT * FROM root.vehicle.d0",
        "NOW(),10,\n", "DELETE FROM root.vehicle.d0.s0 WHERE time <= NOW()",
        "SELECT * FROM root.vehicle.d0",
        "", "DELETE TIMESERIES root.vehicle.*"};
    executeSQL(sqlS);
  }

  public void groupByTest() throws ClassNotFoundException, SQLException {
    String[] sqlS = {"CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(1,110)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(2,109)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(3,108)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(4,107)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(5,106)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(6,105)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(7,104)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(8,103)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(9,102)",
        "INSERT INTO root.vehicle.d0(timestamp,s0) values(10,101)",
        "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT32,ENCODING=RLE",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(1,101)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(2,102)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(3,103)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(4,104)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(5,105)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(6,106)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(7,107)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(8,108)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(9,109)",
        "INSERT INTO root.vehicle.d0(timestamp,s1) values(10,110)",
        "SELECT COUNT(s0), COUNT(s1) FROM root.vehicle.d0 WHERE s1 < 109 GROUP BY(4ms,[1,10])",
        "1,3,3,\n" + "4,4,4,\n" + "8,1,1,\n",
        "SELECT COUNT(s0), MAX_VALUE(s1) FROM root.vehicle.d0 WHERE time < 7 GROUP BY(3ms,2,[1,5])",
        "1,1,101,\n" + "2,3,104,\n" + "5,1,105,\n",
        "SELECT MIN_VALUE(s0), MAX_TIME(s1) FROM root.vehicle.d0 WHERE s1 > 102 and time < 9 GROUP BY(3ms,1,[1,4],[6,9])",
        "1,108,3,\n" + "4,105,6,\n" + "7,103,8,\n", "DELETE TIMESERIES root.vehicle.*"};
    executeSQL(sqlS);
  }

  private void executeSQL(String[] sqls) throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      String result = "";
      Long now_start = 0L;
      boolean cmp = false;
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
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
          Statement statement = connection.createStatement();
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
                if (now_start > 0L && column[i - 1] == Constant.TIMESTAMP_STR) {
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
          statement.close();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
