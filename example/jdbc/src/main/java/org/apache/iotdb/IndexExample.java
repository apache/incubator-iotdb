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
package org.apache.iotdb;

import static org.apache.iotdb.JDBCExample.outputResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IndexExample {

  public static String[] create_sql = new String[]{"SET STORAGE GROUP TO root.vehicle",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s5 WITH DATATYPE=DOUBLE, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE",
  };

  private static String[] stringValue = new String[]{"A", "B", "C", "D", "E"};
  private static String[] booleanValue = new String[]{"true", "false"};

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection = DriverManager
        .getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
//      insertData(statement);

      ResultSet resultSet = statement.executeQuery("SELECT * FROM root WHERE time < 10");
      outputResult(resultSet);

      statement.execute("CREATE INDEX on root.vehicle.d0.s0 USING pisa");
      statement.execute("DROP INDEX pisa ON root.vehicle.d0.s0");
    }
  }

  private static void insertData(Statement statement) throws SQLException {
    // insert large amount of data time range : 3000 ~ 13600
    for (int time = 3000; time < 13600; time++) {
      String sql = String
          .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
      statement.execute(sql);
      sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time,
          stringValue[time % 5]);
      statement.execute(sql);
      sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time,
          booleanValue[time % 2]);
      statement.execute(sql);
      sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
      statement.execute(sql);
    }

    for (int time = 13700; time < 24000; time++) {
      String sql = String
          .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
      statement.execute(sql);
    }

    statement.execute("merge");

    // buffwrite data, unsealed file
    for (int time = 100000; time < 101000; time++) {

      String sql = String
          .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
      statement.execute(sql);
    }

    // sequential data, memory data
    for (int time = 200000; time < 201000; time++) {

      String sql = String
          .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
      statement.execute(sql);
    }

    statement.execute("FLUSH");
    // unsequence insert, time < 3000
    for (int time = 2000; time < 2500; time++) {

      String sql = String
          .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
      statement.execute(sql);
      sql = String.format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time,
          stringValue[time % 5]);
      statement.execute(sql);
    }

    for (int time = 100000; time < 100500; time++) {
      String sql = String
          .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 666);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 777);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 888);
      statement.execute(sql);
    }

    statement.execute("FLUSH");
    // unsequence insert, time > 200000
    for (int time = 200900; time < 201000; time++) {

      String sql = String
          .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
      statement.execute(sql);
      sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 7777);
      statement.execute(sql);
      sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 8888);
      statement.execute(sql);
      sql = String
          .format("insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, "goodman");
      statement.execute(sql);
      sql = String.format("insert into root.vehicle.d0(timestamp,s4) values(%s, %s)", time,
          booleanValue[time % 2]);
      statement.execute(sql);
      sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, 9999);
      statement.execute(sql);
    }
  }

}
