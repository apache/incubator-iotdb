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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class IndexContractExample {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    try (Connection connection = DriverManager
        .getConnection("jdbc:iotdb://127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
//      insertData(statement);

      long startTime = System.nanoTime();
      ResultSet resultSet = statement.executeQuery("SELECT sum(*) FROM root WHERE time < 365000");
      System.out.println((System.nanoTime() - startTime) / 1000000);
      outputResult(resultSet);

      statement.execute("CREATE INDEX on root.vehicle.d0.s0 USING pisa");
      statement.execute("DROP INDEX pisa ON root.vehicle.d0.s0");
    }
  }

  private static void insertData(Statement statement) throws SQLException {
    // insert large amount of data time range : 0 ~ 730000
    for (int time = 0; time < 730000; time++) {
      String sql = String
          .format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
      statement.execute(sql);
      if (time % 667 == 1) {
        statement.execute("FLUSH");
      }
    }
  }
}
