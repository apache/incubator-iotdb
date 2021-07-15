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

import java.sql.*;

public class IoTDBBigDateTimeIT {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSBigDateTime() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.setFetchSize(5);
      statement.execute("SET STORAGE GROUP TO root.sg");

      statement.execute("CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=DOUBLE, ENCODING=PLAIN;");

      statement.execute("insert into root.sg.d1(time,s2) values (1618283005586000, 8.76);");
      statement.execute("select * from root.sg.d1;");
      statement.execute("select * from root.sg.d1 where time=53251-05-07T17:06:26.000+08:00");
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
