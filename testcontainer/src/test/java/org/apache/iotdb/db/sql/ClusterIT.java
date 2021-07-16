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

package org.apache.iotdb.db.sql;

import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.session.Session;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

// do not add tests here.
// add tests into Cases.java instead.
public abstract class ClusterIT extends Cases {

  private static Logger logger = LoggerFactory.getLogger(ClusterIT.class);

  // "root.sg1" is a special storage for testing whether the read and write operations can be run
  // correctly if the data is not on the connected node.
  public String defaultSG = "root.sg1";

  protected int getWriteRpcPort() {
    return getContainer().getServicePort("iotdb-server_1", 6667);
  }

  protected String getWriteRpcIp() {
    return getContainer().getServiceHost("iotdb-server_1", 6667);
  }

  protected int[] getReadRpcPorts() {
    return new int[] {getContainer().getServicePort("iotdb-server_1", 6667)};
  }

  protected String[] getReadRpcIps() {
    return new String[] {getContainer().getServiceHost("iotdb-server_1", 6667)};
  }

  protected void startCluster() {}

  protected abstract DockerComposeContainer getContainer();

  @Before
  public void init() throws Exception {
    startCluster();

    Class.forName(Config.JDBC_DRIVER_NAME);
    writeConnection =
        DriverManager.getConnection(
            "jdbc:iotdb://" + getWriteRpcIp() + ":" + getWriteRpcPort(), "root", "root");
    writeStatement = writeConnection.createStatement();

    int[] readPorts = getReadRpcPorts();
    String[] readIps = getReadRpcIps();
    readConnections = new Connection[readPorts.length];
    readStatements = new Statement[readPorts.length];
    for (int i = 0; i < readPorts.length; i++) {
      readConnections[i] =
          DriverManager.getConnection(
              "jdbc:iotdb://" + readIps[i] + ":" + readPorts[i], "root", "root");
      readStatements[i] = readConnections[i].createStatement();
    }
    session = new Session(getWriteRpcIp(), getWriteRpcPort());
    session.open();
    anotherSession = new Session(getWriteRpcIp(), getWriteRpcPort());
    anotherSession.open();
    TimeUnit.MILLISECONDS.sleep(3000);
  }

  @After
  public void clean() throws Exception {
    super.clean();
  }

  // do not add tests here.
  // add tests into Cases.java instead.
}
