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

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.sql.*;

// do not add tests here.
// add tests into Cases.java instead.
public class SingleNodeIT extends Cases {

  private static Logger logger = LoggerFactory.getLogger(SingleNodeIT.class);

  @Rule
  public GenericContainer dslContainer =
      new GenericContainer(DockerImageName.parse("apache/iotdb:maven-development"))
          .withImagePullPolicy(PullPolicy.defaultPolicy())
          // mount another properties for changing parameters, e.g., open 5555 port (sync module)
          .withFileSystemBind(
              new File("src/test/resources/iotdb-engine.properties").getAbsolutePath(),
              "/iotdb/conf/iotdb-engine.properties",
              BindMode.READ_ONLY)
          .withFileSystemBind(
              new File("src/test/resources/logback-container.xml").getAbsolutePath(),
              "/iotdb/conf/logback.xml",
              BindMode.READ_ONLY)
          .withLogConsumer(new Slf4jLogConsumer(logger))
          .withExposedPorts(6667)
          .waitingFor(Wait.forListeningPort());

  int rpcPort = 6667;
  int syncPort = 5555;

  @Before
  public void init() throws Exception {
    rpcPort = dslContainer.getMappedPort(6667);
    syncPort = dslContainer.getMappedPort(5555);
    Class.forName(Config.JDBC_DRIVER_NAME);
    readConnections = new Connection[1];
    readStatements = new Statement[1];
    writeConnection =
        readConnections[0] =
            DriverManager.getConnection("jdbc:iotdb://127.0.0.1:" + rpcPort, "root", "root");
    writeStatement = readStatements[0] = writeConnection.createStatement();
    session = new Session("127.0.0.1", rpcPort);
    session.open();

    anotherSession = new Session("127.0.0.1", rpcPort);
    anotherSession.open();
  }

  @After
  public void clean() throws Exception {
    super.clean();
  }

  // do not add tests here.
  // add tests into Cases.java instead.
}
