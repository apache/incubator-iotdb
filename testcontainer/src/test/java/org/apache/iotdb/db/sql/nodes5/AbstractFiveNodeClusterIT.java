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

package org.apache.iotdb.db.sql.nodes5;

import org.apache.iotdb.db.sql.ClusterIT;

import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.NoProjectNameDockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;

// just add real ITs into AbstractClusterIT.
// in this case, the data may be not on the node that a read client connects.
// So, we have: write data on node A and read data on either node A, B, and C.
public abstract class AbstractFiveNodeClusterIT extends ClusterIT {

  private static Logger node1Logger = LoggerFactory.getLogger("iotdb-server_1");
  private static Logger node2Logger = LoggerFactory.getLogger("iotdb-server_2");
  private static Logger node3Logger = LoggerFactory.getLogger("iotdb-server_3");
  private static Logger node4Logger = LoggerFactory.getLogger("iotdb-server_4");
  private static Logger node5Logger = LoggerFactory.getLogger("iotdb-server_5");

  // in TestContainer's document, it is @ClassRule, and the environment is `public static`
  // I am not sure the difference now.
  @Rule
  public DockerComposeContainer environment =
      new NoProjectNameDockerComposeContainer(
              "5nodes", new File("src/test/resources/5nodes/docker-compose.yaml"))
          .withExposedService("iotdb-server_1", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_1", new Slf4jLogConsumer(node1Logger))
          .withExposedService("iotdb-server_2", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_2", new Slf4jLogConsumer(node2Logger))
          .withExposedService("iotdb-server_3", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_3", new Slf4jLogConsumer(node3Logger))
          .withExposedService("iotdb-server_4", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_4", new Slf4jLogConsumer(node4Logger))
          .withExposedService("iotdb-server_5", 6667, Wait.forListeningPort())
          .withLogConsumer("iotdb-server_5", new Slf4jLogConsumer(node5Logger))
          .withLocalCompose(true);

  @Override
  protected DockerComposeContainer getContainer() {
    return environment;
  }

  protected int[] getReadRpcPorts() {
    return new int[] {
      getContainer().getServicePort("iotdb-server_1", 6667),
      getContainer().getServicePort("iotdb-server_2", 6667),
      getContainer().getServicePort("iotdb-server_3", 6667),
      getContainer().getServicePort("iotdb-server_4", 6667),
      getContainer().getServicePort("iotdb-server_5", 6667)
    };
  }

  protected String[] getReadRpcIps() {
    return new String[] {
      getContainer().getServiceHost("iotdb-server_1", 6667),
      getContainer().getServiceHost("iotdb-server_2", 6667),
      getContainer().getServiceHost("iotdb-server_3", 6667),
      getContainer().getServiceHost("iotdb-server_4", 6667),
      getContainer().getServiceHost("iotdb-server_5", 6667)
    };
  }
}
