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
package org.apache.iotdb.cluster.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;

public class ClusterConfigureGenerator {

  private static final String TEST_CONF_PATH = "src" + File.separatorChar + "test"
      + File.separatorChar + "resources" + File.separatorChar + "conf";
  private static ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
  private static Map<String, String> configMap = new HashMap<String, String>() {
    {
      put("port", config.getPort() + "");
      put("election_timeout_ms", config.getElectionTimeoutMs() + "");
      put("max_catch_up_log_num", config.getMaxCatchUpLogNum() + "");
      put("delay_snapshot", config.isDelaySnapshot() + "");
      put("delay_hours", config.getDelayHours() + "");
      put("task_redo_count", config.getQpTaskRedoCount() + "");
      put("task_timeout_ms", config.getQpTaskTimeout() + "");
      put("num_of_virtual_nodes", config.getNumOfVirtualNodes() + "");
      put("max_num_of_inner_rpc_client", config.getMaxNumOfInnerRpcClient() + "");
      put("max_queue_num_of_inner_rpc_client", config.getMaxQueueNumOfInnerRpcClient() + "");
      put("read_metadata_consistency_level", config.getReadMetadataConsistencyLevel() + "");
      put("read_data_consistency_level", config.getReadDataConsistencyLevel() + "");
    }
  };

  public static void generateClusterConfigure() throws IOException {
    deleteClusterConfigure();
    File f = new File(TEST_CONF_PATH);
    FileUtils.forceMkdir(f);
    String[][] config = {
        {"192.168.130.14:8888,192.168.130.16:8888,192.168.130.18:8888", "1"},
        {"192.168.130.14:8888,192.168.130.16:8888,192.168.130.18:8888", "3"},
        {"192.168.130.12:8888,192.168.130.14:8888,192.168.130.16:8888,192.168.130.18:8888,192.168.130.19:8888", "3"},
    };
    for(String[] c : config){
      for(String node : c[0].split(",")){
        createConfigureFile(c[0], node.split(":")[0], c[1]);
      }
    }
  }

  public static void deleteClusterConfigure() throws IOException {
    File f = new File(TEST_CONF_PATH);
    if (f.exists() && f.isDirectory()) {
      FileUtils.deleteDirectory(f);
    }
  }

  private static void createConfigureFile(String nodes, String ip, String replicator)
      throws IOException {
    int nodeNum = nodes.split(",").length;

    File configureFile = new File(TEST_CONF_PATH + File.separatorChar +
        String.format("%d-%s-%s.properties",nodeNum, replicator, ip.split("\\.")[3]));
    FileWriter writer = new FileWriter(configureFile, false);
    for (Entry<String, String> entry : configMap.entrySet()) {
      writer.write(String.format("%s=%s%s", entry.getKey(),
          entry.getValue(), System.getProperty("line.separator")));
    }
    writer.write(String.format("%s=%s%s", "nodes", nodes, System.getProperty("line.separator")));
    writer.write(String.format("%s=%s%s", "replication", replicator, System.getProperty("line.separator")));
    writer.write(String.format("%s=%s%s", "ip", ip, System.getProperty("line.separator")));
    writer.close();
  }
}
