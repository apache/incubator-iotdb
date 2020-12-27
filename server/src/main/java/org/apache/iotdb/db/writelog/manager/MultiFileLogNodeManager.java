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
package org.apache.iotdb.db.writelog.manager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.writelog.node.DifferentialWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MultiFileLogNodeManager manages all ExclusiveWriteLogNodes, each manages WALs of a TsFile
 * (either seq or unseq).
 */
public class MultiFileLogNodeManager implements WriteLogNodeManager, IService {

  private static final Logger logger = LoggerFactory.getLogger(MultiFileLogNodeManager.class);
  private Map<String, WriteLogNode> nodeMap;

  private ScheduledExecutorService executorService;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private void forceTask() {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      logger.warn("system mode is read-only, the force flush WAL task is stopped");
      return;
    }
    if (Thread.interrupted()) {
      logger.info("WAL force thread exits.");
      return;
    }

    for (WriteLogNode node : nodeMap.values()) {
      try {
        node.forceSync();
      } catch (IOException e) {
        logger.error("Cannot force {}, because ", node, e);
      }
    }
  }

  private MultiFileLogNodeManager() {
    nodeMap = new ConcurrentHashMap<>();
  }

  public static MultiFileLogNodeManager getInstance() {
    return InstanceHolder.instance;
  }


  @Override
  public WriteLogNode getNode(String identifier) {
    WriteLogNode node = nodeMap.get(identifier);
    if (node == null) {
      node = new DifferentialWriteLogNode(identifier);
      WriteLogNode oldNode = nodeMap.putIfAbsent(identifier, node);
      if (oldNode != null) {
        return oldNode;
      }
    }
    return node;
  }

  @Override
  public void deleteNode(String identifier) throws IOException {
    WriteLogNode node = nodeMap.remove(identifier);
    if (node != null) {
      node.delete();
    }
  }

  @Override
  public void close() {
    logger.info("{} nodes to be closed", nodeMap.size());
    for (WriteLogNode node : nodeMap.values()) {
      try {
        node.close();
      } catch (IOException e) {
        logger.error("failed to close {}", node, e);
      }
    }
    nodeMap.clear();
    logger.info("LogNodeManager closed.");
  }

  @Override
  public void start() throws StartupException {
    try {
      if (!config.isEnableWal()) {
        return;
      }
      if (config.getForceWalPeriodInMs() > 0) {
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(this::forceTask, config.getForceWalPeriodInMs(),
            config.getForceWalPeriodInMs(), TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    if (!config.isEnableWal()) {
      return;
    }
    if (executorService != null) {
      executorService.shutdown();
      try {
        executorService.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.warn("force flush wal thread still doesn't exit after 30s");
        Thread.currentThread().interrupt();
      }
    }
    close();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.WAL_SERVICE;
  }

  private static class InstanceHolder {
    private InstanceHolder(){}

    private static MultiFileLogNodeManager instance = new MultiFileLogNodeManager();
  }

}
