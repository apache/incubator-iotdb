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
package org.apache.iotdb.db.sync.conf;

import org.apache.iotdb.db.conf.IoTDBConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SyncSenderDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(SyncSenderDescriptor.class);
  private SyncSenderConfig conf = new SyncSenderConfig();

  private SyncSenderDescriptor() {
    loadProps();
  }

  public static SyncSenderDescriptor getInstance() {
    return SyncSenderDescriptorHolder.INSTANCE;
  }

  public SyncSenderConfig getConfig() {
    return conf;
  }

  public void setConfig(SyncSenderConfig conf) {
    this.conf = conf;
  }

  /** load an properties file and set sync config variables */
  private void loadProps() {
    InputStream inputStream;
    String url = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    if (url == null) {
      url = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (url != null) {
        url = url + File.separatorChar + "conf" + File.separatorChar + SyncConstant.CONFIG_NAME;
      } else {
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading config file {}, use default configuration",
            SyncConstant.CONFIG_NAME);
        return;
      }
    } else {
      url += (File.separatorChar + SyncConstant.CONFIG_NAME);
    }

    try {
      inputStream = new FileInputStream(url);
    } catch (FileNotFoundException e) {
      logger.warn("Fail to find sync config file {}", url, e);
      return;
    }

    logger.info("Start to read sync config file {}", url);
    Properties properties = new Properties();
    try {
      properties.load(inputStream);

      conf.setServerIp(properties.getProperty("server_ip", conf.getServerIp()));
      conf.setServerPort(
          Integer.parseInt(
              properties.getProperty("server_port", Integer.toString(conf.getServerPort()))));
      conf.setSyncPeriodInSecond(
          Integer.parseInt(
              properties.getProperty(
                  "sync_period_in_second", Integer.toString(conf.getSyncPeriodInSecond()))));
      String storageGroups = properties.getProperty("sync_storage_groups", null);
      if (storageGroups != null) {
        String[] splits = storageGroups.split(",");
        List<String> storageGroupList = new ArrayList<>();
        Arrays.stream(splits).forEach(sg -> storageGroupList.add(sg.trim()));
        conf.setStorageGroupList(storageGroupList);
      }
      conf.setMaxNumOfSyncFileRetry(
          Integer.parseInt(
              properties.getProperty(
                  "max_number_of_sync_file_retry",
                  Integer.toString(conf.getMaxNumOfSyncFileRetry()))));
    } catch (IOException e) {
      logger.warn("Cannot load sync config file, use default sync configuration.", e);
    } catch (Exception e) {
      logger.warn("Error format in sync config file, use default sync configuration.", e);
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        logger.error("Fail to close sync config file input stream.", e);
      }
    }
  }

  private static class SyncSenderDescriptorHolder {

    private static final SyncSenderDescriptor INSTANCE = new SyncSenderDescriptor();
  }
}
