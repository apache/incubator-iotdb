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
package org.apache.iotdb.db.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConfigCheck {

  // this file is located in Data/System/Schema/system_properties.
  // If user delete folder "Data", system_properties can reset.
  public static final String DEFAULT_FILEPATH = "system.properties";
  public static final String PROPERTY_HOME =
      "data" + File.separator + "system" + File.separator + "schema";
  private static final IoTDBConfigCheck INSTANCE = new IoTDBConfigCheck();
  private static final Logger logger = LoggerFactory.getLogger(IoTDBDescriptor.class);
  private Properties properties = new Properties();
  // this is a initial parameter.
  public static String TIMESTAMP_PRECISION = "ms";

  public static final IoTDBConfigCheck getInstance() {
    return IoTDBConfigCheck.INSTANCE;
  }

  public void checkConfig() {
    TIMESTAMP_PRECISION = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();
    createDir(PROPERTY_HOME);
    checkFile(PROPERTY_HOME);
    logger.info("System configuration is ok.");
  }

  public void createDir(String filepath) {
    File dir = new File(filepath);
    if (!dir.exists()) {
      dir.mkdirs();
      logger.info(" {} dir has been made.", PROPERTY_HOME);
    }
  }

  public void checkFile(String filepath) {
    // create file : read timestamp precision from engine.properties, create system_properties.txt
    // use output stream to write timestamp precision to file.
    File file = new File(filepath + File.separator + DEFAULT_FILEPATH);
    try {
      if (!file.exists()) {
        file.createNewFile();
        logger.info(" {} has been created.", file.getAbsolutePath());
        try (FileOutputStream outputStream = new FileOutputStream(file.toString())) {
          properties.setProperty("timestamp_precision", TIMESTAMP_PRECISION);
          properties.store(outputStream, "System properties:");
        }
      }
    } catch (IOException e) {
      logger.error("Can not create {}.", file.getAbsolutePath(), e);
    }
    // get existed properties from system_properties.txt
    File inputFile = new File(filepath + File.separator + DEFAULT_FILEPATH);
    try (FileInputStream inputStream = new FileInputStream(inputFile.toString())) {
      properties.load(new InputStreamReader(inputStream, "utf-8"));
      if (!properties.getProperty("timestamp_precision").equals(TIMESTAMP_PRECISION)) {
        logger.error("Wrong timestamp precision, please set as: " + properties
            .getProperty("timestamp_precision") + " !");
        System.exit(-1);
      }
    } catch (IOException e) {
      logger.error("Load system.properties from {} failed.", file.getAbsolutePath(), e);
    }
  }
}


