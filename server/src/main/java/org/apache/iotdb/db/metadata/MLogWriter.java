/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class MLogWriter {

  private static final Logger logger = LoggerFactory.getLogger(MLogWriter.class);
  private BufferedWriter writer;

  public MLogWriter(String schemaDir, String logFileName) throws IOException {
    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      if (metadataDir.mkdirs()) {
        logger.info("create schema folder {}.", metadataDir);
      } else {
        logger.info("create schema folder {} failed.", metadataDir);
      }
    }

    File logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);

    FileWriter fileWriter;
    fileWriter = new FileWriter(logFile, true);
    writer = new BufferedWriter(fileWriter);
  }


  public void close() throws IOException {
    writer.close();
  }

  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws IOException {
    writer.write(String.format("%s,%s,%s,%s,%s", MetadataOperationType.CREATE_TIMESERIES,
        plan.getPath().getFullPath(), plan.getDataType().serialize(), plan.getEncoding().serialize(),
        plan.getCompressor().serialize()));

    writer.write(",");
    if (plan.getProps() != null) {
      boolean first = true;
      for (Map.Entry<String, String> entry : plan.getProps().entrySet()) {
        if (first) {
          writer.write(String.format("%s=%s", entry.getKey(), entry.getValue()));
          first = false;
        } else {
          writer.write(String.format("&%s=%s", entry.getKey(), entry.getValue()));
        }
      }
    }

    writer.write(",");
    if (plan.getAlias() != null) {
      writer.write(plan.getAlias());
    }

    writer.write(",");
    if (offset >= 0) {
      writer.write(String.valueOf(offset));
    }

    writer.write(",");

    writer.newLine();
    writer.flush();
  }

  public void deleteTimeseries(String path) throws IOException {
    writer.write(MetadataOperationType.DELETE_TIMESERIES + "," + path);
    writer.newLine();
    writer.flush();
  }

  public void setStorageGroup(String storageGroup) throws IOException {
    writer.write(MetadataOperationType.SET_STORAGE_GROUP + "," + storageGroup);
    writer.newLine();
    writer.flush();
  }

  public void deleteStorageGroup(String storageGroup) throws IOException {
    writer.write(MetadataOperationType.DELETE_STORAGE_GROUP + "," + storageGroup);
    writer.newLine();
    writer.flush();
  }

  public void setTTL(String storageGroup, long ttl) throws IOException {
    writer.write(String.format("%s,%s,%s", MetadataOperationType.SET_TTL, storageGroup, ttl));
    writer.newLine();
    writer.flush();
  }

  public static boolean upgradeMLog(String schemaDir, String logFileName) throws IOException {
    File logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    if (!logFile.exists()) {
      // no mlog file to be upgraded
      return false;
    }
    FileReader fileReader;
    String line;
    fileReader = new FileReader(logFile);
    BufferedReader reader = new BufferedReader(fileReader);
    StringBuilder bufAll = new StringBuilder();
    try {
      while ((line = reader.readLine()) != null) {
        StringBuilder buf = new StringBuilder();
        buf.append(line);
        if (line.startsWith(MetadataOperationType.CREATE_TIMESERIES)) {
          buf.append(",,,,");
        }
        buf.append(System.getProperty("line.separator"));
        bufAll.append(buf);
      }
    } catch (IOException e) {
      logger.error("Reading MLog {} failed", logFile);
      return false;
    } finally {
      reader.close();
    }
    File tmpLogFile = new File(logFile.getAbsolutePath() + ".tmp");

    if (!tmpLogFile.exists()) {
      if (!tmpLogFile.delete()) {
        logger.error("Deleting {} failed", tmpLogFile);
        return false;
      }
    }

    FileWriter fileWriter;
    fileWriter = new FileWriter(tmpLogFile, true);
    BufferedWriter writer = new BufferedWriter(fileWriter);
    try {
      writer.write(bufAll.toString());
      writer.flush();
    } catch (IOException e) {
      logger.error("Writing upgraded MLog {} failed", tmpLogFile);
      return false;
    } finally {
      writer.close();
    }
    if (!logFile.delete()) {
      logger.error("Deleting old MLog file {} failed");
      return false;
    }
    FSFactoryProducer.getFSFactory().moveFile(tmpLogFile, logFile);
    if (tmpLogFile.exists()) {
      return false;
    }
    return true;
  }
  
}
