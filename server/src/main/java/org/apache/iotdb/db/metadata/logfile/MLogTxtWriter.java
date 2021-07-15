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
package org.apache.iotdb.db.metadata.logfile;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.MetadataOperationType;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.SetDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.SetUsingDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MLogTxtWriter implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(MLogTxtWriter.class);
  private static final String STRING_TYPE = "%s,%s,%s" + System.lineSeparator();
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private final File logFile;
  private FileOutputStream fileOutputStream;
  private FileChannel channel;
  private final AtomicInteger lineNumber;

  public MLogTxtWriter(String schemaDir, String logFileName) throws IOException {
    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      if (metadataDir.mkdirs()) {
        logger.info("create schema folder {}.", metadataDir);
      } else {
        logger.info("create schema folder {} failed.", metadataDir);
      }
    }

    logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
    lineNumber = new AtomicInteger(0);
  }

  public MLogTxtWriter(String logFileName) throws FileNotFoundException {
    logFile = SystemFileFactory.INSTANCE.getFile(logFileName);
    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
    lineNumber = new AtomicInteger(0);
  }

  @Override
  public void close() throws IOException {
    fileOutputStream.close();
  }

  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(
        String.format(
            "%s,%s,%s,%s,%s",
            MetadataOperationType.CREATE_TIMESERIES,
            plan.getPath().getFullPath(),
            plan.getDataType().serialize(),
            plan.getEncoding().serialize(),
            plan.getCompressor().serialize()));

    buf.append(",");
    if (plan.getProps() != null) {
      boolean first = true;
      for (Map.Entry<String, String> entry : plan.getProps().entrySet()) {
        if (first) {
          buf.append(String.format("%s=%s", entry.getKey(), entry.getValue()));
          first = false;
        } else {
          buf.append(String.format("&%s=%s", entry.getKey(), entry.getValue()));
        }
      }
    }

    buf.append(",");
    if (plan.getAlias() != null) {
      buf.append(plan.getAlias());
    }

    buf.append(",");
    if (offset >= 0) {
      buf.append(offset);
    }
    buf.append(LINE_SEPARATOR);
    channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
    lineNumber.incrementAndGet();
  }

  public void createAlignedTimeseries(CreateAlignedTimeSeriesPlan plan) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(
        String.format(
            "%s,%s,%s,%s,%s,%s",
            MetadataOperationType.CREATE_TIMESERIES,
            plan.getPrefixPath().getFullPath(),
            plan.getMeasurements(),
            plan.getDataTypes().stream().map(TSDataType::serialize),
            plan.getEncodings().stream().map(TSEncoding::serialize),
            plan.getCompressor().serialize()));

    buf.append(",[");
    if (plan.getAliasList() != null) {
      List<String> aliasList = plan.getAliasList();
      for (int i = 0; i < aliasList.size(); i++) {
        buf.append(aliasList.get(i));
        if (i != aliasList.size() - 1) {
          buf.append(",");
        }
      }
    }
    buf.append("]");
    buf.append(LINE_SEPARATOR);
    channel.write(ByteBuffer.wrap(buf.toString().getBytes()));
    lineNumber.incrementAndGet();
  }

  public void deleteTimeseries(String path) throws IOException {
    String outputStr = MetadataOperationType.DELETE_TIMESERIES + "," + path + LINE_SEPARATOR;
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
  }

  public void createContinuousQuery(CreateContinuousQueryPlan plan) throws IOException {
    String buf =
        String.format(
                "%s,%s,%s,%s",
                MetadataOperationType.CREATE_CONTINUOUS_QUERY,
                plan.getContinuousQueryName(),
                plan.getQuerySql(),
                plan.getTargetPath().getFullPath())
            + LINE_SEPARATOR;
    channel.write(ByteBuffer.wrap(buf.getBytes()));
    lineNumber.incrementAndGet();
  }

  public void dropContinuousQuery(DropContinuousQueryPlan plan) throws IOException {

    String buf =
        String.format(
                "%s,%s", MetadataOperationType.DROP_CONTINUOUS_QUERY, plan.getContinuousQueryName())
            + LINE_SEPARATOR;
    channel.write(ByteBuffer.wrap(buf.getBytes()));
    lineNumber.incrementAndGet();
  }

  public void setStorageGroup(String storageGroup) throws IOException {
    String outputStr =
        MetadataOperationType.SET_STORAGE_GROUP + "," + storageGroup + LINE_SEPARATOR;
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void deleteStorageGroup(String storageGroup) throws IOException {
    String outputStr =
        MetadataOperationType.DELETE_STORAGE_GROUP + "," + storageGroup + LINE_SEPARATOR;
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void setTTL(String storageGroup, long ttl) throws IOException {
    String outputStr = String.format(STRING_TYPE, MetadataOperationType.SET_TTL, storageGroup, ttl);
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void changeOffset(String path, long offset) throws IOException {
    String outputStr =
        String.format(STRING_TYPE, MetadataOperationType.CHANGE_OFFSET, path, offset);
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void changeAlias(String path, String alias) throws IOException {
    String outputStr = String.format(STRING_TYPE, MetadataOperationType.CHANGE_ALIAS, path, alias);
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public static void upgradeMLog(String schemaDir, String logFileName) throws IOException {
    File logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);
    File tmpLogFile = SystemFileFactory.INSTANCE.getFile(logFile.getAbsolutePath() + ".tmp");

    // if both old mlog and mlog.tmp do not exist, nothing to do
    if (!logFile.exists() && !tmpLogFile.exists()) {
      return;
    } else if (!logFile.exists() && tmpLogFile.exists()) {
      // if old mlog doesn't exsit but mlog.tmp exists, rename tmp file to mlog
      FSFactoryProducer.getFSFactory().moveFile(tmpLogFile, logFile);
      return;
    }

    // if both old mlog and mlog.tmp exist, delete mlog tmp, then do upgrading
    if (tmpLogFile.exists()) {
      try {
        Files.delete(Paths.get(tmpLogFile.toURI()));
      } catch (IOException e) {
        throw new IOException("Deleting " + tmpLogFile + "failed with exception " + e.getMessage());
      }
    }
    // upgrading
    try (BufferedReader reader = new BufferedReader(new FileReader(logFile));
        BufferedWriter writer = new BufferedWriter(new FileWriter(tmpLogFile, true))) {
      String line;
      while ((line = reader.readLine()) != null) {
        StringBuilder buf = new StringBuilder();
        buf.append(line);
        if (line.startsWith(MetadataOperationType.CREATE_TIMESERIES)) {
          buf.append(",,,");
        }
        writer.write(buf.toString());
        writer.newLine();
        writer.flush();
      }
    }
  }

  public void clear() throws IOException {
    channel.force(true);
    channel.close();
    fileOutputStream.close();
    Files.delete(logFile.toPath());
    fileOutputStream = new FileOutputStream(logFile, true);
    channel = fileOutputStream.getChannel();
    lineNumber.set(0);
  }

  public void serializeMNode(MNodePlan plan) throws IOException {
    StringBuilder s = new StringBuilder(String.valueOf(MetadataConstant.STORAGE_GROUP_MNODE_TYPE));
    s.append(",").append(plan.getName()).append(",");
    s.append(plan.getChildSize());
    s.append(LINE_SEPARATOR);
    ByteBuffer buff = ByteBuffer.wrap(s.toString().getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void serializeMeasurementMNode(MeasurementMNodePlan plan) throws IOException {
    StringBuilder s = new StringBuilder(String.valueOf(MetadataConstant.MEASUREMENT_MNODE_TYPE));
    s.append(",").append(plan.getName()).append(",");
    if (plan.getAlias() != null) {
      s.append(plan.getAlias());
    }
    IMeasurementSchema schema = plan.getSchema();
    s.append(",").append(schema.getType().ordinal()).append(",");
    s.append(schema.getEncodingType().ordinal()).append(",");
    s.append(schema.getCompressor().ordinal()).append(",");
    if (schema.getProps() != null) {
      for (Map.Entry<String, String> entry : schema.getProps().entrySet()) {
        s.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
      }
    }
    s.append(",").append(plan.getOffset()).append(",");
    s.append(plan.getChildSize());
    s.append(LINE_SEPARATOR);
    ByteBuffer buff = ByteBuffer.wrap(s.toString().getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void serializeStorageGroupMNode(StorageGroupMNodePlan plan) throws IOException {
    StringBuilder s = new StringBuilder(String.valueOf(MetadataConstant.STORAGE_GROUP_MNODE_TYPE));
    s.append(",").append(plan.getName()).append(",");
    s.append(plan.getDataTTL()).append(",");
    s.append(plan.getChildSize());
    s.append(LINE_SEPARATOR);
    ByteBuffer buff = ByteBuffer.wrap(s.toString().getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void setTemplate(SetDeviceTemplatePlan plan) throws IOException {
    StringBuilder buf = new StringBuilder(String.valueOf(MetadataOperationType.SET_TEMPLATE));
    buf.append(",");
    buf.append(plan.getTemplateName());
    buf.append(",");
    buf.append(plan.getPrefixPath());
    buf.append(LINE_SEPARATOR);
    ByteBuffer buff = ByteBuffer.wrap(buf.toString().getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void setUsingTemplate(SetUsingDeviceTemplatePlan plan) throws IOException {
    StringBuilder buf = new StringBuilder(String.valueOf(MetadataOperationType.SET_USING_TEMPLATE));
    buf.append(",");
    buf.append(plan.getPrefixPath());
    buf.append(LINE_SEPARATOR);
    ByteBuffer buff = ByteBuffer.wrap(buf.toString().getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  public void createTemplate(CreateTemplatePlan plan) throws IOException {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < plan.getSchemaNames().size(); i++) {
      for (int j = 0; j < plan.getMeasurements().get(i).size(); j++) {
        String measurement;
        boolean isAligned = false;
        if (plan.getMeasurements().get(i).size() == 1) {
          measurement = plan.getSchemaNames().get(i);
        } else {
          // for aligned timeseries
          isAligned = true;
          measurement =
              plan.getSchemaNames().get(i)
                  + TsFileConstant.PATH_SEPARATOR
                  + plan.getMeasurements().get(i).get(j);
        }
        buf.append(
            String.format(
                "%s,%s,%s,%s,%s,%s,%s",
                MetadataOperationType.CREATE_TEMPLATE,
                plan.getName(),
                isAligned ? 1 : 0,
                measurement,
                plan.getDataTypes().get(i).get(j).serialize(),
                plan.getEncodings().get(i).get(j).serialize(),
                plan.getCompressors().get(i).serialize()));
        buf.append(LINE_SEPARATOR);
        lineNumber.incrementAndGet();
      }
    }
    ByteBuffer buff = ByteBuffer.wrap(buf.toString().getBytes());
    channel.write(buff);
  }

  public void autoCreateDeviceNode(String Device) throws IOException {
    String outputStr = MetadataOperationType.AUTO_CREATE_DEVICE + "," + Device + LINE_SEPARATOR;
    ByteBuffer buff = ByteBuffer.wrap(outputStr.getBytes());
    channel.write(buff);
    lineNumber.incrementAndGet();
  }

  int getLineNumber() {
    return lineNumber.get();
  }

  /** only used for initialize a mlog file writer. */
  void setLineNumber(int number) {
    lineNumber.set(number);
  }
}
