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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.CompactionMergeTaskPoolManager;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.exception.UDFRegistrationException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.control.TracingManager;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/** This class is used for cleaning test environment in unit test and integration test */
public class EnvironmentUtils {

  private static final Logger logger = LoggerFactory.getLogger(EnvironmentUtils.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final TierManager tierManager = TierManager.getInstance();

  public static long TEST_QUERY_JOB_ID = 1;
  public static QueryContext TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

  private static final long oldTsFileThreshold = config.getTsFileSizeThreshold();

  private static final long oldGroupSizeInByte = config.getMemtableSizeThreshold();

  private static IoTDB daemon;

  public static boolean examinePorts =
      Boolean.parseBoolean(System.getProperty("test.port.closed", "false"));

  public static void cleanEnv() throws IOException, StorageEngineException {
    // wait all compaction finished
    CompactionMergeTaskPoolManager.getInstance().waitAllCompactionFinish();

    // deregister all user defined classes
    try {
      UDFRegistrationService.getInstance().deregisterAll();
      TriggerRegistrationService.getInstance().deregisterAll();
    } catch (UDFRegistrationException | TriggerManagementException e) {
      fail(e.getMessage());
    }

    logger.warn("EnvironmentUtil cleanEnv...");
    if (daemon != null) {
      daemon.stop();
      daemon = null;
    }
    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);

    // clear opened file streams
    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();

    if (examinePorts) {
      // TODO: this is just too slow, especially on Windows, consider a better way
      boolean closed = examinePorts();
      if (!closed) {
        // sleep 10 seconds
        try {
          TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
          // do nothing
        }

        if (!examinePorts()) {
          fail("failed to close some ports");
        }
      }
    }

    // clean storage group manager
    if (!StorageEngine.getInstance().deleteAll()) {
      logger.error("Can't close the storage group manager in EnvironmentUtils");
      fail();
    }

    IoTDBDescriptor.getInstance().getConfig().setReadOnly(false);

    // clean cache
    if (config.isMetaDataCacheEnable()) {
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
    }
    // close metadata
    IoTDB.metaManager.clear();

    // close tracing
    if (config.isEnablePerformanceTracing()) {
      TracingManager.getInstance().close();
    }

    // close array manager
    PrimitiveArrayManager.close();

    // clear system info
    SystemInfo.getInstance().close();

    // delete all directory
    cleanAllDir();
    config.setTsFileSizeThreshold(oldTsFileThreshold);
    config.setMemtableSizeThreshold(oldGroupSizeInByte);
  }

  private static boolean examinePorts() {
    TTransport transport = new TSocket("127.0.0.1", 6667, 100);
    if (!transport.isOpen()) {
      try {
        transport.open();
        logger.error("stop daemon failed. 6667 can be connected now.");
        transport.close();
        return false;
      } catch (TTransportException e) {
        // do nothing
      }
    }
    // try sync service
    transport = new TSocket("127.0.0.1", 5555, 100);
    if (!transport.isOpen()) {
      try {
        transport.open();
        logger.error("stop Sync daemon failed. 5555 can be connected now.");
        transport.close();
        return false;
      } catch (TTransportException e) {
        // do nothing
      }
    }
    // try jmx connection
    try {
      JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:31999/jmxrmi");
      JMXConnector jmxConnector = JMXConnectorFactory.connect(url);
      logger.error("stop JMX failed. 31999 can be connected now.");
      jmxConnector.close();
      return false;
    } catch (IOException e) {
      // do nothing
    }
    // try MetricService
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("127.0.0.1", 8181), 100);
      logger.error("stop MetricService failed. 8181 can be connected now.");
      return false;
    } catch (Exception e) {
      // do nothing
    }
    // do nothing
    return true;
  }

  public static void cleanAllDir() throws IOException {
    // delete sequential files
    for (FSPath path : tierManager.getAllSequenceFileFolders()) {
      cleanDir(path.toFile());
    }
    // delete unsequence files
    for (FSPath path : tierManager.getAllUnSequenceFileFolders()) {
      cleanDir(path.toFile());
    }
    // delete system info
    cleanDir(new File(config.getSystemDir()));
    // delete wal
    cleanDir(new File(config.getWalDir()));
    // delete query
    cleanDir(new File(config.getQueryDir()));
    // delete tracing
    cleanDir(new File(config.getTracingDir()));
    // delete ulog
    cleanDir(new File(config.getUdfDir()));
    // delete tlog
    cleanDir(new File(config.getTriggerDir()));
    // delete data files
    for (FSPath[] tierDataDirs : config.getDataDirs()) {
      for (FSPath dataDir : tierDataDirs) {
        cleanDir(dataDir.toFile());
      }
    }
  }

  public static void cleanDir(File dir) throws IOException {
    FileUtils.deleteDirectory(dir);
  }

  /** disable the system monitor</br> this function should be called before all code in the setup */
  public static void closeStatMonitor() {
    config.setEnableStatMonitor(false);
  }

  /** disable memory control</br> this function should be called before all code in the setup */
  public static void envSetUp() {
    logger.warn("EnvironmentUtil setup...");
    IoTDBDescriptor.getInstance().getConfig().setThriftServerAwaitTimeForStopService(0);
    // we do not start 8181 port in test.
    IoTDBDescriptor.getInstance().getConfig().setEnableMetricService(false);
    IoTDBDescriptor.getInstance().getConfig().setAvgSeriesPointNumberThreshold(Integer.MAX_VALUE);
    if (daemon == null) {
      daemon = new IoTDB();
    }
    try {
      EnvironmentUtils.daemon.active();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    createAllDir();
    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId(true, 1024, 0);
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
  }

  public static void stopDaemon() {
    if (daemon != null) {
      daemon.stop();
    }
  }

  public static void shutdownDaemon() throws Exception {
    if (daemon != null) {
      daemon.shutdown();
    }
  }

  public static void activeDaemon() {
    if (daemon != null) {
      daemon.active();
    }
  }

  public static void reactiveDaemon() {
    if (daemon == null) {
      daemon = new IoTDB();
      daemon.active();
    } else {
      activeDaemon();
    }
  }

  public static void restartDaemon() throws Exception {
    shutdownDaemon();
    stopDaemon();
    IoTDB.metaManager.clear();
    reactiveDaemon();
  }

  private static void createAllDir() {
    // create sequential files
    for (FSPath path : tierManager.getAllSequenceFileFolders()) {
      createDir(path);
    }
    // create unsequential files
    for (FSPath path : tierManager.getAllUnSequenceFileFolders()) {
      createDir(path);
    }
    // create storage group
    createDir(config.getSystemDir());
    // create sg dir
    String sgDir = FilePathUtils.regularizePath(config.getSystemDir()) + "storage_groups";
    createDir(sgDir);
    // create wal
    createDir(config.getWalDir());
    // create query
    createDir(config.getQueryDir());
    createDir(TestConstant.OUTPUT_DATA_DIR);
    // create data
    for (FSPath[] tierDataDirs : config.getDataDirs()) {
      for (FSPath dataDir : tierDataDirs) {
        createDir(dataDir);
      }
    }
    // create user and roles folder
    try {
      BasicAuthorizer.getInstance().reset();
    } catch (AuthException e) {
      logger.error("create user and role folders failed", e);
      fail(e.getMessage());
    }
  }

  private static void createDir(String dir) {
    File file = new File(dir);
    file.mkdirs();
  }

  private static void createDir(FSPath dir) {
    File file = dir.toFile();
    file.mkdirs();
  }
}
