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
package org.apache.iotdb.db.service;

import java.io.IOException;
import org.apache.iotdb.db.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.CacheHitRatioMonitor;
import org.apache.iotdb.db.engine.compaction.CompactionMergeTaskPoolManager;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.query.control.TracingManager;
import org.apache.iotdb.db.query.udf.service.TemporaryQueryDataFileService;
import org.apache.iotdb.db.query.udf.service.UDFRegistrationService;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.sync.receiver.SyncServerManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB implements IoTDBMBean {

  private static final Logger logger = LoggerFactory.getLogger(IoTDB.class);
  private final String mbeanName = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE,
      IoTDBConstant.JMX_TYPE, "IoTDB");
  private RegisterManager registerManager = new RegisterManager();
  public static MManager metaManager = MManager.getInstance();

  public static IoTDB getInstance() {
    return IoTDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
    if (args.length > 0) {
      IoTDBDescriptor.getInstance().replaceProps(args);
    }
    try {
      IoTDBConfigCheck.getInstance().checkConfig();
    } catch (IOException e) {
      logger.error("meet error when doing start checking", e);
    }
    IoTDB daemon = IoTDB.getInstance();
    daemon.active();
  }

  public static void setMetaManager(MManager metaManager) {
    IoTDB.metaManager = metaManager;
  }

  public void active() {
    long startTime = System.currentTimeMillis();
    StartupChecks checks = new StartupChecks().withDefaultTest();
    try {
      checks.verify();
    } catch (StartupException e) {
      // TODO: what are some checks
      logger.error("{}: failed to start because some checks failed. ",
          IoTDBConstant.GLOBAL_DB_NAME, e);
      return;
    }
    try {
      setUp();
    } catch (StartupException e) {
      logger.error("meet error while starting up.", e);
      deactivate();
      logger.error("{} exit", IoTDBConstant.GLOBAL_DB_NAME);
      return;
    }
    logger.info("{} has started after {}ms.", IoTDBConstant.GLOBAL_DB_NAME,
        System.currentTimeMillis() - startTime);
  }

  private void setUp() throws StartupException {
    logger.info("Setting up IoTDB...");

    Runtime.getRuntime().addShutdownHook(new IoTDBShutdownHook());
    setUncaughtExceptionHandler();
    logger.info("recover the schema...");
    initMManager();
    registerManager.register(JMXService.getInstance());
    registerManager.register(FlushManager.getInstance());
    registerManager.register(MultiFileLogNodeManager.getInstance());
    registerManager.register(Measurement.INSTANCE);
    registerManager.register(TVListAllocator.getInstance());
    registerManager.register(CacheHitRatioMonitor.getInstance());
    JMXService.registerMBean(getInstance(), mbeanName);
    registerManager.register(StorageEngine.getInstance());
    registerManager.register(TemporaryQueryDataFileService.getInstance());
    registerManager.register(UDFRegistrationService.getInstance());

    registerManager.register(RPCService.getInstance());
    if (IoTDBDescriptor.getInstance().getConfig().isEnableMetricService()) {
      registerManager.register(MetricsService.getInstance());
    }
    if (IoTDBDescriptor.getInstance().getConfig().isEnableMQTTService()) {
      registerManager.register(MQTTService.getInstance());
    }

    logger.info("IoTDB is set up, now may some sgs are not ready, please wait several seconds...");

    while (!StorageEngine.getInstance().isAllSgReady()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("IoTDB failed to set up for:" + e.getMessage());
        Thread.currentThread().interrupt();
        return;
      }
    }

    // Warn: registMonitor() method should be called after systemDataRecovery()
    registerManager.register(StatMonitor.getInstance());
    registerManager.register(SyncServerManager.getInstance());
    registerManager.register(UpgradeSevice.getINSTANCE());
    registerManager.register(MergeManager.getINSTANCE());
    registerManager.register(CompactionMergeTaskPoolManager.getInstance());

    logger.info("Congratulation, IoTDB is set up successfully. Now, enjoy yourself!");
  }

  private void deactivate() {
    logger.info("Deactivating IoTDB...");
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB is deactivated.");
  }

  private void initMManager() {
    long time = System.currentTimeMillis();
    IoTDB.metaManager.init();
    long end = System.currentTimeMillis() - time;
    logger.info("spend {}ms to recover schema.", end);
    logger.info(
        "After initializing, tsFile threshold is {}, memtableSize is {}",
        IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold(),
        IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold());

  }

  @Override
  public void stop() {
    deactivate();
  }

  public void shutdown() throws Exception {
    logger.info("Deactivating IoTDB...");
    IoTDB.metaManager.clear();
    TracingManager.getInstance().close();
    registerManager.shutdownAll();
    PrimitiveArrayManager.close();
    SystemInfo.getInstance().close();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB is deactivated.");
  }

  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  private static class IoTDBHolder {

    private static final IoTDB INSTANCE = new IoTDB();

    private IoTDBHolder() {

    }
  }

}
