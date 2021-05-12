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
package org.apache.iotdb.db.monitor;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.virtualSg.VirtualStorageGroupManager;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.monitor.MonitorConstants.StatMeasurementConstants;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.LastQueryExecutor;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StatMonitor implements StatMonitorMBean, IService {

  private static final Logger logger = LoggerFactory.getLogger(StatMonitor.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final MManager mManager = IoTDB.metaManager;
  private static final StorageEngine storageEngine = StorageEngine.getInstance();
  private final String mbeanName =
      String.format(
          "%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, getID().getJmxName());

  List<PartialPath> globalSeries = new ArrayList<>(3);
  // monitor series value.   e.g. root.stats.global.TOTAL_POINTS -> value
  private List<Long> globalSeriesValue = new ArrayList<>(3);

  public StatMonitor() {
    if (config.isEnableStatMonitor()) {
      initMonitorSeriesInfo();
      recovery();
    }
  }

  public static StatMonitor getInstance() {
    return StatMonitorHolder.INSTANCE;
  }

  public void initMonitorSeriesInfo() {
    String[] globalMonitorSeries = MonitorConstants.STAT_GLOBAL_ARRAY;
    for (int i = 0; i < StatMeasurementConstants.values().length; i++) {
      PartialPath globalMonitorPath =
          new PartialPath(globalMonitorSeries)
              .concatNode(StatMeasurementConstants.values()[i].getMeasurement());
      globalSeries.add(globalMonitorPath);
      globalSeriesValue.add(0L);
    }
  }

  /** Generate tsRecords for stat parameters and insert them into StorageEngine. */
  public void saveStatValue(String storageGroupName)
      throws MetadataException, StorageEngineException {
    long insertTime = System.currentTimeMillis();
    // update monitor series of storage group
    PartialPath storageGroupSeries = getStorageGroupMonitorSeries(storageGroupName);
    if (!mManager.isPathExist(storageGroupSeries)) {
      registSeriesToMManager(storageGroupSeries);
    }
    TSRecord tsRecord = new TSRecord(insertTime, storageGroupSeries.getDevice());
    tsRecord.addTuple(
        new LongDataPoint(
            StatMeasurementConstants.TOTAL_POINTS.getMeasurement(),
            storageEngine
                .getProcessorMap()
                .get(new PartialPath(storageGroupName))
                .getMonitorSeriesValue()));
    storageEngine.insert(new InsertRowPlan(tsRecord));

    // update global monitor series
    for (int i = 0; i < globalSeries.size(); i++) {
      PartialPath seriesPath = globalSeries.get(i);
      if (!mManager.isPathExist(seriesPath)) {
        registSeriesToMManager(seriesPath);
      }
      tsRecord = new TSRecord(insertTime, seriesPath.getDevice());
      tsRecord.addTuple(new LongDataPoint(seriesPath.getMeasurement(), globalSeriesValue.get(i)));
      storageEngine.insert(new InsertRowPlan(tsRecord));
    }
  }

  /** Recover the cache values of monitor series using last query if time series exist. */
  public void recovery() {
    try {
      for (int i = 0; i < globalSeries.size(); i++) {
        PartialPath globalMonitorPath = globalSeries.get(i);
        TimeValuePair timeValuePair = getLastValue(globalMonitorPath);
        if (timeValuePair != null) {
          globalSeriesValue.set(i, timeValuePair.getValue().getLong());
        }
      }

      List<PartialPath> storageGroupPaths = mManager.getAllStorageGroupPaths();
      for (PartialPath storageGroupPath : storageGroupPaths) {
        if (!storageGroupPath.getFullPath().equals(MonitorConstants.STAT_STORAGE_GROUP_NAME)) {
          // for storage group which is not global, only TOTAL_POINTS is registered now
          PartialPath monitorSeriesPath =
              getStorageGroupMonitorSeries(storageGroupPath.getFullPath());
          TimeValuePair timeValuePair = getLastValue(monitorSeriesPath);
          if (timeValuePair != null) {
            storageEngine
                .getProcessorMap()
                .get(storageGroupPath)
                .setMonitorSeriesValue(timeValuePair.getValue().getLong());
          }
        }
      }
    } catch (StorageEngineException | IOException | QueryProcessException e) {
      logger.error("Load last value from disk error.", e);
    }
  }

  private TimeValuePair getLastValue(PartialPath monitorSeries)
      throws StorageEngineException, QueryProcessException, IOException {
    if (mManager.isPathExist(monitorSeries)) {
      TimeValuePair timeValuePair =
          LastQueryExecutor.calculateLastPairForSeriesLocally(
                  Collections.singletonList(monitorSeries),
                  Collections.singletonList(TSDataType.INT64),
                  new QueryContext(QueryResourceManager.getInstance().assignQueryId(true, 1024, 1)),
                  null,
                  Collections.singletonMap(
                      monitorSeries.getDevice(),
                      Collections.singleton(monitorSeries.getMeasurement())))
              .get(0)
              .right;
      if (timeValuePair.getValue() != null) {
        return timeValuePair;
      }
    }
    return null;
  }

  private PartialPath getStorageGroupMonitorSeries(String storageGroupName) {
    String[] monitorSeries = Arrays.copyOf(MonitorConstants.STAT_STORAGE_GROUP_ARRAY, 4);
    monitorSeries[2] = "\"" + storageGroupName + "\"";
    monitorSeries[3] = StatMeasurementConstants.TOTAL_POINTS.getMeasurement();
    return new PartialPath(monitorSeries);
  }

  private void registSeriesToMManager(PartialPath fullPath) throws MetadataException {
    mManager.createTimeseries(
        fullPath,
        TSDataType.valueOf(MonitorConstants.INT64),
        TSEncoding.valueOf("TS_2DIFF"),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        null);
  }

  public void updateStatGlobalValue(int successPointsNum) {
    // 0 -> TOTAL_POINTS, 1 -> REQ_SUCCESS
    globalSeriesValue.set(0, globalSeriesValue.get(0) + successPointsNum);
    globalSeriesValue.set(1, globalSeriesValue.get(1) + 1);
  }

  public void updateFailedStatValue() {
    // 2 -> REQ_FAIL
    globalSeriesValue.set(2, globalSeriesValue.get(2) + 1);
  }

  @TestOnly
  public void close() {
    config.setEnableStatMonitor(false);
    config.setEnableMonitorSeriesWrite(false);
  }

  // implements methods of StatMonitorMean from here
  @Override
  public long getGlobalTotalPointsNum() {
    return globalSeriesValue.get(0);
  }

  @Override
  public long getGlobalReqSuccessNum() {
    return globalSeriesValue.get(1);
  }

  @Override
  public long getGlobalReqFailNum() {
    return globalSeriesValue.get(2);
  }

  @Override
  public long getStorageGroupTotalPointsNum(String storageGroupName) {
    try {
      VirtualStorageGroupManager virtualStorageGroupManager =
          storageEngine.getProcessorMap().get(new PartialPath(storageGroupName));
      if (virtualStorageGroupManager == null) {
        return 0;
      }

      return virtualStorageGroupManager.getMonitorSeriesValue();
    } catch (IllegalPathException e) {
      logger.error(e.getMessage());
      return -1;
    }
  }

  @Override
  public String getSystemDirectory() {
    try {
      File file = SystemFileFactory.INSTANCE.getFile(config.getSystemDir());
      return file.getAbsolutePath();
    } catch (Exception e) {
      logger.error("meet error while trying to get base dir.", e);
      return "Unavailable";
    }
  }

  @Override
  public long getDataSizeInByte() {
    try {
      long totalSize = 0;
      for (FSPath[] tierDataDirs : config.getDataDirs()) {
        for (FSPath dataDir : tierDataDirs) {
          totalSize += FileUtils.sizeOfDirectory(dataDir.toFile());
        }
      }
      return totalSize;
    } catch (Exception e) {
      logger.error("meet error while trying to get data size.", e);
      return -1;
    }
  }

  @Override
  public boolean getWriteAheadLogStatus() {
    return config.isEnableWal();
  }

  @Override
  public boolean getEnableStatMonitor() {
    return config.isEnableStatMonitor();
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(getInstance(), mbeanName);
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MONITOR_SERVICE;
  }

  private static class StatMonitorHolder {

    private StatMonitorHolder() {
      // allowed do nothing
    }

    private static final StatMonitor INSTANCE = new StatMonitor();
  }
}
