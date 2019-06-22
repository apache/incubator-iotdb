///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.iotdb.db.engine.filenode;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Future;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//import org.apache.commons.io.FileUtils;
//import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
//import org.apache.iotdb.db.conf.IoTDBConfig;
//import org.apache.iotdb.db.conf.IoTDBConstant;
//import org.apache.iotdb.db.conf.IoTDBDescriptor;
//import org.apache.iotdb.db.conf.directories.DirectoryManager;
//import org.apache.iotdb.db.engine.Processor;
//import org.apache.iotdb.db.engine.bufferwrite.BufferWriteProcessor;
//import org.apache.iotdb.db.engine.memcontrol.BasicMemController;
//import org.apache.iotdb.db.engine.overflow.io.OverflowProcessor;
//import org.apache.iotdb.db.engine.pool.FlushPoolManager;
//import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
//import org.apache.iotdb.db.exception.BufferWriteProcessorException;
//import org.apache.iotdb.db.exception.FileNodeManagerException;
//import org.apache.iotdb.db.exception.FileNodeProcessorException;
//import org.apache.iotdb.db.exception.PathErrorException;
//import org.apache.iotdb.db.exception.ProcessorException;
//import org.apache.iotdb.db.metadata.MManager;
//import org.apache.iotdb.db.monitor.IStatistic;
//import org.apache.iotdb.db.monitor.MonitorConstants;
//import org.apache.iotdb.db.monitor.StatMonitor;
//import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
//import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
//import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
//import org.apache.iotdb.db.query.context.QueryContext;
//import org.apache.iotdb.db.query.control.FileReaderManager;
//import org.apache.iotdb.db.service.IService;
//import org.apache.iotdb.db.service.ServiceType;
//import org.apache.iotdb.db.utils.MemUtils;
//import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
//import org.apache.iotdb.db.writelog.node.WriteLogNode;
//import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
//import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
//import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
//import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
//import org.apache.iotdb.tsfile.read.common.Path;
//import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
//import org.apache.iotdb.tsfile.write.record.TSRecord;
//import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class FileNodeManager implements IStatistic, IService {
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(FileNodeManagerV2.class);
//  private static final IoTDBConfig TsFileDBConf = IoTDBDescriptor.getInstance().getConfig();
//  private static final DirectoryManager DIRECTORY_MANAGER = DirectoryManager.getInstance();
//  /**
//   * a folder that persist FileNodeProcessorStore classes. Each stroage group will have a subfolder.
//   * by default, it is system/info
//   */
//  private final String baseDir;
//
//  /**
//   * This map is used to manage all filenode processor,<br> the key is filenode name which is
//   * storage group seriesPath.
//   */
//  private ConcurrentHashMap<String, FileNodeProcessor> processorMap;
//  /**
//   * This set is used to store overflowed filenode name.<br> The overflowed filenode will be merge.
//   */
//  private volatile FileNodeManagerStatus fileNodeManagerStatus = FileNodeManagerStatus.NONE;
//  // There is no need to add concurrently
//  private HashMap<String, AtomicLong> statParamsHashMap;
//
//  ScheduledExecutorService closedProcessorCleaner = IoTDBThreadPoolFactory.newScheduledThreadPool(1,
//      "Closed FileNodeProcessors Cleaner");
//
//  private FileNodeManager(String baseDir) {
//    processorMap = new ConcurrentHashMap<>();
//    statParamsHashMap = new HashMap<>();
//    //label: A
//    for (MonitorConstants.FileNodeManagerStatConstants fileNodeManagerStatConstant :
//        MonitorConstants.FileNodeManagerStatConstants.values()) {
//      statParamsHashMap.put(fileNodeManagerStatConstant.name(), new AtomicLong(0));
//    }
//
//    String normalizedBaseDir = baseDir;
//    if (normalizedBaseDir.charAt(normalizedBaseDir.length() - 1) != File.separatorChar) {
//      normalizedBaseDir += Character.toString(File.separatorChar);
//    }
//    this.baseDir = normalizedBaseDir;
//    File dir = new File(normalizedBaseDir);
//    if (dir.mkdirs()) {
//      LOGGER.info("{} dir home doesn't exist, create it", dir.getPath());
//    }
//    //TODO merge this with label A
//    if (TsFileDBConf.isEnableStatMonitor()) {
//      StatMonitor statMonitor = StatMonitor.getInstance();
//      registerStatMetadata();
//      statMonitor.registerStatistics(MonitorConstants.STAT_STORAGE_DELTA_NAME, this);
//    }
//
//    closedProcessorCleaner.scheduleWithFixedDelay(()->{
//      int size = 0;
//      for (FileNodeProcessor fileNodeProcessor : processorMap.values()) {
//        size += fileNodeProcessor.getClosingBufferWriteProcessor().size();
//      }
//      if (size > 5) {
//        LOGGER.info("Current closing processor number is {}", size);
//      }
////      for (FileNodeProcessor fileNodeProcessor : processorMap.values()) {
////        fileNodeProcessor.checkAllClosingProcessors();
////      }
//    }, 0, 30000, TimeUnit.MILLISECONDS);
//
//  }
//
//  public static FileNodeManager getInstance() {
//    return FileNodeManagerHolder.INSTANCE;
//  }
//
//  private void updateStatHashMapWhenFail(TSRecord tsRecord) {
//    statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_REQ_FAIL.name())
//        .incrementAndGet();
//    statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS_FAIL.name())
//        .addAndGet(tsRecord.dataPointList.size());
//  }
//
//  /**
//   * get stats parameter hash map.
//   *
//   * @return the key represents the params' name, values is AtomicLong type
//   */
//  @Override
//  public Map<String, AtomicLong> getStatParamsHashMap() {
//    return statParamsHashMap;
//  }
//
//  @Override
//  public List<String> getAllPathForStatistic() {
//    List<String> list = new ArrayList<>();
//    for (MonitorConstants.FileNodeManagerStatConstants statConstant :
//        MonitorConstants.FileNodeManagerStatConstants.values()) {
//      list.add(MonitorConstants.STAT_STORAGE_DELTA_NAME + MonitorConstants.MONITOR_PATH_SEPARATOR
//          + statConstant.name());
//    }
//    return list;
//  }
//
//  @Override
//  public Map<String, TSRecord> getAllStatisticsValue() {
//    long curTime = System.currentTimeMillis();
//    TSRecord tsRecord = StatMonitor
//        .convertToTSRecord(getStatParamsHashMap(), MonitorConstants.STAT_STORAGE_DELTA_NAME,
//            curTime);
//    HashMap<String, TSRecord> ret = new HashMap<>();
//    ret.put(MonitorConstants.STAT_STORAGE_DELTA_NAME, tsRecord);
//    return ret;
//  }
//
//  /**
//   * Init Stat MetaDta.
//   */
//  @Override
//  public void registerStatMetadata() {
//    Map<String, String> hashMap = new HashMap<>();
//    for (MonitorConstants.FileNodeManagerStatConstants statConstant :
//        MonitorConstants.FileNodeManagerStatConstants.values()) {
//      hashMap
//          .put(MonitorConstants.STAT_STORAGE_DELTA_NAME + MonitorConstants.MONITOR_PATH_SEPARATOR
//              + statConstant.name(), MonitorConstants.DATA_TYPE_INT64);
//    }
//    StatMonitor.getInstance().registerStatStorageGroup(hashMap);
//  }
//
//  /**
//   * This function is just for unit test.
//   */
//  public synchronized void resetFileNodeManager() {
//    for (String key : statParamsHashMap.keySet()) {
//      statParamsHashMap.put(key, new AtomicLong());
//    }
//    processorMap.clear();
//  }
//
//  /**
//   * @param filenodeName storage name, e.g., root.a.b
//   */
//  private FileNodeProcessor constructNewProcessor(String filenodeName)
//      throws FileNodeManagerException {
//    try {
//      return new FileNodeProcessor(baseDir, filenodeName);
//    } catch (FileNodeProcessorException e) {
//      LOGGER.error("Can't construct the FileNodeProcessor, the filenode is {}", filenodeName, e);
//      throw new FileNodeManagerException(e);
//    }
//  }
//
//  private FileNodeProcessor getProcessor(String path, boolean isWriteLock)
//      throws FileNodeManagerException {
//    String filenodeName;
//    try {
//      // return the stroage name
//      filenodeName = MManager.getInstance().getFileNameByPath(path);
//    } catch (PathErrorException e) {
//      LOGGER.error("MManager get filenode name error, seriesPath is {}", path);
//      throw new FileNodeManagerException(e);
//    }
//    FileNodeProcessor processor;
//    processor = processorMap.get(filenodeName);
//    if (processor != null) {
//      processor.lock(isWriteLock);
//    } else {
//      filenodeName = filenodeName.intern();
//      // calculate the value with same key synchronously
//      synchronized (filenodeName) {
//        processor = processorMap.get(filenodeName);
//        if (processor != null) {
//          processor.lock(isWriteLock);
//        } else {
//          // calculate the value with the key monitor
//          LOGGER.debug("construct a processor instance, the filenode is {}, Thread is {}",
//              filenodeName, Thread.currentThread().getId());
//          processor = constructNewProcessor(filenodeName);
//          processor.lock(isWriteLock);
//          processorMap.put(filenodeName, processor);
//        }
//      }
//    }
//    return processor;
//  }
//
//  /**
//   * recovery the filenode processor.
//   */
//  public void recovery() {
//    List<String> filenodeNames = null;
//    try {
//      filenodeNames = MManager.getInstance().getAllFileNames();
//    } catch (PathErrorException e) {
//      LOGGER.error("Restoring all FileNodes failed.", e);
//      return;
//    }
//    for (String filenodeName : filenodeNames) {
//      FileNodeProcessor fileNodeProcessor = null;
//      try {
//        // recover in initialization
//        fileNodeProcessor = getProcessor(filenodeName, true);
//      } catch (FileNodeManagerException e) {
//        LOGGER.error("Restoring fileNode {} failed.", filenodeName, e);
//      } finally {
//        if (fileNodeProcessor != null) {
//          fileNodeProcessor.writeUnlock();
//        }
//      }
//      // add index check sum
//    }
//  }
//
//  /**
//   * insert TsRecord into storage group.
//   *
//   * @param tsRecord input Data
//   * @param isMonitor if true, the insertion is done by StatMonitor and the statistic Info will not
//   * be recorded. if false, the statParamsHashMap will be updated.
//   * @return an int value represents the insert type, 0: failed; 1: overflow; 2: bufferwrite
//   */
//  public int insert(TSRecord tsRecord, boolean isMonitor) throws FileNodeManagerException {
//    long timestamp = tsRecord.time;
//
//    String deviceId = tsRecord.deviceId;
//    checkTimestamp(tsRecord);
////    //if memory is dangerous, directly reject
////    long memUsage = MemUtils.getRecordSize(tsRecord);
////    BasicMemController.UsageLevel level = BasicMemController.getInstance()
////        .acquireUsage(this, memUsage);
////    if (level == UsageLevel.DANGEROUS) {
////      return 0;
////    }
//
//    updateStat(isMonitor, tsRecord);
//
//    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
//    int insertType;
//
//    try {
//      long lastUpdateTime = fileNodeProcessor.getFlushLastUpdateTime(deviceId);
//      if (timestamp < lastUpdateTime) {
//
//        long startOverflow = System.currentTimeMillis();
//
//        insertOverflow(fileNodeProcessor, timestamp, tsRecord, isMonitor, deviceId);
//
//        startOverflow = System.currentTimeMillis() - startOverflow;
//        if (startOverflow > 1000) {
//          LOGGER.info("has overflow data, insert cost: {}", startOverflow);
//        }
//
//        insertType = 1;
//      } else {
//        insertBufferWrite(fileNodeProcessor, timestamp, isMonitor, tsRecord, deviceId);
//        insertType = 2;
//      }
//    } catch (FileNodeProcessorException e) {
//      LOGGER.error(String.format("Encounter an error when closing the buffer insert processor %s.",
//          fileNodeProcessor.getProcessorName()), e);
//      throw new FileNodeManagerException(e);
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//    // Modify the insert
//    if (!isMonitor) {
//      fileNodeProcessor.getStatParamsHashMap()
//          .get(MonitorConstants.FileNodeProcessorStatConstants.TOTAL_POINTS_SUCCESS.name())
//          .addAndGet(tsRecord.dataPointList.size());
//      fileNodeProcessor.getStatParamsHashMap()
//          .get(MonitorConstants.FileNodeProcessorStatConstants.TOTAL_REQ_SUCCESS.name())
//          .incrementAndGet();
//      statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_REQ_SUCCESS.name())
//          .incrementAndGet();
//      statParamsHashMap
//          .get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS_SUCCESS.name())
//          .addAndGet(tsRecord.dataPointList.size());
//    }
//    return insertType;
//  }
//
//  private void writeLog(TSRecord tsRecord, boolean isMonitor, WriteLogNode logNode)
//      throws FileNodeManagerException {
//    try {
//      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
//        String[] measurementList = new String[tsRecord.dataPointList.size()];
//        String[] insertValues = new String[tsRecord.dataPointList.size()];
//        int i=0;
//        for (DataPoint dp : tsRecord.dataPointList) {
//          measurementList[i] = dp.getMeasurementId();
//          insertValues[i] = dp.getValue().toString();
//          i++;
//        }
//        logNode.write(new InsertPlan(2, tsRecord.deviceId, tsRecord.time, measurementList,
//            insertValues));
//      }
//    } catch (IOException e) {
//      if (!isMonitor) {
//        updateStatHashMapWhenFail(tsRecord);
//      }
//      throw new FileNodeManagerException(e);
//    }
//  }
//
//  private void checkTimestamp(TSRecord tsRecord) throws FileNodeManagerException {
//    if (tsRecord.time < 0) {
//      LOGGER.error("The insert time lt 0, {}.", tsRecord);
//      throw new FileNodeManagerException("The insert time lt 0, the tsrecord is " + tsRecord);
//    }
//  }
//
//  private void updateStat(boolean isMonitor, TSRecord tsRecord) {
//    if (!isMonitor) {
//      statParamsHashMap.get(MonitorConstants.FileNodeManagerStatConstants.TOTAL_POINTS.name())
//          .addAndGet(tsRecord.dataPointList.size());
//    }
//  }
//
//  private void insertOverflow(FileNodeProcessor fileNodeProcessor, long timestamp,
//      TSRecord tsRecord, boolean isMonitor, String deviceId)
//      throws FileNodeManagerException {
//    // get overflow processor
//    OverflowProcessor overflowProcessor;
//    String filenodeName = fileNodeProcessor.getProcessorName();
//    try {
//      overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName);
//    } catch (ProcessorException e) {
//      LOGGER.error("Get the overflow processor failed, the filenode is {}, insert time is {}",
//          filenodeName, timestamp);
//      if (!isMonitor) {
//        updateStatHashMapWhenFail(tsRecord);
//      }
//      throw new FileNodeManagerException(e);
//    }
//    // insert wal
//    try {
//      writeLog(tsRecord, isMonitor, overflowProcessor.getLogNode());
//    } catch (IOException e) {
//      throw new FileNodeManagerException(e);
//    }
//    // insert overflow data
//    try {
//      overflowProcessor.insert(tsRecord);
//      fileNodeProcessor.changeTypeToChanged(deviceId, timestamp);
//      fileNodeProcessor.setOverflowed(true);
//    } catch (IOException e) {
//      LOGGER.error("Insert into overflow error, the reason is {}", e);
//      if (!isMonitor) {
//        updateStatHashMapWhenFail(tsRecord);
//      }
//      throw new FileNodeManagerException(e);
//    }
//  }
//
//  private void insertBufferWrite(FileNodeProcessor fileNodeProcessor, long timestamp,
//      boolean isMonitor, TSRecord tsRecord, String deviceId)
//      throws FileNodeManagerException, FileNodeProcessorException {
//
//    long start1 = System.currentTimeMillis();
//    // get bufferwrite processor
//    BufferWriteProcessor bufferWriteProcessor;
//    String filenodeName = fileNodeProcessor.getProcessorName();
//    try {
//      bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor(filenodeName, timestamp);
//
//    } catch (FileNodeProcessorException e) {
//      LOGGER.error("Get the bufferwrite processor failed, the filenode is {}, insert time is {}",
//          filenodeName, timestamp);
//      if (!isMonitor) {
//        updateStatHashMapWhenFail(tsRecord);
//      }
//      throw new FileNodeManagerException(e);
//    } finally {
//      long start1_1 = System.currentTimeMillis() - start1;
//      if (start1_1 > 1000) {
//        LOGGER.info("FileNodeManagerV2.insertBufferWrite step-1-1, cost: {}", start1_1);
//      }
//    }
//
//    long start1_2 = System.currentTimeMillis();
//    // Add a new interval file to newfilelist
//    if (bufferWriteProcessor.isNewProcessor()) {
//      bufferWriteProcessor.setNewProcessor(false);
//      String bufferwriteBaseDir = bufferWriteProcessor.getBaseDir();
//      String bufferwriteRelativePath = bufferWriteProcessor.getFileRelativePath();
//      try {
//        bufferWriteProcessor.setCurrentTsFileResource(new TsFileResource(new File(new File(bufferwriteBaseDir), bufferwriteRelativePath), false));
//        fileNodeProcessor.addIntervalFileNode(bufferWriteProcessor.getCurrentTsFileResource());
//      } catch (Exception e) {
//        if (!isMonitor) {
//          updateStatHashMapWhenFail(tsRecord);
//        }
//        throw new FileNodeManagerException(e);
//      }
//    }
//    start1_2 = System.currentTimeMillis() - start1_2;
//    if (start1_2 > 1000) {
//      LOGGER.info("FileNodeManagerV2.insertBufferWrite step-1-2, cost: {}", start1_2);
//    }
//
//    start1 = System.currentTimeMillis() - start1;
//    if (start1 > 1000) {
//      LOGGER.info("FileNodeManagerV2.insertBufferWrite step-1, cost: {}", start1);
//    }
//
//    long start2 = System.currentTimeMillis();
//
//    long start2_1 = start2;
//    // insert wal
//    try {
//      writeLog(tsRecord, isMonitor, bufferWriteProcessor.getLogNode());
//    } catch (IOException e) {
//      throw new FileNodeManagerException(e);
//    }
//    start2_1 = System.currentTimeMillis() - start2_1;
//    if (start2_1 > 1000) {
//      LOGGER.info("FileNodeManagerV2.insertBufferWrite step2-1 cost: {}", start2_1);
//    }
//
//    long start2_2 = System.currentTimeMillis();
//    // Write data
//    long prevStartTime = fileNodeProcessor.getIntervalFileNodeStartTime(deviceId);
//    long prevUpdateTime = fileNodeProcessor.getLastUpdateTime(deviceId);
//
//    fileNodeProcessor.setIntervalFileNodeStartTime(deviceId);
//    fileNodeProcessor.setLastUpdateTime(deviceId, timestamp);
//
//    start2_2 = System.currentTimeMillis() - start2_2;
//    if (start2_2 > 1000) {
//      LOGGER.info("FileNodeManagerV2.insertBufferWrite step2-2 cost: {}", start2_2);
//    }
//    try {
//      long start2_3 = System.currentTimeMillis();
//
//      // insert tsrecord and check flushMetadata
//      if (!bufferWriteProcessor.write(tsRecord)) {
//        start2_3 = System.currentTimeMillis() - start2_3;
//        if (start2_3 > 1000) {
//          LOGGER.info("FileNodeManagerV2.insertBufferWrite step2-3 cost: {}", start2_3);
//        }
//
//        long start2_4 = System.currentTimeMillis();
//        // undo time update
//        fileNodeProcessor.setIntervalFileNodeStartTime(deviceId, prevStartTime);
//        fileNodeProcessor.setLastUpdateTime(deviceId, prevUpdateTime);
//        start2_4 = System.currentTimeMillis() - start2_4;
//        if (start2_4 > 1000) {
//          LOGGER.info("FileNodeManagerV2.insertBufferWrite step2-4 cost: {}", start2_4);
//        }
//      }
//    } catch (BufferWriteProcessorException e) {
//      if (!isMonitor) {
//        updateStatHashMapWhenFail(tsRecord);
//      }
//      throw new FileNodeManagerException(e);
//    }
//    start2 = System.currentTimeMillis() - start2;
//    if (start2 > 1000) {
//      LOGGER.info("FileNodeManagerV2.insertBufferWrite step-2, cost: {}", start2);
//    }
//
//    long start3 = System.currentTimeMillis();
//
//    // check if the file should be closed
//    if (bufferWriteProcessor
//        .getFileSize() > IoTDBDescriptor.getInstance()
//        .getConfig().getBufferwriteFileSizeThreshold()) {
//      if (LOGGER.isInfoEnabled()) {
//        LOGGER.info(
//            "The filenode processor {} will setCloseMark the bufferwrite processor, "
//                + "because the size[{}] of tsfile {} reaches the threshold {}",
//            filenodeName, MemUtils.bytesCntToStr(bufferWriteProcessor.getFileSize()),
//            bufferWriteProcessor.getInsertFilePath(), MemUtils.bytesCntToStr(
//                IoTDBDescriptor.getInstance().getConfig().getBufferwriteFileSizeThreshold()));
//      }
//
//      fileNodeProcessor.closeBufferWrite();
//      start3 = System.currentTimeMillis() - start3;
//      if (start3 > 1000) {
//        LOGGER.info("FileNodeManagerV2.insertBufferWrite step-3, setCloseMark buffer insert cost: {}", start3);
//      }
//    }
//  }
//
//  /**
//   * update data.
//   */
//  public void update(String deviceId, String measurementId, long startTime, long endTime,
//      TSDataType type, String v)
//      throws FileNodeManagerException {
//
//    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
//    try {
//
//      long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deviceId);
//      if (startTime > lastUpdateTime) {
//        LOGGER.warn("The update range is error, startTime {} is great than lastUpdateTime {}",
//            startTime,
//            lastUpdateTime);
//        return;
//      }
//      long finalEndTime = endTime > lastUpdateTime ? lastUpdateTime : endTime;
//
//      String filenodeName = fileNodeProcessor.getProcessorName();
//      // get overflow processor
//      OverflowProcessor overflowProcessor;
//      try {
//        overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName);
//      } catch (ProcessorException e) {
//        LOGGER.error(
//            "Get the overflow processor failed, the filenode is {}, "
//                + "insert time range is from {} to {}",
//            filenodeName, startTime, finalEndTime);
//        throw new FileNodeManagerException(e);
//      }
//      overflowProcessor.update(deviceId, measurementId, startTime, finalEndTime, type, v);
//      // change the type of tsfile to overflowed
//      fileNodeProcessor.changeTypeToChanged(deviceId, startTime, finalEndTime);
//      fileNodeProcessor.setOverflowed(true);
//
//      // insert wal
//      try {
//        if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
//          overflowProcessor.getLogNode().write(
//              new UpdatePlan(startTime, finalEndTime, v, new Path(deviceId
//                  + "." + measurementId)));
//        }
//      } catch (IOException e) {
//        throw new FileNodeManagerException(e);
//      }
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//  }
//
//  /**
//   * delete data.
//   */
//  public void delete(String deviceId, String measurementId, long timestamp)
//      throws FileNodeManagerException {
//
//    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
//    try {
//      long lastUpdateTime = fileNodeProcessor.getLastUpdateTime(deviceId);
//      // no tsfile data, the delete operation is invalid
//      if (lastUpdateTime == -1) {
//        LOGGER.warn("The last update time is -1, delete overflow is invalid, "
//                + "the filenode processor is {}",
//            fileNodeProcessor.getProcessorName());
//      } else {
//        // insert wal
//        if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
//          // get processors for wal
//          String filenodeName = fileNodeProcessor.getProcessorName();
//          OverflowProcessor overflowProcessor;
//          BufferWriteProcessor bufferWriteProcessor;
//          try {
//            overflowProcessor = fileNodeProcessor.getOverflowProcessor(filenodeName);
//            // in case that no BufferWriteProcessor is available, a new BufferWriteProcessor is
//            // needed to access LogNode.
//            // TODO this may make the time range of the next TsFile a little wider
//            bufferWriteProcessor = fileNodeProcessor.getBufferWriteProcessor(filenodeName,
//                lastUpdateTime + 1);
//          } catch (ProcessorException e) {
//            LOGGER.error("Getting the processor failed, the filenode is {}, delete time is {}.",
//                filenodeName, timestamp);
//            throw new FileNodeManagerException(e);
//          }
//          try {
//            overflowProcessor.getLogNode().write(new DeletePlan(timestamp,
//                new Path(deviceId + "." + measurementId)));
//            bufferWriteProcessor.getLogNode().write(new DeletePlan(timestamp,
//                new Path(deviceId + "." + measurementId)));
//          } catch (IOException e) {
//            throw new FileNodeManagerException(e);
//          }
//        }
//
//        try {
//          fileNodeProcessor.delete(deviceId, measurementId, timestamp);
//        } catch (IOException e) {
//          throw new FileNodeManagerException(e);
//        }
//        // change the type of tsfile to overflowed
//        fileNodeProcessor.changeTypeToChangedForDelete(deviceId, timestamp);
//        fileNodeProcessor.setOverflowed(true);
//
//      }
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//  }
//
//  private void delete(String processorName,
//      Iterator<Map.Entry<String, FileNodeProcessor>> processorIterator)
//      throws FileNodeManagerException {
//    if (!processorMap.containsKey(processorName)) {
//      //TODO do we need to call processorIterator.remove() ?
//      LOGGER.warn("The processorMap doesn't contain the filenode processor {}.", processorName);
//      return;
//    }
//    LOGGER.info("Try to delete the filenode processor {}.", processorName);
//    FileNodeProcessor processor = processorMap.get(processorName);
//    if (!processor.tryWriteLock()) {
//      throw new FileNodeManagerException(String
//          .format("Can't delete the filenode processor %s because Can't get the insert lock.",
//              processorName));
//    }
//
//    try {
//      if (!processor.canBeClosed()) {
//        LOGGER.warn("The filenode processor {} can't be deleted.", processorName);
//        return;
//      }
//
//      try {
//        LOGGER.info("Delete the filenode processor {}.", processorName);
//        processor.delete();
//        processorIterator.remove();
//      } catch (ProcessorException e) {
//        LOGGER.error("Delete the filenode processor {} by iterator error.", processorName, e);
//        throw new FileNodeManagerException(e);
//      }
//    } finally {
//      processor.writeUnlock();
//    }
//  }
//
//  /**
//   * Similar to delete(), but only deletes data in BufferWrite. Only used by WAL recovery.
//   */
//  public void deleteBufferWrite(String deviceId, String measurementId, long timestamp)
//      throws FileNodeManagerException {
//    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
//    try {
//      fileNodeProcessor.deleteBufferWrite(deviceId, measurementId, timestamp);
//    } catch (BufferWriteProcessorException | IOException e) {
//      throw new FileNodeManagerException(e);
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//    // change the type of tsfile to overflowed
//    fileNodeProcessor.changeTypeToChangedForDelete(deviceId, timestamp);
//    fileNodeProcessor.setOverflowed(true);
//  }
//
//  /**
//   * Similar to delete(), but only deletes data in Overflow. Only used by WAL recovery.
//   */
//  public void deleteOverflow(String deviceId, String measurementId, long timestamp)
//      throws FileNodeManagerException {
//    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
//    try {
//      fileNodeProcessor.deleteOverflow(deviceId, measurementId, timestamp);
//    } catch (ProcessorException e) {
//      throw new FileNodeManagerException(e);
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//    // change the type of tsfile to overflowed
//    fileNodeProcessor.changeTypeToChangedForDelete(deviceId, timestamp);
//    fileNodeProcessor.setOverflowed(true);
//  }
//
//  /**
//   * begin query.
//   *
//   * @param deviceId queried deviceId
//   * @return a query token for the device.
//   */
//  public int beginQuery(String deviceId) throws FileNodeManagerException {
//    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
//    try {
//      LOGGER.debug("Get the FileNodeProcessor: filenode is {}, begin query.",
//          fileNodeProcessor.getProcessorName());
//      return fileNodeProcessor.addMultiPassCount();
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//  }
//
//  /**
//   * query data.
//   */
//  public QueryDataSource query(SingleSeriesExpression seriesExpression, QueryContext context)
//      throws FileNodeManagerException {
//    String deviceId = seriesExpression.getSeriesPath().getDevice();
//    String measurementId = seriesExpression.getSeriesPath().getMeasurement();
//    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, false);
//    LOGGER.debug("Get the FileNodeProcessor: filenode is {}, query.",
//        fileNodeProcessor.getProcessorName());
//    try {
//      QueryDataSource queryDataSource;
//      // query operation must have overflow processor
//      if (!fileNodeProcessor.hasOverflowProcessor()) {
//        try {
//          fileNodeProcessor.getOverflowProcessor(fileNodeProcessor.getProcessorName());
//        } catch (ProcessorException e) {
//          LOGGER.error("Get the overflow processor failed, the filenode is {}, query is {},{}",
//              fileNodeProcessor.getProcessorName(), deviceId, measurementId);
//          throw new FileNodeManagerException(e);
//        }
//      }
//      try {
//        queryDataSource = fileNodeProcessor.query(deviceId, measurementId, context);
//      } catch (FileNodeProcessorException e) {
//        LOGGER.error("Query error: the deviceId {}, the measurementId {}", deviceId, measurementId,
//            e);
//        throw new FileNodeManagerException(e);
//      }
//      // return query structure
//      return queryDataSource;
//    } finally {
//      fileNodeProcessor.readUnlock();
//    }
//  }
//
//  /**
//   * end query.
//   */
//  public void endQuery(String deviceId, int token) throws FileNodeManagerException {
//
//    FileNodeProcessor fileNodeProcessor = getProcessor(deviceId, true);
//    try {
//      LOGGER.debug("Get the FileNodeProcessor: {} end query.",
//          fileNodeProcessor.getProcessorName());
//      fileNodeProcessor.decreaseMultiPassCount(token);
//    } catch (FileNodeProcessorException e) {
//      LOGGER.error("Failed to end query: the deviceId {}, token {}.", deviceId, token, e);
//      throw new FileNodeManagerException(e);
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//  }
//
//  /**
//   * Append one specified tsfile to the storage group. <b>This method is only provided for
//   * transmission module</b>
//   *
//   * @param fileNodeName the seriesPath of storage group
//   * @param appendFile the appended tsfile information
//   */
//  public boolean appendFileToFileNode(String fileNodeName, TsFileResource appendFile,
//      String appendFilePath) throws FileNodeManagerException {
//    FileNodeProcessor fileNodeProcessor = getProcessor(fileNodeName, true);
//    try {
//      // check append file
//      for (Map.Entry<String, Long> entry : appendFile.getStartTimeMap().entrySet()) {
//        if (fileNodeProcessor.getLastUpdateTime(entry.getKey()) >= entry.getValue()) {
//          return false;
//        }
//      }
//      // setCloseMark bufferwrite file
//      fileNodeProcessor.closeBufferWrite();
//      // append file to storage group.
//      fileNodeProcessor.appendFile(appendFile, appendFilePath);
//    } catch (FileNodeProcessorException e) {
//      LOGGER.error("Cannot append the file {} to {}", appendFile.getFile().getAbsolutePath(), fileNodeName, e);
//      throw new FileNodeManagerException(e);
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//    return true;
//  }
//
//  /**
//   * get all overlap tsfiles which are conflict with the appendFile.
//   *
//   * @param fileNodeName the seriesPath of storage group
//   * @param appendFile the appended tsfile information
//   */
//  public List<String> getOverlapFilesFromFileNode(String fileNodeName, TsFileResource appendFile,
//      String uuid) throws FileNodeManagerException {
//    FileNodeProcessor fileNodeProcessor = getProcessor(fileNodeName, true);
//    List<String> overlapFiles;
//    try {
//      overlapFiles = fileNodeProcessor.getOverlapFiles(appendFile, uuid);
//    } catch (FileNodeProcessorException e) {
//      throw new FileNodeManagerException(e);
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//    return overlapFiles;
//  }
//
//  /**
//   * merge all overflowed filenode.
//   *
//   * @throws FileNodeManagerException FileNodeManagerException
//   */
//  public void mergeAll() throws FileNodeManagerException {
//    if (fileNodeManagerStatus != FileNodeManagerStatus.NONE) {
//      LOGGER.warn("Failed to merge all overflowed filenode, because filenode manager status is {}",
//          fileNodeManagerStatus);
//      return;
//    }
//
//    fileNodeManagerStatus = FileNodeManagerStatus.MERGE;
//    LOGGER.info("Start to merge all overflowed filenode");
//    List<String> allFileNodeNames;
//    try {
//      allFileNodeNames = MManager.getInstance().getAllFileNames();
//    } catch (PathErrorException e) {
//      LOGGER.error("Get all storage group seriesPath error,", e);
//      throw new FileNodeManagerException(e);
//    }
//    List<Future<?>> futureTasks = new ArrayList<>();
//    for (String fileNodeName : allFileNodeNames) {
//      FileNodeProcessor fileNodeProcessor = getProcessor(fileNodeName, true);
//      try {
//        Future<?> task = fileNodeProcessor.submitToMerge();
//        if (task != null) {
//          LOGGER.info("Submit the filenode {} to the merge pool", fileNodeName);
//          futureTasks.add(task);
//        }
//      } finally {
//        fileNodeProcessor.writeUnlock();
//      }
//    }
//    long totalTime = 0;
//    // loop waiting for merge to end, the longest waiting time is
//    // 60s.
//    int time = 2;
//    List<Exception> mergeException = new ArrayList<>();
//    for (Future<?> task : futureTasks) {
//      while (!task.isDone()) {
//        try {
//          LOGGER.info(
//              "Waiting for the end of merge, already waiting for {}s, "
//                  + "continue to wait anothor {}s",
//              totalTime, time);
//          TimeUnit.SECONDS.sleep(time);
//          totalTime += time;
//          time = updateWaitTime(time);
//        } catch (InterruptedException e) {
//          LOGGER.error("Unexpected interruption {}", e);
//          Thread.currentThread().interrupt();
//        }
//      }
//      try {
//        task.get();
//      } catch (InterruptedException e) {
//        LOGGER.error("Unexpected interruption {}", e);
//      } catch (ExecutionException e) {
//        mergeException.add(e);
//        LOGGER.error("The exception for merge: {}", e);
//      }
//    }
//    if (!mergeException.isEmpty()) {
//      // just throw the first exception
//      throw new FileNodeManagerException(mergeException.get(0));
//    }
//    fileNodeManagerStatus = FileNodeManagerStatus.NONE;
//    LOGGER.info("End to merge all overflowed filenode");
//  }
//
//  private int updateWaitTime(int time) {
//    return time < 32 ? time * 2 : 60;
//  }
//
//  /**
//   * delete one filenode.
//   */
//  public void deleteOneFileNode(String processorName) throws FileNodeManagerException {
//    if (fileNodeManagerStatus != FileNodeManagerStatus.NONE) {
//      return;
//    }
//
//    fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
//    try {
//      if (processorMap.containsKey(processorName)) {
//        deleteFileNodeBlocked(processorName);
//      }
//      String fileNodePath = TsFileDBConf.getFileNodeDir();
//      fileNodePath = standardizeDir(fileNodePath) + processorName;
//      FileUtils.deleteDirectory(new File(fileNodePath));
//
//      cleanBufferWrite(processorName);
//
//      MultiFileLogNodeManager.getInstance()
//          .deleteNode(processorName + IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX);
//      MultiFileLogNodeManager.getInstance()
//          .deleteNode(processorName + IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX);
//    } catch (IOException e) {
//      LOGGER.error("Delete the filenode processor {} error.", processorName, e);
//      throw new FileNodeManagerException(e);
//    } finally {
//      fileNodeManagerStatus = FileNodeManagerStatus.NONE;
//    }
//  }
//
//  private void cleanBufferWrite(String processorName) throws IOException {
//    List<String> bufferwritePathList = DIRECTORY_MANAGER.getAllTsFileFolders();
//    for (String bufferwritePath : bufferwritePathList) {
//      bufferwritePath = standardizeDir(bufferwritePath) + processorName;
//      File bufferDir = new File(bufferwritePath);
//      // free and setCloseMark the streams under this bufferwrite directory
//      if (!bufferDir.exists()) {
//        continue;
//      }
//      File[] bufferFiles = bufferDir.listFiles();
//      if (bufferFiles != null) {
//        for (File bufferFile : bufferFiles) {
//          FileReaderManager.getInstance().closeFileAndRemoveReader(bufferFile.getPath());
//        }
//      }
//      FileUtils.deleteDirectory(new File(bufferwritePath));
//    }
//  }
//
//  private void deleteFileNodeBlocked(String processorName) throws FileNodeManagerException {
//    LOGGER.info("Forced to delete the filenode processor {}", processorName);
//    FileNodeProcessor processor = processorMap.get(processorName);
//    while (true) {
//      if (processor.tryWriteLock()) {
//        try {
//          if (processor.canBeClosed()) {
//            LOGGER.info("Delete the filenode processor {}.", processorName);
//            processor.delete();
//            processorMap.remove(processorName);
//            break;
//          } else {
//            LOGGER.info(
//                "Can't delete the filenode processor {}, "
//                    + "because the filenode processor can't be closed."
//                    + " Wait 100ms to retry");
//          }
//        } catch (ProcessorException e) {
//          LOGGER.error("Delete the filenode processor {} error.", processorName, e);
//          throw new FileNodeManagerException(e);
//        } finally {
//          processor.writeUnlock();
//        }
//      } else {
//        LOGGER.info(
//            "Can't delete the filenode processor {}, because it can't get the insert lock."
//                + " Wait 100ms to retry", processorName);
//      }
//      try {
//        TimeUnit.MILLISECONDS.sleep(100);
//      } catch (InterruptedException e) {
//        LOGGER.error(e.getMessage());
//        Thread.currentThread().interrupt();
//      }
//    }
//  }
//
//  private String standardizeDir(String originalPath) {
//    String res = originalPath;
//    if ((originalPath.length() > 0
//        && originalPath.charAt(originalPath.length() - 1) != File.separatorChar)
//        || originalPath.length() == 0) {
//      res = originalPath + File.separatorChar;
//    }
//    return res;
//  }
//
//  /**
//   * add time series.
//   */
//  public void addTimeSeries(Path path, TSDataType dataType, TSEncoding encoding,
//      CompressionType compressor,
//      Map<String, String> props) throws FileNodeManagerException {
//    FileNodeProcessor fileNodeProcessor = getProcessor(path.getFullPath(), true);
//    try {
//      fileNodeProcessor.addTimeSeries(path.getMeasurement(), dataType, encoding, compressor, props);
//    } finally {
//      fileNodeProcessor.writeUnlock();
//    }
//  }
//
//
//  /**
//   * Force to setCloseMark the filenode processor.
//   */
//  public void closeOneFileNode(String processorName) throws FileNodeManagerException {
//    if (fileNodeManagerStatus != FileNodeManagerStatus.NONE) {
//      return;
//    }
//
//    fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
//    try {
//      LOGGER.info("Force to setCloseMark the filenode processor {}.", processorName);
//      while (!closeOneProcessor(processorName)) {
//        try {
//          LOGGER.info("Can't force to setCloseMark the filenode processor {}, wait 100ms to retry",
//              processorName);
//          TimeUnit.MILLISECONDS.sleep(100);
//        } catch (InterruptedException e) {
//          // ignore the interrupted exception
//          LOGGER.error("Unexpected interruption {}", e);
//          Thread.currentThread().interrupt();
//        }
//      }
//    } finally {
//      fileNodeManagerStatus = FileNodeManagerStatus.NONE;
//    }
//  }
//
//
//  /**
//   * try to setCloseMark the filenode processor. The name of filenode processor is processorName
//   * notice: this method has the same function with close()
//   */
//  private boolean closeOneProcessor(String processorName) throws FileNodeManagerException {
//    if (!processorMap.containsKey(processorName)) {
//      return true;
//    }
//
//    Processor processor = processorMap.get(processorName);
//    if (processor.tryWriteLock()) {
//      try {
//        if (processor.canBeClosed()) {
//          processor.close();
//          return true;
//        } else {
//          return false;
//        }
//      } catch (ProcessorException e) {
//        LOGGER.error("Close the filenode processor {} error.", processorName, e);
//        throw new FileNodeManagerException(e);
//      } finally {
//        processor.writeUnlock();
//      }
//    } else {
//      return false;
//    }
//  }
//
//  /**
//   * try to setCloseMark the filenode processor.
//   * notice: This method has the same function with closeOneProcessor()
//   */
//  private void close(String processorName) throws FileNodeManagerException {
//    if (!processorMap.containsKey(processorName)) {
//      LOGGER.warn("The processorMap doesn't contain the filenode processor {}.", processorName);
//      return;
//    }
//    LOGGER.info("Try to setCloseMark the filenode processor {}.", processorName);
//    FileNodeProcessor processor = processorMap.get(processorName);
//    if (!processor.tryWriteLock()) {
//      LOGGER.warn("Can't get the insert lock of the filenode processor {}.", processorName);
//      return;
//    }
//    try {
//      if (processor.canBeClosed()) {
//        try {
//          LOGGER.info("Close the filenode processor {}.", processorName);
//          processor.close();
//        } catch (ProcessorException e) {
//          LOGGER.error("Close the filenode processor {} error.", processorName, e);
//          throw new FileNodeManagerException(e);
//        }
//      } else {
//        LOGGER.warn("The filenode processor {} can't be closed.", processorName);
//      }
//    } finally {
//      processor.writeUnlock();
//    }
//  }
//
//  /**
//   * delete all filenode.
//   */
//  public synchronized boolean deleteAll() throws FileNodeManagerException {
//    LOGGER.info("Start deleting all filenode");
//    if (fileNodeManagerStatus != FileNodeManagerStatus.NONE) {
//      LOGGER.info("Failed to delete all filenode processor because of merge operation");
//      return false;
//    }
//
//    fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
//    try {
//      Iterator<Map.Entry<String, FileNodeProcessor>> processorIterator = processorMap.entrySet()
//          .iterator();
//      while (processorIterator.hasNext()) {
//        Map.Entry<String, FileNodeProcessor> processorEntry = processorIterator.next();
//        delete(processorEntry.getKey(), processorIterator);
//      }
//      return processorMap.isEmpty();
//    } finally {
//      LOGGER.info("Deleting all FileNodeProcessors ends");
//      fileNodeManagerStatus = FileNodeManagerStatus.NONE;
//    }
//  }
//
//  /**
//   * Try to setCloseMark All.
//   */
//  public void closeAll() throws FileNodeManagerException {
//    LOGGER.info("Start closing all filenode processor");
//    if (fileNodeManagerStatus != FileNodeManagerStatus.NONE) {
//      LOGGER.info("Failed to setCloseMark all filenode processor because of merge operation");
//      return;
//    }
//    fileNodeManagerStatus = FileNodeManagerStatus.CLOSE;
//    try {
//      for (Map.Entry<String, FileNodeProcessor> processorEntry : processorMap.entrySet()) {
//        close(processorEntry.getKey());
//      }
//    } finally {
//      LOGGER.info("Close all FileNodeProcessors ends");
//      fileNodeManagerStatus = FileNodeManagerStatus.NONE;
//    }
//  }
//
//  /**
//   * force flushMetadata to control memory usage.
//   */
//  public void forceFlush(BasicMemController.UsageLevel level) {
//    // you may add some delicate process like below
//    // or you could provide multiple methods for different urgency
//    switch (level) {
//      // only select the most urgent (most active or biggest in size)
//      // processors to flushMetadata
//      // only select top 10% active memory user to flushMetadata
//      case WARNING:
//        try {
//          flushTop(0.1f);
//        } catch (IOException e) {
//          LOGGER.error("force flushMetadata memory data error: {}", e);
//        }
//        break;
//      // force all processors to flushMetadata
//      case DANGEROUS:
//        try {
//          flushAll();
//        } catch (IOException e) {
//          LOGGER.error("force flushMetadata memory data error: {}", e);
//        }
//        break;
//      // if the flushMetadata thread pool is not full ( or half full), start a new
//      // flushMetadata task
//      case SAFE:
//        if (FlushPoolManager.getInstance().getActiveCnt() < 0.5 * FlushPoolManager.getInstance()
//            .getThreadCnt()) {
//          try {
//            flushTop(0.01f);
//          } catch (IOException e) {
//            LOGGER.error("force flushMetadata memory data error: ", e);
//          }
//        }
//        break;
//      default:
//    }
//  }
//
//  private void flushAll() throws IOException {
//    for (FileNodeProcessor processor : processorMap.values()) {
//      if (!processor.tryLock(true)) {
//        continue;
//      }
//      try {
//        boolean isMerge = processor.flush().isHasOverflowFlushTask();
//        if (isMerge) {
//          processor.submitToMerge();
//        }
//      } finally {
//        processor.unlock(true);
//      }
//    }
//  }
//
//  private void flushTop(float percentage) throws IOException {
//    List<FileNodeProcessor> tempProcessors = new ArrayList<>(processorMap.values());
//    // sort the tempProcessors as descending order
//    tempProcessors.sort((o1, o2) -> (int) (o2.memoryUsage() - o1.memoryUsage()));
//    int flushNum =
//        (int) (tempProcessors.size() * percentage) > 1
//            ? (int) (tempProcessors.size() * percentage)
//            : 1;
//    for (int i = 0; i < flushNum && i < tempProcessors.size(); i++) {
//      FileNodeProcessor processor = tempProcessors.get(i);
//      // 64M
//      if (processor.memoryUsage() <= TSFileConfig.groupSizeInByte / 2) {
//        continue;
//      }
//      long start = System.currentTimeMillis();
//      processor.writeLock();
//      try {
//        boolean isMerge = processor.flush().isHasOverflowFlushTask();
//        if (isMerge) {
//          processor.submitToMerge();
//        }
//      } finally {
//        processor.writeUnlock();
//      }
//      start = System.currentTimeMillis() - start;
//      LOGGER.info("flushMetadata Top cost: {}", start);
//    }
//  }
//
//  @Override
//  public void start() {
//    // do no thing
//  }
//
//  @Override
//  public void stop() {
//    try {
//      closeAll();
//    } catch (FileNodeManagerException e) {
//      LOGGER.error("Failed to setCloseMark file node manager because .", e);
//    }
//
//    boolean notFinished = true;
//    while (notFinished) {
//      int size = 0;
//      for (FileNodeProcessor fileNodeProcessor : processorMap.values()) {
//        size += fileNodeProcessor.getClosingBufferWriteProcessor().size();
//      }
//      if (size == 0) {
//        notFinished = false;
//      } else {
//        try {
//          Thread.sleep(10);
//        } catch (InterruptedException e) {
//          LOGGER.error("File node Manager Stop process is interrupted", e);
//        }
//      }
//    }
//    closedProcessorCleaner.shutdownNow();
//  }
//
//  @Override
//  public ServiceType getID() {
//    return ServiceType.FILE_NODE_SERVICE;
//  }
//
//  private enum FileNodeManagerStatus {
//    NONE, MERGE, CLOSE
//  }
//
//  private static class FileNodeManagerHolder {
//
//    private FileNodeManagerHolder() {
//    }
//
//    private static final FileNodeManager INSTANCE = new FileNodeManager(
//        TsFileDBConf.getFileNodeDir());
//  }
//
//}
//
//
//
