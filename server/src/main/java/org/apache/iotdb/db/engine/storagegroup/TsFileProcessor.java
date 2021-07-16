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
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.CompressionRatio;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.flush.CloseFileListener;
import org.apache.iotdb.db.engine.flush.FlushListener;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.flush.NotifyFlushMemTable;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.UpdateEndTimeCallBack;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.writelog.WALFlushListener;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@SuppressWarnings("java:S1135") // ignore todos
public class TsFileProcessor {

  /** logger fot this class */
  private static final Logger logger = LoggerFactory.getLogger(TsFileProcessor.class);

  /** storgae group name of this tsfile */
  private final String storageGroupName;

  /** IoTDB config */
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  /** whether it's enable mem control */
  private final boolean enableMemControl = config.isEnableMemControl();

  /** storage group info for mem control */
  private StorageGroupInfo storageGroupInfo;
  /** tsfile processor info for mem control */
  private TsFileProcessorInfo tsFileProcessorInfo;

  /** sync this object in query() and asyncTryToFlush() */
  private final ConcurrentLinkedDeque<IMemTable> flushingMemTables = new ConcurrentLinkedDeque<>();

  /** modification to memtable mapping */
  private List<Pair<Modification, IMemTable>> modsToMemtable = new ArrayList<>();

  /** writer for restore tsfile and flushing */
  private RestorableTsFileIOWriter writer;

  /** tsfile resource for index this tsfile */
  private final TsFileResource tsFileResource;

  /** time range index to indicate this processor belongs to which time range */
  private long timeRangeId;
  /**
   * Whether the processor is in the queue of the FlushManager or being flushed by a flush thread.
   */
  private volatile boolean managedByFlushManager;

  /** a lock to mutual exclude query and query */
  private final ReadWriteLock flushQueryLock = new ReentrantReadWriteLock();
  /**
   * It is set by the StorageGroupProcessor and checked by flush threads. (If shouldClose == true
   * and its flushingMemTables are all flushed, then the flush thread will close this file.)
   */
  private volatile boolean shouldClose;

  /** working memtable */
  private IMemTable workMemTable;

  /** this callback is called before the workMemtable is added into the flushingMemTables. */
  private final UpdateEndTimeCallBack updateLatestFlushTimeCallback;

  /** Wal log node */
  private WriteLogNode logNode;

  /** whether it's a sequence file or not */
  private final boolean sequence;

  /** total memtable size for mem control */
  private long totalMemTableSize;

  private static final String FLUSH_QUERY_WRITE_LOCKED = "{}: {} get flushQueryLock write lock";
  private static final String FLUSH_QUERY_WRITE_RELEASE =
      "{}: {} get flushQueryLock write lock released";

  /** close file listener */
  private List<CloseFileListener> closeFileListeners = new ArrayList<>();

  /** flush file listener */
  private List<FlushListener> flushListeners = new ArrayList<>();

  @SuppressWarnings("squid:S107")
  TsFileProcessor(
      String storageGroupName,
      File tsfile,
      StorageGroupInfo storageGroupInfo,
      CloseFileListener closeTsFileCallback,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback,
      boolean sequence)
      throws IOException {
    this.storageGroupName = storageGroupName;
    this.tsFileResource = new TsFileResource(tsfile, this);
    this.storageGroupInfo = storageGroupInfo;
    this.writer = new RestorableTsFileIOWriter(tsfile);
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    logger.info("create a new tsfile processor {}", tsfile.getAbsolutePath());
    flushListeners.add(new WALFlushListener(this));
    closeFileListeners.add(closeTsFileCallback);
  }

  @SuppressWarnings("java:S107") // ignore number of arguments
  public TsFileProcessor(
      String storageGroupName,
      StorageGroupInfo storageGroupInfo,
      TsFileResource tsFileResource,
      CloseFileListener closeUnsealedTsFileProcessor,
      UpdateEndTimeCallBack updateLatestFlushTimeCallback,
      boolean sequence,
      RestorableTsFileIOWriter writer) {
    this.storageGroupName = storageGroupName;
    this.tsFileResource = tsFileResource;
    this.storageGroupInfo = storageGroupInfo;
    this.writer = writer;
    this.updateLatestFlushTimeCallback = updateLatestFlushTimeCallback;
    this.sequence = sequence;
    logger.info("reopen a tsfile processor {}", tsFileResource.getTsFile());
    flushListeners.add(new WALFlushListener(this));
    closeFileListeners.add(closeUnsealedTsFileProcessor);
  }

  /**
   * insert data in an InsertRowPlan into the workingMemtable.
   *
   * @param insertRowPlan physical plan of insertion
   */
  public void insert(InsertRowPlan insertRowPlan) throws WriteProcessException {

    if (workMemTable == null) {
      if (enableMemControl) {
        workMemTable = new PrimitiveMemTable(enableMemControl);
        MemTableManager.getInstance().addMemtableNumber();
      } else {
        workMemTable = MemTableManager.getInstance().getAvailableMemTable(storageGroupName);
      }
    }

    if (enableMemControl) {
      checkMemCostAndAddToTspInfo(insertRowPlan);
    }

    workMemTable.insert(insertRowPlan);

    if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
      try {
        getLogNode().write(insertRowPlan);
      } catch (Exception e) {
        throw new WriteProcessException(
            String.format(
                "%s: %s write WAL failed",
                storageGroupName, tsFileResource.getTsFile().getAbsolutePath()),
            e);
      }
    }

    // update start time of this memtable
    tsFileResource.updateStartTime(
        insertRowPlan.getPrefixPath().getFullPath(), insertRowPlan.getTime());
    // for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    // for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(
          insertRowPlan.getPrefixPath().getFullPath(), insertRowPlan.getTime());
    }
    tsFileResource.updatePlanIndexes(insertRowPlan.getIndex());
  }

  /**
   * insert batch data of insertTabletPlan into the workingMemtable The rows to be inserted are in
   * the range [start, end)
   *
   * @param insertTabletPlan insert a tablet of a device
   * @param start start index of rows to be inserted in insertTabletPlan
   * @param end end index of rows to be inserted in insertTabletPlan
   * @param results result array
   */
  public void insertTablet(
      InsertTabletPlan insertTabletPlan, int start, int end, TSStatus[] results)
      throws WriteProcessException {

    if (workMemTable == null) {
      if (enableMemControl) {
        workMemTable = new PrimitiveMemTable(enableMemControl);
        MemTableManager.getInstance().addMemtableNumber();
      } else {
        workMemTable = MemTableManager.getInstance().getAvailableMemTable(storageGroupName);
      }
    }

    try {
      if (enableMemControl) {
        checkMemCostAndAddToTspInfo(insertTabletPlan, start, end);
      }
    } catch (WriteProcessException e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, e.getMessage());
      }
      throw new WriteProcessException(e);
    }
    try {
      workMemTable.insertTablet(insertTabletPlan, start, end);
      if (IoTDBDescriptor.getInstance().getConfig().isEnableWal()) {
        insertTabletPlan.setStart(start);
        insertTabletPlan.setEnd(end);
        getLogNode().write(insertTabletPlan);
      }
    } catch (Exception e) {
      for (int i = start; i < end; i++) {
        results[i] = RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
      }
      throw new WriteProcessException(e);
    }
    for (int i = start; i < end; i++) {
      results[i] = RpcUtils.SUCCESS_STATUS;
    }
    tsFileResource.updateStartTime(
        insertTabletPlan.getPrefixPath().getFullPath(), insertTabletPlan.getTimes()[start]);

    // for sequence tsfile, we update the endTime only when the file is prepared to be closed.
    // for unsequence tsfile, we have to update the endTime for each insertion.
    if (!sequence) {
      tsFileResource.updateEndTime(
          insertTabletPlan.getPrefixPath().getFullPath(), insertTabletPlan.getTimes()[end - 1]);
    }
    tsFileResource.updatePlanIndexes(insertTabletPlan.getIndex());
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  private void checkMemCostAndAddToTspInfo(InsertRowPlan insertRowPlan)
      throws WriteProcessException {
    // memory of increased PrimitiveArray and TEXT values, e.g., add a long[128], add 128*8
    long memTableIncrement = 0L;
    long textDataIncrement = 0L;
    long chunkMetadataIncrement = 0L;
    String deviceId = insertRowPlan.getPrefixPath().getFullPath();
    int columnIndex = 0;
    for (int i = 0; i < insertRowPlan.getMeasurementMNodes().length; i++) {
      // skip failed Measurements
      if (insertRowPlan.getDataTypes()[columnIndex] == null
          || insertRowPlan.getMeasurements()[i] == null) {
        columnIndex++;
        continue;
      }
      if (workMemTable.checkIfChunkDoesNotExist(deviceId, insertRowPlan.getMeasurements()[i])) {
        // ChunkMetadataIncrement
        IMeasurementSchema schema = insertRowPlan.getMeasurementMNodes()[i].getSchema();
        if (schema.getType() == TSDataType.VECTOR) {
          chunkMetadataIncrement +=
              schema.getValueTSDataTypeList().size()
                  * ChunkMetadata.calculateRamSize(
                      schema.getValueMeasurementIdList().get(0),
                      schema.getValueTSDataTypeList().get(0));
          memTableIncrement += TVList.vectorTvListArrayMemSize(schema.getValueTSDataTypeList());
        } else {
          chunkMetadataIncrement +=
              ChunkMetadata.calculateRamSize(
                  insertRowPlan.getMeasurements()[i], insertRowPlan.getDataTypes()[columnIndex]);
          memTableIncrement += TVList.tvListArrayMemSize(insertRowPlan.getDataTypes()[columnIndex]);
        }
      } else {
        // here currentChunkPointNum >= 1
        int currentChunkPointNum =
            workMemTable.getCurrentChunkPointNum(deviceId, insertRowPlan.getMeasurements()[i]);
        memTableIncrement +=
            (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE) == 0
                ? TVList.tvListArrayMemSize(insertRowPlan.getDataTypes()[columnIndex])
                : 0;
      }
      // TEXT data mem size
      if (insertRowPlan.getDataTypes()[columnIndex] == TSDataType.TEXT) {
        textDataIncrement +=
            MemUtils.getBinarySize((Binary) insertRowPlan.getValues()[columnIndex]);
      }
    }
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
  }

  private void checkMemCostAndAddToTspInfo(InsertTabletPlan insertTabletPlan, int start, int end)
      throws WriteProcessException {
    if (start >= end) {
      return;
    }
    long[] memIncrements = new long[3]; // memTable, text, chunk metadata

    String deviceId = insertTabletPlan.getPrefixPath().getFullPath();

    int columnIndex = 0;
    for (int i = 0; i < insertTabletPlan.getMeasurementMNodes().length; i++) {
      // for aligned timeseries
      if (insertTabletPlan.isAligned()) {
        VectorMeasurementSchema vectorSchema =
            (VectorMeasurementSchema) insertTabletPlan.getMeasurementMNodes()[i].getSchema();
        Object[] columns = new Object[vectorSchema.getValueMeasurementIdList().size()];
        for (int j = 0; j < vectorSchema.getValueMeasurementIdList().size(); j++) {
          columns[j] = insertTabletPlan.getColumns()[columnIndex++];
        }
        updateVectorMemCost(vectorSchema, deviceId, start, end, memIncrements, columns);
        break;
      }
      // for non aligned
      else {
        // skip failed Measurements
        TSDataType dataType = insertTabletPlan.getDataTypes()[columnIndex];
        String measurement = insertTabletPlan.getMeasurements()[i];
        Object column = insertTabletPlan.getColumns()[columnIndex];
        columnIndex++;
        if (dataType == null || column == null || measurement == null) {
          continue;
        }
        updateMemCost(dataType, measurement, deviceId, start, end, memIncrements, column);
      }
    }
    long memTableIncrement = memIncrements[0];
    long textDataIncrement = memIncrements[1];
    long chunkMetadataIncrement = memIncrements[2];
    updateMemoryInfo(memTableIncrement, chunkMetadataIncrement, textDataIncrement);
  }

  private void updateMemCost(
      TSDataType dataType,
      String measurement,
      String deviceId,
      int start,
      int end,
      long[] memIncrements,
      Object column) {
    // memIncrements = [memTable, text, chunk metadata] respectively

    if (workMemTable.checkIfChunkDoesNotExist(deviceId, measurement)) {
      // ChunkMetadataIncrement
      memIncrements[2] += ChunkMetadata.calculateRamSize(measurement, dataType);
      memIncrements[0] +=
          ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
              * TVList.tvListArrayMemSize(dataType);
    } else {
      int currentChunkPointNum = workMemTable.getCurrentChunkPointNum(deviceId, measurement);
      if (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE == 0) {
        memIncrements[0] +=
            ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
                * TVList.tvListArrayMemSize(dataType);
      } else {
        int acquireArray =
            (end - start - 1 + (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE))
                / PrimitiveArrayManager.ARRAY_SIZE;
        memIncrements[0] +=
            acquireArray == 0 ? 0 : acquireArray * TVList.tvListArrayMemSize(dataType);
      }
    }
    // TEXT data size
    if (dataType == TSDataType.TEXT) {
      Binary[] binColumn = (Binary[]) column;
      memIncrements[1] += MemUtils.getBinaryColumnSize(binColumn, start, end);
    }
  }

  private void updateVectorMemCost(
      VectorMeasurementSchema vectorSchema,
      String deviceId,
      int start,
      int end,
      long[] memIncrements,
      Object[] columns) {
    // memIncrements = [memTable, text, chunk metadata] respectively

    List<String> measurementIds = vectorSchema.getValueMeasurementIdList();
    List<TSDataType> dataTypes = vectorSchema.getValueTSDataTypeList();
    if (workMemTable.checkIfChunkDoesNotExist(deviceId, vectorSchema.getMeasurementId())) {
      // ChunkMetadataIncrement
      memIncrements[2] +=
          dataTypes.size()
              * ChunkMetadata.calculateRamSize(measurementIds.get(0), dataTypes.get(0));
      memIncrements[0] +=
          ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
              * TVList.vectorTvListArrayMemSize(dataTypes);
    } else {
      int currentChunkPointNum =
          workMemTable.getCurrentChunkPointNum(deviceId, vectorSchema.getMeasurementId());
      if (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE == 0) {
        memIncrements[0] +=
            ((end - start) / PrimitiveArrayManager.ARRAY_SIZE + 1)
                * TVList.vectorTvListArrayMemSize(dataTypes);
      } else {
        int acquireArray =
            (end - start - 1 + (currentChunkPointNum % PrimitiveArrayManager.ARRAY_SIZE))
                / PrimitiveArrayManager.ARRAY_SIZE;
        memIncrements[0] +=
            acquireArray == 0 ? 0 : acquireArray * TVList.vectorTvListArrayMemSize(dataTypes);
      }
    }
    // TEXT data size
    for (int i = 0; i < dataTypes.size(); i++) {
      if (dataTypes.get(i) == TSDataType.TEXT) {
        Binary[] binColumn = (Binary[]) columns[i];
        memIncrements[1] += MemUtils.getBinaryColumnSize(binColumn, start, end);
      }
    }
  }

  private void updateMemoryInfo(
      long memTableIncrement, long chunkMetadataIncrement, long textDataIncrement)
      throws WriteProcessException {
    memTableIncrement += textDataIncrement;
    storageGroupInfo.addStorageGroupMemCost(memTableIncrement);
    tsFileProcessorInfo.addTSPMemCost(chunkMetadataIncrement);
    if (storageGroupInfo.needToReportToSystem()) {
      try {
        if (!SystemInfo.getInstance().reportStorageGroupStatus(storageGroupInfo, this)) {
          StorageEngine.blockInsertionIfReject(this);
        }
      } catch (WriteProcessRejectException e) {
        storageGroupInfo.releaseStorageGroupMemCost(memTableIncrement);
        tsFileProcessorInfo.releaseTSPMemCost(chunkMetadataIncrement);
        SystemInfo.getInstance().resetStorageGroupStatus(storageGroupInfo);
        throw e;
      }
    }
    workMemTable.addTVListRamCost(memTableIncrement);
    workMemTable.addTextDataSize(textDataIncrement);
  }

  /**
   * Delete data which belongs to the timeseries `deviceId.measurementId` and the timestamp of which
   * <= 'timestamp' in the deletion. <br>
   *
   * <p>Delete data in both working MemTable and flushing MemTables.
   */
  public void deleteDataInMemory(Deletion deletion, Set<PartialPath> devicePaths) {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable != null) {
        for (PartialPath device : devicePaths) {
          workMemTable.delete(
              deletion.getPath(), device, deletion.getStartTime(), deletion.getEndTime());
          logger.info(
              "[Deletion] Delete in-memory data with deletion path: {}, time:{}-{}.",
              deletion.getPath(),
              deletion.getStartTime(),
              deletion.getEndTime());
        }
      }
      // flushing memTables are immutable, only record this deletion in these memTables for query
      if (!flushingMemTables.isEmpty()) {
        modsToMemtable.add(new Pair<>(deletion, flushingMemTables.getLast()));
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  public TsFileResource getTsFileResource() {
    return tsFileResource;
  }

  public boolean shouldFlush() {
    if (workMemTable == null) {
      return false;
    }
    if (workMemTable.shouldFlush()) {
      logger.info(
          "The memtable size {} of tsfile {} reaches the mem control threshold",
          workMemTable.memSize(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    if (!enableMemControl && workMemTable.memSize() >= getMemtableSizeThresholdBasedOnSeriesNum()) {
      logger.info(
          "The memtable size {} of tsfile {} reaches the threshold",
          workMemTable.memSize(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    if (workMemTable.reachTotalPointNumThreshold()) {
      logger.info(
          "The avg series points num {} of tsfile {} reaches the threshold",
          workMemTable.getTotalPointsNum() / workMemTable.getSeriesNumber(),
          tsFileResource.getTsFile().getAbsolutePath());
      return true;
    }
    return false;
  }

  private long getMemtableSizeThresholdBasedOnSeriesNum() {
    return config.getMemtableSizeThreshold();
  }

  public boolean shouldClose() {
    long fileSize = tsFileResource.getTsFileSize();
    long fileSizeThreshold = IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold();
    if (fileSize >= fileSizeThreshold) {
      logger.info(
          "{} fileSize {} >= fileSizeThreshold {}",
          tsFileResource.getTsFilePath(),
          fileSize,
          fileSizeThreshold);
    }
    return fileSize >= fileSizeThreshold;
  }

  void syncClose() {
    logger.info(
        "Sync close file: {}, will firstly async close it",
        tsFileResource.getTsFile().getAbsolutePath());
    if (shouldClose) {
      return;
    }
    synchronized (flushingMemTables) {
      try {
        asyncClose();
        logger.info("Start to wait until file {} is closed", tsFileResource);
        long startTime = System.currentTimeMillis();
        while (!flushingMemTables.isEmpty()) {
          flushingMemTables.wait(60_000);
          if (System.currentTimeMillis() - startTime > 60_000 && !flushingMemTables.isEmpty()) {
            logger.warn(
                "{} has spent {}s for waiting flushing one memtable; {} left (first: {}). FlushingManager info: {}",
                this.tsFileResource.getTsFile().getAbsolutePath(),
                (System.currentTimeMillis() - startTime) / 1000,
                flushingMemTables.size(),
                flushingMemTables.getFirst(),
                FlushManager.getInstance());
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "{}: {} wait close interrupted",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
        Thread.currentThread().interrupt();
      }
    }
    logger.info("File {} is closed synchronously", tsFileResource.getTsFile().getAbsolutePath());
  }

  /** async close one tsfile, register and close it by another thread */
  void asyncClose() {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {

      if (logger.isInfoEnabled()) {
        if (workMemTable != null) {
          logger.info(
              "{}: flush a working memtable in async close tsfile {}, memtable size: {}, tsfile "
                  + "size: {}, plan index: [{}, {}]",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              workMemTable.memSize(),
              tsFileResource.getTsFileSize(),
              workMemTable.getMinPlanIndex(),
              workMemTable.getMaxPlanIndex());
        } else {
          logger.info(
              "{}: flush a NotifyFlushMemTable in async close tsfile {}, tsfile size: {}",
              storageGroupName,
              tsFileResource.getTsFile().getAbsolutePath(),
              tsFileResource.getTsFileSize());
        }
      }

      if (shouldClose) {
        return;
      }
      // when a flush thread serves this TsFileProcessor (because the processor is submitted by
      // registerTsFileProcessor()), the thread will seal the corresponding TsFile and
      // execute other cleanup works if "shouldClose == true and flushingMemTables is empty".

      // To ensure there must be a flush thread serving this processor after the field `shouldClose`
      // is set true, we need to generate a NotifyFlushMemTable as a signal task and submit it to
      // the FlushManager.

      // we have to add the memtable into flushingList first and then set the shouldClose tag.
      // see https://issues.apache.org/jira/browse/IOTDB-510
      IMemTable tmpMemTable =
          workMemTable == null || workMemTable.memSize() == 0
              ? new NotifyFlushMemTable()
              : workMemTable;

      try {
        // When invoke closing TsFile after insert data to memTable, we shouldn't flush until invoke
        // flushing memTable in System module.
        addAMemtableIntoFlushingList(tmpMemTable);
        logger.info("Memtable {} has been added to flushing list", tmpMemTable);
        shouldClose = true;
      } catch (Exception e) {
        logger.error(
            "{}: {} async close failed, because",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
      }
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * TODO if the flushing thread is too fast, the tmpMemTable.wait() may never wakeup Tips: I am
   * trying to solve this issue by checking whether the table exist before wait()
   */
  public void syncFlush() throws IOException {
    IMemTable tmpMemTable;
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      tmpMemTable = workMemTable == null ? new NotifyFlushMemTable() : workMemTable;
      if (logger.isDebugEnabled() && tmpMemTable.isSignalMemTable()) {
        logger.debug(
            "{}: {} add a signal memtable into flushing memtable list when sync flush",
            storageGroupName,
            tsFileResource.getTsFile().getName());
      }
      addAMemtableIntoFlushingList(tmpMemTable);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }

    synchronized (tmpMemTable) {
      try {
        long startWait = System.currentTimeMillis();
        while (flushingMemTables.contains(tmpMemTable)) {
          tmpMemTable.wait(1000);

          if ((System.currentTimeMillis() - startWait) > 60_000) {
            logger.warn(
                "has waited for synced flushing a memtable in {} for 60 seconds.",
                this.tsFileResource.getTsFile().getAbsolutePath());
            startWait = System.currentTimeMillis();
          }
        }
      } catch (InterruptedException e) {
        logger.error(
            "{}: {} wait flush finished meets error",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
        Thread.currentThread().interrupt();
      }
    }
  }

  /** put the working memtable into flushing list and set the working memtable to null */
  public void asyncFlush() {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      if (workMemTable == null) {
        return;
      }
      logger.info(
          "Async flush a memtable to tsfile: {}", tsFileResource.getTsFile().getAbsolutePath());
      addAMemtableIntoFlushingList(workMemTable);
    } catch (Exception e) {
      logger.error(
          "{}: {} add a memtable into flushing list failed",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * this method calls updateLatestFlushTimeCallback and move the given memtable into the flushing
   * queue, set the current working memtable as null and then register the tsfileProcessor into the
   * flushManager again.
   */
  private void addAMemtableIntoFlushingList(IMemTable tobeFlushed) throws IOException {
    if (!tobeFlushed.isSignalMemTable()
        && (!updateLatestFlushTimeCallback.call(this) || tobeFlushed.memSize() == 0)) {
      logger.warn(
          "This normal memtable is empty, skip it in flush. {}: {} Memetable info: {}",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          tobeFlushed.getMemTableMap());
      return;
    }

    for (FlushListener flushListener : flushListeners) {
      flushListener.onFlushStart(tobeFlushed);
    }

    if (enableMemControl) {
      SystemInfo.getInstance().addFlushingMemTableCost(tobeFlushed.getTVListsRamCost());
    }
    flushingMemTables.addLast(tobeFlushed);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} Memtable (signal = {}) is added into the flushing Memtable, queue size = {}",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          tobeFlushed.isSignalMemTable(),
          flushingMemTables.size());
    }

    if (!tobeFlushed.isSignalMemTable()) {
      totalMemTableSize += tobeFlushed.memSize();
    }
    workMemTable = null;
    FlushManager.getInstance().registerTsFileProcessor(this);
  }

  /** put back the memtable to MemTablePool and make metadata in writer visible */
  private void releaseFlushedMemTable(IMemTable memTable) {
    flushQueryLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug(
          FLUSH_QUERY_WRITE_LOCKED, storageGroupName, tsFileResource.getTsFile().getName());
    }
    try {
      writer.makeMetadataVisible();
      if (!flushingMemTables.remove(memTable)) {
        logger.warn(
            "{}: {} put the memtable (signal={}) out of flushingMemtables but it is not in the queue.",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTable.isSignalMemTable());
      } else if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} memtable (signal={}) is removed from the queue. {} left.",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTable.isSignalMemTable(),
            flushingMemTables.size());
      }
      memTable.release();
      MemTableManager.getInstance().decreaseMemtableNumber();
      if (enableMemControl) {
        // reset the mem cost in StorageGroupProcessorInfo
        storageGroupInfo.releaseStorageGroupMemCost(memTable.getTVListsRamCost());
        if (logger.isDebugEnabled()) {
          logger.debug(
              "[mem control] {}: {} flush finished, try to reset system memcost, "
                  + "flushing memtable list size: {}",
              storageGroupName,
              tsFileResource.getTsFile().getName(),
              flushingMemTables.size());
        }
        // report to System
        SystemInfo.getInstance().resetStorageGroupStatus(storageGroupInfo);
        SystemInfo.getInstance().resetFlushingMemTableCost(memTable.getTVListsRamCost());
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} flush finished, remove a memtable from flushing list, "
                + "flushing memtable list size: {}",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            flushingMemTables.size());
      }
    } catch (Exception e) {
      logger.error("{}: {}", storageGroupName, tsFileResource.getTsFile().getName(), e);
    } finally {
      flushQueryLock.writeLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            FLUSH_QUERY_WRITE_RELEASE, storageGroupName, tsFileResource.getTsFile().getName());
      }
    }
  }

  /**
   * Take the first MemTable from the flushingMemTables and flush it. Called by a flush thread of
   * the flush manager pool
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void flushOneMemTable() {
    IMemTable memTableToFlush = flushingMemTables.getFirst();

    // signal memtable only may appear when calling asyncClose()
    if (!memTableToFlush.isSignalMemTable()) {
      try {
        writer.mark();
        MemTableFlushTask flushTask =
            new MemTableFlushTask(memTableToFlush, writer, storageGroupName);
        flushTask.syncFlushMemTable();
      } catch (Exception e) {
        if (writer == null) {
          logger.info(
              "{}: {} is closed during flush, abandon flush task",
              storageGroupName,
              tsFileResource.getTsFile().getName());
          synchronized (flushingMemTables) {
            flushingMemTables.notifyAll();
          }
        } else {
          logger.error(
              "{}: {} meet error when flushing a memtable, change system mode to read-only",
              storageGroupName,
              tsFileResource.getTsFile().getName(),
              e);
          IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
          try {
            logger.error(
                "{}: {} IOTask meets error, truncate the corrupted data",
                storageGroupName,
                tsFileResource.getTsFile().getName(),
                e);
            writer.reset();
          } catch (IOException e1) {
            logger.error(
                "{}: {} Truncate corrupted data meets error",
                storageGroupName,
                tsFileResource.getTsFile().getName(),
                e1);
          }
          Thread.currentThread().interrupt();
        }
      }
    }

    for (FlushListener flushListener : flushListeners) {
      flushListener.onFlushEnd(memTableToFlush);
    }

    try {
      Iterator<Pair<Modification, IMemTable>> iterator = modsToMemtable.iterator();
      while (iterator.hasNext()) {
        Pair<Modification, IMemTable> entry = iterator.next();
        if (entry.right.equals(memTableToFlush)) {
          entry.left.setFileOffset(tsFileResource.getTsFileSize());
          this.tsFileResource.getModFile().write(entry.left);
          tsFileResource.getModFile().close();
          iterator.remove();
          logger.info(
              "[Deletion] Deletion with path: {}, time:{}-{} written when flush memtable",
              entry.left.getPath(),
              ((Deletion) (entry.left)).getStartTime(),
              ((Deletion) (entry.left)).getEndTime());
        }
      }
    } catch (IOException e) {
      logger.error(
          "Meet error when writing into ModificationFile file of {} ",
          tsFileResource.getTsFile().getName(),
          e);
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} try get lock to release a memtable (signal={})",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          memTableToFlush.isSignalMemTable());
    }
    // for sync flush
    synchronized (memTableToFlush) {
      releaseFlushedMemTable(memTableToFlush);
      memTableToFlush.notifyAll();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} released a memtable (signal={}), flushingMemtables size ={}",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            memTableToFlush.isSignalMemTable(),
            flushingMemTables.size());
      }
    }

    if (shouldClose && flushingMemTables.isEmpty() && writer != null) {
      try {
        writer.mark();
        updateCompressionRatio(memTableToFlush);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{}: {} flushingMemtables is empty and will close the file",
              storageGroupName,
              tsFileResource.getTsFile().getName());
        }
        endFile();
        if (logger.isDebugEnabled()) {
          logger.debug("{} flushingMemtables is clear", storageGroupName);
        }
      } catch (Exception e) {
        logger.error(
            "{} meet error when flush FileMetadata to {}, change system mode to read-only",
            storageGroupName,
            tsFileResource.getTsFile().getAbsolutePath(),
            e);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        try {
          writer.reset();
        } catch (IOException e1) {
          logger.error(
              "{}: {} truncate corrupted data meets error",
              storageGroupName,
              tsFileResource.getTsFile().getName(),
              e1);
        }
        logger.error(
            "{}: {} marking or ending file meet error",
            storageGroupName,
            tsFileResource.getTsFile().getName(),
            e);
      }
      // for sync close
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} try to get flushingMemtables lock.",
            storageGroupName,
            tsFileResource.getTsFile().getName());
      }
      synchronized (flushingMemTables) {
        flushingMemTables.notifyAll();
      }
    }
  }

  private void updateCompressionRatio(IMemTable memTableToFlush) {
    try {
      double compressionRatio = ((double) totalMemTableSize) / writer.getPos();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "The compression ratio of tsfile {} is {}, totalMemTableSize: {}, the file size: {}",
            writer.getFile().getAbsolutePath(),
            compressionRatio,
            totalMemTableSize,
            writer.getPos());
      }
      if (compressionRatio == 0 && !memTableToFlush.isSignalMemTable()) {
        logger.error(
            "{} The compression ratio of tsfile {} is 0, totalMemTableSize: {}, the file size: {}",
            storageGroupName,
            writer.getFile().getAbsolutePath(),
            totalMemTableSize,
            writer.getPos());
      }
      CompressionRatio.getInstance().updateRatio(compressionRatio);
    } catch (IOException e) {
      logger.error(
          "{}: {} update compression ratio failed",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          e);
    }
  }

  /** end file and write some meta */
  private void endFile() throws IOException, TsFileProcessorException {
    logger.info("Start to end file {}", tsFileResource);
    long closeStartTime = System.currentTimeMillis();
    tsFileResource.serialize();
    writer.endFile();
    logger.info("Ended file {}", tsFileResource);

    // remove this processor from Closing list in StorageGroupProcessor,
    // mark the TsFileResource closed, no need writer anymore
    for (CloseFileListener closeFileListener : closeFileListeners) {
      closeFileListener.onClosed(this);
    }

    if (enableMemControl) {
      tsFileProcessorInfo.clear();
      storageGroupInfo.closeTsFileProcessorAndReportToSystem(this);
    }
    if (logger.isInfoEnabled()) {
      long closeEndTime = System.currentTimeMillis();
      logger.info(
          "Storage group {} close the file {}, TsFile size is {}, "
              + "time consumption of flushing metadata is {}ms",
          storageGroupName,
          tsFileResource.getTsFile().getAbsoluteFile(),
          writer.getFile().length(),
          closeEndTime - closeStartTime);
    }

    writer = null;
  }

  public boolean isManagedByFlushManager() {
    return managedByFlushManager;
  }

  public void setManagedByFlushManager(boolean managedByFlushManager) {
    this.managedByFlushManager = managedByFlushManager;
  }

  /**
   * get WAL log node
   *
   * @return WAL log node
   */
  public WriteLogNode getLogNode() {
    if (logNode == null) {
      logNode =
          MultiFileLogNodeManager.getInstance()
              .getNode(
                  storageGroupName + "-" + tsFileResource.getTsFile().getName(),
                  storageGroupInfo.getWalSupplier());
    }
    return logNode;
  }

  /** close this tsfile */
  public void close() throws TsFileProcessorException {
    try {
      // when closing resource file, its corresponding mod file is also closed.
      tsFileResource.close();
      MultiFileLogNodeManager.getInstance()
          .deleteNode(
              storageGroupName + "-" + tsFileResource.getTsFile().getName(),
              storageGroupInfo.getWalConsumer());
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public int getFlushingMemTableSize() {
    return flushingMemTables.size();
  }

  RestorableTsFileIOWriter getWriter() {
    return writer;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  /** get modifications from a memtable */
  private List<Modification> getModificationsForMemtable(IMemTable memTable) {
    List<Modification> modifications = new ArrayList<>();
    boolean foundMemtable = false;
    for (Pair<Modification, IMemTable> entry : modsToMemtable) {
      if (foundMemtable || entry.right.equals(memTable)) {
        modifications.add(entry.left);
        foundMemtable = true;
      }
    }
    return modifications;
  }

  /**
   * construct a deletion list from a memtable
   *
   * @param memTable memtable
   * @param deviceId device id
   * @param measurement measurement name
   * @param timeLowerBound time water mark
   */
  private List<TimeRange> constructDeletionList(
      IMemTable memTable, String deviceId, String measurement, long timeLowerBound)
      throws MetadataException {
    List<TimeRange> deletionList = new ArrayList<>();
    deletionList.add(new TimeRange(Long.MIN_VALUE, timeLowerBound));
    for (Modification modification : getModificationsForMemtable(memTable)) {
      if (modification instanceof Deletion) {
        Deletion deletion = (Deletion) modification;
        if (deletion.getPath().matchFullPath(new PartialPath(deviceId, measurement))
            && deletion.getEndTime() > timeLowerBound) {
          long lowerBound = Math.max(deletion.getStartTime(), timeLowerBound);
          deletionList.add(new TimeRange(lowerBound, deletion.getEndTime()));
        }
      }
    }
    return TimeRange.sortAndMerge(deletionList);
  }

  /**
   * get the chunk(s) in the memtable (one from work memtable and the other ones in flushing
   * memtables and then compact them into one TimeValuePairSorter). Then get the related
   * ChunkMetadata of data on disk.
   *
   * @param deviceId device id
   * @param measurementId measurements id
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void query(
      String deviceId,
      String measurementId,
      IMeasurementSchema schema,
      QueryContext context,
      List<TsFileResource> tsfileResourcesForQuery)
      throws IOException, MetadataException {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: {} get flushQueryLock and hotCompactionMergeLock read lock",
          storageGroupName,
          tsFileResource.getTsFile().getName());
    }
    flushQueryLock.readLock().lock();
    try {
      List<ReadOnlyMemChunk> readOnlyMemChunks = new ArrayList<>();
      for (IMemTable flushingMemTable : flushingMemTables) {
        if (flushingMemTable.isSignalMemTable()) {
          continue;
        }
        List<TimeRange> deletionList =
            constructDeletionList(
                flushingMemTable, deviceId, measurementId, context.getQueryTimeLowerBound());
        ReadOnlyMemChunk memChunk =
            flushingMemTable.query(
                deviceId, measurementId, schema, context.getQueryTimeLowerBound(), deletionList);
        if (memChunk != null) {
          readOnlyMemChunks.add(memChunk);
        }
      }
      if (workMemTable != null) {
        ReadOnlyMemChunk memChunk =
            workMemTable.query(
                deviceId, measurementId, schema, context.getQueryTimeLowerBound(), null);
        if (memChunk != null) {
          readOnlyMemChunks.add(memChunk);
        }
      }

      ModificationFile modificationFile = tsFileResource.getModFile();
      List<Modification> modifications =
          context.getPathModifications(
              modificationFile,
              new PartialPath(deviceId + IoTDBConstant.PATH_SEPARATOR + measurementId));

      List<IChunkMetadata> chunkMetadataList = new ArrayList<>();
      if (schema instanceof VectorMeasurementSchema) {
        List<ChunkMetadata> timeChunkMetadataList =
            writer.getVisibleMetadataList(deviceId, measurementId, schema.getType());
        List<List<ChunkMetadata>> valueChunkMetadataList = new ArrayList<>();
        List<String> valueMeasurementIdList = schema.getValueMeasurementIdList();
        List<TSDataType> valueDataTypeList = schema.getValueTSDataTypeList();
        for (int i = 0; i < valueMeasurementIdList.size(); i++) {
          valueChunkMetadataList.add(
              writer.getVisibleMetadataList(
                  deviceId, valueMeasurementIdList.get(i), valueDataTypeList.get(i)));
        }

        for (int i = 0; i < timeChunkMetadataList.size(); i++) {
          List<IChunkMetadata> valueChunkMetadata = new ArrayList<>();
          for (List<ChunkMetadata> chunkMetadata : valueChunkMetadataList) {
            valueChunkMetadata.add(chunkMetadata.get(i));
          }
          chunkMetadataList.add(
              new VectorChunkMetadata(timeChunkMetadataList.get(i), valueChunkMetadata));
        }
      } else {
        chunkMetadataList =
            new ArrayList<>(
                writer.getVisibleMetadataList(deviceId, measurementId, schema.getType()));
      }

      QueryUtils.modifyChunkMetaData(chunkMetadataList, modifications);
      chunkMetadataList.removeIf(context::chunkNotSatisfy);

      // get in memory data
      if (!readOnlyMemChunks.isEmpty() || !chunkMetadataList.isEmpty()) {
        tsfileResourcesForQuery.add(
            new TsFileResource(readOnlyMemChunks, chunkMetadataList, tsFileResource));
      }
    } catch (QueryProcessException e) {
      logger.error(
          "{}: {} get ReadOnlyMemChunk has error",
          storageGroupName,
          tsFileResource.getTsFile().getName(),
          e);
    } finally {
      flushQueryLock.readLock().unlock();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: {} release flushQueryLock",
            storageGroupName,
            tsFileResource.getTsFile().getName());
      }
    }
  }

  public long getTimeRangeId() {
    return timeRangeId;
  }

  public void setTimeRangeId(long timeRangeId) {
    this.timeRangeId = timeRangeId;
  }

  /** release resource of a memtable */
  public void putMemTableBackAndClose() throws TsFileProcessorException {
    if (workMemTable != null) {
      workMemTable.release();
      workMemTable = null;
    }
    try {
      writer.close();
    } catch (IOException e) {
      throw new TsFileProcessorException(e);
    }
  }

  public TsFileProcessorInfo getTsFileProcessorInfo() {
    return tsFileProcessorInfo;
  }

  public void setTsFileProcessorInfo(TsFileProcessorInfo tsFileProcessorInfo) {
    this.tsFileProcessorInfo = tsFileProcessorInfo;
  }

  public long getWorkMemTableRamCost() {
    return workMemTable != null ? workMemTable.getTVListsRamCost() : 0;
  }

  public boolean isSequence() {
    return sequence;
  }

  public void setWorkMemTableShouldFlush() {
    workMemTable.setShouldFlush();
  }

  public void addFlushListener(FlushListener listener) {
    flushListeners.add(listener);
  }

  public void addCloseFileListener(CloseFileListener listener) {
    closeFileListeners.add(listener);
  }

  public void addFlushListeners(Collection<FlushListener> listeners) {
    flushListeners.addAll(listeners);
  }

  public void addCloseFileListeners(Collection<CloseFileListener> listeners) {
    closeFileListeners.addAll(listeners);
  }

  public void submitAFlushTask() {
    this.storageGroupInfo.getStorageGroupProcessor().submitAFlushTaskWhenShouldFlush(this);
  }

  public boolean alreadyMarkedClosing() {
    return shouldClose;
  }
}
