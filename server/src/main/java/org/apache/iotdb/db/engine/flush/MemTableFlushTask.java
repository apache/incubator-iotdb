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
package org.apache.iotdb.db.engine.flush;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.index.IndexManager;
import org.apache.iotdb.db.index.IndexMemTableFlushTask;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.VectorTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.VectorChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class MemTableFlushTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);
  private static final FlushSubTaskPoolManager SUB_TASK_POOL_MANAGER =
      FlushSubTaskPoolManager.getInstance();
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Future<?> encodingTaskFuture;
  private final Future<?> ioTaskFuture;
  private RestorableTsFileIOWriter writer;

  private final LinkedBlockingQueue<Object> encodingTaskQueue = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<Object> ioTaskQueue =
      (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo())
          ? new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing())
          : new LinkedBlockingQueue<>();

  private final boolean enabledIndex;
  private String storageGroup;
  private final boolean sequence;

  private IMemTable memTable;

  private volatile long memSerializeTime = 0L;
  private volatile long ioTime = 0L;

  /**
   * @param memTable the memTable to flush
   * @param writer the writer where memTable will be flushed to (current tsfile writer or vm writer)
   * @param storageGroup current storage group
   */
  public MemTableFlushTask(
      IMemTable memTable, RestorableTsFileIOWriter writer, String storageGroup, boolean sequence) {
    this.memTable = memTable;
    this.writer = writer;
    this.storageGroup = storageGroup;
    this.encodingTaskFuture = SUB_TASK_POOL_MANAGER.submit(encodingTask);
    this.ioTaskFuture = SUB_TASK_POOL_MANAGER.submit(ioTask);
    LOGGER.debug(
        "flush task of Storage group {} memtable is created, flushing to file {}.",
        storageGroup,
        writer.getFile().getName());
    this.enabledIndex = IoTDBDescriptor.getInstance().getConfig().isEnableIndex();
    this.sequence = sequence;
  }

  /** the function for flushing memtable. */
  public void syncFlushMemTable() throws ExecutionException, InterruptedException {
    LOGGER.info(
        "The memTable size of SG {} is {}, the avg series points num in chunk is {}, total timeseries number is {}",
        storageGroup,
        memTable.memSize(),
        memTable.getTotalPointsNum() / memTable.getSeriesNumber(),
        memTable.getSeriesNumber());

    long estimatedTemporaryMemSize = 0L;
    if (config.isEnableMemControl() && SystemInfo.getInstance().isEncodingFasterThanIo()) {
      estimatedTemporaryMemSize =
          memTable.memSize() / memTable.getSeriesNumber() * config.getIoTaskQueueSizeForFlushing();
      SystemInfo.getInstance().applyTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
    }
    long start = System.currentTimeMillis();
    long sortTime = 0;
    IndexMemTableFlushTask indexFlushTask = null;
    if (enabledIndex) {
      try {
        indexFlushTask = IndexManager.getInstance().getIndexMemFlushTask(storageGroup, sequence);
      } catch (Exception e) {
        LOGGER.error("meet Exception in getIndexMemFlushTask, not affect the memtable flushing", e);
      }
    }

    // for map do not use get(key) to iterate
    for (Map.Entry<String, Map<String, IWritableMemChunk>> memTableEntry :
        memTable.getMemTableMap().entrySet()) {
      encodingTaskQueue.put(new StartFlushGroupIOTask(memTableEntry.getKey()));

      final Map<String, IWritableMemChunk> value = memTableEntry.getValue();
      for (Map.Entry<String, IWritableMemChunk> iWritableMemChunkEntry : value.entrySet()) {
        long startTime = System.currentTimeMillis();
        IWritableMemChunk series = iWritableMemChunkEntry.getValue();
        IMeasurementSchema desc = series.getSchema();
        TVList tvList = series.getSortedTvListForFlush();
        sortTime += System.currentTimeMillis() - startTime;
        encodingTaskQueue.put(new Pair<>(tvList, desc));
        if (enabledIndex && indexFlushTask != null) {
          try {
            String deviceId = memTableEntry.getKey();
            String measurementId = iWritableMemChunkEntry.getKey();
            indexFlushTask.buildIndexForOneSeries(new PartialPath(deviceId, measurementId), tvList);
          } catch (Exception e) {
            LOGGER.error(
                "meet Exception in buildIndexForOneSeries, not affect the memtable flushing", e);
          }
        }
      }

      encodingTaskQueue.put(new EndChunkGroupIoTask());
    }
    encodingTaskQueue.put(new TaskEnd());
    LOGGER.debug(
        "Storage group {} memtable flushing into file {}: data sort time cost {} ms.",
        storageGroup,
        writer.getFile().getName(),
        sortTime);

    try {
      encodingTaskFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      ioTaskFuture.cancel(true);
      throw e;
    }

    ioTaskFuture.get();
    if (enabledIndex && indexFlushTask != null) {
      try {
        indexFlushTask.endFlush();
      } catch (Exception e) {
        LOGGER.error("meet Exception in endFlush, not affect the memtable flushing", e);
      }
    }

    try {
      writer.writePlanIndices();
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    if (config.isEnableMemControl()) {
      if (estimatedTemporaryMemSize != 0) {
        SystemInfo.getInstance().releaseTemporaryMemoryForFlushing(estimatedTemporaryMemSize);
      }
      SystemInfo.getInstance().setEncodingFasterThanIo(ioTime >= memSerializeTime);
    }

    LOGGER.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup,
        memTable,
        System.currentTimeMillis() - start);
  }

  private Runnable encodingTask =
      new Runnable() {
        private void writeOneSeries(
            TVList tvPairs, IChunkWriter seriesWriterImpl, TSDataType dataType) {
          for (int sortedRowIndex = 0; sortedRowIndex < tvPairs.size(); sortedRowIndex++) {
            long time = tvPairs.getTime(sortedRowIndex);

            // skip duplicated data
            if ((sortedRowIndex + 1 < tvPairs.size()
                && (time == tvPairs.getTime(sortedRowIndex + 1)))) {
              continue;
            }

            // store last point for SDT
            if (dataType != TSDataType.VECTOR && sortedRowIndex + 1 == tvPairs.size()) {
              ((ChunkWriterImpl) seriesWriterImpl).setLastPoint(true);
            }

            switch (dataType) {
              case BOOLEAN:
                seriesWriterImpl.write(time, tvPairs.getBoolean(sortedRowIndex), false);
                break;
              case INT32:
                seriesWriterImpl.write(time, tvPairs.getInt(sortedRowIndex), false);
                break;
              case INT64:
                seriesWriterImpl.write(time, tvPairs.getLong(sortedRowIndex), false);
                break;
              case FLOAT:
                seriesWriterImpl.write(time, tvPairs.getFloat(sortedRowIndex), false);
                break;
              case DOUBLE:
                seriesWriterImpl.write(time, tvPairs.getDouble(sortedRowIndex), false);
                break;
              case TEXT:
                seriesWriterImpl.write(time, tvPairs.getBinary(sortedRowIndex), false);
                break;
              case VECTOR:
                VectorTVList vectorTvPairs = (VectorTVList) tvPairs;
                List<TSDataType> dataTypes = vectorTvPairs.getTsDataTypes();
                int originRowIndex = vectorTvPairs.getValueIndex(sortedRowIndex);
                for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
                  boolean isNull = vectorTvPairs.isValueMarked(originRowIndex, columnIndex);
                  switch (dataTypes.get(columnIndex)) {
                    case BOOLEAN:
                      seriesWriterImpl.write(
                          time,
                          vectorTvPairs.getBooleanByValueIndex(originRowIndex, columnIndex),
                          isNull);
                      break;
                    case INT32:
                      seriesWriterImpl.write(
                          time,
                          vectorTvPairs.getIntByValueIndex(originRowIndex, columnIndex),
                          isNull);
                      break;
                    case INT64:
                      seriesWriterImpl.write(
                          time,
                          vectorTvPairs.getLongByValueIndex(originRowIndex, columnIndex),
                          isNull);
                      break;
                    case FLOAT:
                      seriesWriterImpl.write(
                          time,
                          vectorTvPairs.getFloatByValueIndex(originRowIndex, columnIndex),
                          isNull);
                      break;
                    case DOUBLE:
                      seriesWriterImpl.write(
                          time,
                          vectorTvPairs.getDoubleByValueIndex(originRowIndex, columnIndex),
                          isNull);
                      break;
                    case TEXT:
                      seriesWriterImpl.write(
                          time,
                          vectorTvPairs.getBinaryByValueIndex(originRowIndex, columnIndex),
                          isNull);
                      break;
                    default:
                      LOGGER.error(
                          "Storage group {} does not support data type: {}",
                          storageGroup,
                          dataType);
                      break;
                  }
                }
                seriesWriterImpl.write(time);
                break;
              default:
                LOGGER.error(
                    "Storage group {} does not support data type: {}", storageGroup, dataType);
                break;
            }
          }
        }

        @SuppressWarnings("squid:S135")
        @Override
        public void run() {
          LOGGER.debug(
              "Storage group {} memtable flushing to file {} starts to encoding data.",
              storageGroup,
              writer.getFile().getName());
          while (true) {

            Object task = null;
            try {
              task = encodingTaskQueue.take();
            } catch (InterruptedException e1) {
              LOGGER.error("Take task into ioTaskQueue Interrupted");
              Thread.currentThread().interrupt();
              break;
            }
            if (task instanceof StartFlushGroupIOTask || task instanceof EndChunkGroupIoTask) {
              try {
                ioTaskQueue.put(task);
              } catch (
                  @SuppressWarnings("squid:S2142")
                  InterruptedException e) {
                LOGGER.error(
                    "Storage group {} memtable flushing to file {}, encoding task is interrupted.",
                    storageGroup,
                    writer.getFile().getName(),
                    e);
                // generally it is because the thread pool is shutdown so the task should be aborted
                break;
              }
            } else if (task instanceof TaskEnd) {
              break;
            } else {
              long starTime = System.currentTimeMillis();
              Pair<TVList, IMeasurementSchema> encodingMessage =
                  (Pair<TVList, IMeasurementSchema>) task;
              IChunkWriter seriesWriter;
              if (encodingMessage.left.getDataType() == TSDataType.VECTOR) {
                seriesWriter = new VectorChunkWriterImpl(encodingMessage.right);
              } else {
                seriesWriter = new ChunkWriterImpl(encodingMessage.right);
              }
              writeOneSeries(encodingMessage.left, seriesWriter, encodingMessage.right.getType());
              seriesWriter.sealCurrentPage();
              seriesWriter.clearPageWriter();
              try {
                ioTaskQueue.put(seriesWriter);
              } catch (InterruptedException e) {
                LOGGER.error("Put task into ioTaskQueue Interrupted");
                Thread.currentThread().interrupt();
              }
              memSerializeTime += System.currentTimeMillis() - starTime;
            }
          }
          try {
            ioTaskQueue.put(new TaskEnd());
          } catch (InterruptedException e) {
            LOGGER.error("Put task into ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
          }

          LOGGER.debug(
              "Storage group {}, flushing memtable {} into disk: Encoding data cost " + "{} ms.",
              storageGroup,
              writer.getFile().getName(),
              memSerializeTime);
        }
      };

  @SuppressWarnings("squid:S135")
  private Runnable ioTask =
      () -> {
        LOGGER.debug(
            "Storage group {} memtable flushing to file {} start io.",
            storageGroup,
            writer.getFile().getName());
        while (true) {
          Object ioMessage = null;
          try {
            ioMessage = ioTaskQueue.take();
          } catch (InterruptedException e1) {
            LOGGER.error("take task from ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
            break;
          }
          long starTime = System.currentTimeMillis();
          try {
            if (ioMessage instanceof StartFlushGroupIOTask) {
              this.writer.startChunkGroup(((StartFlushGroupIOTask) ioMessage).deviceId);
            } else if (ioMessage instanceof TaskEnd) {
              break;
            } else if (ioMessage instanceof ChunkWriterImpl) {
              ChunkWriterImpl chunkWriter = (ChunkWriterImpl) ioMessage;
              chunkWriter.writeToFileWriter(this.writer);
            } else if (ioMessage instanceof VectorChunkWriterImpl) {
              VectorChunkWriterImpl chunkWriter = (VectorChunkWriterImpl) ioMessage;
              chunkWriter.writeToFileWriter(this.writer);
            } else {
              this.writer.setMinPlanIndex(memTable.getMinPlanIndex());
              this.writer.setMaxPlanIndex(memTable.getMaxPlanIndex());
              this.writer.endChunkGroup();
            }
          } catch (IOException e) {
            LOGGER.error(
                "Storage group {} memtable {}, io task meets error.", storageGroup, memTable, e);
            throw new FlushRunTimeException(e);
          }
          ioTime += System.currentTimeMillis() - starTime;
        }
        LOGGER.debug(
            "flushing a memtable to file {} in storage group {}, io cost {}ms",
            writer.getFile().getName(),
            storageGroup,
            ioTime);
      };

  static class TaskEnd {

    TaskEnd() {}
  }

  static class EndChunkGroupIoTask {

    EndChunkGroupIoTask() {}
  }

  static class StartFlushGroupIOTask {

    private final String deviceId;

    StartFlushGroupIOTask(String deviceId) {
      this.deviceId = deviceId;
    }
  }
}
