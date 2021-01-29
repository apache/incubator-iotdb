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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.pool.FlushSubTaskPoolManager;
import org.apache.iotdb.db.engine.measurementorderoptimizer.MeasurementOrderOptimizer;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.exception.runtime.FlushRunTimeException;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemTableFlushTask {

  private static final Logger logger = LoggerFactory.getLogger(MemTableFlushTask.class);
  private static final FlushSubTaskPoolManager subTaskPoolManager = FlushSubTaskPoolManager
      .getInstance();
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Future<?> encodingTaskFuture;
  private final Future<?> ioTaskFuture;
  private RestorableTsFileIOWriter writer;

  private final LinkedBlockingQueue<Object> ioTaskQueue =
      new LinkedBlockingQueue<>(config.getIoTaskQueueSizeForFlushing());
  private final LinkedBlockingQueue<Object> encodingTaskQueue = 
      new LinkedBlockingQueue<>(config.getEncodingTaskQueueSizeForFlushing());
  private String storageGroup;

  private IMemTable memTable;


  /**
   * @param memTable the memTable to flush
   * @param writer the writer where memTable will be flushed to (current tsfile writer or vm writer)
   * @param storageGroup current storage group
   */

  public MemTableFlushTask(IMemTable memTable, RestorableTsFileIOWriter writer, String storageGroup) {
    this.memTable = memTable;
    this.writer = writer;
    this.storageGroup = storageGroup;
    this.encodingTaskFuture = subTaskPoolManager.submit(encodingTask);
    this.ioTaskFuture = subTaskPoolManager.submit(ioTask);
    logger.debug("flush task of Storage group {} memtable {} is created ",
        storageGroup, memTable.getVersion());
  }

  /**
   * the function for flushing memtable.
   */
  public void syncFlushMemTable()
      throws ExecutionException, InterruptedException, IOException {
    logger.info("The memTable size of SG {} is {}, the avg series points num in chunk is {} ",
        storageGroup,
        memTable.memSize(),
        memTable.getTotalPointsNum() / memTable.getSeriesNumber());
    long start = System.currentTimeMillis();
    long sortTime = 0;

    for(String deviceId : memTable.getMemTableMap().keySet()) {
      encodingTaskQueue.put(new StartFlushGroupIOTask(deviceId));
      List<String> measurements = MeasurementOrderOptimizer.getInstance().getMeasurementsOrder(deviceId);
      logger.info(String.format("Flush {} in order: {}",deviceId, measurements.toString()));
      for(String measurementId : measurements) {
        if (!memTable.getMemTableMap().get(deviceId).containsKey(measurementId)) {
          continue;
        }
        // print log to show the order of measurements
        logger.info(String.format("Flush %s.%s", deviceId, measurementId));
        long startTime = System.currentTimeMillis();
        IWritableMemChunk series = memTable.getMemTableMap().get(deviceId).get(measurementId);
        MeasurementSchema desc = series.getSchema();
        TVList tvList = series.getSortedTVList();
        sortTime += System.currentTimeMillis() - startTime;
        encodingTaskQueue.put(new Pair<>(tvList, desc));
      }
      encodingTaskQueue.put(new EndChunkGroupIoTask());
    }
    encodingTaskQueue.put(new TaskEnd());

    logger.debug(
        "Storage group {} memtable {}, flushing into disk: data sort time cost {} ms.",
        storageGroup, memTable.getVersion(), sortTime);

    try {
      encodingTaskFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      ioTaskFuture.cancel(true);
      throw e;
    }

    ioTaskFuture.get();

    try {
      writer.writeVersion(memTable.getVersion());
    } catch (IOException e) {
      throw new ExecutionException(e);
    }

    logger.info(
        "Storage group {} memtable {} flushing a memtable has finished! Time consumption: {}ms",
        storageGroup, memTable, System.currentTimeMillis() - start);
  }

  private Runnable encodingTask = new Runnable() {
    private void writeOneSeries(TVList tvPairs, IChunkWriter seriesWriterImpl,
        TSDataType dataType) {
      for (int i = 0; i < tvPairs.size(); i++) {
        long time = tvPairs.getTime(i);

        // skip duplicated data
        if ((i + 1 < tvPairs.size() && (time == tvPairs.getTime(i + 1)))) {
          continue;
        }

        switch (dataType) {
          case BOOLEAN:
            seriesWriterImpl.write(time, tvPairs.getBoolean(i));
            break;
          case INT32:
            seriesWriterImpl.write(time, tvPairs.getInt(i));
            break;
          case INT64:
            seriesWriterImpl.write(time, tvPairs.getLong(i));
            break;
          case FLOAT:
            seriesWriterImpl.write(time, tvPairs.getFloat(i));
            break;
          case DOUBLE:
            seriesWriterImpl.write(time, tvPairs.getDouble(i));
            break;
          case TEXT:
            seriesWriterImpl.write(time, tvPairs.getBinary(i));
            break;
          default:
            logger.error("Storage group {} does not support data type: {}", storageGroup,
                dataType);
            break;
        }
      }
    }

    @SuppressWarnings("squid:S135")
    @Override
    public void run() {
      long memSerializeTime = 0;
      logger.debug("Storage group {} memtable {}, starts to encoding data.", storageGroup,
          memTable.getVersion());
      while (true) {

        Object task = null;
        try {
          task = encodingTaskQueue.take();
        } catch (InterruptedException e1) {
          logger.error("Take task into ioTaskQueue Interrupted");
          Thread.currentThread().interrupt();
          break;
        }
        if (task instanceof StartFlushGroupIOTask || task instanceof EndChunkGroupIoTask) {
          try {
            ioTaskQueue.put(task);
          } catch (InterruptedException e) {
            logger.error("Put task into ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
            break;
          }
        } else if (task instanceof TaskEnd) {
          break;
        } else {
          long starTime = System.currentTimeMillis();
          Pair<TVList, MeasurementSchema> encodingMessage = (Pair<TVList, MeasurementSchema>) task;
          IChunkWriter seriesWriter = new ChunkWriterImpl(encodingMessage.right);
          writeOneSeries(encodingMessage.left, seriesWriter, encodingMessage.right.getType());
          seriesWriter.sealCurrentPage();
          seriesWriter.clearPageWriter();
          try {
            ioTaskQueue.put(seriesWriter);
          } catch (InterruptedException e) {
            logger.error("Put task into ioTaskQueue Interrupted");
            Thread.currentThread().interrupt();
          }
          memSerializeTime += System.currentTimeMillis() - starTime;
        }
      }
      try {
        ioTaskQueue.put(new TaskEnd());
      } catch (InterruptedException e) {
        logger.error("Put task into ioTaskQueue Interrupted");
        Thread.currentThread().interrupt();
      }
      logger.debug("Storage group {}, flushing memtable {} into disk: Encoding data cost "
              + "{} ms.",
          storageGroup, memTable.getVersion(), memSerializeTime);
    }
  };

  @SuppressWarnings("squid:S135")
  private Runnable ioTask = () -> {
    long ioTime = 0;
    logger.debug("Storage group {} memtable {}, start io.", storageGroup, memTable.getVersion());
    // record the total chunk size for the chunk group
    long totalChunkSize = 0;
    long chunkCount = 0;
    while (true) {
      Object ioMessage = null;
      try {
        ioMessage = ioTaskQueue.take();
      } catch (InterruptedException e1) {
        logger.error("take task from ioTaskQueue Interrupted");
        Thread.currentThread().interrupt();
        break;
      }
      long starTime = System.currentTimeMillis();
      try {
        if (ioMessage instanceof StartFlushGroupIOTask) {
          this.writer.startChunkGroup(((StartFlushGroupIOTask) ioMessage).deviceId);
        } else if (ioMessage instanceof TaskEnd) {
          break;
        }
        else if (ioMessage instanceof IChunkWriter) {
          ChunkWriterImpl chunkWriter = (ChunkWriterImpl) ioMessage;
          totalChunkSize += chunkWriter.getCurrentChunkSize();
          chunkCount += 1;
          chunkWriter.writeToFileWriter(this.writer);
        } else {
          this.writer.endChunkGroup();
        }
      } catch (IOException e) {
        logger.error("Storage group {} memtable {}, io task meets error.", storageGroup,
            memTable.getVersion(), e);
        throw new FlushRunTimeException(e);
      }
      ioTime += System.currentTimeMillis() - starTime;
    }
    logger.debug("flushing a memtable {} in storage group {}, io cost {}ms", memTable.getVersion(),
        storageGroup, ioTime);
    logger.info("flushing a memtable {} in storage group {}, with {} chunk, {} bytes in total, {} bytes average",
            memTable.getVersion(), storageGroup, chunkCount, totalChunkSize, totalChunkSize / chunkCount);
  };
  
  static class TaskEnd {
    
    TaskEnd() {
      
    }
  }

  static class EndChunkGroupIoTask {

    EndChunkGroupIoTask() {

    }
  }

  static class StartFlushGroupIOTask {

    private final String deviceId;

    StartFlushGroupIOTask(String deviceId) {
      this.deviceId = deviceId;
    }
  }
}
