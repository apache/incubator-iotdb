/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Each flush task allocates new {@linkplain ChunkBuffer} which might be very large and lead to
 * high-cost GC. In new design, we try to reuse ChunkBuffer objects by ChunkBufferPool, referring to
 * {@linkplain MemTablePool}.
 *
 * Only for TEST up to now.
 *
 * @author kangrong
 */
public class ChunkBufferPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkBufferPool.class);

  private static final Deque<ChunkBuffer> availableChunkBuffer = new ArrayDeque<>();

  /**
   * the number of required FlushTasks is no more than {@linkplain MemTablePool}.
   */
  private static final int capacity = IoTDBDescriptor.getInstance().getConfig()
      .getMemtableNumber();

  private int size = 0;

  private static final int WAIT_TIME = 2000;

  private ChunkBufferPool() {
  }

  public ChunkBuffer getEmptyChunkBuffer(Object applier, MeasurementSchema schema) {
    synchronized (availableChunkBuffer) {
      if (availableChunkBuffer.isEmpty() && size < capacity) {
        size++;
//        LOGGER.info("For fask, generated a new ChunkBuffer for {}, system ChunkBuffer size: {}, stack size: {}",
//            applier, size, availableChunkBuffer.size());
        return new ChunkBuffer(schema);
      } else if (!availableChunkBuffer.isEmpty()) {
//        LOGGER
//            .info("ReusableChunkBuffer size: {}, stack size: {}, then get a ChunkBuffer from stack for {}",
//                size, availableChunkBuffer.size(), applier);
        ChunkBuffer chunkBuffer =  availableChunkBuffer.pop();
        chunkBuffer.reInit(schema);
        return chunkBuffer;
      }

      // wait until some one has released a ChunkBuffer
      int waitCount = 1;
      while (true) {
        if (!availableChunkBuffer.isEmpty()) {
//          LOGGER.info(
//              "ReusableChunkBuffer size: {}, stack size: {}, then get a ChunkBuffer from stack for {}",
//              size, availableChunkBuffer.size(), applier);
          return availableChunkBuffer.pop();
        }
        try {
          availableChunkBuffer.wait(WAIT_TIME);
        } catch (InterruptedException e) {
          LOGGER.error("{} fails to wait fot ReusableChunkBuffer {}, continue to wait", applier, e);
        }
        LOGGER.info("{} has waited for a ReusableChunkBuffer for {}ms", applier, waitCount++ * WAIT_TIME);
      }
    }
  }

  public void putBack(ChunkBuffer chunkBuffer) {
    synchronized (availableChunkBuffer) {
      chunkBuffer.reset();
      availableChunkBuffer.push(chunkBuffer);
      availableChunkBuffer.notify();
//      LOGGER.info("a chunk buffer returned, stack size {}", availableChunkBuffer.size());
    }
  }

  public void putBack(ChunkBuffer chunkBuffer, String storageGroup) {
    synchronized (availableChunkBuffer) {
      chunkBuffer.reset();
      availableChunkBuffer.push(chunkBuffer);
      availableChunkBuffer.notify();
//      LOGGER.info("{} return a chunk buffer, stack size {}", storageGroup, availableChunkBuffer.size());
    }
  }

  public static ChunkBufferPool getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {

    private InstanceHolder() {
    }

    private static final ChunkBufferPool INSTANCE = new ChunkBufferPool();
  }
}
