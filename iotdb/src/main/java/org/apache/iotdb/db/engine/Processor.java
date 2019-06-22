/**
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
package org.apache.iotdb.db.engine;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.iotdb.db.engine.bufferwrite.BufferWriteProcessor;
import org.apache.iotdb.db.exception.ProcessorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processor is used for implementing different processor with different operation.<br>
 *
 * @see BufferWriteProcessor
 */
public abstract class Processor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
  private final ReadWriteLock lock;
//  private long start;
  protected String processorName;

  /**
   * Construct processor using name space seriesPath
   *
   * @param processorName
   */
  public Processor(String processorName) {
    this.processorName = processorName;
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Release the cloneList lock
   */
  public void readUnlock() {
    lock.readLock().unlock();
//    start = System.currentTimeMillis() - start;
//    if (start > 1000) {
//      LOGGER.info("Processor {} hold lock for {}ms", processorName, start, new RuntimeException());
//    }
  }

  /**
   * Acquire the cloneList lock
   */
  public void readLock() {
    lock.readLock().lock();
//    start = System.currentTimeMillis();
  }

  /**
   * Acquire the insert lock
   */
  public void writeLock() {
    lock.writeLock().lock();
//    start = System.currentTimeMillis();
  }

  /**
   * Release the insert lock
   */
  public void writeUnlock() {
//    start = System.currentTimeMillis() - start;
//    if (start > 1000) {
//      LOGGER.info("Processor {} hold lock for {}ms", processorName, start, new RuntimeException());
//    }
    lock.writeLock().unlock();
  }

  /**
   * @param isWriteLock
   *            true acquire insert lock, false acquire cloneList lock
   */
  public void lock(boolean isWriteLock) {
    if (isWriteLock) {
      lock.writeLock().lock();
    } else {
      lock.readLock().lock();
    }
//    start = System.currentTimeMillis();
  }

  public boolean tryLock(boolean isWriteLock) {
    if (isWriteLock) {
      return tryWriteLock();
    } else {
      return tryReadLock();
    }
  }

  /**
   * @param isWriteUnlock
   *            true putBack insert lock, false putBack cloneList unlock
   */
  public void unlock(boolean isWriteUnlock) {
//    start = System.currentTimeMillis() - start;
//    if (start > 1000) {
//      LOGGER.info("Processor {} hold lock for {}ms", processorName, start, new RuntimeException());
//    }
    if (isWriteUnlock) {
      writeUnlock();
    } else {
      readUnlock();
    }
  }

  /**
   * Get the name space seriesPath
   *
   * @return
   */
  public String getProcessorName() {
    return processorName;
  }

  /**
   * Try to get the insert lock
   *
   * @return
   */
  public boolean tryWriteLock() {
    boolean result = lock.writeLock().tryLock();
//    if (result) {
//      start = System.currentTimeMillis();
//    }
    return result;
  }

  /**
   * Try to get the cloneList lock
   *
   * @return
   */
  public boolean tryReadLock() {
    boolean result = lock.readLock().tryLock();
//    if (result) {
//      start = System.currentTimeMillis();
//    }
    return result;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((processorName == null) ? 0 : processorName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Processor other = (Processor) obj;
    if (processorName == null) {
      if (other.processorName != null) {
        return false;
      }
    } else if (!processorName.equals(other.processorName)) {
      return false;
    }
    return true;
  }

  /**
   * Judge whether this processor can be closed.
   *
   * @return true if subclass doesn't have other implementation.
   */
  public abstract boolean canBeClosed();

  /**
   * call flushMetadata operation asynchronously
   * @return a future that returns true if successfully, otherwise false.
   * @throws IOException
   */
  public abstract Future<Boolean> flush() throws IOException;

  /**
   * Close the processor.<br>
   * Notice: Thread is not safe
   *
   * @throws IOException
   * @throws ProcessorException
   */
  public abstract void close() throws ProcessorException;

  public abstract long memoryUsage();
}
