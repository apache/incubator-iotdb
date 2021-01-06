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
package org.apache.iotdb.db.writelog.node;

import java.io.IOException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.writelog.io.ILogReader;

/**
 * WriteLogNode is the minimum unit of managing WALs.
 */
public interface WriteLogNode {

  /**
   * Write a wal for a PhysicalPlan. First, the PhysicalPlan will be conveyed to byte[].
   * Then the byte[] will be put into a cache. When the cache is full, the logs in the cache will be
   * synced to disk.
   *
   * @param plan        - a PhysicalPlan
   * @param toPrevious  - whether the insert plan is to flush memtable
   */
  void write(PhysicalPlan plan, boolean toPrevious) throws IOException;

  /**
   * Sync and close streams.
   */
  void close() throws IOException;

  /**
   * Write what in cache to disk.
   */
  void forceSync() throws IOException;

  /**
   * work when change working wal to flush window wal
   */
  void flushWindowChange() throws IOException;

  /**
   * work when flush window wal ends
   */
  void flushWindowEnd();

  /**
   * When data that have WALs in this node start to be flushed, this method must be called to
   * change the working WAL file.
   */
  void notifyStartFlush() throws IOException;

  /**
   * When data that have WALs in this node end flushing, this method must be called to check and
   * remove the out-dated logs file.
   */
  void notifyEndFlush();

  /**
   * return identifier of the log node.
   *
   * @return The identifier of this log node.
   */
  String getIdentifier();

  /**
   * return the directory where wal files of this node are placed.
   *
   * @return The directory where wal files of this node are placed.
   */
  String getLogDirectory();

  /**
   * Abandon all logs in this node and delete the log directory. Calling insert() after calling
   * this method is undefined.
   */
  void delete() throws IOException;

  /**
   * return an ILogReader which can iterate each log in this log node.
   * @return an ILogReader which can iterate each log in this log node.
   */
  ILogReader getLogReader();

}
