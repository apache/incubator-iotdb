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
package org.apache.iotdb.db.writelog.manager;

import org.apache.iotdb.db.writelog.node.WriteLogNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** This interface provides accesses to WriteLogNode. */
public interface WriteLogNodeManager {

  /**
   * Get a WriteLogNode by a identifier like "{fullStorageGroupName}-{BufferWrite/Overflow}-{
   * nameOfTsFile}". The WriteLogNode will be automatically created if not exist.
   *
   * @param identifier -identifier, the format: "{fullStorageGroupName}-{BufferWrite/Overflow}-{
   *     nameOfTsFile}"
   */
  WriteLogNode getNode(String identifier, Supplier<ByteBuffer[]> supplier);

  /**
   * Delete a log node. If the log node does not exist, this will be an empty operation.
   *
   * @param identifier -identifier
   */
  void deleteNode(String identifier, Consumer<ByteBuffer[]> consumer) throws IOException;

  /** Close all nodes. */
  void close();
}
