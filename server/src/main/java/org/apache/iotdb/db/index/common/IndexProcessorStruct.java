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
package org.apache.iotdb.db.index.common;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.metadata.PartialPath;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IndexProcessorStruct {

  public IndexProcessor processor;
  public PartialPath representativePath;
  public Map<IndexType, IndexInfo> infos;

  public IndexProcessorStruct(
      IndexProcessor processor, PartialPath representativePath, Map<IndexType, IndexInfo> infos) {
    this.processor = processor;
    this.representativePath = representativePath;
    this.infos = infos;
  }

  public List<StorageGroupProcessor> addMergeLock() throws StorageEngineException {
    return StorageEngine.getInstance().mergeLock(Collections.singletonList(representativePath));
  }

  @Override
  public String toString() {
    return "<" + infos + "\n" + processor + ">";
  }
}
