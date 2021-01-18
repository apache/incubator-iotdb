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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.compaction.dynamic.DynamicCompactionTsFileManagement;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.compaction.no.NoCompactionTsFileManagement;

public enum CompactionStrategy {
  DYNAMIC_STRATEGY,
  LEVEL_COMPACTION,
  NO_COMPACTION;

  public TsFileManagement getTsFileManagement(String storageGroupName, String storageGroupDir) {
    switch (this) {
      case DYNAMIC_STRATEGY:
        return new DynamicCompactionTsFileManagement(storageGroupName, storageGroupDir);
      case LEVEL_COMPACTION:
        return new LevelCompactionTsFileManagement(storageGroupName, storageGroupDir);
      case NO_COMPACTION:
      default:
        return new NoCompactionTsFileManagement(storageGroupName, storageGroupDir);
    }
  }
}
