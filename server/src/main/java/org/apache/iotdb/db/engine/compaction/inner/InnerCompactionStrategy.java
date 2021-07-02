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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

import java.io.File;
import java.util.List;

public enum InnerCompactionStrategy {
  SIZE_TIRED_COMPACTION;

  public AbstractInnerSpaceCompactionTask getCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroup,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence) {
    switch (this) {
      case SIZE_TIRED_COMPACTION:
      default:
        return new SizeTiredCompactionTask(
            logicalStorageGroupName,
            virtualStorageGroup,
            timePartition,
            tsFileResourceManager,
            tsFileResourceList,
            selectedTsFileResourceList,
            sequence);
    }
  }

  public AbstractInnerSpaceCompactionTask getCompactionRecoverTask(
      String logicalStorageGroupName,
      String virtualStorageGroup,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      File compactionLogFile,
      String storageGroupDir,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> recoverTsFileResources,
      boolean sequence) {
    switch (this) {
      case SIZE_TIRED_COMPACTION:
      default:
        return new SizeTiredCompactionRecoverTask(
            logicalStorageGroupName,
            virtualStorageGroup,
            timePartition,
            tsFileResourceManager,
            compactionLogFile,
            storageGroupDir,
            tsFileResourceList,
            recoverTsFileResources,
            sequence);
    }
  }

  public AbstractInnerSpaceCompactionSelector getCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    switch (this) {
      case SIZE_TIRED_COMPACTION:
      default:
        return new SizeTiredCompactionSelector(
            logicalStorageGroupName,
            virtualStorageGroupName,
            timePartition,
            tsFileResourceManager,
            tsFileResources,
            sequence,
            taskFactory);
    }
  }
}
