/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.FakedTsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

import java.util.List;

public class FakedInnerSpaceCompactionTask extends SizeTiredCompactionTask {

  public FakedInnerSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResourceList,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence) {
    super(
        logicalStorageGroupName,
        virtualStorageGroupName,
        timePartition,
        tsFileResourceManager,
        tsFileResourceList,
        selectedTsFileResourceList,
        sequence);
  }

  @Override
  protected void doCompaction() {
    FakedTsFileResource targetTsFileResource = new FakedTsFileResource(0);
    long targetFileSize = 0;
    for (TsFileResource resource : selectedTsFileResourceList) {
      targetFileSize += resource.getTsFileSize();
    }
    targetTsFileResource.setTsFileSize(targetFileSize);
    this.tsFileResourceList.insertBefore(selectedTsFileResourceList.get(0), targetTsFileResource);
    for (TsFileResource tsFileResource : selectedTsFileResourceList) {
      this.tsFileResourceList.remove(tsFileResource);
    }
  }
}
