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
package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

public abstract class AbstractInnerSpaceCompactionSelector extends AbstractCompactionSelector {
  protected String logicalStorageGroupName;
  protected String virtualStorageGroupName;
  protected long timePartition;
  protected TsFileResourceList tsFileResources;
  protected boolean sequence;
  protected InnerSpaceCompactionTaskFactory taskFactory;
  protected TsFileResourceManager tsFileResourceManager;

  public AbstractInnerSpaceCompactionSelector(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileResourceManager tsFileResourceManager,
      TsFileResourceList tsFileResources,
      boolean sequence,
      InnerSpaceCompactionTaskFactory taskFactory) {
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupName = virtualStorageGroupName;
    this.timePartition = timePartition;
    this.tsFileResources = tsFileResources;
    this.tsFileResourceManager = tsFileResourceManager;
    this.sequence = sequence;
    this.taskFactory = taskFactory;
  }

  public abstract boolean selectAndSubmit();
}
