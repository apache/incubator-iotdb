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

package org.apache.iotdb.cluster;

import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterFileFlushPolicy implements TsFileFlushPolicy {

  private static final Logger logger = LoggerFactory.getLogger(ClusterFileFlushPolicy.class);

  private MetaGroupMember metaGroupMember;

  public ClusterFileFlushPolicy(
      MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public void apply(StorageGroupProcessor storageGroupProcessor, TsFileProcessor processor,
      boolean isSeq) {
    logger.info("The memtable size {} reaches the threshold, async flush it to tsfile: {}",
        processor.getWorkMemTableMemory(),
        processor.getTsFileResource().getFile().getAbsolutePath());

    if (processor.shouldClose()) {
      // find the related DataGroupMember and close the processor through it
      if (!metaGroupMember.closePartition(storageGroupProcessor.getStorageGroupName(),
          processor.getTimeRangeId(), isSeq)) {
        // the corresponding DataGroupMember is not a leader, just do a flush
        processor.asyncFlush();
      }
    } else {
      processor.asyncFlush();
    }
  }
}
