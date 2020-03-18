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

package org.apache.iotdb.cluster.log.applier;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataLogApplier applies logs like data insertion/deletion/update and timeseries creation to IoTDB.
 */
public class DataLogApplier extends BaseApplier {

  private static final Logger logger = LoggerFactory.getLogger(DataLogApplier.class);

  public DataLogApplier(MetaGroupMember metaGroupMember) {
    super(metaGroupMember);
  }

  @Override
  public void apply(Log log) throws QueryProcessException {
    if (log instanceof PhysicalPlanLog) {
      applyPhysicalPlan(((PhysicalPlanLog) log).getPlan());
    } else if (log instanceof CloseFileLog) {
      CloseFileLog closeFileLog = ((CloseFileLog) log);
      try {
        StorageEngine.getInstance().asyncCloseProcessor(closeFileLog.getStorageGroupName(),
            closeFileLog.getPartitionId(),
            closeFileLog.isSeq());
      } catch (StorageGroupNotSetException e) {
        logger.error("Cannot close {} file in {}, partitionId {}",
            closeFileLog.isSeq() ? "seq" : "unseq",
            closeFileLog.getStorageGroupName(), closeFileLog.getPartitionId());
      }
    } else {
      // TODO-Cluster#348 support more types of logs
      logger.error("Unsupported log: {}", log);
    }
  }
}
