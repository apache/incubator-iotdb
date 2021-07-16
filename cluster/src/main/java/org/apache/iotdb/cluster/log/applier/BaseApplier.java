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

import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.metadata.MetaPuller;
import org.apache.iotdb.cluster.query.ClusterPlanExecutor;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.UndefinedTemplateException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.BatchPlan;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/** BaseApplier use PlanExecutor to execute PhysicalPlans. */
abstract class BaseApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(BaseApplier.class);

  MetaGroupMember metaGroupMember;
  private PlanExecutor queryExecutor;

  BaseApplier(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  /**
   * @param plan
   * @param dataGroupMember the data group member that is applying the log, null if the log is
   *     applied by a meta group member
   * @throws QueryProcessException
   * @throws StorageGroupNotSetException
   * @throws StorageEngineException
   */
  void applyPhysicalPlan(PhysicalPlan plan, DataGroupMember dataGroupMember)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (plan instanceof InsertPlan) {
      processPlanWithTolerance((InsertPlan) plan, dataGroupMember);
    } else if (plan != null && !plan.isQuery()) {
      try {
        getQueryExecutor().processNonQuery(plan);
      } catch (BatchProcessException e) {
        handleBatchProcessException(e, plan);
      } catch (QueryProcessException e) {
        if (e.getCause() instanceof StorageGroupNotSetException
            || e.getCause() instanceof UndefinedTemplateException) {
          executeAfterSync(plan);
        } else {
          throw e;
        }
      } catch (StorageGroupNotSetException e) {
        executeAfterSync(plan);
      }
    } else if (plan != null) {
      logger.error("Unsupported physical plan: {}", plan);
    }
  }

  private void handleBatchProcessException(BatchProcessException e, PhysicalPlan plan)
      throws QueryProcessException, StorageEngineException, StorageGroupNotSetException {
    TSStatus[] failingStatus = e.getFailingStatus();
    boolean needThrow = false;
    for (int i = 0; i < failingStatus.length; i++) {
      TSStatus status = failingStatus[i];
      // skip succeeded plans in later execution
      if (status != null
          && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && plan instanceof BatchPlan) {
        ((BatchPlan) plan).setIsExecuted(i);
      }

      if (plan instanceof DeleteTimeSeriesPlan) {
        if (status != null && status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          if (status.getCode() == TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode()) {
            logger.info("{} doesn't exist, it may has been deleted.", plan.getPaths().get(i));
          } else {
            needThrow = true;
          }
        }
      }
    }
    boolean needRetry = false;
    for (int i = 0, failingStatusLength = failingStatus.length; i < failingStatusLength; i++) {
      TSStatus status = failingStatus[i];
      if (status != null
          && (status.getCode() == TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode()
              || status.getCode() == TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode())
          && plan instanceof BatchPlan) {
        ((BatchPlan) plan).unsetIsExecuted(i);
        needRetry = true;
      }
    }
    if (needRetry) {
      executeAfterSync(plan);
      return;
    }

    if (!(plan instanceof DeleteTimeSeriesPlan) || needThrow) {
      throw e;
    }
  }

  private void executeAfterSync(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    try {
      metaGroupMember.syncLeaderWithConsistencyCheck(true);
    } catch (CheckConsistencyException ce) {
      throw new QueryProcessException(ce.getMessage());
    }
    getQueryExecutor().processNonQuery(plan);
  }

  /**
   * @param plan
   * @param dataGroupMember the data group member that is applying the log, null if the log is
   *     applied by a meta group member
   * @throws QueryProcessException
   * @throws StorageGroupNotSetException
   * @throws StorageEngineException
   */
  private void processPlanWithTolerance(InsertPlan plan, DataGroupMember dataGroupMember)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    try {
      getQueryExecutor().processNonQuery(plan);
    } catch (QueryProcessException | StorageGroupNotSetException | StorageEngineException e) {
      // check if this is caused by metadata missing, if so, pull metadata and retry
      Throwable metaMissingException = SchemaUtils.findMetaMissingException(e);
      boolean causedByPathNotExist = metaMissingException instanceof PathNotExistException;

      if (causedByPathNotExist) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Timeseries is not found locally[{}], try pulling it from another group: {}",
              metaGroupMember.getName(),
              e.getCause().getMessage());
        }
        pullTimeseriesSchema(plan, dataGroupMember.getHeader());
        plan.recoverFromFailure();
        getQueryExecutor().processNonQuery(plan);
      } else {
        throw e;
      }
    }
  }

  /**
   * @param plan
   * @param ignoredGroup do not pull schema from the group to avoid backward dependency
   * @throws QueryProcessException
   */
  private void pullTimeseriesSchema(InsertPlan plan, RaftNode ignoredGroup)
      throws QueryProcessException {
    try {
      PartialPath path = plan.getPrefixPath();
      MetaPuller.getInstance().pullTimeSeriesSchemas(Collections.singletonList(path), ignoredGroup);
    } catch (MetadataException e1) {
      throw new QueryProcessException(e1);
    }
  }

  private PlanExecutor getQueryExecutor() throws QueryProcessException {
    if (queryExecutor == null) {
      queryExecutor = new ClusterPlanExecutor(metaGroupMember);
    }
    return queryExecutor;
  }
}
