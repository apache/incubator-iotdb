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
package org.apache.iotdb.db.qp.logical;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

/** This class is a superclass of all operator. */
public abstract class Operator {

  // operator type in int format
  protected int tokenIntType;
  // flag of "explain"
  protected boolean isDebug;

  protected OperatorType operatorType = OperatorType.NULL;

  protected Operator(int tokenIntType) {
    this.tokenIntType = tokenIntType;
    this.isDebug = false;
  }

  public OperatorType getType() {
    return operatorType;
  }

  public boolean isQuery() {
    return operatorType == OperatorType.QUERY;
  }

  public int getTokenIntType() {
    return tokenIntType;
  }

  public void setOperatorType(OperatorType operatorType) {
    this.operatorType = operatorType;
  }

  public boolean isDebug() {
    return isDebug;
  }

  public void setDebug(boolean debug) {
    isDebug = debug;
  }

  @Override
  public String toString() {
    return SQLConstant.tokenNames.get(tokenIntType);
  }

  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    throw new LogicalOperatorException(operatorType.toString(), "");
  }

  /** If you want to add new OperatorType, you must add it in the last. */
  public enum OperatorType {
    NULL,

    AUTHOR,
    LOAD_DATA,
    CREATE_USER,
    DELETE_USER,
    MODIFY_PASSWORD,
    GRANT_USER_PRIVILEGE,
    REVOKE_USER_PRIVILEGE,
    GRANT_USER_ROLE,
    REVOKE_USER_ROLE,
    CREATE_ROLE,
    DELETE_ROLE,
    GRANT_ROLE_PRIVILEGE,
    REVOKE_ROLE_PRIVILEGE,
    LIST_USER,
    LIST_ROLE,
    LIST_USER_PRIVILEGE,
    LIST_ROLE_PRIVILEGE,
    LIST_USER_ROLES,
    LIST_ROLE_USERS,
    GRANT_WATERMARK_EMBEDDING,
    REVOKE_WATERMARK_EMBEDDING,

    SET_STORAGE_GROUP,
    DELETE_STORAGE_GROUP,
    CREATE_TIMESERIES,
    CREATE_ALIGNED_TIMESERIES,
    CREATE_MULTI_TIMESERIES,
    DELETE_TIMESERIES,
    ALTER_TIMESERIES,
    CHANGE_ALIAS,
    CHANGE_TAG_OFFSET,

    INSERT,
    BATCH_INSERT,
    BATCH_INSERT_ROWS,
    BATCH_INSERT_ONE_DEVICE,
    MULTI_BATCH_INSERT,

    DELETE,

    QUERY,
    LAST,
    GROUP_BY_TIME,
    GROUP_BY_FILL,
    AGGREGATION,
    FILL,
    UDAF,
    UDTF,

    CREATE_FUNCTION,
    DROP_FUNCTION,

    SHOW,
    SHOW_MERGE_STATUS,

    CREATE_INDEX,
    DROP_INDEX,
    QUERY_INDEX,

    LOAD_FILES,
    REMOVE_FILE,
    MOVE_FILE,

    CREATE_TRIGGER,
    DROP_TRIGGER,
    START_TRIGGER,
    STOP_TRIGGER,

    CREATE_TEMPLATE,
    SET_DEVICE_TEMPLATE,
    SET_USING_DEVICE_TEMPLATE,
    MERGE,
    FULL_MERGE,

    MNODE,
    MEASUREMENT_MNODE,
    STORAGE_GROUP_MNODE,
    AUTO_CREATE_DEVICE_MNODE,

    TTL,
    KILL,
    FLUSH,
    TRACING,
    CLEAR_CACHE,
    DELETE_PARTITION,
    LOAD_CONFIGURATION,
    CREATE_SCHEMA_SNAPSHOT,
    CREATE_CONTINUOUS_QUERY,
    DROP_CONTINUOUS_QUERY,
    SHOW_CONTINUOUS_QUERIES,
    UPDATE_STORAGE_GROUP
  }
}
