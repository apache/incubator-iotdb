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

import org.apache.iotdb.db.qp.constant.SQLConstant;

/**
 * This class is a superclass of all operator.
 */
public abstract class Operator {

  // operator type in int format
  protected int tokenIntType;
  // operator type in String format
  protected String tokenName;

  protected OperatorType operatorType = OperatorType.NULL;

  public Operator(int tokenIntType) {
    this.tokenIntType = tokenIntType;
    this.tokenName = SQLConstant.tokenNames.get(tokenIntType);
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

  public String getTokenName() {
    return tokenName;
  }

  public void setOperatorType(OperatorType operatorType) {
    this.operatorType = operatorType;
  }

  @Override
  public String toString() {
    return tokenName;
  }

  /**
   * If you want to add new OperatorType, you must add it in the last.
   */
  public enum OperatorType {
    SFW, JOIN, UNION, FILTER, GROUPBYTIME, ORDERBY, LIMIT, SELECT, SEQTABLESCAN, HASHTABLESCAN,
    MERGEJOIN, FILEREAD, NULL, TABLESCAN, INSERT, BATCHINSERT, DELETE, BASIC_FUNC, IN, QUERY, MERGEQUERY,
    AGGREGATION, AUTHOR, FROM, FUNC, LOADDATA, METADATA, FILL,
    SET_STORAGE_GROUP, CREATE_TIMESERIES, DELETE_TIMESERIES, CREATE_USER, DELETE_USER, MODIFY_PASSWORD,
    GRANT_USER_PRIVILEGE, REVOKE_USER_PRIVILEGE, GRANT_USER_ROLE, REVOKE_USER_ROLE, CREATE_ROLE,
    DELETE_ROLE, GRANT_ROLE_PRIVILEGE, REVOKE_ROLE_PRIVILEGE, LIST_USER, LIST_ROLE,
    LIST_USER_PRIVILEGE, LIST_ROLE_PRIVILEGE, LIST_USER_ROLES, LIST_ROLE_USERS,
    GRANT_WATERMARK_EMBEDDING, REVOKE_WATERMARK_EMBEDDING,
    TTL, DELETE_STORAGE_GROUP, LOAD_CONFIGURATION, SHOW, LOAD_FILES, REMOVE_FILE, MOVE_FILE, LAST, GROUP_BY_FILL,
    ALTER_TIMESERIES, FLUSH, MERGE, FULL_MERGE, CLEAR_CACHE,
    SHOW_MERGE_STATUS, CREATE_SCHEMA_SNAPSHOT, TRACING, DELETE_PARTITION,
    UDAF, UDTF, CREATE_FUNCTION, DROP_FUNCTION,
    CREATE_MULTI_TIMESERIES, CREATE_INDEX, DROP_INDEX, QUERY_INDEX, KILL,
    CHANGE_TAG_OFFSET, CHANGE_ALIAS, MNODE,
    MEASUREMENT_MNODE, STORAGE_GROUP_MNODE,
    BATCH_INSERT_ONE_DEVICE, MULTI_BATCH_INSERT, ALTER_TIMESERIES_BASIC_INFO;
  }
}
