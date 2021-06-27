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
package org.apache.iotdb.db.auth;

import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AuthorityChecker {

  private static final String SUPER_USER = IoTDBDescriptor.getInstance().getConfig().getAdminName();
  private static final Logger logger = LoggerFactory.getLogger(AuthorityChecker.class);

  private AuthorityChecker() {}

  /**
   * check permission.
   *
   * @param username username
   * @param paths paths in List structure
   * @param type Operator type
   * @param targetUser target user
   * @return if permission-check is passed
   * @throws AuthException Authentication Exception
   */
  public static boolean check(
      String username, List<PartialPath> paths, Operator.OperatorType type, String targetUser)
      throws AuthException {
    if (SUPER_USER.equals(username)) {
      return true;
    }

    int permission = translateToPermissionId(type);
    if (permission == -1) {
      return false;
    } else if (permission == PrivilegeType.MODIFY_PASSWORD.ordinal()
        && username.equals(targetUser)) {
      // a user can modify his own password
      return true;
    }

    if (!paths.isEmpty()) {
      for (PartialPath path : paths) {
        if (!checkOnePath(username, path, permission)) {
          return false;
        }
      }
    } else {
      return checkOnePath(username, null, permission);
    }

    return true;
  }

  private static boolean checkOnePath(String username, PartialPath path, int permission)
      throws AuthException {
    IAuthorizer authorizer = BasicAuthorizer.getInstance();
    try {
      String fullPath = path == null ? IoTDBConstant.PATH_ROOT : path.getFullPath();
      if (authorizer.checkUserPrivileges(username, fullPath, permission)) {
        return true;
      }
    } catch (AuthException e) {
      logger.error("Error occurs when checking the seriesPath {} for user {}", path, username, e);
    }
    return false;
  }

  private static int translateToPermissionId(Operator.OperatorType type) {
    switch (type) {
      case GRANT_ROLE_PRIVILEGE:
        return PrivilegeType.GRANT_ROLE_PRIVILEGE.ordinal();
      case CREATE_ROLE:
        return PrivilegeType.CREATE_ROLE.ordinal();
      case CREATE_USER:
        return PrivilegeType.CREATE_USER.ordinal();
      case MODIFY_PASSWORD:
        return PrivilegeType.MODIFY_PASSWORD.ordinal();
      case GRANT_USER_PRIVILEGE:
        return PrivilegeType.GRANT_USER_PRIVILEGE.ordinal();
      case REVOKE_ROLE_PRIVILEGE:
        return PrivilegeType.REVOKE_ROLE_PRIVILEGE.ordinal();
      case REVOKE_USER_PRIVILEGE:
        return PrivilegeType.REVOKE_USER_PRIVILEGE.ordinal();
      case GRANT_USER_ROLE:
        return PrivilegeType.GRANT_USER_ROLE.ordinal();
      case DELETE_USER:
        return PrivilegeType.DELETE_USER.ordinal();
      case DELETE_ROLE:
        return PrivilegeType.DELETE_ROLE.ordinal();
      case REVOKE_USER_ROLE:
        return PrivilegeType.REVOKE_USER_ROLE.ordinal();
      case SET_STORAGE_GROUP:
        return PrivilegeType.SET_STORAGE_GROUP.ordinal();
      case DELETE_STORAGE_GROUP:
        return PrivilegeType.DELETE_STORAGE_GROUP.ordinal();
      case CREATE_TIMESERIES:
      case CREATE_ALIGNED_TIMESERIES:
      case CREATE_MULTI_TIMESERIES:
        return PrivilegeType.CREATE_TIMESERIES.ordinal();
      case ALTER_TIMESERIES:
        return PrivilegeType.ALTER_TIMESERIES.ordinal();
      case DELETE_TIMESERIES:
      case DELETE:
      case DROP_INDEX:
        return PrivilegeType.DELETE_TIMESERIES.ordinal();
      case QUERY:
      case GROUP_BY_TIME:
      case QUERY_INDEX:
      case AGGREGATION:
      case UDAF:
      case UDTF:
      case LAST:
      case FILL:
      case GROUP_BY_FILL:
        return PrivilegeType.READ_TIMESERIES.ordinal();
      case LOAD_DATA:
      case INSERT:
      case BATCH_INSERT:
      case BATCH_INSERT_ROWS:
      case BATCH_INSERT_ONE_DEVICE:
      case MULTI_BATCH_INSERT:
      case CREATE_INDEX:
        return PrivilegeType.INSERT_TIMESERIES.ordinal();
      case LIST_ROLE:
      case LIST_ROLE_USERS:
      case LIST_ROLE_PRIVILEGE:
        return PrivilegeType.LIST_ROLE.ordinal();
      case LIST_USER:
      case LIST_USER_ROLES:
      case LIST_USER_PRIVILEGE:
        return PrivilegeType.LIST_USER.ordinal();
      case CREATE_FUNCTION:
        return PrivilegeType.CREATE_FUNCTION.ordinal();
      case DROP_FUNCTION:
        return PrivilegeType.DROP_FUNCTION.ordinal();
      case CREATE_TRIGGER:
        return PrivilegeType.CREATE_TRIGGER.ordinal();
      case DROP_TRIGGER:
        return PrivilegeType.DROP_TRIGGER.ordinal();
      case START_TRIGGER:
        return PrivilegeType.START_TRIGGER.ordinal();
      case STOP_TRIGGER:
        return PrivilegeType.STOP_TRIGGER.ordinal();
      case TTL:
        return PrivilegeType.TTL.ordinal();
      default:
        logger.error("Unrecognizable operator type ({}) for AuthorityChecker.", type);
        return -1;
    }
  }
}
