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
package org.apache.iotdb.db.auth.entity;

/** This enum class contains all available privileges in IoTDB. */
public enum PrivilegeType {
  SET_STORAGE_GROUP,
  INSERT_TIMESERIES,
  @Deprecated
  UPDATE_TIMESERIES,
  READ_TIMESERIES,
  CREATE_TIMESERIES,
  DELETE_TIMESERIES,
  CREATE_USER,
  DELETE_USER,
  MODIFY_PASSWORD,
  LIST_USER,
  GRANT_USER_PRIVILEGE,
  REVOKE_USER_PRIVILEGE,
  GRANT_USER_ROLE,
  REVOKE_USER_ROLE,
  CREATE_ROLE,
  DELETE_ROLE,
  LIST_ROLE,
  GRANT_ROLE_PRIVILEGE,
  REVOKE_ROLE_PRIVILEGE,
  CREATE_FUNCTION,
  DROP_FUNCTION,
  CREATE_TRIGGER,
  DROP_TRIGGER,
  START_TRIGGER,
  STOP_TRIGGER,
  ALL,
  TTL,
  DELETE_STORAGE_GROUP,
  ALTER_TIMESERIES;

  /**
   * Some privileges need a seriesPath as parameter, while others do not. This method returns which
   * privileges need a seriesPath.
   *
   * @param type An integer that represents a privilege.
   * @return Whether this privilege need a seriesPath or not.
   */
  public static boolean isPathRelevant(int type) {
    return type <= DELETE_TIMESERIES.ordinal()
        || type == DELETE_STORAGE_GROUP.ordinal()
        || type == ALTER_TIMESERIES.ordinal();
  }
}
