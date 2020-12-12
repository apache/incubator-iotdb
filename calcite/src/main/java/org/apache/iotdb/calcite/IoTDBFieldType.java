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
package org.apache.iotdb.calcite;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;

enum IoTDBFieldType {
  TEXT(SqlTypeName.VARCHAR, "TEXT"),
  BOOLEAN(SqlTypeName.BOOLEAN, "BOOLEAN"),
  INT32(SqlTypeName.INTEGER, "INT32"),
  INT64(SqlTypeName.BIGINT, "INT64"),
  // this must be real
  FLOAT(SqlTypeName.REAL, "FLOAT"),
  DOUBLE(SqlTypeName.DOUBLE, "DOUBLE");

  private final SqlTypeName sqlType;
  private final String TSDataType;

  IoTDBFieldType(SqlTypeName sqlTypeName, String TSDataTypeName) {
    this.sqlType = sqlTypeName;
    this.TSDataType = TSDataTypeName;
  }

  private static final Map<String, IoTDBFieldType> MAP = new HashMap<>();

  static {
    for (IoTDBFieldType value : values()) {
      MAP.put(value.TSDataType, value);
    }
  }

  public SqlTypeName getSqlType() {
    return this.sqlType;
  }

  public static IoTDBFieldType of(String typeString) {
    return MAP.get(typeString);
  }
}

// End IoTDBFieldType.java