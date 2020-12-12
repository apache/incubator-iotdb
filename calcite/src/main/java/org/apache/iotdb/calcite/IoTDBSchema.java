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

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.jdbc.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSchema extends AbstractSchema {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBSchema.class);
  final Connection connection;
  private Map<String, Table> tableMap;
  private final SchemaPlus parentSchema;
  final String name;
  public static Map<String, List<String>> sgToDeviceMap = new HashMap<>();

  /**
   * Creates a IoTDB schema.
   *
   * @param host     IoTDB host, e.g. "localhost"
   * @param port     IoTDB port, e.g. 6667
   * @param username IoTDB username
   * @param password IoTDB password
   */
  public IoTDBSchema(String host, int port, String username, String password,
      SchemaPlus parentSchema, String name) {
    super();
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
      this.connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + host + ":" + port + "/", username, password);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    this.parentSchema = parentSchema;
    this.name = name;
  }

  /**
   * Generate the columns' names and data types in the given table.
   * @param storageGroup the table name
   * @return the columns' names and data types
   */
  RelProtoDataType getRelDataType(String storageGroup) throws SQLException, QueryProcessException {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();

    // add time, device columns in relational table
    fieldInfo.add(IoTDBConstant.TIME_COLUMN,
        typeFactory.createSqlType(IoTDBFieldType.INT64.getSqlType()));
    fieldInfo.add(IoTDBConstant.DEVICE_COLUMN,
        typeFactory.createSqlType(IoTDBFieldType.TEXT.getSqlType()));

    // get devices in this storage group
    Statement statement = connection.createStatement();
    boolean hasDevices = statement.execute("show devices " + storageGroup);
    if (hasDevices) {
      ResultSet devices = statement.getResultSet();
      List<String> deviceList = new ArrayList<>();
      while (devices.next()) {
        deviceList.add(devices.getString(1));
      }
      this.sgToDeviceMap.put(storageGroup, deviceList);
    }

    // get deduplicated measurements in table
    boolean hasTS = statement.execute("show timeseries " + storageGroup);
    if (hasTS) {
      ResultSet timeseries = statement.getResultSet();
      Map<String, IoTDBFieldType> tmpMeasurementMap = new HashMap<>();
      while (timeseries.next()) {
        String sensorName = timeseries.getString(1);
        sensorName = sensorName.substring(sensorName.lastIndexOf('.') + 1);
        IoTDBFieldType sensorType = IoTDBFieldType.of(timeseries.getString(4));

        if (!tmpMeasurementMap.containsKey(sensorName)) {
          tmpMeasurementMap.put(sensorName, sensorType);
          fieldInfo.add(sensorName, typeFactory.createSqlType(sensorType.getSqlType()));
        } else {
          if (!tmpMeasurementMap.get(sensorName).equals(sensorType)) {
            throw new QueryProcessException(
                "The data types of the same measurement column should be the same across "
                    + "devices in ALIGN_BY_DEVICE sql. For more details please refer to the "
                    + "SQL document.");
          }
        }
      }
    }

    return RelDataTypeImpl.proto(fieldInfo.build());
  }

  /**
   * Generate a map whose key is table name and value is table instance. The implementations of
   * AbstractSchema.getTableNames() and AbstractSchema.getTable(String) depend on this map.
   */
  @Override
  protected Map<String, Table> getTableMap() {
    try {
      if (tableMap == null) {
        tableMap = createTableMap();
      }
    } catch (SQLException e) {
      logger.error("Error while creating table map: ", e);
    }
    return tableMap;
  }

  public Map<String, Table> createTableMap() throws SQLException {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    List<String> storageGroups = new ArrayList<>();
    Statement statement = connection.createStatement();
    boolean hasResultSet = statement.execute("show storage group");
    if (hasResultSet) {
      ResultSet resultSet = statement.getResultSet();
      while (resultSet.next()) {
        storageGroups.add(resultSet.getString(1));
      }
    }
    for (String storageGroup : storageGroups) {
      builder.put(storageGroup, new IoTDBTable(this, storageGroup));
    }

    return builder.build();
  }

}

// End IoTDBSchema.java