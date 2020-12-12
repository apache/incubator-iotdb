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

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBTable extends AbstractQueryableTable
    implements TranslatableTable {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBTable.class);
  RelProtoDataType protoRowType;
  private final IoTDBSchema schema;
  private final String storageGroup;

  public IoTDBTable(IoTDBSchema schema, String storageGroup) {
    super(Object[].class);
    this.schema = schema;
    this.storageGroup = storageGroup;
  }

  public String toString() {
    return "IoTDBTable {" + storageGroup + "}";
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    try {
      if (protoRowType == null) {
        protoRowType = schema.getRelDataType(storageGroup);
      }
    } catch (SQLException e) {
      logger.error("Error while executing show statement: ", e);
    } catch (QueryProcessException e) {
      logger.error(e.getMessage());
    }
    return protoRowType.apply(typeFactory);
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new IoTDBQueryable<>(queryProvider, schema, this, storageGroup);
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new IoTDBTableScan(cluster, cluster.traitSetOf(IoTDBRel.CONVENTION),
        relOptTable, this, null);
  }

  public Enumerable<Object> query(final Connection connection) {
    return query(connection, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
        ImmutableList.of(), 0, 0);
  }

  /**
   * Executes a IoTDB SQL query.
   *
   * @param connection IoTDB connection
   * @param fields     List of fields to project
   * @return Enumerator of results
   */
  public Enumerable<Object> query(final Connection connection,
      List<Map.Entry<String, Class>> fields,
      final List<String> selectFields, final List<Map.Entry<String, String>> deviceToFilterList,
      List<String> globalPredicates, final Integer limit, final Integer offset) {
    // Build the type of the resulting row based on the provided fields
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    final RelDataType rowType = getRowType(typeFactory);

    Function1<String, Void> addField = fieldName -> {
      SqlTypeName typeName =
          rowType.getField(fieldName, true, false).getType().getSqlTypeName();
      fieldInfo.add(fieldName, typeFactory.createSqlType(typeName))
          .nullable(true);
      return null;
    };

    if (selectFields.isEmpty()) {
      for (Map.Entry<String, Class> field : fields) {
        addField.apply(field.getKey());
      }
    } else {
      for (String field : selectFields) {
        if (field.startsWith("'")) {
          fieldInfo.add(field, SqlTypeName.VARCHAR);
        } else {
          addField.apply(field);
        }
      }
    }

    final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

    // Construct the list of fields to project
    String selectString = "";
    if (selectFields.isEmpty()) {
      selectString = "*";
    } else {
      // delete the 'time', 'device' string in select list
      // this has to be here rather than init "selectFields" otherwise the resultRowType will be wrong
      StringBuilder selectBuilder = new StringBuilder();
      for (int i = 0; i < selectFields.size(); i++) {
        String selectField = selectFields.get(i);
        if (!selectField.equals(IoTDBConstant.DEVICE_COLUMN) && !selectField
            .equals(IoTDBConstant.TIME_COLUMN)) {
          selectBuilder.append(selectField);
          if (i < selectFields.size() - 1) {
            selectBuilder.append(", ");
          }
        }
      }
      selectString = selectBuilder.toString();
      if (selectString.equals("")) {
        selectString = "*";
      }
    }

    List<String> queryList = new ArrayList<>();
    Set<String> tmpDevices = new HashSet<>(); // to deduplicate in global query
    // construct query by device
    if (!deviceToFilterList.isEmpty()) {
      for (Entry<String, String> deviceToFilter : deviceToFilterList) {
        String fromClause = " FROM ";
        fromClause += deviceToFilter.getKey();
        tmpDevices.add(deviceToFilter.getKey());

        String whereClause = "";
        if (deviceToFilter.getValue() != null) {
          whereClause = " WHERE ";
          whereClause += deviceToFilter.getValue();
        }

        // Build and issue the query and return an Enumerator over the results
        StringBuilder queryBuilder = new StringBuilder("SELECT ");
        queryBuilder.append(selectString).append(fromClause).append(whereClause);
        if (limit > 0) {
          queryBuilder.append(" LIMIT " + limit);
        }
        if (offset > 0) {
          queryBuilder.append(" OFFSET " + offset);
        }

        // append align by device
        queryBuilder.append(" " + IoTDBConstant.ALIGN_BY_DEVICE);
        queryList.add(queryBuilder.toString());
      }
    }

    // construct global query
    if (deviceToFilterList.isEmpty() || !globalPredicates.isEmpty()) {
      String fromClause = " FROM ";
      // deduplicate redundant device
      if (!deviceToFilterList.isEmpty()) {
        List<String> deduplicatedDevices = new ArrayList<>();
        for (String device : IoTDBSchema.sgToDeviceMap.get(storageGroup)) {
          if (!tmpDevices.contains(device)) {
            deduplicatedDevices.add(device);
          }
        }
        fromClause += Util.toString(deduplicatedDevices, "", ", ", "");
      } else {
        fromClause += storageGroup + IoTDBConstant.PATH_SEPARATOR + "*";
      }

      String whereClause = "";
      if (!globalPredicates.isEmpty()) {
        whereClause = " WHERE ";
        whereClause += Util.toString(globalPredicates, "", " OR ", "");
      }

      // Build and issue the query and return an Enumerator over the results
      StringBuilder queryBuilder = new StringBuilder("SELECT ");
      queryBuilder.append(selectString).append(fromClause).append(whereClause);

      if (limit > 0) {
        queryBuilder.append(" LIMIT " + limit);
      }
      if (offset > 0) {
        queryBuilder.append(" OFFSET " + offset);
      }

      // append align by device
      queryBuilder.append(" " + IoTDBConstant.ALIGN_BY_DEVICE);
      queryList.add(queryBuilder.toString());
    }

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        Enumerator<Object> enumerator = null;
        try {
          Statement statement = connection.createStatement();
          List<ResultSet> resultList = new ArrayList<>();
          for (String query : queryList) {
            resultList.add(statement.executeQuery(query));
          }
          enumerator = new IoTDBEnumerator(resultList, resultRowType);
        } catch (SQLException e) {
          logger.error("Error while querying from IOTDB: ", e);
        }
        return enumerator;
      }
    };
  }

  /**
   * Implementation of {@link Queryable}
   *
   * @param <T> element type
   */
  public static class IoTDBQueryable<T> extends AbstractTableQueryable<T> {

    public IoTDBQueryable(QueryProvider queryProvider, SchemaPlus schema,
        IoTDBTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      //noinspection unchecked
      final Enumerable<T> enumerable =
          (Enumerable<T>) getTable().query(getConnection());
      return enumerable.enumerator();
    }

    private IoTDBTable getTable() {
      return (IoTDBTable) table;
    }

    private Connection getConnection() {
      return schema.unwrap(IoTDBSchema.class).connection;
    }

    /**
     * Called via code-generation.
     */
    @SuppressWarnings("UnusedDeclaration")
    public Enumerable<Object> query(List<Map.Entry<String, Class>> fields,
        List<String> selectFields, List<Map.Entry<String, String>> deviceToFilterList,
        List<String> predicates,
        Integer limit, Integer offset) {
      return getTable()
          .query(getConnection(), fields, selectFields, deviceToFilterList, predicates, limit,
              offset);
    }
  }
}

// End IoTDBTable.java