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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

public interface IoTDBRel extends RelNode {

  void implement(Implementor implementor);

  /**
   * Calling convention for relational operations that occur in IoTDB.
   */
  Convention CONVENTION = new Convention.Impl("IOTDB", IoTDBRel.class);

  /**
   * Callback for the implementation process that converts a tree of {@link IoTDBRel} nodes into a
   * IoTDB SQL query.
   */
  class Implementor {

    final List<String> selectFields = new ArrayList<>();
    final Map<String, String> deviceToFilterMap = new LinkedHashMap<>();
    final List<String> globalPredicate = new ArrayList<>();
    int limit = 0;
    int offset = 0;

    RelOptTable table;
    IoTDBTable ioTDBTable;

    /**
     * Adds newly projected fields.
     *
     * @param fields New fields to be projected from a query
     */
    public void addFields(List<String> fields) {
      selectFields.addAll(fields);
    }

    /**
     * Adds newly restricted devices and predicates.
     *
     * @param deviceToFilterMap predicate of given device
     * @param predicates        global predicates to be applied to the query
     */
    public void add(Map<String, String> deviceToFilterMap, List<String> predicates) {
      this.deviceToFilterMap.putAll(deviceToFilterMap);
      this.globalPredicate.addAll(predicates);
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((IoTDBRel) input).implement(this);
    }
  }
}

// End IoTDBRel.java