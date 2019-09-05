/**
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
package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.qp.logical.ExecutableOperator;

/**
 * this class extends {@code RootOperator} and process update statement.
 */
public final class UpdateOperator extends ExecutableOperator {

  private String value;
  private SetPathOperator setPathOperator;
  private FromOperator fromOperator;
  private FilterOperator filterOperator;

  public UpdateOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.UPDATE;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public SetPathOperator getSetPathOperator() {
    return setPathOperator;
  }

  @Override
  public boolean setSetPathOperator(SetPathOperator setPathOperator) {
    this.setPathOperator = setPathOperator;
    return true;
  }

  @Override
  public FromOperator getFromOperator() {
    return fromOperator;
  }

  @Override
  public boolean setFromOperator(FromOperator fromOperator) {
    this.fromOperator = fromOperator;
    return true;
  }

  @Override
  public FilterOperator getFilterOperator() {
    return filterOperator;
  }

  @Override
  public boolean setFilterOperator(FilterOperator filterOperator) {
    this.filterOperator = filterOperator;
    return true;
  }
}
