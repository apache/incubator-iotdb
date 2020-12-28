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

package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.RootOperator;

public class CreateFunctionOperator extends RootOperator {

  private boolean isTemporary;
  private String udfName;
  private String className;

  public CreateFunctionOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.CREATE_FUNCTION;
  }

  public void setTemporary(boolean temporary) {
    isTemporary = temporary;
  }

  public void setUdfName(String udfName) {
    this.udfName = udfName;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public boolean isTemporary() {
    return isTemporary;
  }

  public String getUdfName() {
    return udfName;
  }

  public String getClassName() {
    return className;
  }
}
