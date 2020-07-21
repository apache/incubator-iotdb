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

package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.index.IndexManager.IndexType;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.tsfile.read.common.Path;

public final class IndexOperator extends RootOperator {

  private Path path;
  private final IndexOperatorType indexOperatorType;
  private final IndexType indexType;

  public IndexOperator(int tokenIntType, Path path, IndexOperatorType indexOperatorType, IndexType indexType) {
    super(tokenIntType);
    this.path = path;
    this.indexOperatorType = indexOperatorType;
    this.indexType = indexType;
    operatorType = OperatorType.INDEX;
  }

  public Path getPath() {
    return path;
  }

  public IndexOperatorType getIndexOperatorType() {
    return indexOperatorType;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setPath(Path path) {
    this.path = path;
  }

  public enum IndexOperatorType {
    CREATE_INDEX, DROP_INDEX
  }
}
