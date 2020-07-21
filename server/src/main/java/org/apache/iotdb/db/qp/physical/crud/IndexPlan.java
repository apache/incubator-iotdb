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

package org.apache.iotdb.db.qp.physical.crud;


import static org.apache.iotdb.db.qp.logical.Operator.OperatorType.INDEX;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.index.IndexManager.IndexType;
import org.apache.iotdb.db.qp.logical.crud.IndexOperator.IndexOperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

public class IndexPlan extends PhysicalPlan {

  private Path path;
  private final IndexType indexType;
  private final IndexOperatorType indexOperatorType;

  public IndexPlan(Path path, IndexOperatorType indexOperatorType, IndexType indexType) {
    super(false, INDEX);
    this.path = path;
    this.indexOperatorType = indexOperatorType;
    this.indexType = indexType;
  }

  @Override
  public List<Path> getPaths() {
    List<Path> list = new ArrayList<>();
    if (path != null) {
      list.add(path);
    }
    return list;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public IndexOperatorType getIndexOperatorType() {
    return indexOperatorType;
  }
}
