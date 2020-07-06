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
package org.apache.iotdb.db.qp.physical.sys;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

public class FlushPlan extends PhysicalPlan {
  private List<Path> storeGroups;

  public Boolean isSeq() {
    return isSeq;
  }

  private Boolean isSeq;

  public FlushPlan(Boolean isSeq, List<Path> storeGroups) {
    super(false, OperatorType.FLUSH);
    this.storeGroups = storeGroups;
    this.isSeq = isSeq;
  }

  @Override
  public List<Path> getPaths() {
    return storeGroups;
  }

  @Override
  public List<String> getPathsStrings() {
    List<String> ret = new ArrayList<>();
    for(Path path : storeGroups){
      ret.add(path.toString());
    }
    return ret;
  }


}
