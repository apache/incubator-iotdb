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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.read.common.Path;

public class DeleteTimeSeriesPlan extends PhysicalPlan {

  private List<Path> deletePathList;

  public DeleteTimeSeriesPlan(List<Path> deletePathList) {
    super(false, Operator.OperatorType.DELETE_TIMESERIES);
    this.deletePathList = deletePathList;
  }

  public DeleteTimeSeriesPlan() {
    super(false, Operator.OperatorType.DELETE_TIMESERIES);
  }

  public void setPaths(List<Path> paths){
    this.deletePathList = paths;
  }

  @Override
  public List<Path> getPaths() {
    return deletePathList;
  }

  @Override
  public List<String> getPathsStrings() {
    List<String> ret = new ArrayList<>();
    for(Path path : deletePathList){
      ret.add(path.toString());
    }
    return ret;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.DELETE_TIMESERIES.ordinal();
    stream.writeByte((byte) type);
    stream.writeInt(deletePathList.size());
    for (Path path : deletePathList) {
      putString(stream, path.getFullPath());
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.DELETE_TIMESERIES.ordinal();
    buffer.put((byte) type);
    buffer.putInt(deletePathList.size());
    for (Path path : deletePathList) {
      putString(buffer, path.getFullPath());
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    int pathNumber = buffer.getInt();
    deletePathList = new ArrayList<>();
    for (int i = 0; i < pathNumber; i++) {
      deletePathList.add(new Path(readString(buffer)));
    }
  }
}
