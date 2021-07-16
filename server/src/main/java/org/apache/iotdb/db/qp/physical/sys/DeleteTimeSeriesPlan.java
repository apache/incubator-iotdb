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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.StatusUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DeleteTimeSeriesPlan extends PhysicalPlan {

  private List<PartialPath> deletePathList;
  private Map<Integer, TSStatus> results = new TreeMap<>();

  public DeleteTimeSeriesPlan(List<PartialPath> deletePathList) {
    super(false, Operator.OperatorType.DELETE_TIMESERIES);
    this.deletePathList = deletePathList;
  }

  public DeleteTimeSeriesPlan() {
    super(false, Operator.OperatorType.DELETE_TIMESERIES);
  }

  @Override
  public List<PartialPath> getPaths() {
    return deletePathList;
  }

  public void setDeletePathList(List<PartialPath> deletePathList) {
    this.deletePathList = deletePathList;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.DELETE_TIMESERIES.ordinal();
    stream.writeByte((byte) type);
    stream.writeInt(deletePathList.size());
    for (PartialPath path : deletePathList) {
      putString(stream, path.getFullPath());
      stream.writeLong(path.getMajorVersion());
      stream.writeLong(path.getMinorVersion());
    }

    stream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.DELETE_TIMESERIES.ordinal();
    buffer.put((byte) type);
    buffer.putInt(deletePathList.size());
    for (PartialPath path : deletePathList) {
      putString(buffer, path.getFullPath());
      buffer.putLong(path.getMajorVersion());
      buffer.putLong(path.getMinorVersion());
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int pathNumber = buffer.getInt();
    deletePathList = new ArrayList<>();
    for (int i = 0; i < pathNumber; i++) {
      PartialPath partialPath = new PartialPath(readString(buffer));
      partialPath.setMajorVersion(buffer.getLong());
      partialPath.setMinorVersion(buffer.getLong());
      deletePathList.add(partialPath);
    }

    this.index = buffer.getLong();
  }

  @Override
  public void setPaths(List<PartialPath> fullPaths) {
    this.deletePathList = fullPaths;
  }

  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  public TSStatus[] getFailingStatus() {
    return StatusUtils.getFailingStatus(results, deletePathList.size());
  }
}
