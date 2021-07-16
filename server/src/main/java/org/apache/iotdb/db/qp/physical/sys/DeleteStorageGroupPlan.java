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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeleteStorageGroupPlan extends PhysicalPlan {

  private List<PartialPath> deletePathList;

  public DeleteStorageGroupPlan(List<PartialPath> deletePathList) {
    super(false, Operator.OperatorType.DELETE_STORAGE_GROUP);
    this.deletePathList = deletePathList;
  }

  public DeleteStorageGroupPlan() {
    super(false, Operator.OperatorType.DELETE_STORAGE_GROUP);
  }

  @Override
  public List<PartialPath> getPaths() {
    return deletePathList;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    this.deletePathList = paths;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlan.PhysicalPlanType.DELETE_STORAGE_GROUP.ordinal();
    stream.writeByte((byte) type);
    stream.writeInt(this.getPaths().size());
    for (PartialPath path : this.getPaths()) {
      putString(stream, path.getFullPath());
      stream.writeLong(path.getMajorVersion());
      stream.writeLong(path.getMinorVersion());
    }

    stream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.DELETE_STORAGE_GROUP.ordinal();
    buffer.put((byte) type);
    buffer.putInt(this.getPaths().size());
    for (PartialPath path : this.getPaths()) {
      putString(buffer, path.getFullPath());
      buffer.putLong(path.getMajorVersion());
      buffer.putLong(path.getMinorVersion());
    }

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int pathNum = buffer.getInt();
    this.deletePathList = new ArrayList<>();
    for (int i = 0; i < pathNum; i++) {
      PartialPath partialPath = new PartialPath(readString(buffer));
      partialPath.setMajorVersion(buffer.getLong());
      partialPath.setMinorVersion(buffer.getLong());
      deletePathList.add(partialPath);
    }

    this.index = buffer.getLong();
  }

  @Override
  public String toString() {
    return "DeleteStorageGroupPlan{" + " deletePathList=" + deletePathList + "}";
  }
}
