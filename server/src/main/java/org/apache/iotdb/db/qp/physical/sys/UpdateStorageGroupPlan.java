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
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class UpdateStorageGroupPlan extends PhysicalPlan {

  private PartialPath path;

  public UpdateStorageGroupPlan() {
    super(false, OperatorType.UPDATE_STORAGE_GROUP);
  }

  public UpdateStorageGroupPlan(PartialPath path) {
    super(false, Operator.OperatorType.UPDATE_STORAGE_GROUP);
    this.path = path;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  @Override
  public List<PartialPath> getPaths() {
    return path != null ? Collections.singletonList(path) : Collections.emptyList();
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.write((byte) PhysicalPlanType.UPDATE_STORAGE_GROUP.ordinal());
    putString(stream, path.getFullPath());
    stream.writeLong(index);
    stream.writeLong(path.getMajorVersion());
    stream.writeLong(path.getMinorVersion());
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.UPDATE_STORAGE_GROUP.ordinal());
    putString(buffer, path.getFullPath());
    buffer.putLong(index);
    buffer.putLong(path.getMajorVersion());
    buffer.putLong(path.getMinorVersion());
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    path = new PartialPath(readString(buffer));
    this.index = buffer.getLong();
    path.setMajorVersion(buffer.getLong());
    path.setMinorVersion(buffer.getLong());
  }

  @Override
  public String toString() {
    return "UpdateStorageGroupPlan{" + " path=" + path + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    UpdateStorageGroupPlan that = (UpdateStorageGroupPlan) o;

    return Objects.equals(path, that.path)
        && (path.getMajorVersion() == that.path.getMajorVersion())
        && (path.getMinorVersion() == that.path.getMinorVersion());
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }
}
