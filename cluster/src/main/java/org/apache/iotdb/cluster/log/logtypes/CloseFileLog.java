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

package org.apache.iotdb.cluster.log.logtypes;

import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.db.utils.SerializeUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.cluster.log.Log.Types.CLOSE_FILE;

public class CloseFileLog extends Log {

  private String storageGroupName;
  private boolean isSeq;
  private long partitionId;

  public CloseFileLog() {}

  public CloseFileLog(String storageGroupName, long partitionId, boolean isSeq) {
    this.storageGroupName = storageGroupName;
    this.isSeq = isSeq;
    this.partitionId = partitionId;
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeByte((byte) CLOSE_FILE.ordinal());

      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getCurrLogTerm());

      SerializeUtils.serialize(storageGroupName, dataOutputStream);
      dataOutputStream.writeBoolean(isSeq);
      dataOutputStream.writeLong(partitionId);

    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {

    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());

    storageGroupName = SerializeUtils.deserializeString(buffer);
    isSeq = buffer.get() == 1;
    partitionId = buffer.getLong();
  }

  public boolean isSeq() {
    return isSeq;
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public long getPartitionId() {
    return partitionId;
  }

  @Override
  public String toString() {
    return "CloseFileLog{"
        + "fullStorageGroupName='"
        + storageGroupName
        + '\''
        + ", isSeq="
        + isSeq
        + ", partitionId="
        + partitionId
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    CloseFileLog that = (CloseFileLog) o;
    return isSeq == that.isSeq
        && Objects.equals(storageGroupName, that.storageGroupName)
        && partitionId == that.partitionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), storageGroupName, partitionId, isSeq);
  }
}
