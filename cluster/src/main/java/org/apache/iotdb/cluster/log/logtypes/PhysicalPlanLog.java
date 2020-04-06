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

import static org.apache.iotdb.cluster.log.Log.Types.PHYSICAL_PLAN;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.BatchInsertPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PhysicalPlanLog contains a non-partitioned physical plan like set storage group.
 */
public class PhysicalPlanLog extends Log {

  private static final Logger logger = LoggerFactory.getLogger(PhysicalPlanLog.class);
  private PhysicalPlan plan;

  public PhysicalPlanLog() {
  }

  public PhysicalPlanLog(PhysicalPlan plan) {
    this.plan = plan;
  }

  @Override
  public ByteBuffer serialize() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      dataOutputStream.writeByte((byte) PHYSICAL_PLAN.ordinal());

      dataOutputStream.writeLong(getPreviousLogIndex());
      dataOutputStream.writeLong(getPreviousLogTerm());
      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getCurrLogTerm());

      plan.serialize(dataOutputStream);
    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {

    setPreviousLogIndex(buffer.getLong());
    setPreviousLogTerm(buffer.getLong());
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());

    try {
      plan = PhysicalPlan.Factory.create(buffer);
    } catch (IOException e) {
      logger.error("Cannot parse a physical plan", e);
    }
  }

  public PhysicalPlan getPlan() {
    return plan;
  }

  public void setPlan(PhysicalPlan plan) {
    this.plan = plan;
  }

  @Override
  public String toString() {
    return plan.toString() + ",term:" + getCurrLogTerm() + ",index:" + getCurrLogIndex() +
        ",prevTerm:" + getPreviousLogTerm() + ",prevIndex:" + getPreviousLogIndex();
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
    PhysicalPlanLog that = (PhysicalPlanLog) o;
    return Objects.equals(plan, that.plan);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), plan);
  }
}
