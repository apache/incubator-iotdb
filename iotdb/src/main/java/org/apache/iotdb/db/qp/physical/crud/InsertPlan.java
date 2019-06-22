/**
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.record.TSRecord;

public class InsertPlan extends PhysicalPlan {

  private static final long serialVersionUID = 6102845312368561515L;
  private String deviceId;
  private String[] measurements;
  private TSDataType[] dataTypes;
  private String[] values;
  private long time;

  // insertType
  // 1 : BufferWrite Insert 2 : Overflow Insert
  private int insertType;

  public InsertPlan(String deviceId, long insertTime, String measurement, String insertValue) {
    super(false, OperatorType.INSERT);
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = new String[] {measurement};
    this.values = new String[] {insertValue};
  }

  public InsertPlan(TSRecord tsRecord) {
    super(false, OperatorType.INSERT);
    this.deviceId = tsRecord.deviceId;
    this.time = tsRecord.time;
    this.measurements = new String[tsRecord.dataPointList.size()];
    this.dataTypes = new TSDataType[tsRecord.dataPointList.size()];
    this.values = new String[tsRecord.dataPointList.size()];
    for (int i = 0; i < tsRecord.dataPointList.size(); i++) {
      measurements[i] = tsRecord.dataPointList.get(i).getMeasurementId();
      dataTypes[i] = tsRecord.dataPointList.get(i).getType();
      values[i] = tsRecord.dataPointList.get(i).getValue().toString();
    }
  }

  public InsertPlan(String deviceId, long insertTime, String[] measurementList,
      String[] insertValues) {
    super(false, Operator.OperatorType.INSERT);
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = measurementList;
    this.values = insertValues;
  }

  public InsertPlan(int insertType, String deviceId, long insertTime, String[] measurementList,
      String[] insertValues) {
    super(false, Operator.OperatorType.INSERT);
    this.insertType = insertType;
    this.time = insertTime;
    this.deviceId = deviceId;
    this.measurements = measurementList;
    this.values = insertValues;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public TSDataType[] getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(TSDataType[] dataTypes) {
    this.dataTypes = dataTypes;
  }

  @Override
  public List<Path> getPaths() {
    List<Path> ret = new ArrayList<>();

    for (String m : measurements) {
      ret.add(new Path(deviceId, m));
    }
    return ret;
  }

  public int getInsertType() {
    return insertType;
  }

  public void setInsertType(int insertType) {
    this.insertType = insertType;
  }

  public String getDeviceId() {
    return this.deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public String[] getMeasurements() {
    return this.measurements;
  }

  public void setMeasurements(String[] measurements) {
    this.measurements = measurements;
  }

  public String[] getValues() {
    return this.values;
  }

  public void setValues(String[] values) {
    this.values = values;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InsertPlan that = (InsertPlan) o;
    return time == that.time && Objects.equals(deviceId, that.deviceId)
        && Arrays.equals(measurements, that.measurements)
        && Arrays.equals(values, that.values);
  }

}
