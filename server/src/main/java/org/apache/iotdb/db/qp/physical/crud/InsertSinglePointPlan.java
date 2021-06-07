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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class InsertSinglePointPlan extends PhysicalPlan {

  private static final Logger logger = LoggerFactory.getLogger(InsertSinglePointPlan.class);

  private static final byte TYPE_RAW_STRING = -1;

  protected PartialPath deviceId;
  protected String measurement;
  // get from client
  protected TSDataType dataType;
  // get from MManager
  protected MeasurementMNode measurementMNode;

  // record the failed measurements, their reasons, and positions in "measurements"
  String failedMeasurement;
  private Exception failedException;

  private long time;
  private Object value;

  private Object failedValue;

  // if isNeedInferType is true, the values must be String[], so we could infer types from them
  // if values is object[], we could use the raw type of them, and we should set this to false
  private boolean isNeedInferType = false;

  public InsertSinglePointPlan() {
    super(false, OperatorType.INSERTSINGLEPOINT);
    super.canBeSplit = false;
  }

  public PartialPath getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(PartialPath deviceId) {
    this.deviceId = deviceId;
  }

  public String getMeasurement() {
    return this.measurement;
  }

  public void setMeasurement(String measurement) {
    this.measurement = measurement;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataTypes(TSDataType dataType) {
    this.dataType = dataType;
  }

  public MeasurementMNode getMeasurementMNode() {
    return measurementMNode;
  }

  public void setMeasurementMNode(MeasurementMNode mNode) {
    this.measurementMNode = mNode;
  }

  public String getFailedMeasurement() {
    return failedMeasurement;
  }

  public Exception getFailedException() {
    return failedException;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public boolean isNeedInferType() {
    return isNeedInferType;
  }

  public void setNeedInferType(boolean inferType) {
    this.isNeedInferType = inferType;
  }

  public int getFailedMeasurementNumber() {
    return failedMeasurement == null ? 0 : 1;
  }

  public InsertSinglePointPlan(InsertSinglePointPlan another) {
    super(false, OperatorType.INSERTSINGLEPOINT);
    this.deviceId = another.deviceId;
    this.time = another.time;
    this.measurement = another.measurement;
    this.value = another.value;
    this.dataType = another.dataType;
  }

  public InsertSinglePointPlan(
      PartialPath deviceId, long time, String measurement, String insertValue) {
    super(false, OperatorType.INSERTSINGLEPOINT);
    this.time = time;
    this.deviceId = deviceId;
    this.measurement = measurement;
    // We need to create an Object[] for the data type casting, because we can not set Float, Long
    // to String[i]
    this.value = insertValue;
    isNeedInferType = true;
  }

  public InsertSinglePointPlan(
      PartialPath deviceId, long time, String measurement, ByteBuffer value)
      throws QueryProcessException {
    super(false, OperatorType.INSERTSINGLEPOINT);
    this.time = time;
    this.deviceId = deviceId;
    this.measurement = measurement;
    this.dataType = dataType;
    this.fillValue(value);
    isNeedInferType = false;
  }

  public long getMinTime() {
    return getTime();
  }

  /** @param */
  public void markFailedMeasurementInsertion(Exception e) {
    if (measurement == null) {
      return;
    }
    failedValue = value;
    value = null;
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> ret = new ArrayList<>();

    PartialPath fullPath = deviceId.concatNode(measurement);
    ret.add(fullPath);

    return ret;
  }

  public Object getValue() {
    return this.value;
  }

  public void setValues(Object value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InsertSinglePointPlan that = (InsertSinglePointPlan) o;
    return time == that.time
        && Objects.equals(deviceId, that.deviceId)
        && Objects.equals(measurement, that.measurement)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deviceId, time);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.INSERT.ordinal();
    stream.writeByte((byte) type);
    subSerialize(stream);
  }

  public void subSerialize(DataOutputStream stream) throws IOException {
    stream.writeLong(time);
    putString(stream, deviceId.getFullPath());
    serializeMeasurementsAndValues(stream);
  }

  void serializeMeasurementsAndValues(DataOutputStream stream) throws IOException {
    stream.writeInt(1);

    if (measurement != null) {
      putString(stream, measurement);
    }

    try {
      putValues(stream);
    } catch (QueryProcessException e) {
      throw new IOException(e);
    }

    // the types are not inferred before the plan is serialized
    stream.write((byte) (isNeedInferType ? 1 : 0));

    stream.writeLong(index);
  }

  private void putValues(DataOutputStream outputStream) throws QueryProcessException, IOException {

    if (measurement == null) {
      return;
    }
    // types are not determined, the situation mainly occurs when the plan uses string values
    // and is forwarded to other nodes
    if (dataType == null) {
      ReadWriteIOUtils.write(TYPE_RAW_STRING, outputStream);
      ReadWriteIOUtils.write((String) value, outputStream);
    } else {
      ReadWriteIOUtils.write(dataType, outputStream);
      switch (dataType) {
        case BOOLEAN:
          ReadWriteIOUtils.write((Boolean) value, outputStream);
          break;
        case INT32:
          ReadWriteIOUtils.write((Integer) value, outputStream);
          break;
        case INT64:
          ReadWriteIOUtils.write((Long) value, outputStream);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((Float) value, outputStream);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((Double) value, outputStream);
          break;
        case TEXT:
          ReadWriteIOUtils.write((Binary) value, outputStream);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    }
  }

  private void putValues(ByteBuffer buffer) throws QueryProcessException {
    if (measurement == null) {
      return;
    }
    // types are not determined, the situation mainly occurs when the plan uses string values
    // and is forwarded to other nodes
    if (dataType == null) {
      ReadWriteIOUtils.write(TYPE_RAW_STRING, buffer);
      ReadWriteIOUtils.write((String) value, buffer);
    } else {
      ReadWriteIOUtils.write(dataType, buffer);
      switch (dataType) {
        case BOOLEAN:
          ReadWriteIOUtils.write((Boolean) value, buffer);
          break;
        case INT32:
          ReadWriteIOUtils.write((Integer) value, buffer);
          break;
        case INT64:
          ReadWriteIOUtils.write((Long) value, buffer);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((Float) value, buffer);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((Double) value, buffer);
          break;
        case TEXT:
          ReadWriteIOUtils.write((Binary) value, buffer);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    }
  }

  /** Make sure the values is already inited before calling this */
  public void fillValue(ByteBuffer buffer) throws QueryProcessException {
    // types are not determined, the situation mainly occurs when the plan uses string values
    // and is forwarded to other nodes
    byte typeNum = (byte) ReadWriteIOUtils.read(buffer);
    if (typeNum == TYPE_RAW_STRING) {
      value = ReadWriteIOUtils.readString(buffer);
    }

    dataType = TSDataType.values()[typeNum];
    switch (dataType) {
      case BOOLEAN:
        value = ReadWriteIOUtils.readBool(buffer);
        break;
      case INT32:
        value = ReadWriteIOUtils.readInt(buffer);
        break;
      case INT64:
        value = ReadWriteIOUtils.readLong(buffer);
        break;
      case FLOAT:
        value = ReadWriteIOUtils.readFloat(buffer);
        break;
      case DOUBLE:
        value = ReadWriteIOUtils.readDouble(buffer);
        break;
      case TEXT:
        value = ReadWriteIOUtils.readBinary(buffer);
        break;
      default:
        throw new QueryProcessException("Unsupported data type:" + dataType);
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.INSERT.ordinal();
    buffer.put((byte) type);
    subSerialize(buffer);
  }

  public void subSerialize(ByteBuffer buffer) {
    buffer.putLong(time);
    putString(buffer, deviceId.getFullPath());
    serializeMeasurementsAndValues(buffer);
  }

  void serializeMeasurementsAndValues(ByteBuffer buffer) {
    buffer.putInt(1);

    if (measurement != null) {
      putString(buffer, measurement);
    }

    try {
      putValues(buffer);
    } catch (QueryProcessException e) {
      logger.error("Failed to serialize values for {}", this, e);
    }

    // the types are not inferred before the plan is serialized
    buffer.put((byte) (isNeedInferType ? 1 : 0));
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.time = buffer.getLong();
    this.deviceId = new PartialPath(readString(buffer));
    deserializeMeasurementsAndValues(buffer);
  }

  void deserializeMeasurementsAndValues(ByteBuffer buffer) {
    int measurementSize = buffer.getInt();

    measurement = readString(buffer);

    this.dataType = dataType;
    this.value = value;
    try {
      fillValue(buffer);
    } catch (QueryProcessException e) {
      e.printStackTrace();
    }

    isNeedInferType = buffer.get() == 1;
    this.index = buffer.getLong();
  }

  @Override
  public String toString() {
    return "deviceId: "
        + deviceId
        + ", time: "
        + time
        + ", measurement: "
        + measurement
        + ", value: "
        + value;
  }

  boolean hasFailedValues() {
    return failedValue != null;
  }

  public TimeValuePair composeTimeValuePair() {
    return new TimeValuePair(time, TsPrimitiveType.getByType(dataType, value));
  }

  public InsertSinglePointPlan getPlanFromFailed() {
    value = failedValue;
    failedValue = null;
    return this;
  }

  /** Reset measurement from failed measurement (if any), as if no failure had ever happened. */
  public void recoverFromFailure() {
    if (failedMeasurement == null) {
      return;
    }
    value = failedValue;
    recoverFromFailure();

    failedValue = null;
  }

  @Override
  public void checkIntegrity() throws QueryProcessException {
    super.checkIntegrity();
    if (value == null) {
      throw new QueryProcessException("Value are null");
    }
  }

  public void transferType() throws QueryProcessException {
    if (isNeedInferType) {
      if (measurementMNode == null) {
        if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          markFailedMeasurementInsertion(
              new QueryProcessException(
                  new PathNotExistException(
                      deviceId.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurement)));
        } else {
          throw new QueryProcessException(
              new PathNotExistException(
                  deviceId.getFullPath() + IoTDBConstant.PATH_SEPARATOR + measurement));
        }
      }
      dataType = measurementMNode.getSchema().getType();
      try {
        value = CommonUtils.parseValue(dataType, value.toString());
      } catch (Exception e) {
        logger.warn(
            "{}.{} data type is not consistent, input {}, registered {}",
            deviceId,
            measurement,
            value,
            dataType);
        if (IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
          markFailedMeasurementInsertion(e);
          measurementMNode = null;
        } else {
          throw e;
        }
      }
    }
  }
}
