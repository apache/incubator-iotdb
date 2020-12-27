/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.qp.physical.crud;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.QueryDataSetUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsBoolean;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsLong;

@SuppressWarnings("java:S1135") // ignore todos
public class InsertTabletPlan extends InsertPlan {

  private static final String DATATYPE_UNSUPPORTED = "Data type %s is not supported.";

  private long[] times; // times should be sorted. It is done in the session API.
  private ByteBuffer timeBuffer;

  private Object[] columns;
  private ByteBuffer valueBuffer;
  private int rowCount = 0;
  // indicate whether this plan has been set 'start' or 'end' in order to support plan transmission without data loss in cluster version
  boolean isExecuting = false;
  // cached values
  private Long maxTime = Long.MIN_VALUE;
  private Long minTime = Long.MAX_VALUE;
  private List<PartialPath> paths;
  private int start;
  private int end;
  // when this plan is sub-plan split from another InsertTabletPlan, this indicates the original positions of values in
  // this plan. For example, if the plan contains 5 timestamps, and range = [1,4,10,12], then it means that the first 3
  // timestamps in this plan are from range[1,4) of the parent plan, and the last 2 timestamps are from range[10,12)
  // of the parent plan.
  // this is usually used to back-propagate exceptions to the parent plan without losing their proper positions.
  private List<Integer> range;

  private List<Object> failedColumns;


  public InsertTabletPlan() {
    super(OperatorType.BATCHINSERT);
  }

  public InsertTabletPlan(PartialPath deviceId, List<String> measurements) {
    super(OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    this.measurements = measurements.toArray(new String[0]);
    this.canBeSplit = true;
  }

  public InsertTabletPlan(PartialPath deviceId, String[] measurements) {
    super(OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    this.measurements = measurements;
    this.canBeSplit = true;
  }

  public InsertTabletPlan(PartialPath deviceId, String[] measurements, List<Integer> dataTypes) {
    super(OperatorType.BATCHINSERT);
    this.deviceId = deviceId;
    this.measurements = measurements;
    setDataTypes(dataTypes);
    this.canBeSplit = true;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.isExecuting = true;
    this.start = start;
  }

  public int getEnd() {
    return end;
  }

  public void setEnd(int end) {
    this.isExecuting = true;
    this.end = end;
  }

  public List<Integer> getRange() {
    return range;
  }

  public void setRange(List<Integer> range) {
    this.range = range;
  }

  @Override
  public List<PartialPath> getPaths() {
    if (paths != null) {
      return paths;
    }
    List<PartialPath> ret = new ArrayList<>();
    for (String m : measurements) {
      PartialPath fullPath = deviceId.concatNode(m);
      ret.add(fullPath);
    }
    paths = ret;
    return ret;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    stream.writeByte((byte) type);

    putString(stream, deviceId.getFullPath());

    writeMeasurements(stream);
    writeDataTypes(stream);
    writeTimes(stream);
    writeValues(stream);
  }

  private void writeMeasurements(DataOutputStream stream) throws IOException {
    stream.writeInt(
        measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));
    for (String m : measurements) {
      if (m == null) {
        continue;
      }
      putString(stream, m);
    }
  }

  private void writeDataTypes(DataOutputStream stream) throws IOException {
    for (int i = 0; i < dataTypes.length; i++) {
      if (measurements[i] == null) {
        continue;
      }
      TSDataType dataType = dataTypes[i];
      stream.writeShort(dataType.serialize());
    }
  }

  private void writeTimes(DataOutputStream stream) throws IOException {
    if (isExecuting) {
      stream.writeInt(end - start);
    } else {
      stream.writeInt(rowCount);
    }

    if (timeBuffer == null) {
      if (isExecuting) {
        for (int i = start; i < end; i++) {
          stream.writeLong(times[i]);
        }
      } else {
        for (long time : times) {
          stream.writeLong(time);
        }
      }
    } else {
      stream.write(timeBuffer.array());
      timeBuffer = null;
    }
  }

  private void writeValues(DataOutputStream stream) throws IOException {
    if (valueBuffer == null) {
      serializeValues(stream);
    } else {
      stream.write(valueBuffer.array());
      valueBuffer = null;
    }

    stream.writeLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    buffer.put((byte) type);

    putString(buffer, deviceId.getFullPath());
    writeMeasurements(buffer);
    writeDataTypes(buffer);
    writeTimes(buffer);
    writeValues(buffer);
  }

  private void writeMeasurements(ByteBuffer buffer) {
    buffer
        .putInt(measurements.length - (failedMeasurements == null ? 0 : failedMeasurements.size()));
    for (String m : measurements) {
      if (m != null) {
        putString(buffer, m);
      }
    }
  }

  private void writeDataTypes(ByteBuffer buffer) {
    for (int i = 0, dataTypesLength = dataTypes.length; i < dataTypesLength; i++) {
      TSDataType dataType = dataTypes[i];
      if (measurements[i] != null) {
        dataType.serializeTo(buffer);
      }
    }
  }

  private void writeTimes(ByteBuffer buffer) {
    if (isExecuting) {
      buffer.putInt(end - start);
    } else {
      buffer.putInt(rowCount);
    }

    if (timeBuffer == null) {
      if (isExecuting) {
        for (int i = start; i < end; i++) {
          buffer.putLong(times[i]);
        }
      } else {
        for (long time : times) {
          buffer.putLong(time);
        }
      }
    } else {
      buffer.put(timeBuffer.array());
      timeBuffer = null;
    }
  }

  private void writeValues(ByteBuffer buffer) {
    if (valueBuffer == null) {
      serializeValues(buffer);
    } else {
      buffer.put(valueBuffer.array());
      valueBuffer = null;
    }

    buffer.putLong(index);
  }

  @Override
  public void serialize(ByteBuffer buffer, PhysicalPlan base) {
    int type = PhysicalPlanType.BATCHINSERT.ordinal();
    buffer.put((byte) type);

    writeTimes(buffer);
    writeValues(buffer);
    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer, PhysicalPlan base) {
    InsertTabletPlan baseInsertTabletPlan = (InsertTabletPlan) base;

    this.deviceId = baseInsertTabletPlan.deviceId;

    this.measurements = baseInsertTabletPlan.getMeasurements();

    this.dataTypes = baseInsertTabletPlan.dataTypes;

    int rows = buffer.getInt();
    rowCount = rows;
    this.times = new long[rows];
    deserializeTimes(buffer, rows);

    columns = QueryDataSetUtils.readValuesFromBuffer(buffer, dataTypes, measurements.length, rows);
    this.index = buffer.getLong();
  }

  private void deserializeTimes(ByteBuffer buffer, int number) {
    for (int i = 0; i < number; i++) {
      times[i] = buffer.getLong();
    }
  }

  private void serializeTimes(ByteBuffer buffer) {
    for (int i = start; i < end; i++) {
      buffer.putLong(times[i]);
    }
  }

  public void serializeTimeValue(ByteBuffer buffer) {
    buffer.putInt(end - start);

    if (timeBuffer == null) {
      serializeTimes(buffer);
    } else {
      buffer.put(timeBuffer.array());
      timeBuffer = null;
    }

    if (valueBuffer == null) {
      serializeValues(buffer);
    } else {
      buffer.put(valueBuffer.array());
      valueBuffer = null;
    }
  }


  private void serializeValues(DataOutputStream outputStream) throws IOException {
    for (int i = 0; i < measurements.length; i++) {
      serializeColumn(dataTypes[i], columns[i], outputStream, start, end);
    }
  }

  private void serializeValues(ByteBuffer buffer) {
    for (int i = 0; i < measurements.length; i++) {
      if (measurements[i] == null) {
        continue;
      }
      serializeColumn(dataTypes[i], columns[i], buffer, start, end);
    }
  }

  private void serializeColumn(TSDataType dataType, Object column, ByteBuffer buffer,
      int start, int end) {
    int curStart = isExecuting ? start : 0;
    int curEnd = isExecuting ? end : rowCount;
    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) column;
        for (int j = curStart; j < curEnd; j++) {
          buffer.putInt(intValues[j]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) column;
        for (int j = curStart; j < curEnd; j++) {
          buffer.putLong(longValues[j]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = curStart; j < curEnd; j++) {
          buffer.putFloat(floatValues[j]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = curStart; j < curEnd; j++) {
          buffer.putDouble(doubleValues[j]);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = curStart; j < curEnd; j++) {
          buffer.put(BytesUtils.boolToByte(boolValues[j]));
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = curStart; j < curEnd; j++) {
          buffer.putInt(binaryValues[j].getLength());
          buffer.put(binaryValues[j].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  private void serializeColumn(TSDataType dataType, Object column, DataOutputStream outputStream,
      int start, int end) throws IOException {
    int curStart = isExecuting ? start : 0;
    int curEnd = isExecuting ? end : rowCount;
    switch (dataType) {
      case INT32:
        int[] intValues = (int[]) column;
        for (int j = curStart; j < curEnd; j++) {
          outputStream.writeInt(intValues[j]);
        }
        break;
      case INT64:
        long[] longValues = (long[]) column;
        for (int j = curStart; j < curEnd; j++) {
          outputStream.writeLong(longValues[j]);
        }
        break;
      case FLOAT:
        float[] floatValues = (float[]) column;
        for (int j = curStart; j < curEnd; j++) {
          outputStream.writeFloat(floatValues[j]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) column;
        for (int j = curStart; j < curEnd; j++) {
          outputStream.writeDouble(doubleValues[j]);
        }
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) column;
        for (int j = curStart; j < curEnd; j++) {
          outputStream.writeByte(BytesUtils.boolToByte(boolValues[j]));
        }
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) column;
        for (int j = curStart; j < curEnd; j++) {
          outputStream.writeInt(binaryValues[j].getLength());
          outputStream.write(binaryValues[j].getValues());
        }
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataType));
    }
  }

  public void setTimeBuffer(ByteBuffer timeBuffer) {
    this.timeBuffer = timeBuffer;
    this.timeBuffer.position(0);
  }

  public void setValueBuffer(ByteBuffer valueBuffer) {
    this.valueBuffer = valueBuffer;
    this.timeBuffer.position(0);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    this.deviceId = new PartialPath(readString(buffer));

    int measurementSize = buffer.getInt();
    this.measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = readString(buffer);
    }

    this.dataTypes = new TSDataType[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      dataTypes[i] = TSDataType.deserialize(buffer.getShort());
    }

    int rows = buffer.getInt();
    rowCount = rows;
    this.times = new long[rows];
    deserializeTimes(buffer, rows);
    updateTimesCache();

    columns = QueryDataSetUtils.readValuesFromBuffer(buffer, dataTypes, measurementSize, rows);
    this.index = buffer.getLong();
  }

  public void setDataTypes(List<Integer> dataTypes) {
    this.dataTypes = new TSDataType[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      this.dataTypes[i] = TSDataType.values()[dataTypes.get(i)];
    }
  }

  public Object[] getColumns() {
    return columns;
  }

  public void setColumns(Object[] columns) {
    this.columns = columns;
  }

  public void setColumn(int index, Object column) {
    columns[index] = column;
  }

  @Override
  public long getMinTime() {
    return minTime;
  }

  public long getMaxTime() {
    return maxTime;
  }

  public TimeValuePair composeLastTimeValuePair(int measurementIndex) {
    if (measurementIndex >= columns.length) {
      return null;
    }
    TsPrimitiveType value;
    switch (dataTypes[measurementIndex]) {
      case INT32:
        int[] intValues = (int[]) columns[measurementIndex];
        value = new TsInt(intValues[rowCount - 1]);
        break;
      case INT64:
        long[] longValues = (long[]) columns[measurementIndex];
        value = new TsLong(longValues[rowCount - 1]);
        break;
      case FLOAT:
        float[] floatValues = (float[]) columns[measurementIndex];
        value = new TsFloat(floatValues[rowCount - 1]);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) columns[measurementIndex];
        value = new TsDouble(doubleValues[rowCount - 1]);
        break;
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) columns[measurementIndex];
        value = new TsBoolean(boolValues[rowCount - 1]);
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) columns[measurementIndex];
        value = new TsBinary(binaryValues[rowCount - 1]);
        break;
      default:
        throw new UnSupportedDataTypeException(
            String.format(DATATYPE_UNSUPPORTED, dataTypes[measurementIndex]));
    }
    return new TimeValuePair(times[rowCount - 1], value);
  }

  public long[] getTimes() {
    return times;
  }

  public void setTimes(long[] times) {
    this.times = times;
    updateTimesCache();
  }

  private void updateTimesCache() {
    for (Long time : times) {
      if (time > maxTime) {
        maxTime = time;
      }
      if (time < minTime) {
        minTime = time;
      }
    }
  }

  public int getRowCount() {
    return rowCount;
  }

  public void setRowCount(int size) {
    this.rowCount = size;
  }

  @Override
  public String toString() {
    return "InsertTabletPlan {" +
        "deviceId:" + deviceId +
        ", timesRange[" + times[0] + "," + times[times.length - 1] + "]" +
        '}';
  }

  @Override
  public void markFailedMeasurementInsertion(int index, Exception e) {
    if (measurements[index] == null) {
      return;
    }
    super.markFailedMeasurementInsertion(index, e);
    if (failedColumns == null) {
      failedColumns = new ArrayList<>();
    }
    failedColumns.add(columns[index]);
    columns[index] = null;
  }

  @Override
  public InsertPlan getPlanFromFailed() {
    if (super.getPlanFromFailed() == null) {
      return null;
    }
    // TODO anything else?
    columns = failedColumns.toArray(new Object[0]);
    failedColumns = null;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InsertTabletPlan that = (InsertTabletPlan) o;

    return rowCount == that.rowCount &&
        Arrays.equals(times, that.times) &&
        Objects.equals(timeBuffer, that.timeBuffer) &&
        Objects.equals(valueBuffer, that.valueBuffer) &&
        Objects.equals(maxTime, that.maxTime) &&
        Objects.equals(minTime, that.minTime) &&
        Objects.equals(paths, that.paths) &&
        Objects.equals(range, that.range);
  }

  @Override
  public int hashCode() {
    int result = Objects
        .hash(timeBuffer, valueBuffer, rowCount, maxTime, minTime, paths,
            range);
    result = 31 * result + Arrays.hashCode(times);
    return result;
  }

  @Override
  public void recoverFromFailure() {
    if (failedMeasurements == null) {
      return;
    }

    for (int i = 0; i < failedMeasurements.size(); i++) {
      int index = failedIndices.get(i);
      columns[index] = failedColumns.get(i);
    }
    super.recoverFromFailure();

    failedColumns = null;
  }

  @Override
  public void checkIntegrity() throws QueryProcessException {
    super.checkIntegrity();
    if (columns == null) {
      throw new QueryProcessException("Values are null");
    }
    if (measurements.length != columns.length) {
      throw new QueryProcessException(String.format("Measurements length [%d] does not match "
          + "columns length [%d]", measurements.length, columns.length));
    }
    for (Object value : columns) {
      if (value == null) {
        throw new QueryProcessException("Columns contain null: " + Arrays.toString(columns));
      }
    }
  }
}
