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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class CreateAlignedTimeSeriesPlan extends PhysicalPlan {

  private static final Logger logger = LoggerFactory.getLogger(CreateAlignedTimeSeriesPlan.class);

  private PartialPath prefixPath;
  private List<String> measurements;
  private List<TSDataType> dataTypes;
  private List<TSEncoding> encodings;
  private CompressionType compressor;
  private List<String> aliasList;

  public CreateAlignedTimeSeriesPlan() {
    super(false, Operator.OperatorType.CREATE_ALIGNED_TIMESERIES);
    canBeSplit = false;
  }

  public CreateAlignedTimeSeriesPlan(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      CompressionType compressor,
      List<String> aliasList) {
    super(false, Operator.OperatorType.CREATE_ALIGNED_TIMESERIES);
    this.prefixPath = prefixPath;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressor = compressor;
    this.aliasList = aliasList;
    this.canBeSplit = false;
  }

  public PartialPath getPrefixPath() {
    return prefixPath;
  }

  public void setPrefixPath(PartialPath prefixPath) {
    this.prefixPath = prefixPath;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<TSEncoding> encodings) {
    this.encodings = encodings;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public void setCompressor(CompressionType compressor) {
    this.compressor = compressor;
  }

  public List<String> getAliasList() {
    return aliasList;
  }

  public void setAliasList(List<String> aliasList) {
    this.aliasList = aliasList;
  }

  @Override
  public String toString() {
    return String.format(
        "devicePath: %s, measurements: %s, dataTypes: %s, encodings: %s, compression: %s",
        prefixPath, measurements, dataTypes, encodings, compressor);
  }

  @Override
  public List<PartialPath> getPaths() {
    List<PartialPath> paths = new ArrayList<>();
    for (String measurement : measurements) {
      try {
        paths.add(new PartialPath(prefixPath.getFullPath(), measurement));
      } catch (IllegalPathException e) {
        logger.error("Failed to get paths of CreateAlignedTimeSeriesPlan. ", e);
      }
    }
    return paths;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_ALIGNED_TIMESERIES.ordinal());
    byte[] bytes = prefixPath.getFullPath().getBytes();
    stream.writeInt(bytes.length);
    stream.write(bytes);

    ReadWriteIOUtils.write(measurements.size(), stream);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, stream);
    }
    for (TSDataType dataType : dataTypes) {
      stream.write(dataType.ordinal());
    }
    for (TSEncoding encoding : encodings) {
      stream.write(encoding.ordinal());
    }
    stream.write(compressor.ordinal());

    // alias
    if (aliasList != null) {
      stream.write(1);
      for (String alias : aliasList) {
        ReadWriteIOUtils.write(alias, stream);
      }
    } else {
      stream.write(0);
    }
    stream.writeLong(index);
    stream.writeLong(prefixPath.getMajorVersion());
    stream.writeLong(prefixPath.getMinorVersion());
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_ALIGNED_TIMESERIES.ordinal());
    byte[] bytes = prefixPath.getFullPath().getBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);

    ReadWriteIOUtils.write(measurements.size(), buffer);
    for (String measurement : measurements) {
      ReadWriteIOUtils.write(measurement, buffer);
    }
    for (TSDataType dataType : dataTypes) {
      buffer.put((byte) dataType.ordinal());
    }
    for (TSEncoding encoding : encodings) {
      buffer.put((byte) encoding.ordinal());
    }
    buffer.put((byte) compressor.ordinal());

    // alias
    if (aliasList != null) {
      buffer.put((byte) 1);
      for (String alias : aliasList) {
        ReadWriteIOUtils.write(alias, buffer);
      }
    } else {
      buffer.put((byte) 0);
    }

    buffer.putLong(index);
    buffer.putLong(prefixPath.getMajorVersion());
    buffer.putLong(prefixPath.getMinorVersion());
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);

    prefixPath = new PartialPath(new String(bytes));
    int size = ReadWriteIOUtils.readInt(buffer);
    measurements = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      measurements.add(ReadWriteIOUtils.readString(buffer));
    }
    dataTypes = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      dataTypes.add(TSDataType.values()[buffer.get()]);
    }
    encodings = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      encodings.add(TSEncoding.values()[buffer.get()]);
    }
    compressor = CompressionType.values()[buffer.get()];

    // alias
    if (buffer.get() == 1) {
      aliasList = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        aliasList.add(ReadWriteIOUtils.readString(buffer));
      }
    }

    this.index = buffer.getLong();
    prefixPath.setMajorVersion(buffer.getLong());
    prefixPath.setMinorVersion(buffer.getLong());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateAlignedTimeSeriesPlan that = (CreateAlignedTimeSeriesPlan) o;

    return Objects.equals(prefixPath, that.prefixPath)
        && Objects.equals(measurements, that.measurements)
        && Objects.equals(dataTypes, that.dataTypes)
        && Objects.equals(encodings, that.encodings)
        && compressor == that.compressor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(prefixPath, measurements, dataTypes, encodings, compressor);
  }
}
