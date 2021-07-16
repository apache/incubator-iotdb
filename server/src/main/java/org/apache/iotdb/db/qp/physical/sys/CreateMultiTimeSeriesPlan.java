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
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.BatchPlan;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.StatusUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * create multiple timeSeries, could be split to several sub Plans to execute in different DataGroup
 */
public class CreateMultiTimeSeriesPlan extends PhysicalPlan implements BatchPlan {

  private List<PartialPath> paths;
  private List<TSDataType> dataTypes;
  private List<TSEncoding> encodings;
  private List<CompressionType> compressors;
  private List<String> alias = null;
  private List<Map<String, String>> props = null;
  private List<Map<String, String>> tags = null;
  private List<Map<String, String>> attributes = null;

  boolean[] isExecuted;

  /** record the result of creation of time series */
  private Map<Integer, TSStatus> results = new TreeMap<>();

  private List<Integer> indexes;

  public CreateMultiTimeSeriesPlan() {
    super(false, Operator.OperatorType.CREATE_MULTI_TIMESERIES);
  }

  @Override
  public List<PartialPath> getPaths() {
    return paths;
  }

  @Override
  public void setPaths(List<PartialPath> paths) {
    this.paths = paths;
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

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  public List<String> getAlias() {
    return alias;
  }

  public void setAlias(List<String> alias) {
    this.alias = alias;
  }

  public List<Map<String, String>> getProps() {
    return props;
  }

  public void setProps(List<Map<String, String>> props) {
    this.props = props;
  }

  public List<Map<String, String>> getTags() {
    return tags;
  }

  public void setTags(List<Map<String, String>> tags) {
    this.tags = tags;
  }

  public List<Map<String, String>> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<Map<String, String>> attributes) {
    this.attributes = attributes;
  }

  public List<Integer> getIndexes() {
    return indexes;
  }

  public void setIndexes(List<Integer> indexes) {
    this.indexes = indexes;
  }

  @Override
  public Map<Integer, TSStatus> getResults() {
    return results;
  }

  public TSStatus[] getFailingStatus() {
    return StatusUtils.getFailingStatus(results, paths.size());
  }

  public void setResults(Map<Integer, TSStatus> results) {
    this.results = results;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    int type = PhysicalPlanType.CREATE_MULTI_TIMESERIES.ordinal();
    stream.write(type);
    stream.writeInt(paths.size());
    stream.writeInt(dataTypes.size()); // size of datatypes, encodings for aligned timeseries

    for (PartialPath path : paths) {
      putString(stream, path.getFullPath());
      stream.writeLong(path.getMajorVersion());
      stream.writeLong(path.getMinorVersion());
    }

    for (TSDataType dataType : dataTypes) {
      stream.write(dataType.ordinal());
    }

    for (TSEncoding encoding : encodings) {
      stream.write(encoding.ordinal());
    }

    for (CompressionType compressor : compressors) {
      stream.write(compressor.ordinal());
    }

    serializeOptional(stream);

    stream.writeLong(index);
  }

  private void serializeOptional(DataOutputStream stream) throws IOException {
    if (alias != null) {
      stream.write(1);
      putStrings(stream, alias);
    } else {
      stream.write(0);
    }

    if (props != null) {
      stream.write(1);
      ReadWriteIOUtils.write(props, stream);
    } else {
      stream.write(0);
    }

    if (tags != null) {
      stream.write(1);
      ReadWriteIOUtils.write(tags, stream);
    } else {
      stream.write(0);
    }

    if (attributes != null) {
      stream.write(1);
      ReadWriteIOUtils.write(attributes, stream);
    } else {
      stream.write(0);
    }
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    int type = PhysicalPlanType.CREATE_MULTI_TIMESERIES.ordinal();
    buffer.put((byte) type);
    buffer.putInt(paths.size());
    buffer.putInt(dataTypes.size()); // size of datatypes, encodings for aligned timeseries

    for (PartialPath path : paths) {
      putString(buffer, path.getFullPath());
      buffer.putLong(path.getMajorVersion());
      buffer.putLong(path.getMinorVersion());
    }

    for (TSDataType dataType : dataTypes) {
      buffer.put((byte) dataType.ordinal());
    }

    for (TSEncoding encoding : encodings) {
      buffer.put((byte) encoding.ordinal());
    }

    for (CompressionType compressor : compressors) {
      buffer.put((byte) compressor.ordinal());
    }

    serializeOptional(buffer);

    buffer.putLong(index);
  }

  private void serializeOptional(ByteBuffer buffer) {
    if (alias != null) {
      buffer.put((byte) 1);
      putStrings(buffer, alias);
    } else {
      buffer.put((byte) 0);
    }

    if (props != null) {
      buffer.put((byte) 1);
      ReadWriteIOUtils.write(props, buffer);
    } else {
      buffer.put((byte) 0);
    }

    if (tags != null) {
      buffer.put((byte) 1);
      ReadWriteIOUtils.write(tags, buffer);
    } else {
      buffer.put((byte) 0);
    }

    if (attributes != null) {
      buffer.put((byte) 1);
      ReadWriteIOUtils.write(attributes, buffer);
    } else {
      buffer.put((byte) 0);
    }
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int totalSize = buffer.getInt();
    int dataTypeSize = buffer.getInt();
    paths = new ArrayList<>(totalSize);
    for (int i = 0; i < totalSize; i++) {
      PartialPath partialPath = new PartialPath(readString(buffer));
      partialPath.setMajorVersion(buffer.getLong());
      partialPath.setMinorVersion(buffer.getLong());
      paths.add(partialPath);
    }
    dataTypes = new ArrayList<>(totalSize);
    for (int i = 0; i < dataTypeSize; i++) {
      dataTypes.add(TSDataType.values()[buffer.get()]);
    }
    encodings = new ArrayList<>(totalSize);
    for (int i = 0; i < dataTypeSize; i++) {
      encodings.add(TSEncoding.values()[buffer.get()]);
    }
    compressors = new ArrayList<>(totalSize);
    for (int i = 0; i < totalSize; i++) {
      compressors.add(CompressionType.values()[buffer.get()]);
    }

    deserializeOptional(buffer, totalSize);

    this.index = buffer.getLong();
  }

  private void deserializeOptional(ByteBuffer buffer, int totalSize) {
    if (buffer.get() == 1) {
      alias = readStrings(buffer, totalSize);
    }

    if (buffer.get() == 1) {
      props = ReadWriteIOUtils.readMaps(buffer, totalSize);
    }

    if (buffer.get() == 1) {
      tags = ReadWriteIOUtils.readMaps(buffer, totalSize);
    }

    if (buffer.get() == 1) {
      attributes = ReadWriteIOUtils.readMaps(buffer, totalSize);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateMultiTimeSeriesPlan that = (CreateMultiTimeSeriesPlan) o;
    return Objects.equals(paths, that.paths)
        && Objects.equals(dataTypes, that.dataTypes)
        && Objects.equals(encodings, that.encodings)
        && Objects.equals(compressors, that.compressors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(paths, dataTypes, encodings, compressors);
  }

  @Override
  public void checkIntegrity() throws QueryProcessException {
    if (paths == null || paths.isEmpty()) {
      throw new QueryProcessException("sub timeseries are empty");
    }
    if (paths.size() != dataTypes.size()) {
      throw new QueryProcessException(
          String.format(
              "Measurements length [%d] does not match " + " datatype length [%d]",
              paths.size(), dataTypes.size()));
    }
    for (PartialPath path : paths) {
      if (path == null) {
        throw new QueryProcessException("Paths contain null: " + Arrays.toString(paths.toArray()));
      }
    }
  }

  @Override
  public void setIsExecuted(int i) {
    if (isExecuted == null) {
      isExecuted = new boolean[getBatchSize()];
    }
    isExecuted[i] = true;
  }

  @Override
  public boolean isExecuted(int i) {
    if (isExecuted == null) {
      isExecuted = new boolean[getBatchSize()];
    }
    return isExecuted[i];
  }

  @Override
  public int getBatchSize() {
    return paths.size();
  }

  @Override
  public void unsetIsExecuted(int i) {
    if (isExecuted == null) {
      isExecuted = new boolean[getBatchSize()];
    }
    isExecuted[i] = false;
    if (indexes != null && !indexes.isEmpty()) {
      results.remove(indexes.get(i));
    } else {
      results.remove(i);
    }
  }
}
