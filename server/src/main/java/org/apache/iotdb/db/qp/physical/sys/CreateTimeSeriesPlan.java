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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class CreateTimeSeriesPlan extends PhysicalPlan {

  private PartialPath path;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressor;
  private String alias;
  private Map<String, String> props = null;
  private Map<String, String> tags = null;
  private Map<String, String> attributes = null;
  private long tagOffset = -1;

  public CreateTimeSeriesPlan() {
    super(false, Operator.OperatorType.CREATE_TIMESERIES);
    canBeSplit = false;
  }

  public CreateTimeSeriesPlan(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String alias) {
    super(false, Operator.OperatorType.CREATE_TIMESERIES);
    this.path = path;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    this.tags = tags;
    this.attributes = attributes;
    this.alias = alias;
    canBeSplit = false;
    if (props != null) {
      this.props = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      this.props.putAll(props);
    }
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public void setCompressor(CompressionType compressor) {
    this.compressor = compressor;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public long getTagOffset() {
    return tagOffset;
  }

  public void setTagOffset(long tagOffset) {
    this.tagOffset = tagOffset;
  }

  @Override
  public String toString() {
    return String.format(
        "seriesPath: %s, resultDataType: %s, encoding: %s, compression: %s, tagOffset: %s",
        path, dataType, encoding, compressor, tagOffset);
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(path);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.CREATE_TIMESERIES.ordinal());
    byte[] bytes = path.getFullPath().getBytes();
    stream.writeInt(bytes.length);
    stream.write(bytes);
    stream.write(dataType.ordinal());
    stream.write(encoding.ordinal());
    stream.write(compressor.ordinal());
    stream.writeLong(tagOffset);

    // alias
    if (alias != null) {
      stream.write(1);
      ReadWriteIOUtils.write(alias, stream);
    } else {
      stream.write(0);
    }

    // props
    if (props != null && !props.isEmpty()) {
      stream.write(1);
      ReadWriteIOUtils.write(props, stream);
    } else {
      stream.write(0);
    }

    // tags
    if (tags != null && !tags.isEmpty()) {
      stream.write(1);
      ReadWriteIOUtils.write(tags, stream);
    } else {
      stream.write(0);
    }

    // attributes
    if (attributes != null && !attributes.isEmpty()) {
      stream.write(1);
      ReadWriteIOUtils.write(attributes, stream);
    } else {
      stream.write(0);
    }

    stream.writeLong(index);
    stream.writeLong(path.getMajorVersion());
    stream.writeLong(path.getMinorVersion());
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.CREATE_TIMESERIES.ordinal());
    byte[] bytes = path.getFullPath().getBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);
    buffer.put((byte) dataType.ordinal());
    buffer.put((byte) encoding.ordinal());
    buffer.put((byte) compressor.ordinal());
    buffer.putLong(tagOffset);

    // alias
    if (alias != null) {
      buffer.put((byte) 1);
      ReadWriteIOUtils.write(alias, buffer);
    } else {
      buffer.put((byte) 0);
    }

    // props
    if (props != null && !props.isEmpty()) {
      buffer.put((byte) 1);
      ReadWriteIOUtils.write(props, buffer);
    } else {
      buffer.put((byte) 0);
    }

    // tags
    if (tags != null && !tags.isEmpty()) {
      buffer.put((byte) 1);
      ReadWriteIOUtils.write(tags, buffer);
    } else {
      buffer.put((byte) 0);
    }

    // attributes
    if (attributes != null && !attributes.isEmpty()) {
      buffer.put((byte) 1);
      ReadWriteIOUtils.write(attributes, buffer);
    } else {
      buffer.put((byte) 0);
    }

    buffer.putLong(index);

    buffer.putLong(path.getMajorVersion());
    buffer.putLong(path.getMinorVersion());
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    path = new PartialPath(new String(bytes));
    dataType = TSDataType.values()[buffer.get()];
    encoding = TSEncoding.values()[buffer.get()];
    compressor = CompressionType.values()[buffer.get()];
    tagOffset = buffer.getLong();

    // alias
    if (buffer.get() == 1) {
      alias = ReadWriteIOUtils.readString(buffer);
    }

    // props
    if (buffer.get() == 1) {
      props = ReadWriteIOUtils.readMap(buffer);
    }

    // tags
    if (buffer.get() == 1) {
      tags = ReadWriteIOUtils.readMap(buffer);
    }

    // attributes
    if (buffer.get() == 1) {
      attributes = ReadWriteIOUtils.readMap(buffer);
    }

    this.index = buffer.getLong();

    path.setMajorVersion(buffer.getLong());
    path.setMinorVersion(buffer.getLong());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateTimeSeriesPlan that = (CreateTimeSeriesPlan) o;

    return Objects.equals(path, that.path)
        && dataType == that.dataType
        && encoding == that.encoding
        && compressor == that.compressor
        && tagOffset == that.tagOffset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, dataType, encoding, compressor, tagOffset);
  }
}
