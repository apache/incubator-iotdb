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
package org.apache.iotdb.tsfile.write.schema;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.StringContainer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class describes a measurement's information registered in {@linkplain Schema FileSchema},
 * including measurement id, data type, encoding and compressor type. For each TSEncoding,
 * MeasurementSchema maintains respective TSEncodingBuilder; For TSDataType, only ENUM has
 * TSDataTypeConverter up to now.
 */
public class MeasurementSchema
    implements IMeasurementSchema, Comparable<MeasurementSchema>, Serializable {

  private String measurementId;
  private byte type;
  private byte encoding;
  private TSEncodingBuilder encodingConverter;
  private byte compressor;
  private Map<String, String> props = null;

  public MeasurementSchema() {}

  public MeasurementSchema(String measurementId, TSDataType tsDataType) {
    this(
        measurementId,
        tsDataType,
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        null);
  }

  /** set properties as an empty Map. */
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding) {
    this(
        measurementId,
        type,
        encoding,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        null);
  }

  public MeasurementSchema(
      String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType) {
    this(measurementId, type, encoding, compressionType, null);
  }

  /**
   * Constructor of MeasurementSchema.
   *
   * <p>props - information in encoding method. For RLE, Encoder.MAX_POINT_NUMBER For PLAIN,
   * Encoder.maxStringLength
   */
  public MeasurementSchema(
      String measurementId,
      TSDataType type,
      TSEncoding encoding,
      CompressionType compressionType,
      Map<String, String> props) {
    this.type = type.serialize();
    this.measurementId = measurementId;
    this.encoding = encoding.serialize();
    this.props = props;
    this.compressor = compressionType.serialize();
  }

  public MeasurementSchema(
      String measurementId,
      byte type,
      byte encoding,
      byte compressionType,
      Map<String, String> props) {
    this.type = type;
    this.measurementId = measurementId;
    this.encoding = encoding;
    this.props = props;
    this.compressor = compressionType;
  }

  /** function for deserializing data from input stream. */
  public static MeasurementSchema deserializeFrom(InputStream inputStream) throws IOException {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(inputStream);

    measurementSchema.type = ReadWriteIOUtils.readByte(inputStream);

    measurementSchema.encoding = ReadWriteIOUtils.readByte(inputStream);

    measurementSchema.compressor = ReadWriteIOUtils.readByte(inputStream);

    int size = ReadWriteIOUtils.readInt(inputStream);
    if (size > 0) {
      measurementSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(inputStream);
        value = ReadWriteIOUtils.readString(inputStream);
        measurementSchema.props.put(key, value);
      }
    }

    return measurementSchema;
  }

  /** function for deserializing data from byte buffer. */
  public static MeasurementSchema deserializeFrom(ByteBuffer buffer) {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(buffer);

    measurementSchema.type = ReadWriteIOUtils.readByte(buffer);

    measurementSchema.encoding = ReadWriteIOUtils.readByte(buffer);

    measurementSchema.compressor = ReadWriteIOUtils.readByte(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      measurementSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = ReadWriteIOUtils.readString(buffer);
        measurementSchema.props.put(key, value);
      }
    }

    return measurementSchema;
  }

  public static MeasurementSchema partialDeserializeFrom(ByteBuffer buffer) {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(buffer);

    measurementSchema.type = ReadWriteIOUtils.readByte(buffer);

    measurementSchema.encoding = ReadWriteIOUtils.readByte(buffer);

    measurementSchema.compressor = ReadWriteIOUtils.readByte(buffer);

    return measurementSchema;
  }

  @Override
  public String getMeasurementId() {
    return measurementId;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
  }

  @Override
  public Map<String, String> getProps() {
    return props;
  }

  @Override
  public TSEncoding getEncodingType() {
    return TSEncoding.deserialize(encoding);
  }

  @Override
  public TSDataType getType() {
    return TSDataType.deserialize(type);
  }

  @Override
  public TSEncoding getTimeTSEncoding() {
    return TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  /** function for getting time encoder. */
  @Override
  public Encoder getTimeEncoder() {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType =
        TSDataType.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType());
    return TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType);
  }

  @Override
  public List<String> getValueMeasurementIdList() {
    throw new UnsupportedOperationException("unsupported method for MeasurementSchema");
  }

  @Override
  public List<TSDataType> getValueTSDataTypeList() {
    throw new UnsupportedOperationException("unsupported method for MeasurementSchema");
  }

  @Override
  public List<TSEncoding> getValueTSEncodingList() {
    throw new UnsupportedOperationException("unsupported method for MeasurementSchema");
  }

  @Override
  public List<Encoder> getValueEncoderList() {
    throw new UnsupportedOperationException("unsupported method for MeasurementSchema");
  }

  /**
   * get Encoder of value from encodingConverter by measurementID and data type.
   *
   * @return Encoder for value
   */
  public Encoder getValueEncoder() {
    // it is ok even if encodingConverter is constructed two instances for concurrent scenario
    if (encodingConverter == null) {
      // initialize TSEncoding. e.g. set max error for PLA and SDT
      encodingConverter = TSEncodingBuilder.getEncodingBuilder(TSEncoding.deserialize(encoding));
      encodingConverter.initFromProps(props);
    }
    return encodingConverter.getEncoder(TSDataType.deserialize(type));
  }

  @Override
  public CompressionType getCompressor() {
    return CompressionType.deserialize(compressor);
  }

  /** function for serializing data to output stream. */
  @Override
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, outputStream);

    byteLen += ReadWriteIOUtils.write(type, outputStream);

    byteLen += ReadWriteIOUtils.write(encoding, outputStream);

    byteLen += ReadWriteIOUtils.write(compressor, outputStream);

    if (props == null) {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(props.size(), outputStream);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
        byteLen += ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
    }

    return byteLen;
  }

  /** function for serializing data to byte buffer. */
  @Override
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, buffer);

    byteLen += ReadWriteIOUtils.write(type, buffer);

    byteLen += ReadWriteIOUtils.write(encoding, buffer);

    byteLen += ReadWriteIOUtils.write(compressor, buffer);

    if (props == null) {
      byteLen += ReadWriteIOUtils.write(0, buffer);
    } else {
      byteLen += ReadWriteIOUtils.write(props.size(), buffer);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
        byteLen += ReadWriteIOUtils.write(entry.getValue(), buffer);
      }
    }

    return byteLen;
  }

  @Override
  public int partialSerializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write((byte) 0, outputStream);
    byteLen += ReadWriteIOUtils.write(measurementId, outputStream);
    byteLen += ReadWriteIOUtils.write(type, outputStream);
    byteLen += ReadWriteIOUtils.write(encoding, outputStream);
    byteLen += ReadWriteIOUtils.write(compressor, outputStream);

    return byteLen;
  }

  @Override
  public int partialSerializeTo(ByteBuffer buffer) {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write((byte) 0, buffer);
    byteLen += ReadWriteIOUtils.write(measurementId, buffer);
    byteLen += ReadWriteIOUtils.write(type, buffer);
    byteLen += ReadWriteIOUtils.write(encoding, buffer);
    byteLen += ReadWriteIOUtils.write(compressor, buffer);

    return byteLen;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MeasurementSchema that = (MeasurementSchema) o;
    return type == that.type
        && encoding == that.encoding
        && Objects.equals(measurementId, that.measurementId)
        && Objects.equals(compressor, that.compressor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, encoding, measurementId, compressor);
  }

  /** compare by measurementID. */
  @Override
  public int compareTo(MeasurementSchema o) {
    if (equals(o)) {
      return 0;
    } else {
      return this.measurementId.compareTo(o.measurementId);
    }
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer("");
    sc.addTail(
        "[",
        measurementId,
        ",",
        TSDataType.deserialize(type).toString(),
        ",",
        TSEncoding.deserialize(encoding).toString(),
        ",",
        props == null ? "" : props.toString(),
        ",",
        CompressionType.deserialize(compressor).toString());
    sc.addTail("]");
    return sc.toString();
  }

  public void setType(TSDataType type) {
    this.type = type.serialize();
  }

  @Override
  public int getMeasurementIdColumnIndex(String measurementId) {
    return this.measurementId.equals(measurementId) ? 0 : -1;
  }

  @Override
  public int getMeasurementCount() {
    return 1;
  }

  @Override
  public boolean isCompatible(String measurementId) {
    return this.measurementId.equals(measurementId);
  }
}
