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

import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface IMeasurementSchema {

  String getMeasurementId();

  CompressionType getCompressor();

  TSEncoding getEncodingType();

  TSDataType getType();

  void setType(TSDataType dataType);

  TSEncoding getTimeTSEncoding();

  Encoder getTimeEncoder();

  Encoder getValueEncoder();

  Map<String, String> getProps();

  List<String> getValueMeasurementIdList();

  List<TSDataType> getValueTSDataTypeList();

  List<TSEncoding> getValueTSEncodingList();

  List<Encoder> getValueEncoderList();

  int getMeasurementIdColumnIndex(String measurementId);

  int getMeasurementCount();

  /* test whether the schema contains Measurement with given measurementId */
  boolean isCompatible(String measurementId);

  int serializeTo(ByteBuffer buffer);

  int serializeTo(OutputStream outputStream) throws IOException;

  /*
   1. used in cluster module to avoid useless field transfer(such as props in MeasurementSchema)
   2. add a flag bit at the beginning to distinguish between MeasurementSchema(0) and VectorMeasurementSchema(1)
  */
  int partialSerializeTo(ByteBuffer buffer);

  /*
   1. used in cluster module to avoid useless field transfer(such as props in MeasurementSchema)
   2. add a flag bit at the beginning to distinguish between MeasurementSchema(0) and VectorMeasurementSchema(1)
  */
  int partialSerializeTo(OutputStream outputStream) throws IOException;
}
