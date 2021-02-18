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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

public class AlterTimeSeriesBasicInfoPlan extends PhysicalPlan {

  public void setPath(PartialPath path) {
    this.path = path;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public TSEncoding getEncodingType() {
    return encodingType;
  }

  public void setEncodingType(TSEncoding encodingType) {
    this.encodingType = encodingType;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public void setCompressor(CompressionType compressor) {
    this.compressor = compressor;
  }

  private PartialPath path;
  private TSDataType dataType;
  private TSEncoding encodingType;
  private CompressionType compressor;

  public AlterTimeSeriesBasicInfoPlan() {
    super(false, Operator.OperatorType.ALTER_TIMESERIES_BASIC_INFO);
  }

  public AlterTimeSeriesBasicInfoPlan(PartialPath path,
      TSDataType dataType, TSEncoding encodingType, CompressionType compressor) {
    super(false, Operator.OperatorType.ALTER_TIMESERIES_BASIC_INFO);
    this.path = path;
    this.dataType = dataType;
    this.encodingType = encodingType;
    this.compressor = compressor;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  public PartialPath getPath() {
    return this.path;
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) PhysicalPlanType.ALTER_TIMESERIES_BASIC_INFO.ordinal());
    byte[] bytes = path.getFullPath().getBytes();
    buffer.putInt(bytes.length);
    buffer.put(bytes);
    buffer.put((byte) dataType.ordinal());
    buffer.put((byte) encodingType.ordinal());
    buffer.put((byte) compressor.ordinal());

    buffer.putLong(index);
  }

  @Override
  public void deserialize(ByteBuffer buffer) throws IllegalPathException {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    path = new PartialPath(new String(bytes));
    dataType = TSDataType.values()[buffer.get()];
    encodingType = TSEncoding.values()[buffer.get()];
    compressor = CompressionType.values()[buffer.get()];

    this.index = buffer.getLong();
  }


}
