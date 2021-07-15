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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.flink.options;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.Serializable;
import java.util.List;

/** IoTDBOptions describes the configuration related information for IoTDB and timeseries. */
public class IoTDBSinkOptions extends IoTDBOptions {

  private List<TimeseriesOption> timeseriesOptionList;

  public IoTDBSinkOptions() {}

  public IoTDBSinkOptions(
      String host,
      int port,
      String user,
      String password,
      List<TimeseriesOption> timeseriesOptionList) {
    super(host, port, user, password);
    this.timeseriesOptionList = timeseriesOptionList;
  }

  public List<TimeseriesOption> getTimeseriesOptionList() {
    return timeseriesOptionList;
  }

  public void setTimeseriesOptionList(List<TimeseriesOption> timeseriesOptionList) {
    this.timeseriesOptionList = timeseriesOptionList;
  }

  public static class TimeseriesOption implements Serializable {

    private String path;
    private TSDataType dataType = TSDataType.TEXT;
    private TSEncoding encoding = TSEncoding.PLAIN;
    private CompressionType compressor = CompressionType.SNAPPY;

    public TimeseriesOption() {}

    public TimeseriesOption(String path) {
      this.path = path;
    }

    public TimeseriesOption(
        String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor) {
      this.path = path;
      this.dataType = dataType;
      this.encoding = encoding;
      this.compressor = compressor;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public TSDataType getDataType() {
      return dataType;
    }

    public void setDataType(TSDataType dataType) {
      this.dataType = dataType;
    }

    public TSEncoding getEncoding() {
      return encoding;
    }

    public void setEncoding(TSEncoding encoding) {
      this.encoding = encoding;
    }

    public CompressionType getCompressor() {
      return compressor;
    }

    public void setCompressor(CompressionType compressor) {
      this.compressor = compressor;
    }
  }
}
