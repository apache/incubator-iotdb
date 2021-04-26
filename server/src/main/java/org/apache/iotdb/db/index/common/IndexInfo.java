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
package org.apache.iotdb.db.index.common;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class IndexInfo implements Cloneable {

  private Map<String, String> props;
  private long time;
  private IndexType indexType;

  public IndexInfo(IndexType indexType, long time, Map<String, String> props) {
    this.props = props;
    this.time = time;
    this.indexType = indexType;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(indexType.serialize(), outputStream);
    ReadWriteIOUtils.write(time, outputStream);
    ReadWriteIOUtils.write(props, outputStream);
  }

  public static IndexInfo deserialize(InputStream inputStream) throws IOException {
    short indexTypeShort = ReadWriteIOUtils.readShort(inputStream);
    IndexType indexType = IndexType.deserialize(indexTypeShort);
    long time = ReadWriteIOUtils.readLong(inputStream);
    Map<String, String> indexProps = ReadWriteIOUtils.readMap(inputStream);
    return new IndexInfo(indexType, time, indexProps);
  }

  @Override
  public String toString() {
    return String.format("[type: %s, time: %d, props: %s]", indexType, time, props);
  }

  @Override
  public Object clone() {
    return new IndexInfo(indexType, time, new HashMap<>(props));
  }
}
