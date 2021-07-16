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
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator;
import org.apache.iotdb.db.qp.logical.sys.AlterTimeSeriesOperator.AlterType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AlterTimeSeriesPlan extends PhysicalPlan {

  private static final Logger logger = LoggerFactory.getLogger(AlterTimeSeriesPlan.class);

  private PartialPath path;
  private AlterType alterType;

  /**
   * used when the alterType is RENAME, SET, DROP, ADD_TAGS, ADD_ATTRIBUTES. when the alterType is
   * RENAME, alterMap has only one entry, key is the beforeName, value is the currentName. when the
   * alterType is DROP, only the keySet of alterMap is useful, it contains all the key names needed
   * to be removed
   */
  private Map<String, String> alterMap;

  /** used when the alterType is UPSERT */
  private String alias;

  private Map<String, String> tagsMap;
  private Map<String, String> attributesMap;

  /** used only for deserialize */
  public AlterTimeSeriesPlan() {
    super(false, OperatorType.ALTER_TIMESERIES);
  }

  public AlterTimeSeriesPlan(
      PartialPath path,
      AlterType alterType,
      Map<String, String> alterMap,
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap) {
    super(false, Operator.OperatorType.ALTER_TIMESERIES);
    this.path = path;
    this.alterType = alterType;
    this.alterMap = alterMap;
    this.alias = alias;
    this.tagsMap = tagsMap;
    this.attributesMap = attributesMap;
  }

  public PartialPath getPath() {
    return path;
  }

  public AlterTimeSeriesOperator.AlterType getAlterType() {
    return alterType;
  }

  public Map<String, String> getAlterMap() {
    return alterMap;
  }

  public String getAlias() {
    return alias;
  }

  public Map<String, String> getTagsMap() {
    return tagsMap;
  }

  public Map<String, String> getAttributesMap() {
    return attributesMap;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(path);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeByte((byte) PhysicalPlanType.ALTER_TIMESERIES.ordinal());
    byte[] bytes = path.getFullPath().getBytes();
    stream.writeInt(bytes.length);
    stream.write(bytes);

    stream.write(alterType.ordinal());

    // alias
    if (alias != null) {
      stream.write(1);
      ReadWriteIOUtils.write(alias, stream);
    } else {
      stream.write(0);
    }

    // alterMap
    if (alterMap != null && !alterMap.isEmpty()) {
      stream.write(1);
      ReadWriteIOUtils.write(alterMap, stream);
    } else {
      stream.write(0);
    }

    // tagsMap
    if (tagsMap != null && !tagsMap.isEmpty()) {
      stream.write(1);
      ReadWriteIOUtils.write(tagsMap, stream);
    } else {
      stream.write(0);
    }

    // attributesMap
    if (attributesMap != null && !attributesMap.isEmpty()) {
      stream.write(1);
      ReadWriteIOUtils.write(attributesMap, stream);
    } else {
      stream.write(0);
    }
    stream.writeLong(index);

    stream.writeLong(path.getMajorVersion());
    stream.writeLong(path.getMinorVersion());
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    int length = buffer.getInt();
    byte[] bytes = new byte[length];
    buffer.get(bytes);
    try {
      path = new PartialPath(new String(bytes));
    } catch (IllegalPathException e) {
      logger.error("Illegal path in plan deserialization:", e);
    }

    alterType = AlterType.values()[buffer.get()];

    // alias
    if (buffer.get() == 1) {
      alias = ReadWriteIOUtils.readString(buffer);
    }

    // alterMap
    if (buffer.get() == 1) {
      alterMap = ReadWriteIOUtils.readMap(buffer);
    }

    // tagsMap
    if (buffer.get() == 1) {
      tagsMap = ReadWriteIOUtils.readMap(buffer);
    }

    // attributesMap
    if (buffer.get() == 1) {
      attributesMap = ReadWriteIOUtils.readMap(buffer);
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

    AlterTimeSeriesPlan that = (AlterTimeSeriesPlan) o;

    return Objects.equals(path, that.path)
        && alterType == that.alterType
        && Objects.equals(alterMap, that.alterMap)
        && Objects.equals(alias, that.alias)
        && Objects.equals(tagsMap, that.tagsMap)
        && Objects.equals(attributesMap, that.attributesMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, alias, alterType, alterMap, attributesMap, tagsMap);
  }

  @Override
  public String toString() {
    return "AlterTimeSeriesPlan{"
        + " path="
        + path
        + ", alterType="
        + alterType
        + ", alterMap="
        + alterMap
        + ", alias='"
        + alias
        + '\''
        + ", tagsMap="
        + tagsMap
        + ", attributesMap="
        + attributesMap
        + "}";
  }
}
