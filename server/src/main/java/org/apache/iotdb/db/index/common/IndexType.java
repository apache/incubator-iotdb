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

import org.apache.iotdb.db.exception.index.UnsupportedIndexTypeException;
import org.apache.iotdb.db.index.algorithm.IoTDBIndex;
import org.apache.iotdb.db.index.algorithm.NoIndex;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public enum IndexType {
  NO_INDEX,
  RTREE_PAA,
  ELB_INDEX;

  /**
   * judge the index type.
   *
   * @param i an integer used to determine index type
   * @return index type
   */
  public static IndexType deserialize(short i) {
    switch (i) {
      case 0:
        return NO_INDEX;
      case 1:
        return RTREE_PAA;
      case 2:
        return ELB_INDEX;
      default:
        throw new NotImplementedException("Given index is not implemented");
    }
  }

  /**
   * judge the index deserialize type.
   *
   * @return the integer used to determine index type
   */
  public short serialize() {
    switch (this) {
      case NO_INDEX:
        return 0;
      case RTREE_PAA:
        return 1;
      case ELB_INDEX:
        return 2;
      default:
        throw new NotImplementedException("Given index is not implemented");
    }
  }

  public static IndexType getIndexType(String indexTypeString)
      throws UnsupportedIndexTypeException {
    String normalized = indexTypeString.toUpperCase();
    try {
      return IndexType.valueOf(normalized);
    } catch (IllegalArgumentException e) {
      throw new UnsupportedIndexTypeException(indexTypeString);
    }
  }

  private static IoTDBIndex newIndexByType(
      PartialPath path,
      TSDataType tsDataType,
      String indexDir,
      IndexType indexType,
      IndexInfo indexInfo) {
    switch (indexType) {
      case NO_INDEX:
        return new NoIndex(path, tsDataType, indexInfo);
      case ELB_INDEX:
        //        return new ELBIndex(path, tsDataType, indexDir, indexInfo);
      case RTREE_PAA:
        //        return new RTreePAAIndex(path, tsDataType, indexDir, indexInfo);
      default:
        throw new NotImplementedException("unsupported index type:" + indexType);
    }
  }

  public static IoTDBIndex constructIndex(
      PartialPath indexSeries,
      TSDataType tsDataType,
      String indexDir,
      IndexType indexType,
      IndexInfo indexInfo,
      ByteBuffer previous) {
    indexInfo.setProps(uppercaseStringProps(indexInfo.getProps()));
    IoTDBIndex index = newIndexByType(indexSeries, tsDataType, indexDir, indexType, indexInfo);
    index.initFeatureExtractor(previous, false);
    return index;
  }

  private static Map<String, String> uppercaseStringProps(Map<String, String> props) {
    Map<String, String> uppercase = new HashMap<>(props.size());
    props.forEach((k, v) -> uppercase.put(k.toUpperCase(), v));
    return uppercase;
  }
}
