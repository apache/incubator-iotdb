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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.List;

public interface IWritableMemChunk {

  void putLong(long t, long v);

  void putInt(long t, int v);

  void putFloat(long t, float v);

  void putDouble(long t, double v);

  void putBinary(long t, Binary v);

  void putBoolean(long t, boolean v);

  void putVector(long t, Object[] v);

  void putLongs(long[] t, BitMap bitMap, long[] v, int start, int end);

  void putInts(long[] t, BitMap bitMap, int[] v, int start, int end);

  void putFloats(long[] t, BitMap bitMap, float[] v, int start, int end);

  void putDoubles(long[] t, BitMap bitMap, double[] v, int start, int end);

  void putBinaries(long[] t, BitMap bitMap, Binary[] v, int start, int end);

  void putBooleans(long[] t, BitMap bitMap, boolean[] v, int start, int end);

  void putVectors(long[] t, BitMap[] bitMaps, Object[] v, int start, int end);

  void write(long insertTime, Object objectValue);

  /** [start, end) */
  void write(
      long[] times, Object bitMaps, Object valueList, TSDataType dataType, int start, int end);

  long count();

  IMeasurementSchema getSchema();

  /**
   * served for query requests.
   *
   * <p>if tv list has been sorted, just return reference of it
   *
   * <p>if tv list hasn't been sorted and has no reference, sort and return reference of it
   *
   * <p>if tv list hasn't been sorted and has reference we should copy and sort it, then return ths
   * list
   *
   * <p>the mechanism is just like copy on write
   *
   * @return sorted tv list
   */
  TVList getSortedTVListForQuery();

  /**
   * served for vector query requests.
   *
   * <p>the mechanism is just like copy on write
   *
   * @param columnIndexList indices of queried columns in the full VectorTVList
   * @return sorted tv list
   */
  TVList getSortedTVListForQuery(List<Integer> columnIndexList);

  /**
   * served for flush requests. The logic is just same as getSortedTVListForQuery, but without add
   * reference count
   *
   * @return sorted tv list
   */
  TVList getSortedTVListForFlush();

  default TVList getTVList() {
    return null;
  }

  default long getMinTime() {
    return Long.MIN_VALUE;
  }

  /** @return how many points are deleted */
  int delete(long lowerBound, long upperBound);

  // For delete one column in the vector
  int delete(long lowerBound, long upperBound, int columnIndex);
}
