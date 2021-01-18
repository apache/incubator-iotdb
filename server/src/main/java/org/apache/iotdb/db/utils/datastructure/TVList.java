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

package org.apache.iotdb.db.utils.datastructure;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.rescon.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class TVList {

  protected static final int SMALL_ARRAY_LENGTH = 32;
  private static final String ERR_DATATYPE_NOT_CONSISTENT = "DataType not consistent";
  protected List<long[]> timestamps;
  protected int size;

  protected long[][] sortedTimestamps;
  protected boolean sorted = true;
  // record reference count of this tv list
  // currently this reference will only be increase because we can't know when to decrease it
  protected AtomicInteger referenceCount;
  protected long pivotTime;
  protected long minTime;

  private long version;

  public TVList() {
    timestamps = new ArrayList<>();
    size = 0;
    minTime = Long.MAX_VALUE;
    referenceCount = new AtomicInteger();
  }

  public static TVList newList(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
        return new BinaryTVList();
      case FLOAT:
        return new FloatTVList();
      case INT32:
        return new IntTVList();
      case INT64:
        return new LongTVList();
      case DOUBLE:
        return new DoubleTVList();
      case BOOLEAN:
        return new BooleanTVList();
      default:
        break;
    }
    return null;
  }

  public static long tvListArrayMemSize(TSDataType type) {
    long size = 0;
    // time size
    size += (long) PrimitiveArrayManager.ARRAY_SIZE * 8L;
    // value size
    size += (long) PrimitiveArrayManager.ARRAY_SIZE * (long) type.getDataTypeSize();
    return size;
  }

  public boolean isSorted() {
    return sorted;
  }

  public void increaseReferenceCount() {
    referenceCount.incrementAndGet();
  }

  public int getReferenceCount() {
    return referenceCount.get();
  }

  public int size() {
    return size;
  }

  public long getTime(int index) {
    if (index >= size) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return timestamps.get(arrayIndex)[elementIndex];
  }

  public void putLong(long time, long value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putInt(long time, int value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putFloat(long time, float value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putDouble(long time, double value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBinary(long time, Binary value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBoolean(long time, boolean value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putLongs(long[] time, long[] value, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putInts(long[] time, int[] value, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putFloats(long[] time, float[] value, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putDoubles(long[] time, double[] value, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBinaries(long[] time, Binary[] value, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBooleans(long[] time, boolean[] value, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public int getInt(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public float getFloat(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public double getDouble(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public Binary getBinary(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public boolean getBoolean(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public abstract void sort();

  public long getMinTime() {
    return minTime;
  }

  public long getVersion() {
    return version;
  }

  protected abstract void set(int src, int dest);

  protected abstract void setFromSorted(int src, int dest);

  protected abstract void setToSorted(int src, int dest);

  protected abstract void reverseRange(int lo, int hi);

  protected abstract void expandValues();

  public abstract TVList clone();

  public TVList clone(long version) {
    this.version = version;
    return clone();
  }

  protected abstract void releaseLastValueArray();

  protected void releaseLastTimeArray() {
    PrimitiveArrayManager.release(timestamps.remove(timestamps.size() - 1));
  }

  public int delete(long lowerBound, long upperBound) {
    int newSize = 0;
    minTime = Long.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      long time = getTime(i);
      if (time < lowerBound || time > upperBound) {
        set(i, newSize++);
        minTime = Math.min(time, minTime);
      }
    }
    int deletedNumber = size - newSize;
    size = newSize;
    // release primitive arrays that are empty
    int newArrayNum = newSize / ARRAY_SIZE;
    if (newSize % ARRAY_SIZE != 0) {
      newArrayNum++;
    }
    for (int releaseIdx = newArrayNum; releaseIdx < timestamps.size(); releaseIdx++) {
      releaseLastTimeArray();
      releaseLastValueArray();
    }
    return deletedNumber;
  }

  protected void cloneAs(TVList cloneList) {
    for (long[] timestampArray : timestamps) {
      cloneList.timestamps.add(cloneTime(timestampArray));
    }
    cloneList.size = size;
    cloneList.sorted = sorted;
    cloneList.minTime = minTime;
  }

  public void clear() {
    size = 0;
    sorted = true;
    minTime = Long.MAX_VALUE;
    clearTime();
    clearSortedTime();

    clearValue();
    clearSortedValue();
  }

  protected void clearTime() {
    if (timestamps != null) {
      for (long[] dataArray : timestamps) {
        PrimitiveArrayManager.release(dataArray);
      }
      timestamps.clear();
    }
  }

  protected void clearSortedTime() {
    if (sortedTimestamps != null) {
      sortedTimestamps = null;
    }
  }

  abstract void clearValue();

  /**
   * The arrays for sorting are not including in write memory now,
   * the memory usage is considered as temporary memory.
   */
  abstract void clearSortedValue();

  protected void checkExpansion() {
    if ((size % ARRAY_SIZE) == 0) {
      expandValues();
      timestamps.add((long[]) getPrimitiveArraysByType(TSDataType.INT64));
    }
  }

  protected Object getPrimitiveArraysByType(TSDataType dataType) {
    return PrimitiveArrayManager.getPrimitiveArraysByType(dataType);
  }

  protected long[] cloneTime(long[] array) {
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  protected void sort(int lo, int hi) {
    if (sorted) {
      return;
    }

    if (lo == hi) {
      return;
    }
    if (hi - lo <= SMALL_ARRAY_LENGTH) {
      int initRunLen = countRunAndMakeAscending(lo, hi);
      binarySort(lo, hi, lo + initRunLen);
      return;
    }
    int mid = (lo + hi) >>> 1;
    sort(lo, mid);
    sort(mid, hi);
    merge(lo, mid, hi);
  }

  protected int countRunAndMakeAscending(int lo, int hi) {
    assert lo < hi;
    int runHi = lo + 1;
    if (runHi == hi) {
      return 1;
    }

    // Find end of run, and reverse range if descending
    if (getTime(runHi++) < getTime(lo)) { // Descending
      while (runHi < hi && getTime(runHi) < getTime(runHi - 1)) {
        runHi++;
      }
      reverseRange(lo, runHi);
    } else {                              // Ascending
      while (runHi < hi && getTime(runHi) >= getTime(runHi - 1)) {
        runHi++;
      }
    }

    return runHi - lo;
  }

  protected int compare(int idx1, int idx2) {
    long t1 = getTime(idx1);
    long t2 = getTime(idx2);
    return Long.compare(t1, t2);
  }

  protected abstract void saveAsPivot(int pos);

  protected abstract void setPivotTo(int pos);

  /**
   * From TimSort.java
   */
  protected void binarySort(int lo, int hi, int start) {
    assert lo <= start && start <= hi;
    if (start == lo) {
      start++;
    }
    for (; start < hi; start++) {

      saveAsPivot(start);
      // Set left (and right) to the index where a[start] (pivot) belongs
      int left = lo;
      int right = start;
      assert left <= right;
      /*
       * Invariants:
       *   pivot >= all in [lo, left).
       *   pivot <  all in [right, start).
       */
      while (left < right) {
        int mid = (left + right) >>> 1;
        if (compare(start, mid) < 0) {
          right = mid;
        } else {
          left = mid + 1;
        }
      }
      assert left == right;

      /*
       * The invariants still hold: pivot >= all in [lo, left) and
       * pivot < all in [left, start), so pivot belongs at left.  Note
       * that if there are elements equal to pivot, left points to the
       * first slot after them -- that's why this sort is stable.
       * Slide elements over to make room for pivot.
       */
      int n = start - left;  // The number of elements to move
      for (int i = n; i >= 1; i--) {
        set(left + i - 1, left + i);
      }
      setPivotTo(left);
    }
    for (int i = lo; i < hi; i++) {
      setToSorted(i, i);
    }
  }

  protected void merge(int lo, int mid, int hi) {
    // end of sorting buffer
    int tmpIdx = 0;

    // start of unmerged parts of each sequence
    int leftIdx = lo;
    int rightIdx = mid;

    // copy the minimum elements to sorting buffer until one sequence is exhausted
    int endSide = 0;
    while (endSide == 0) {
      if (compare(leftIdx, rightIdx) <= 0) {
        setToSorted(leftIdx, lo + tmpIdx);
        tmpIdx++;
        leftIdx++;
        if (leftIdx == mid) {
          endSide = 1;
        }
      } else {
        setToSorted(rightIdx, lo + tmpIdx);
        tmpIdx++;
        rightIdx++;
        if (rightIdx == hi) {
          endSide = 2;
        }
      }
    }

    // copy the remaining elements of another sequence
    int start;
    int end;
    if (endSide == 1) {
      start = rightIdx;
      end = hi;
    } else {
      start = leftIdx;
      end = mid;
    }
    for (; start < end; start++) {
      setToSorted(start, lo + tmpIdx);
      tmpIdx++;
    }

    // copy from sorting buffer to the original arrays so that they can be further sorted
    // potential speed up: change the place of sorting buffer and origin data between mergeUnseq
    // iterations
    for (int i = lo; i < hi; i++) {
      setFromSorted(i, i);
    }
  }

  void updateMinTimeAndSorted(long[] time) {
    updateMinTimeAndSorted(time, 0, time.length);
  }

  void updateMinTimeAndSorted(long[] time, int start, int end) {
    int length = time.length;
    long inPutMinTime = Long.MAX_VALUE;
    boolean inputSorted = true;
    for (int i = start; i < end; i++) {
      inPutMinTime = Math.min(inPutMinTime, time[i]);
      if (inputSorted && i < length - 1 && time[i] > time[i + 1]) {
        inputSorted = false;
      }
    }
    minTime = Math.min(inPutMinTime, minTime);
    sorted = sorted && inputSorted && (size == 0 || inPutMinTime >= getTime(size - 1));
  }

  /**
   * for log
   */
  public abstract TimeValuePair getTimeValuePair(int index);

  protected abstract TimeValuePair getTimeValuePair(int index, long time,
      Integer floatPrecision, TSEncoding encoding);

  @TestOnly
  public IPointReader getIterator() {
    return new Ite();
  }

  public IPointReader getIterator(int floatPrecision, TSEncoding encoding, int size,
      List<TimeRange> deletionList) {
    return new Ite(floatPrecision, encoding, size, deletionList);
  }

  private class Ite implements IPointReader {

    private TimeValuePair cachedTimeValuePair;
    private boolean hasCachedPair;
    private int cur;
    private Integer floatPrecision;
    private TSEncoding encoding;
    private int deleteCursor = 0;
    /**
     * because TV list may be share with different query, each iterator has to record it's own size
     */
    private int iteSize = 0;
    /**
     * this field is effective only in the Tvlist in a RealOnlyMemChunk.
     */
    private List<TimeRange> deletionList;

    public Ite() {
      this.iteSize = TVList.this.size;
    }

    public Ite(int floatPrecision, TSEncoding encoding, int size, List<TimeRange> deletionList) {
      this.floatPrecision = floatPrecision;
      this.encoding = encoding;
      this.iteSize = size;
      this.deletionList = deletionList;
    }

    @Override
    public boolean hasNextTimeValuePair() {
      if (hasCachedPair) {
        return true;
      }

      while (cur < iteSize) {
        long time = getTime(cur);
        if (isPointDeleted(time) || (cur + 1 < size() && (time == getTime(cur + 1)))) {
          cur++;
          continue;
        }
        cachedTimeValuePair = getTimeValuePair(cur, time, floatPrecision, encoding);
        hasCachedPair = true;
        cur++;
        return true;
      }

      return false;
    }

    private boolean isPointDeleted(long timestamp) {
      while (deletionList != null && deleteCursor < deletionList.size()) {
        if (deletionList.get(deleteCursor).contains(timestamp)) {
          return true;
        } else if (deletionList.get(deleteCursor).getMax() < timestamp) {
          deleteCursor++;
        } else {
          return false;
        }
      }
      return false;
    }

    @Override
    public TimeValuePair nextTimeValuePair() throws IOException {
      if (hasCachedPair || hasNextTimeValuePair()) {
        hasCachedPair = false;
        return cachedTimeValuePair;
      } else {
        throw new IOException("no next time value pair");
      }
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      return cachedTimeValuePair;
    }

    @Override
    public void close() throws IOException {
      // Do nothing because of this is an in memory object
    }
  }

  public abstract TSDataType getDataType();

  public long getLastTime() {
    return getTime(size - 1);
  }

}
