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

package org.apache.iotdb.db.engine.storagegroup.timeindex;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.rescon.CachedStringPool;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DeviceTimeIndex implements ITimeIndex {

  public static final int INIT_ARRAY_SIZE = 64;

  protected static final Map<String, String> cachedDevicePool =
      CachedStringPool.getInstance().getCachedPool();

  /** start times array. */
  protected long[] startTimes;

  /**
   * end times array. The values in this array are Long.MIN_VALUE if it's an unsealed sequence
   * tsfile
   */
  protected long[] endTimes;

  /** device -> index of start times array and end times array */
  protected Map<String, Integer> deviceToIndex;

  public DeviceTimeIndex() {
    this.deviceToIndex = new ConcurrentHashMap<>();
    this.startTimes = new long[INIT_ARRAY_SIZE];
    this.endTimes = new long[INIT_ARRAY_SIZE];
    initTimes(startTimes, Long.MAX_VALUE);
    initTimes(endTimes, Long.MIN_VALUE);
  }

  public DeviceTimeIndex(Map<String, Integer> deviceToIndex, long[] startTimes, long[] endTimes) {
    this.startTimes = startTimes;
    this.endTimes = endTimes;
    this.deviceToIndex = deviceToIndex;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    int deviceNum = deviceToIndex.size();

    ReadWriteIOUtils.write(deviceNum, outputStream);
    for (int i = 0; i < deviceNum; i++) {
      ReadWriteIOUtils.write(startTimes[i], outputStream);
      ReadWriteIOUtils.write(endTimes[i], outputStream);
    }

    Map<String, Integer> stringMemoryReducedMap = new ConcurrentHashMap<>();
    for (Entry<String, Integer> stringIntegerEntry : deviceToIndex.entrySet()) {
      String deviceName = stringIntegerEntry.getKey();
      int index = stringIntegerEntry.getValue();
      // To reduce the String number in memory,
      // use the deviceId from cached pool
      stringMemoryReducedMap.put(cachedDevicePool.computeIfAbsent(deviceName, k -> k), index);
      ReadWriteIOUtils.write(deviceName, outputStream);
      ReadWriteIOUtils.write(index, outputStream);
    }
    deviceToIndex = stringMemoryReducedMap;
  }

  @Override
  public DeviceTimeIndex deserialize(InputStream inputStream) throws IOException {
    int deviceNum = ReadWriteIOUtils.readInt(inputStream);
    Map<String, Integer> deviceMap = new ConcurrentHashMap<>();
    long[] startTimesArray = new long[deviceNum];
    long[] endTimesArray = new long[deviceNum];

    for (int i = 0; i < deviceNum; i++) {
      startTimesArray[i] = ReadWriteIOUtils.readLong(inputStream);
      endTimesArray[i] = ReadWriteIOUtils.readLong(inputStream);
    }

    for (int i = 0; i < deviceNum; i++) {
      String path = ReadWriteIOUtils.readString(inputStream);
      // To reduce the String number in memory,
      // use the deviceId from memory instead of the deviceId read from disk
      String cachedPath = cachedDevicePool.computeIfAbsent(path, k -> k);
      int index = ReadWriteIOUtils.readInt(inputStream);
      deviceMap.put(cachedPath, index);
    }
    return new DeviceTimeIndex(deviceMap, startTimesArray, endTimesArray);
  }

  @Override
  public DeviceTimeIndex deserialize(ByteBuffer buffer) {
    int deviceNum = buffer.getInt();
    Map<String, Integer> deviceMap = new ConcurrentHashMap<>(deviceNum);
    long[] startTimesArray = new long[deviceNum];
    long[] endTimesArray = new long[deviceNum];

    for (int i = 0; i < deviceNum; i++) {
      startTimesArray[i] = buffer.getLong();
      endTimesArray[i] = buffer.getLong();
    }

    for (int i = 0; i < deviceNum; i++) {
      String path = SerializeUtils.deserializeString(buffer);
      // To reduce the String number in memory,
      // use the deviceId from memory instead of the deviceId read from disk
      String cachedPath = cachedDevicePool.computeIfAbsent(path, k -> k);
      int index = buffer.getInt();
      deviceMap.put(cachedPath, index);
    }
    return new DeviceTimeIndex(deviceMap, startTimesArray, endTimesArray);
  }

  @Override
  public void close() {
    startTimes = Arrays.copyOfRange(startTimes, 0, deviceToIndex.size());
    endTimes = Arrays.copyOfRange(endTimes, 0, deviceToIndex.size());
  }

  @Override
  public Set<String> getDevices() {
    return deviceToIndex.keySet();
  }

  @Override
  public boolean endTimeEmpty() {
    for (long endTime : endTimes) {
      if (endTime != Long.MIN_VALUE) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean stillLives(long ttlLowerBound) {
    if (ttlLowerBound == Long.MAX_VALUE) {
      return true;
    }
    for (long endTime : endTimes) {
      // the file cannot be deleted if any device still lives
      if (endTime >= ttlLowerBound) {
        return true;
      }
    }
    return false;
  }

  @Override
  public long calculateRamSize() {
    return RamUsageEstimator.sizeOf(deviceToIndex)
        + RamUsageEstimator.sizeOf(startTimes)
        + RamUsageEstimator.sizeOf(endTimes);
  }

  private int getDeviceIndex(String deviceId) {
    int index;
    if (deviceToIndex.containsKey(deviceId)) {
      index = deviceToIndex.get(deviceId);
    } else {
      index = deviceToIndex.size();
      deviceToIndex.put(deviceId, index);
      if (startTimes.length <= index) {
        startTimes = enLargeArray(startTimes, Long.MAX_VALUE);
        endTimes = enLargeArray(endTimes, Long.MIN_VALUE);
      }
    }
    return index;
  }

  private void initTimes(long[] times, long defaultTime) {
    Arrays.fill(times, defaultTime);
  }

  private long[] enLargeArray(long[] array, long defaultValue) {
    long[] tmp = new long[(int) (array.length * 2)];
    initTimes(tmp, defaultValue);
    System.arraycopy(array, 0, tmp, 0, array.length);
    return tmp;
  }

  @Override
  public long getTimePartition(String tsFilePath) {
    try {
      if (deviceToIndex != null && !deviceToIndex.isEmpty()) {
        return StorageEngine.getTimePartition(startTimes[deviceToIndex.values().iterator().next()]);
      }
      String[] filePathSplits = FilePathUtils.splitTsFilePath(tsFilePath);
      return Long.parseLong(filePathSplits[filePathSplits.length - 2]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /** @return the time partition id, if spans multi time partitions, return -1. */
  private long getTimePartitionWithCheck() {
    long partitionId = SPANS_MULTI_TIME_PARTITIONS_FLAG_ID;
    for (int index : deviceToIndex.values()) {
      long p = StorageEngine.getTimePartition(startTimes[index]);
      if (partitionId == SPANS_MULTI_TIME_PARTITIONS_FLAG_ID) {
        partitionId = p;
      } else {
        if (partitionId != p) {
          return SPANS_MULTI_TIME_PARTITIONS_FLAG_ID;
        }
      }

      p = StorageEngine.getTimePartition(endTimes[index]);
      if (partitionId != p) {
        return SPANS_MULTI_TIME_PARTITIONS_FLAG_ID;
      }
    }
    return partitionId;
  }

  @Override
  public long getTimePartitionWithCheck(String tsFilePath) throws PartitionViolationException {
    long partitionId = getTimePartitionWithCheck();
    if (partitionId == SPANS_MULTI_TIME_PARTITIONS_FLAG_ID) {
      throw new PartitionViolationException(tsFilePath);
    }
    return partitionId;
  }

  @Override
  public boolean isSpanMultiTimePartitions() {
    long partitionId = getTimePartitionWithCheck();
    return partitionId == SPANS_MULTI_TIME_PARTITIONS_FLAG_ID;
  }

  @Override
  public void updateStartTime(String deviceId, long time) {
    long startTime = getStartTime(deviceId);
    if (time < startTime) {
      int index = getDeviceIndex(deviceId);
      startTimes[index] = time;
    }
  }

  @Override
  public void updateEndTime(String deviceId, long time) {
    long endTime = getEndTime(deviceId);
    if (time > endTime) {
      int index = getDeviceIndex(deviceId);
      endTimes[index] = time;
    }
  }

  @Override
  public void putStartTime(String deviceId, long time) {
    int index = getDeviceIndex(deviceId);
    startTimes[index] = time;
  }

  @Override
  public void putEndTime(String deviceId, long time) {
    int index = getDeviceIndex(deviceId);
    endTimes[index] = time;
  }

  @Override
  public long getStartTime(String deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MAX_VALUE;
    }
    return startTimes[deviceToIndex.get(deviceId)];
  }

  @Override
  public long getEndTime(String deviceId) {
    if (!deviceToIndex.containsKey(deviceId)) {
      return Long.MIN_VALUE;
    }
    return endTimes[deviceToIndex.get(deviceId)];
  }

  @Override
  public boolean checkDeviceIdExist(String deviceId) {
    return deviceToIndex.containsKey(deviceId);
  }
}
