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

import io.netty.util.internal.ConcurrentSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.PartitionViolationException;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class FileTimeIndex implements ITimeIndex {

  /**
   * start time
   */
  protected long startTime;

  /**
   * end times. The value is Long.MIN_VALUE if it's an unsealed sequence tsfile
   */
  protected long endTime;

  /**
   * devices
   */
  protected Set<String> devices;

  public FileTimeIndex() {
    init();
  }

  public FileTimeIndex(Set<String> devices, long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.devices = devices;
  }

  @Override
  public void init() {
    this.devices = new ConcurrentSet<>();
    this.startTime = Long.MAX_VALUE;
    this.endTime = Long.MIN_VALUE;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(devices.size(), outputStream);
    for (String device : devices) {
      ReadWriteIOUtils.write(device, outputStream);
    }
    ReadWriteIOUtils.write(startTime, outputStream);
    ReadWriteIOUtils.write(endTime, outputStream);
  }

  @Override
  public FileTimeIndex deserialize(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    Set<String> deviceSet = new HashSet<>();
    for (int i = 0; i < size; i++) {
      String path = ReadWriteIOUtils.readString(inputStream);
      // To reduce the String number in memory,
      // use the deviceId from memory instead of the deviceId read from disk
      String cachedPath = cachedDevicePool.computeIfAbsent(path, k -> k);
      deviceSet.add(cachedPath);
    }
    long startTime = ReadWriteIOUtils.readLong(inputStream);
    long endTime = ReadWriteIOUtils.readLong(inputStream);
    return new FileTimeIndex(deviceSet, startTime, endTime);
  }

  @Override
  public void close() {
    // allowed to be null
  }

  @Override
  public Set<String> getDevices() {
    return devices;
  }

  @Override
  public boolean endTimeEmpty() {
    return endTime == Long.MIN_VALUE;
  }

  @Override
  public boolean stillLives(long timeLowerBound) {
    if (timeLowerBound == Long.MAX_VALUE) {
      return true;
    }
    // the file cannot be deleted if any device still lives
    return endTime >= timeLowerBound;
  }

  @Override
  public long calculateRamSize() {
    return RamUsageEstimator.sizeOf(devices) + RamUsageEstimator.sizeOf(startTime) +
        RamUsageEstimator.sizeOf(endTime);
  }

  @Override
  public long estimateRamIncrement(String deviceToBeChecked) {
    return devices.contains(deviceToBeChecked) ? 0L : RamUsageEstimator.sizeOf(deviceToBeChecked);
  }

  @Override
  public long getTimePartition(String file) {
    try {
      if (devices != null && !devices.isEmpty()) {
        return StorageEngine.getTimePartition(startTime);
      }
      String[] filePathSplits = FilePathUtils.splitTsFilePath(file);
      return Long.parseLong(filePathSplits[filePathSplits.length - 2]);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public long getTimePartitionWithCheck(String file) throws PartitionViolationException {
    long startPartitionId = StorageEngine.getTimePartition(startTime);
    long endPartitionId = StorageEngine.getTimePartition(endTime);
    if (startPartitionId != endPartitionId) {
      throw new PartitionViolationException(file);
    }
    return startPartitionId;
  }

  @Override
  public void updateStartTime(String deviceId, long startTime) {
    devices.add(deviceId);
    if (this.startTime > startTime) {
      this.startTime = startTime;
    }
  }

  @Override
  public void updateEndTime(String deviceId, long endTime) {
    devices.add(deviceId);
    if (this.endTime < endTime) {
      this.endTime = endTime;
    }
  }

  @Override
  public long getStartTime(String deviceId) {
    return startTime;
  }

  @Override
  public long getEndTime(String deviceId) {
    return endTime;
  }
}
