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

public enum TimeIndexLevel {
  /** file to time index (small memory foot print) */
  FILE_TIME_INDEX,

  /** device to time index (large memory foot print) */
  DEVICE_TIME_INDEX;

  public ITimeIndex getTimeIndex() {
    switch (this) {
      case FILE_TIME_INDEX:
        return new FileTimeIndex();
      case DEVICE_TIME_INDEX:
      default:
        return new DeviceTimeIndex();
    }
  }

  public static TimeIndexLevel valueOf(int ordinal) {
    if (ordinal < 0 || ordinal >= values().length) {
      throw new IndexOutOfBoundsException("Invalid ordinal");
    }
    return values()[ordinal];
  }
}
