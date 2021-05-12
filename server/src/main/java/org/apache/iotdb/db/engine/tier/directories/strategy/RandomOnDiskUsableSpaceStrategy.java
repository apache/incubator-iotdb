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
package org.apache.iotdb.db.engine.tier.directories.strategy;

import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomOnDiskUsableSpaceStrategy extends DirectoryStrategy {

  private Random random = new Random(System.currentTimeMillis());

  @Override
  public int nextFolderIndex() throws DiskSpaceInsufficientException {
    List<Long> spaceList = getFolderUsableSpaceList();
    long spaceSum = spaceList.stream().mapToLong(Long::longValue).sum();

    if (spaceSum <= 0) {
      throw new DiskSpaceInsufficientException(folders);
    }

    // The reason that avoid using Math.abs() is that, according to the doc of Math.abs(),
    // if the argument is equal to the value of Long.MIN_VALUE, the result is that same value, which
    // is negative.
    long randomV = (random.nextLong() & Long.MAX_VALUE) % spaceSum;
    int index = 0;
    /* In fact, index will never equals spaceList.size(),
    for that randomV is less than sum of spaceList. */
    while (index < spaceList.size() && randomV >= spaceList.get(index)) {
      randomV -= spaceList.get(index);
      index++;
    }

    return index;
  }

  /** get space list of all folders. */
  public List<Long> getFolderUsableSpaceList() {
    List<Long> spaceList = new ArrayList<>();
    for (int i = 0; i < folders.size(); i++) {
      FSPath folder = folders.get(i);
      spaceList.add(CommonUtils.getUsableSpace(folder));
    }
    return spaceList;
  }
}
