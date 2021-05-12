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

public class MaxDiskUsableSpaceFirstStrategy extends DirectoryStrategy {

  @Override
  public int nextFolderIndex() throws DiskSpaceInsufficientException {
    return getMaxSpaceFolder();
  }

  /** get max space folder. */
  public int getMaxSpaceFolder() throws DiskSpaceInsufficientException {
    int maxIndex = -1;
    long maxSpace = 0;

    for (int i = 0; i < folders.size(); i++) {
      FSPath folder = folders.get(i);
      if (!CommonUtils.hasSpace(folder)) {
        continue;
      }

      long space = CommonUtils.getUsableSpace(folder);
      if (space > maxSpace) {
        maxSpace = space;
        maxIndex = i;
      }
    }

    if (maxIndex == -1) {
      throw new DiskSpaceInsufficientException(folders);
    }

    return maxIndex;
  }
}
