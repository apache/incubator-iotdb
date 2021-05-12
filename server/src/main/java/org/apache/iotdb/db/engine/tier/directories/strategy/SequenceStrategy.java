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

import java.util.List;

public class SequenceStrategy extends DirectoryStrategy {

  private int currentIndex;

  @Override
  public void setFolders(List<FSPath> folders) throws DiskSpaceInsufficientException {
    super.setFolders(folders);

    // super.setFolders() ensures at least one folder is not full,
    // so currentIndex will not be -1 after loop
    currentIndex = -1;
    for (int i = 0; i < folders.size(); i++) {
      if (CommonUtils.hasSpace(folders.get(i))) {
        currentIndex = i;
        break;
      }
    }
  }

  @Override
  public int nextFolderIndex() throws DiskSpaceInsufficientException {
    int index = currentIndex;
    currentIndex = tryGetNextIndex(index);

    return index;
  }

  private int tryGetNextIndex(int start) throws DiskSpaceInsufficientException {
    int index = (start + 1) % folders.size();
    while (!CommonUtils.hasSpace(folders.get(index))) {
      index = (index + 1) % folders.size();
      if (index == start) {
        throw new DiskSpaceInsufficientException(folders);
      }
    }
    return index;
  }
}
