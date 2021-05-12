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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The basic class of all the strategies of multiple directories. If a user wants to define his own
 * strategy, his strategy has to extend this class and implement the abstract method.
 */
public abstract class DirectoryStrategy {

  protected static final Logger logger = LoggerFactory.getLogger(DirectoryStrategy.class);

  /** All the folders of data files, should be init once the subclass is created. */
  List<FSPath> folders = new ArrayList<>();

  /**
   * To init folders. Do not recommend to overwrite. This method guarantees that at least one folder
   * has available space.
   *
   * @param folders the folders from conf
   */
  public void setFolders(List<FSPath> folders) throws DiskSpaceInsufficientException {
    boolean hasSpace = false;
    for (FSPath folder : folders) {
      if (CommonUtils.hasSpace(folder)) {
        hasSpace = true;
        break;
      }
    }
    if (!hasSpace) {
      IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
      throw new DiskSpaceInsufficientException(folders);
    }

    this.folders = folders;
  }

  /**
   * Choose a folder to allocate. The user should implement this method to define his own strategy.
   *
   * @return the index of folder that will be allocated
   */
  public abstract int nextFolderIndex() throws DiskSpaceInsufficientException;
}
