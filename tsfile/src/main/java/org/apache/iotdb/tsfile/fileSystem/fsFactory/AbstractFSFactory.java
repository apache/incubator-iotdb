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
package org.apache.iotdb.tsfile.fileSystem.fsFactory;

import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.utils.FSUtils;

import java.io.File;

/**
 * The {@code AbstractFSFactory} class gives default implements for some methods of {@code
 * FSFactory}.
 */
public abstract class AbstractFSFactory implements FSFactory {
  @Override
  public void moveFile(File srcFile, File destFile) {
    if (FSUtils.getFSType(srcFile).equals(FSUtils.getFSType(destFile))) {
      moveFileInSameFS(srcFile, destFile);
      return;
    }
    HDFSFactory hdfsFactory = (HDFSFactory) FSFactoryProducer.getFSFactory(FSType.HDFS);
    if (FSUtils.getFSType(srcFile).equals(FSType.HDFS)) {
      hdfsFactory.moveToLocalFile(srcFile, destFile);
    } else {
      hdfsFactory.moveFromLocalFile(srcFile, destFile);
    }
  }

  @Override
  public void copyFile(File srcFile, File destFile) {
    if (FSUtils.getFSType(srcFile).equals(FSUtils.getFSType(destFile))) {
      copyFileInSameFS(srcFile, destFile);
      return;
    }
    HDFSFactory hdfsFactory = (HDFSFactory) FSFactoryProducer.getFSFactory(FSType.HDFS);
    if (FSUtils.getFSType(srcFile).equals(FSType.HDFS)) {
      hdfsFactory.copyToLocalFile(srcFile, destFile);
    } else {
      hdfsFactory.copyFromLocalFile(srcFile, destFile);
    }
  }

  /**
   * move file in same file system.
   *
   * @param srcFile src file
   * @param destFile dest file
   */
  abstract void moveFileInSameFS(File srcFile, File destFile);

  /**
   * copy file in same file system.
   *
   * @param srcFile src file
   * @param destFile dest file
   */
  abstract void copyFileInSameFS(File srcFile, File destFile);
}
