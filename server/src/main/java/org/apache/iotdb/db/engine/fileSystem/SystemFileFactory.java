/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.fileSystem;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public enum SystemFileFactory {
  INSTANCE;

  private static final Logger logger = LoggerFactory.getLogger(SystemFileFactory.class);

  private static FSType fsType = IoTDBDescriptor.getInstance().getConfig().getSystemFileStorageFs();
  private static final String UNSUPPORT_FILE_SYSTEM = "Unsupported file system: ";

  public File getFile(String pathname) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORT_FILE_SYSTEM + fsType.name());
      // return new HDFSFile(pathname);
    } else {
      return new File(pathname);
    }
  }

  public File getFile(String parent, String child) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORT_FILE_SYSTEM + fsType.name());
      // return new HDFSFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(File parent, String child) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORT_FILE_SYSTEM + fsType.name());
      // return new HDFSFile(parent, child);
    } else {
      return new File(parent, child);
    }
  }

  public File getFile(URI uri) {
    if (fsType.equals(FSType.HDFS)) {
      throw new UnsupportedOperationException(UNSUPPORT_FILE_SYSTEM + fsType.name());
      // return new HDFSFile(uri);
    } else {
      return new File(uri);
    }
  }

  public void moveFile(File srcFile, File destFile) {
    try {
      FileUtils.moveFile(srcFile, destFile);
    } catch (IOException e) {
      logger.error(
          "Failed to move file from {} to {}. ",
          srcFile.getAbsolutePath(),
          destFile.getAbsolutePath(),
          e);
    }
  }
}
