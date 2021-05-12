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

package org.apache.iotdb.tsfile.fileSystem;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.filesystem.FileSystemNotSupportedException;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.FileInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.HDFSInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileInputFactory.LocalFSInputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileOutputFactory.FileOutputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileOutputFactory.HDFSOutputFactory;
import org.apache.iotdb.tsfile.fileSystem.fileOutputFactory.LocalFSOutputFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.HDFSFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.LocalFSFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * FSFactoryProducer contains several static factories that creates {@code FSFactory} , {@code
 * FileInputFactory}, {@code FileOutputFactory} for different filesystems.
 */
public class FSFactoryProducer {
  private static final Map<FSType, FSFactory> fsFactory;
  private static final Map<FSType, FileInputFactory> fileInputFactory;
  private static final Map<FSType, FileOutputFactory> fileOutputFactory;

  static {
    fsFactory = new HashMap<>();
    fileInputFactory = new HashMap<>();
    fileOutputFactory = new HashMap<>();
    reload();
  }

  /** load the FSFactory of each filesystem. */
  public static synchronized void reload() {
    fsFactory.clear();
    fileInputFactory.clear();
    fileOutputFactory.clear();
    for (FSType fs : TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs()) {
      switch (fs) {
        case LOCAL:
          fsFactory.put(fs, new LocalFSFactory());
          fileInputFactory.put(fs, new LocalFSInputFactory());
          fileOutputFactory.put(fs, new LocalFSOutputFactory());
          break;
        case HDFS:
          fsFactory.put(fs, new HDFSFactory());
          fileInputFactory.put(fs, new HDFSInputFactory());
          fileOutputFactory.put(fs, new HDFSOutputFactory());
          break;
        default:
          throw new FileSystemNotSupportedException(fs.name());
      }
    }
  }

  /**
   * Get the {@code FSFactory} according to the filesystem type
   *
   * @param fsType filesystem type
   * @return the FSFactory object represented by the fsType argument
   */
  public static FSFactory getFSFactory(FSType fsType) {
    if (!fsFactory.containsKey(fsType)) {
      throw new FileSystemNotSupportedException(fsType.name());
    }
    return fsFactory.get(fsType);
  }

  /**
   * Get the {@code FileInputFactory} according to the filesystem type
   *
   * @param fsType filesystem type
   * @return the FileInputFactory object represented by the fsType argument
   */
  public static FileInputFactory getFileInputFactory(FSType fsType) {
    if (!fileInputFactory.containsKey(fsType)) {
      throw new FileSystemNotSupportedException(fsType.name());
    }
    return fileInputFactory.get(fsType);
  }

  /**
   * Get the {@code FileOutputFactory} according to the filesystem type
   *
   * @param fsType filesystem type
   * @return the FileOutputFactory object represented by the fsType argument
   */
  public static FileOutputFactory getFileOutputFactory(FSType fsType) {
    if (!fileOutputFactory.containsKey(fsType)) {
      throw new FileSystemNotSupportedException(fsType.name());
    }
    return fileOutputFactory.get(fsType);
  }
}
