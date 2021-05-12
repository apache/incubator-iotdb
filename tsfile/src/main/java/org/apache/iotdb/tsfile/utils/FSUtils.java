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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.filesystem.FileSystemNotSupportedException;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.FSType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/** FSUtils is a utility class. It provides various methods for judging the filesystem type. */
public class FSUtils {
  private static final Logger logger = LoggerFactory.getLogger(FSUtils.class);

  // sorted by priority, LOCAL must at last
  private static final FSType[] fsTypes = {FSType.HDFS, FSType.LOCAL};
  private static final String[] fsFileClassName = {
    "org.apache.iotdb.hadoop.fileSystem.HDFSFile", "java.io.File"
  };

  private static final Map<FSType, Class<?>> fsFileClass;

  private FSUtils() {}

  static {
    fsFileClass = new LinkedHashMap<>();
    reload();
  }

  /** load the {@code File} Class of each filesystem. */
  public static synchronized void reload() {
    fsFileClass.clear();
    for (int i = 0; i < fsTypes.length; ++i) {
      FSType fs = fsTypes[i];
      if (TSFileDescriptor.getInstance().getConfig().isFSSupported(fs)) {
        try {
          Class<?> clazz = Class.forName(fsFileClassName[i]);
          fsFileClass.put(fs, clazz);
        } catch (ClassNotFoundException e) {
          logger.error(
              "Failed to get "
                  + fs.name()
                  + " file system. Please check your dependency of "
                  + fs.name()
                  + " module.",
              e);
        }
      }
    }
  }

  /**
   * Get filesystem type of a file
   *
   * @param file a file of any filesystem
   * @return fsType of the file argument
   */
  public static FSType getFSType(File file) {
    for (FSType fs : fsFileClass.keySet()) {
      Class<?> clazz = fsFileClass.get(fs);
      if (clazz != null && clazz.isInstance(file)) {
        return fs;
      }
    }
    logger.error("Unsupported file system: {}", file);
    throw new FileSystemNotSupportedException(file.getPath());
  }

  /**
   * Get filesystem type of a path
   *
   * @param path a path of any filesystem
   * @return fsType of the path argument
   */
  public static FSType getFSType(Path path) {
    return getFSType(path.toFile());
  }

  /**
   * Get filesystem type of a path string
   *
   * @param rawFSPath path string
   * @return fsType of the string argument
   */
  public static FSType getFSType(String rawFSPath) {
    return FSPath.parse(rawFSPath).getFsType();
  }
}
