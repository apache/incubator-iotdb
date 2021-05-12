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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.filesystem.FileSystemNotSupportedException;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

/**
 * The {@code FSPath} class wraps filesystem and path value in an object. It also provides a method
 * for converting a {@code String} to a {@code FSPath}.
 */
public class FSPath {
  public static final String FS_PATH_SEPARATOR = "@";

  private static final Logger logger = LoggerFactory.getLogger(FSPath.class);
  private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  private final FSType fsType;

  private final String path;

  public FSPath(FSType fsType, String path) {
    this.fsType = fsType;
    this.path = path;
  }

  /**
   * Parses the string argument as a FSPath object.FsType and path information are separated by
   * semicolon, e.g., local@data/data.
   *
   * @param fsPath a {@code String} containing FSType and path information to be parsed.
   * @return the FSPath object represented by the string argument
   */
  public static FSPath parse(String fsPath) {
    int sepIdx = fsPath.indexOf(FS_PATH_SEPARATOR);
    if (sepIdx != -1) {
      String fs = fsPath.substring(0, sepIdx);
      for (FSType fsType : FSType.values()) {
        if (fs.equalsIgnoreCase(fsType.name())) {
          if (config.isFSSupported(fsType)) {
            return new FSPath(fsType, fsPath.substring(fsType.name().length() + 1));
          } else {
            logger.error("Unsupported file system: {}", fsType);
            throw new FileSystemNotSupportedException(fsType.name());
          }
        }
      }
    }
    // use default filesystem as FSType
    if (config.getTSFileStorageFs().length == 1) {
      return new FSPath(config.getTSFileStorageFs()[0], fsPath);
    }
    // use LOCAL as default
    return new FSPath(FSType.LOCAL, fsPath);
  }

  /**
   * Parses the file argument as a FSPath object.
   *
   * @param file a file of any filesystem
   * @return the FSPath object represented by the file argument
   */
  public static FSPath parse(File file) {
    return new FSPath(FSUtils.getFSType(file), file.getPath());
  }

  public FSType getFsType() {
    return fsType;
  }

  public String getPath() {
    return path;
  }

  /**
   * Gets the raw path value, e.g., local@data/data.
   *
   * @return a string that contains both filesystem and path information
   */
  public String getRawFSPath() {
    return fsType.name() + FS_PATH_SEPARATOR + path;
  }

  /**
   * Gets the {@code Path} object of this fsPath
   *
   * @return a {@code Path} object that represents this filesystem and path
   */
  public Path toPath() {
    return toFile().toPath();
  }

  /**
   * Gets the file of this fsPath
   *
   * @return a file that represents this filesystem and path
   */
  public File toFile() {
    return FSFactoryProducer.getFSFactory(fsType).getFile(path);
  }

  /**
   * Creates a child File instance from a child pathname string based on this fsPath.
   *
   * @param child The child pathname string
   * @return a file that represents this filesystem and child path
   */
  public File getChildFile(String child) {
    return FSFactoryProducer.getFSFactory(fsType).getFile(path, child);
  }

  /**
   * Concatenates the specified string array to the start of this path.
   *
   * @param prefix the {@code String} array that is concatenated to the start of this path.
   * @return a fsPath that represents the concatenation of the array argument's string followed by
   *     this object's path.
   */
  public FSPath preConcat(String... prefix) {
    return new FSPath(fsType, String.join("", prefix) + path);
  }

  /**
   * Concatenates the specified string array to the end of this path.
   *
   * @param suffix the {@code String} array that is concatenated to the end of this path.
   * @return a fsPath that represents the concatenation of this object's path followed by the array
   *     argument's string.
   */
  public FSPath postConcat(String... suffix) {
    return new FSPath(fsType, path + String.join("", suffix));
  }

  /**
   * Gets the absolute path representation of this fsPath.
   *
   * @return a fsPath whose path part is absolute
   */
  public FSPath getAbsoluteFSPath() {
    return new FSPath(this.fsType, toFile().getAbsolutePath());
  }

  @Override
  public String toString() {
    return getRawFSPath();
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    FSPath other = (FSPath) obj;
    return Objects.equals(fsType, other.fsType) && Objects.equals(path, other.path);
  }
}
