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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;

public class HDFSFactory extends AbstractFSFactory {

  private static final Logger logger = LoggerFactory.getLogger(HDFSFactory.class);
  private static Constructor<?> constructorWithPathname;
  private static Constructor<?> constructorWithParentStringAndChild;
  private static Constructor<?> constructorWithParentFileAndChild;
  private static Constructor<?> constructorWithUri;
  private static Method getBufferedReader;
  private static Method getBufferedWriter;
  private static Method getBufferedInputStream;
  private static Method getBufferedOutputStream;
  private static Method listFilesBySuffix;
  private static Method listFilesByPrefix;
  private static Method moveTo;
  private static Method moveFromLocalFile;
  private static Method moveToLocalFile;
  private static Method copyTo;
  private static Method copyFromLocalFile;
  private static Method copyToLocalFile;

  static {
    try {
      Class<?> clazz = Class.forName("org.apache.iotdb.hadoop.fileSystem.HDFSFile");
      constructorWithPathname = clazz.getConstructor(String.class);
      constructorWithParentStringAndChild = clazz.getConstructor(String.class, String.class);
      constructorWithParentFileAndChild = clazz.getConstructor(File.class, String.class);
      constructorWithUri = clazz.getConstructor(URI.class);
      getBufferedReader = clazz.getMethod("getBufferedReader", String.class);
      getBufferedWriter = clazz.getMethod("getBufferedWriter", String.class, boolean.class);
      getBufferedInputStream = clazz.getMethod("getBufferedInputStream", String.class);
      getBufferedOutputStream = clazz.getMethod("getBufferedOutputStream", String.class);
      listFilesBySuffix = clazz.getMethod("listFilesBySuffix", String.class, String.class);
      listFilesByPrefix = clazz.getMethod("listFilesByPrefix", String.class, String.class);
      moveTo = clazz.getMethod("moveTo", File.class);
      moveFromLocalFile = clazz.getMethod("moveFromLocalFile", File.class);
      moveToLocalFile = clazz.getMethod("moveToLocalFile", File.class);
      copyTo = clazz.getMethod("copyTo", File.class);
      copyFromLocalFile = clazz.getMethod("copyFromLocalFile", File.class);
      copyToLocalFile = clazz.getMethod("copyToLocalFile", File.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      logger.error(
          "Failed to get Hadoop file system. Please check your dependency of Hadoop module.", e);
    }
  }

  @Override
  public File getFileWithParent(String pathname) {
    try {
      File res = (File) constructorWithPathname.newInstance(pathname);
      if (!res.exists()) {
        res.getParentFile().mkdirs();
      }
      return res;
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.", pathname, e);
      return null;
    }
  }

  @Override
  public File getFile(String pathname) {
    try {
      return (File) constructorWithPathname.newInstance(pathname);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.", pathname, e);
      return null;
    }
  }

  @Override
  public File getFile(String parent, String child) {
    try {
      return (File) constructorWithParentStringAndChild.newInstance(parent, child);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.",
          parent + File.separator + child,
          e);
      return null;
    }
  }

  @Override
  public File getFile(File parent, String child) {
    try {
      return (File) constructorWithParentFileAndChild.newInstance(parent, child);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.",
          parent.getAbsolutePath() + File.separator + child,
          e);
      return null;
    }
  }

  @Override
  public File getFile(URI uri) {
    try {
      return (File) constructorWithUri.newInstance(uri);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get file: {}. Please check your dependency of Hadoop module.", uri, e);
      return null;
    }
  }

  @Override
  public BufferedReader getBufferedReader(String filePath) {
    try {
      return (BufferedReader)
          getBufferedReader.invoke(constructorWithPathname.newInstance(filePath), filePath);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered reader for {}. Please check your dependency of Hadoop module.",
          filePath,
          e);
      return null;
    }
  }

  @Override
  public BufferedWriter getBufferedWriter(String filePath, boolean append) {
    try {
      return (BufferedWriter)
          getBufferedWriter.invoke(constructorWithPathname.newInstance(filePath), filePath, append);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered writer for {}. Please check your dependency of Hadoop module.",
          filePath,
          e);
      return null;
    }
  }

  @Override
  public BufferedInputStream getBufferedInputStream(String filePath) {
    try {
      return (BufferedInputStream)
          getBufferedInputStream.invoke(constructorWithPathname.newInstance(filePath), filePath);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered input stream for {}. Please check your dependency of Hadoop module.",
          filePath,
          e);
      return null;
    }
  }

  @Override
  public BufferedOutputStream getBufferedOutputStream(String filePath) {
    try {
      return (BufferedOutputStream)
          getBufferedOutputStream.invoke(constructorWithPathname.newInstance(filePath), filePath);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to get buffered output stream for {}. Please check your dependency of Hadoop module.",
          filePath,
          e);
      return null;
    }
  }

  @Override
  public File[] listFilesBySuffix(String fileFolder, String suffix) {
    try {
      return (File[])
          listFilesBySuffix.invoke(
              constructorWithPathname.newInstance(fileFolder), fileFolder, suffix);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to list files in {} with SUFFIX {}. Please check your dependency of Hadoop module.",
          fileFolder,
          suffix,
          e);
      return null;
    }
  }

  @Override
  public File[] listFilesByPrefix(String fileFolder, String prefix) {
    try {
      return (File[])
          listFilesByPrefix.invoke(
              constructorWithPathname.newInstance(fileFolder), fileFolder, prefix);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to list files in {} with PREFIX {}. Please check your dependency of Hadoop module.",
          fileFolder,
          prefix,
          e);
      return null;
    }
  }

  @Override
  public boolean deleteIfExists(File file) {
    return file.delete();
  }

  @Override
  void moveFileInSameFS(File srcFile, File destFile) {
    try {
      moveTo.invoke(constructorWithPathname.newInstance(srcFile.getAbsolutePath()), destFile);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to move file from {} to {}. Please check your dependency of Hadoop module.",
          srcFile.getName(),
          destFile.getName());
    }
  }

  void moveFromLocalFile(File srcFile, File destFile) {
    try {
      moveFromLocalFile.invoke(
          constructorWithPathname.newInstance(destFile.getAbsolutePath()), srcFile);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to move local file {} to hdfs {}. Please check your dependency of Hadoop module.",
          srcFile,
          destFile,
          e);
    }
  }

  void moveToLocalFile(File srcFile, File destFile) {
    try {
      moveToLocalFile.invoke(
          constructorWithPathname.newInstance(srcFile.getAbsolutePath()), destFile);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to move hdfs file {} to local {}. Please check your dependency of Hadoop module.",
          srcFile,
          destFile,
          e);
    }
  }

  @Override
  void copyFileInSameFS(File srcFile, File destFile) {
    try {
      copyTo.invoke(constructorWithPathname.newInstance(srcFile.getAbsolutePath()), destFile);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to move file from {} to {}. Please check your dependency of Hadoop module.",
          srcFile.getName(),
          destFile.getName());
    }
  }

  void copyFromLocalFile(File srcFile, File destFile) {
    try {
      copyFromLocalFile.invoke(
          constructorWithPathname.newInstance(destFile.getAbsolutePath()), srcFile);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to copy local file {} to hdfs {}. Please check your dependency of Hadoop module.",
          srcFile,
          destFile,
          e);
    }
  }

  void copyToLocalFile(File srcFile, File destFile) {
    try {
      copyToLocalFile.invoke(
          constructorWithPathname.newInstance(srcFile.getAbsolutePath()), destFile);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
      logger.error(
          "Failed to copy hdfs file {} to local {}. Please check your dependency of Hadoop module.",
          srcFile,
          destFile,
          e);
    }
  }
}
