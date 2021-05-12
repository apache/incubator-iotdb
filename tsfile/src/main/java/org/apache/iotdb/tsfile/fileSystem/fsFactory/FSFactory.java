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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;

public interface FSFactory {

  /**
   * get file with parent
   *
   * @param pathname pathname
   * @return file with parent
   */
  File getFileWithParent(String pathname);

  /**
   * get file
   *
   * @param pathname pathname
   * @return file
   */
  File getFile(String pathname);

  /**
   * get file
   *
   * @param parent parent file path
   * @param child child file path
   * @return file
   */
  File getFile(String parent, String child);

  /**
   * get file
   *
   * @param parent parent file
   * @param child child file path
   * @return file
   */
  File getFile(File parent, String child);

  /**
   * get file by uri
   *
   * @param uri uri
   * @return file
   */
  File getFile(URI uri);

  /**
   * get buffered reader
   *
   * @param filePath file path
   * @return buffered reader
   */
  BufferedReader getBufferedReader(String filePath);

  /**
   * get buffered writer
   *
   * @param filePath file path
   * @param append whether is append
   * @return buffered writer
   */
  BufferedWriter getBufferedWriter(String filePath, boolean append);

  /**
   * get input stream
   *
   * @param filePath file path
   * @return input stream
   */
  BufferedInputStream getBufferedInputStream(String filePath);

  /**
   * get output stream
   *
   * @param filePath file path
   * @return output stream
   */
  BufferedOutputStream getBufferedOutputStream(String filePath);

  /**
   * move file, when the destination file is on another file system, do a "copy and delete".
   *
   * @param srcFile src file
   * @param destFile dest file
   */
  void moveFile(File srcFile, File destFile);

  /**
   * copy file.
   *
   * @param srcFile src file
   * @param destFile dest file
   */
  void copyFile(File srcFile, File destFile);

  /**
   * list file by suffix
   *
   * @param fileFolder file folder
   * @param suffix suffix
   * @return list of files
   */
  File[] listFilesBySuffix(String fileFolder, String suffix);

  /**
   * list file by prefix
   *
   * @param fileFolder file folder
   * @param prefix prefix
   * @return list of files
   */
  File[] listFilesByPrefix(String fileFolder, String prefix);

  /**
   * delete the file if it exists
   *
   * @param file local file or HDFS file
   */
  boolean deleteIfExists(File file) throws IOException;
}
