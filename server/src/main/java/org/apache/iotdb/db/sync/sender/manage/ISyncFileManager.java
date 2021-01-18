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
package org.apache.iotdb.db.sync.sender.manage;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * This interface is used to manage deleted files and new closed files that need to be synchronized
 * in each sync task.
 */
public interface ISyncFileManager {

  /**
   * Find out all closed and unmodified files, which means there has a .resource file and doesn't
   * have a .mod file and .mergeUnseq file. For these files, they will eventually generate a new tsfile
   * file as the mergeUnseq operation is executed and executed in subsequent synchronization tasks.
   *
   * @param dataDir data directory
   */
  void getCurrentLocalFiles(String dataDir);

  /**
   * Load last local files from file<lastLocalFile> which does not contain those tsfiles which are
   * not synced successfully in previous sync tasks.
   *
   * @param lastLocalFile last local file, which may not exist in first sync task.
   */
  void getLastLocalFiles(File lastLocalFile) throws IOException;

  /**
   * Based on current local files and last local files, we can distinguish two kinds of files
   * between them, one is deleted files, the other is new files. These two kinds of files are valid
   * files that need to be synchronized to the receiving end.
   *
   * @param dataDir data directory
   */
  void getValidFiles(String dataDir) throws IOException;

  Map<String, Map<Long, Set<File>>> getCurrentSealedLocalFilesMap();

  Map<String, Map<Long, Set<File>>> getLastLocalFilesMap();

  Map<String, Map<Long, Set<File>>> getDeletedFilesMap();

  Map<String, Map<Long, Set<File>>> getToBeSyncedFilesMap();

  Map<String, Set<Long>> getAllSGs();
}
