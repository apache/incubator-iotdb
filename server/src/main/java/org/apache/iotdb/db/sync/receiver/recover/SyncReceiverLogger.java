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
package org.apache.iotdb.db.sync.receiver.recover;

import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.utils.FSUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

public class SyncReceiverLogger implements ISyncReceiverLogger {

  private BufferedWriter bw;

  public SyncReceiverLogger(File logFile) throws IOException {
    if (!logFile.getParentFile().exists()) {
      logFile.getParentFile().mkdirs();
    }
    bw =
        FSFactoryProducer.getFSFactory(FSUtils.getFSType(logFile))
            .getBufferedWriter(logFile.getAbsolutePath(), false);
  }

  @Override
  public void startSyncDeletedFilesName() throws IOException {
    bw.write(SYNC_DELETED_FILE_NAME_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void finishSyncDeletedFileName(File file) throws IOException {
    bw.write(FSPath.parse(file).getAbsoluteFSPath().getRawFSPath());
    bw.newLine();
    bw.flush();
  }

  @Override
  public void startSyncTsFiles() throws IOException {
    bw.write(SYNC_TSFILE_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void finishSyncTsfile(File file) throws IOException {
    bw.write(FSPath.parse(file).getAbsoluteFSPath().getRawFSPath());
    bw.newLine();
    bw.flush();
  }

  @Override
  public void close() throws IOException {
    if (bw != null) {
      bw.close();
      bw = null;
    }
  }
}
