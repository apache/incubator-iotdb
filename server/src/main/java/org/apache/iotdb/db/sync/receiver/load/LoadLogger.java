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
package org.apache.iotdb.db.sync.receiver.load;

import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.utils.FSUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

public class LoadLogger implements ILoadLogger {

  private BufferedWriter bw;

  public LoadLogger(File logFile) throws IOException {
    if (!logFile.getParentFile().exists()) {
      logFile.getParentFile().mkdirs();
    }

    bw =
        FSFactoryProducer.getFSFactory(FSUtils.getFSType(logFile))
            .getBufferedWriter(logFile.getAbsolutePath(), false);
  }

  @Override
  public void startLoadDeletedFiles() throws IOException {
    bw.write(LOAD_DELETED_FILE_NAME_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void finishLoadDeletedFile(File file) throws IOException {
    bw.write(FSPath.parse(file).getAbsoluteFSPath().getRawFSPath());
    bw.newLine();
    bw.flush();
  }

  @Override
  public void startLoadTsFiles() throws IOException {
    bw.write(LOAD_TSFILE_START);
    bw.newLine();
    bw.flush();
  }

  @Override
  public void finishLoadTsfile(File file) throws IOException {
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
