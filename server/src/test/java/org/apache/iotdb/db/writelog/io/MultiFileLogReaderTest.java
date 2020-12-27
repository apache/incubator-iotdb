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

package org.apache.iotdb.db.writelog.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.tsfile.read.common.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultiFileLogReaderTest {

  private File[] logFiles;
  private PhysicalPlan[][] fileLogs;
  private int logsPerFile = 5;
  private int fileNum = 3;

  @Before
  public void setup() throws IOException, IllegalPathException {
    logFiles = new File[fileNum];
    fileLogs = new PhysicalPlan[fileNum][logsPerFile];
    for (int i = 0; i < fileNum; i++) {
      logFiles[i] = new File(i + ".log");
      for (int j = 0; j < logsPerFile; j++) {
        fileLogs[i][j] = new DeletePlan(Long.MIN_VALUE, i * logsPerFile + j, new PartialPath("path" + j));
      }

      ByteBuffer buffer = ByteBuffer.allocate(64*1024);
      for (PhysicalPlan plan : fileLogs[i]) {
        plan.serialize(buffer);
      }
      ILogWriter writer = new LogWriter(logFiles[i],
        IoTDBDescriptor.getInstance().getConfig().getForceWalPeriodInMs() == 0);
      writer.write(buffer);
      writer.force();
      writer.close();
    }
  }

  @After
  public void teardown() throws IOException {
    for (File logFile : logFiles) {
      FileUtils.forceDelete(logFile);
    }
  }

  @Test
  public void test() throws IOException {
    MultiFileLogReader reader = new MultiFileLogReader(logFiles, SingleFileLogReader::new);
    int i = 0;
    while (reader.hasNext()) {
      PhysicalPlan plan = reader.next();
      assertEquals(fileLogs[i/logsPerFile][i%logsPerFile], plan);
      i ++;
    }
    reader.close();
    assertEquals(fileNum * logsPerFile, i);
  }
}
