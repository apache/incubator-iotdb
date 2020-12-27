/*
 *   * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.db.writelog.io;

import static org.apache.iotdb.db.writelog.node.DifferentialWriteLogNode.WINDOW_LENGTH;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.datastructure.RandomAccessArrayDeque;

public class DifferentialSingleFileLogReader extends SingleFileLogReader {

  private RandomAccessArrayDeque<PhysicalPlan> planWindow;

  public DifferentialSingleFileLogReader(File logFile) throws FileNotFoundException {
    super(logFile);
    this.planWindow = new RandomAccessArrayDeque<>(WINDOW_LENGTH);
  }

  @Override
  BatchLogReader getBatchLogReader(ByteBuffer byteBuffer) {
    return new DifferentialBatchLogReader(byteBuffer, planWindow);
  }
}
