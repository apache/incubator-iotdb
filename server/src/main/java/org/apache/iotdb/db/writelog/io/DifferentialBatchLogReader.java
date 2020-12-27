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

import static org.apache.iotdb.db.writelog.node.DifferentialWriteLogNode.WINDOW_LENGTH;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.PhysicalPlan.Factory;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.datastructure.RandomAccessArrayDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DifferentialBatchLogReader extends BatchLogReader {

  private static final Logger logger = LoggerFactory.getLogger(DifferentialBatchLogReader.class);
  private RandomAccessArrayDeque<PhysicalPlan> planWindow;

  public DifferentialBatchLogReader(ByteBuffer buffer, RandomAccessArrayDeque<PhysicalPlan> planWindow) {
    this.planWindow = planWindow;
    List<PhysicalPlan> logs = readLogs(buffer);
    this.planIterator = logs.iterator();
  }

  @Override
  List<PhysicalPlan> readLogs(ByteBuffer buffer) {
    List<PhysicalPlan> plans = new ArrayList<>();
    while (buffer.position() != buffer.limit()) {
      try {
        PhysicalPlan physicalPlan = Factory.create(buffer, planWindow);
        CommonUtils.updatePlanWindow(physicalPlan, WINDOW_LENGTH, planWindow);
        plans.add(physicalPlan);
      } catch (IOException | IllegalPathException e) {
        logger.error("Cannot deserialize PhysicalPlans from ByteBuffer, ignore remaining logs", e);
        fileCorrupted = true;
        break;
      }
    }
    return plans;
  }
}
