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

package org.apache.iotdb.db.writelog.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.BufferOverflowException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.db.utils.datastructure.RandomAccessArrayDeque;
import org.apache.iotdb.db.writelog.io.DifferentialSingleFileLogReader;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DifferentialWriteLogNode maintains a reduced plan window (reduced means the plans do not
 * contain unnecessary fields for differentiation, e.g., values and timestamps in InsertPlan).
 * When a plan is to be serialized, if a similar log can be found with in the window, identical
 * fields (like deviceId, measurementIds, dataTypes) will be referenced from the similar log and
 * remove the necessity of serializing them more than once.
 */
public class DifferentialWriteLogNode extends ExclusiveWriteLogNode {

  private static final Logger logger = LoggerFactory.getLogger(DifferentialWriteLogNode.class);
  // we can only use a linear search now, so the window length should not be too large
  public static final int WINDOW_LENGTH = 2000;
  private RandomAccessArrayDeque<PhysicalPlan> planWindow;

  /**
   * constructor of ExclusiveWriteLogNode.
   *
   * @param identifier ExclusiveWriteLogNode identifier
   */
  public DifferentialWriteLogNode(String identifier) {
    super(identifier);
    planWindow = new RandomAccessArrayDeque<>(WINDOW_LENGTH);
  }

  @Override
  void putLog(PhysicalPlan plan) {
    logBufferWorking.mark();
    Pair<PhysicalPlan, Short> similarPlanIndex = findSimilarPlan(plan);
    try {
      serialize(plan, similarPlanIndex);
    } catch (BufferOverflowException e) {
      logger.info("WAL BufferOverflow !");
      logBufferWorking.reset();
      sync();
      serialize(plan, similarPlanIndex);
    }
    bufferedLogNum ++;
    CommonUtils.updatePlanWindow(plan, WINDOW_LENGTH, planWindow);
  }

  @Override
  public void notifyStartFlush() throws FileNotFoundException {
    lock.lock();
    try {
      close();
      nextFileWriter();
      planWindow.clear();
    } finally {
      lock.unlock();
    }
  }

  private void serialize(PhysicalPlan plan, Pair<PhysicalPlan, Short> similarPlanIndex) {
    if (similarPlanIndex == null) {
      serializeNonDifferentially(plan);
    } else {
      serializeDifferentially(plan, similarPlanIndex);
    }
  }

  private void serializeNonDifferentially(PhysicalPlan plan) {
    logBufferWorking.putShort((short) -1);
    plan.serialize(logBufferWorking);
  }

  private void serializeDifferentially(PhysicalPlan plan,
      Pair<PhysicalPlan, Short> similarPlanIndex) {
    if (plan instanceof InsertPlan) {
      serializeDifferentially(((InsertPlan) plan), ((InsertPlan) similarPlanIndex.left),
          similarPlanIndex.right);
    } else {
      serializeNonDifferentially(plan);
    }
  }

  private void serializeDifferentially(InsertPlan plan, InsertPlan base, short index) {
    logBufferWorking.putShort(index);
    plan.serialize(logBufferWorking, base);
  }

  private Pair<PhysicalPlan, Short> findSimilarPlan(PhysicalPlan plan) {
    for (short i = 0; i < planWindow.size(); i++) {
      PhysicalPlan next = planWindow.get(i);
      if (isPlanSimilarEnough(plan, next)) {
        return new Pair<>(next, i);
      }
    }
    return null;
  }

  private boolean isPlanSimilarEnough(PhysicalPlan planA, PhysicalPlan planB) {
    if (!planA.getClass().equals(planB.getClass())) {
      return false;
    }
    // we only do differentiation for InsertPlans now, as they are the majority
    if (planA instanceof InsertPlan && planB instanceof InsertPlan) {
      return isPlanSimilarEnough(((InsertPlan) planA), ((InsertPlan) planB));
    }
    return false;
  }

  private boolean isPlanSimilarEnough(InsertPlan planA, InsertPlan planB) {
    // data types are also compared because the timeseries may be deleted and recreated with
    // different types between two insertions
    return planA.getDeviceId().equals(planB.getDeviceId()) &&
        Arrays.equals(planA.getMeasurements(), planB.getMeasurements()) &&
        Arrays.equals(planA.getDataTypes(), planB.getDataTypes());
  }

  @Override
  public ILogReader getLogReader() {
    File[] logFiles = SystemFileFactory.INSTANCE.getFile(logDirectory).listFiles();
    Arrays.sort(logFiles,
        Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace(WAL_FILE_NAME, ""))));
    return new MultiFileLogReader(logFiles, DifferentialSingleFileLogReader::new);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
