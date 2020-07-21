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
package org.apache.iotdb.db.index;

import org.apache.iotdb.db.exception.index.IndexException;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;

public class PisaIndexNode {

  private long leafNumber;
  private long nodeNumber;
  private int level;
  private boolean isInMem;
  private Statistics statistics;

  public PisaIndexNode(long leafNumber, Statistics statistics) {
    this.leafNumber = leafNumber;
    this.nodeNumber = getNodeNumberByLeafNumber(leafNumber);
    this.level = 1;
    this.statistics = statistics;
    this.isInMem = true;
  }

  public PisaIndexNode(long nodeNumber, int level, Statistics statistics) {
    this.nodeNumber = nodeNumber;
    this.level = level;
    this.statistics = statistics;
    this.isInMem = true;
  }

  public long getLeafNumber() throws IndexException {
    if (level != 0) {
      throw new IndexException("Internal node doesn't have leaf number");
    }
    return leafNumber;
  }

  public long getNodeNumber() {
    return nodeNumber;
  }

  public Statistics getStatistics() {
    return statistics;
  }

  public int getLevel() {
    return level;
  }

  public boolean isInMem() {
    return isInMem;
  }

  public void setInMem(boolean inMem) {
    isInMem = inMem;
  }

  public static long getNodeNumberByLeafNumber(long leafNumber) {
    if (leafNumber % 2 == 1) {
      return getRootNodeNumber(leafNumber);
    } else {
      return getRootNodeNumber(leafNumber - 1) + 1;
    }
  }

  public static long getRootNodeNumber(long leafNumber) {
    long count = 0;
    long temp = leafNumber;
    while (temp != 0) {
      temp &= (temp - 1);
      count++;
    }
    return (leafNumber << 1) - count;
  }

  public static long getLeafNumberByNodeNumber(long nodeNumber) {
    long i = nodeNumber / 2;
    while (i <= nodeNumber) {
      if (getNodeNumberByLeafNumber(i) == nodeNumber) {
        return i;
      }
      i++;
    }
    return -1L;
  }

  public static long getLeftestLeafNumber(long rightestLeafNumber) {
    long root = getRootNodeNumber(rightestLeafNumber);
    long depth = root - getNodeNumberByLeafNumber(rightestLeafNumber);
    long nodesOfTree = 2 << depth;
    return getLeafNumberByNodeNumber(root - nodesOfTree + 2L);
  }
}
