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
package org.apache.iotdb.jdbc;

import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ListDataSet extends QueryDataSet {
  private final List<RowRecord> records = new ArrayList<>();
  private int index = 0;

  @Override
  public boolean hasNextWithoutConstraint() {
    return index < records.size();
  }

  @Override
  public RowRecord nextWithoutConstraint() {
    return records.get(index++);
  }

  public void putRecord(RowRecord newRecord) {
    records.add(newRecord);
  }

  public void sortByTime() {
    records.sort((o1, o2) -> Long.compare(o2.getTimestamp(), o1.getTimestamp()));
  }

  public void sort(Comparator<RowRecord> c) {
    records.sort(c);
  }
}
