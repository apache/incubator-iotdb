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
package org.apache.iotdb.db.qp.physical.crud;

import static org.apache.iotdb.db.qp.constant.SQLConstant.LINE_FEED_SIGNAL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;

public class UpdatePlan extends PhysicalPlan {

  private List<Pair<Long, Long>> intervals = new ArrayList<>();
  private String value;
  private PartialPath path;

  public UpdatePlan() {
    super(false, Operator.OperatorType.UPDATE);
  }

  /**
   * Construct function for UpdatePlan.
   *
   * @param startTime -start time
   * @param endTime   -end time
   * @param value     -value
   * @param path      -path
   */
  public UpdatePlan(long startTime, long endTime, String value, PartialPath path) {
    super(false, Operator.OperatorType.UPDATE);
    setValue(value);
    setPath(path);
    addInterval(new Pair<>(startTime, endTime));
  }

  /**
   * Construct function for UpdatePlan.
   *
   * @param list  -list to initial intervals
   * @param value -value
   * @param path  -path
   */
  public UpdatePlan(List<Pair<Long, Long>> list, String value, PartialPath path) {
    super(false, Operator.OperatorType.UPDATE);
    setValue(value);
    setPath(path);
    intervals = list;
  }

  public List<Pair<Long, Long>> getIntervals() {
    return intervals;
  }

  public void addInterval(Pair<Long, Long> interval) {
    this.intervals.add(interval);
  }

  public void addIntervals(List<Pair<Long, Long>> intervals) {
    this.intervals.addAll(intervals);
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  @Override
  public List<PartialPath> getPaths() {
    return path != null ? Collections.singletonList(path) : Collections.emptyList();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdatePlan that = (UpdatePlan) o;
    return Objects.equals(intervals, that.intervals) && Objects.equals(value, that.value)
        && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, path);
  }

}
