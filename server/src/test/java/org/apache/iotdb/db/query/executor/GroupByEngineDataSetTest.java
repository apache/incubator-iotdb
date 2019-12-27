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
package org.apache.iotdb.db.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.path.PathException;
import org.apache.iotdb.db.qp.physical.crud.GroupByPlan;
import org.apache.iotdb.db.query.aggregation.impl.CountAggrFunc;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

public class GroupByEngineDataSetTest {

  @Test
  public void test1() throws IOException, PathException, StorageEngineException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 5;
    long startTime = 8;
    long endTime = 8 + 4 * 5 + 3;

    long[] startTimeArray = {8, 13, 18, 23, 28};
    long[] endTimeArray = {11, 16, 21, 26, 31};

    GroupByPlan groupByPlan = new GroupByPlan();
    groupByPlan.setUnit(unit);
    groupByPlan.setSlidingStep(slidingStep);
    groupByPlan.setStartTime(startTime);
    groupByPlan.setEndTime(endTime);

    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByPlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertTrue(cnt < startTimeArray.length);
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  @Test
  public void test2() throws IOException, PathException, StorageEngineException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 5;
    long startTime = 8;
    long endTime = 8 + 4 * 5 + 2;

    long[] startTimeArray = {8, 13, 18, 23, 28};
    long[] endTimeArray = {11, 16, 21, 26, 31};

    GroupByPlan groupByPlan = new GroupByPlan();
    groupByPlan.setUnit(unit);
    groupByPlan.setSlidingStep(slidingStep);
    groupByPlan.setStartTime(startTime);
    groupByPlan.setEndTime(endTime);
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByPlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertTrue(cnt < startTimeArray.length);
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  @Test
  public void test3() throws IOException, PathException, StorageEngineException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 3;
    long startTime = 8;
    long endTime = 8 + 5 * 3;

    long[] startTimeArray = {8, 11, 14, 17, 20, 23};
    long[] endTimeArray = {11, 14, 17, 20, 23, 24};

    QueryContext context = new QueryContext(queryId);
    GroupByPlan groupByPlan = new GroupByPlan();
    groupByPlan.setUnit(unit);
    groupByPlan.setSlidingStep(slidingStep);
    groupByPlan.setStartTime(startTime);
    groupByPlan.setEndTime(endTime);
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(context, groupByPlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertTrue(cnt < startTimeArray.length);
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  @Test
  public void test4() throws IOException, PathException, StorageEngineException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 3;
    long startTime = 8;
    long endTime = 8 + 5 * 3 - 1;

    long[] startTimeArray = {8, 11, 14, 17, 20};
    long[] endTimeArray = {11, 14, 17, 20, 23};

    GroupByPlan groupByPlan = new GroupByPlan();
    groupByPlan.setUnit(unit);
    groupByPlan.setSlidingStep(slidingStep);
    groupByPlan.setStartTime(startTime);
    groupByPlan.setEndTime(endTime);
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByPlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertTrue(cnt < startTimeArray.length);
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  @Test
  public void test5() throws IOException, PathException, StorageEngineException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 3;
    long startTime = 8;
    long endTime = 8 + 5 * 3 + 1;

    long[] startTimeArray = {8, 11, 14, 17, 20, 23};
    long[] endTimeArray = {11, 14, 17, 20, 23, 25};

    GroupByPlan groupByPlan = new GroupByPlan();
    groupByPlan.setUnit(unit);
    groupByPlan.setSlidingStep(slidingStep);
    groupByPlan.setStartTime(startTime);
    groupByPlan.setEndTime(endTime);
    ArrayList<Object> aggrList = new ArrayList<>();
    aggrList.add(new CountAggrFunc());
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByPlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertTrue(cnt < startTimeArray.length);
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }
}