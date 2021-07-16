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

package org.apache.iotdb.cluster.query.groupby;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;

public class ClusterGroupByNoVFilterDataSetTest extends BaseQueryTest {

  @Test
  public void test()
      throws StorageEngineException, IOException, QueryProcessException, IllegalPathException {
    QueryContext queryContext =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    try {
      GroupByTimePlan groupByPlan = new GroupByTimePlan();
      List<PartialPath> pathList = new ArrayList<>();
      List<TSDataType> dataTypes = new ArrayList<>();
      List<String> aggregations = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        pathList.add(new PartialPath(TestUtils.getTestSeries(i, 0)));
        dataTypes.add(TSDataType.DOUBLE);
        aggregations.add(SQLConstant.COUNT);
      }
      groupByPlan.setPaths(pathList);
      groupByPlan.setDeduplicatedPathsAndUpdate(pathList);
      groupByPlan.setDataTypes(dataTypes);
      groupByPlan.setDeduplicatedDataTypes(dataTypes);
      groupByPlan.setAggregations(aggregations);
      groupByPlan.setDeduplicatedAggregations(aggregations);

      groupByPlan.setStartTime(0);
      groupByPlan.setEndTime(20);
      groupByPlan.setSlidingStep(5);
      groupByPlan.setInterval(5);
      groupByPlan.setExpression(new GlobalTimeExpression(new GroupByFilter(5, 5, 0, 20)));

      ClusterGroupByNoVFilterDataSet dataSet =
          new ClusterGroupByNoVFilterDataSet(queryContext, groupByPlan, testMetaMember);

      Object[][] answers =
          new Object[][] {
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
            new Object[] {5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0},
          };
      for (Object[] answer : answers) {
        checkDoubleDataset(dataSet, answer);
      }
      assertFalse(dataSet.hasNext());
    } finally {
      QueryResourceManager.getInstance().endQuery(queryContext.getQueryId());
    }
  }
}
