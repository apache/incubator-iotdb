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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.crud.FromComponent;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectComponent;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.expression.unary.TimeSeriesOperand;

import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ClusterPhysicalGeneratorTest extends BaseQueryTest {

  private ClusterPhysicalGenerator physicalGenerator;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    physicalGenerator = new ClusterPhysicalGenerator();
  }

  @Test
  public void test() throws QueryProcessException, IllegalPathException {
    QueryOperator operator = new QueryOperator();

    SelectComponent selectComponent = new SelectComponent(ZoneId.systemDefault());
    List<ResultColumn> resultColumns = new ArrayList<>();
    for (PartialPath partialPath : pathList) {
      resultColumns.add(new ResultColumn(new TimeSeriesOperand(partialPath)));
    }
    selectComponent.setResultColumns(resultColumns);
    FromComponent fromComponent = new FromComponent();
    fromComponent.addPrefixTablePath(new PartialPath(TestUtils.getTestSg(0)));

    operator.setSelectComponent(selectComponent);
    operator.setFromComponent(fromComponent);
    RawDataQueryPlan plan = (RawDataQueryPlan) physicalGenerator.transformToPhysicalPlan(operator);

    assertEquals(pathList, plan.getDeduplicatedPaths());
    assertEquals(dataTypes, plan.getDeduplicatedDataTypes());
  }
}
