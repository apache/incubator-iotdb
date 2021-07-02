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
import org.apache.iotdb.cluster.server.member.BaseMember;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;

/**
 * allNodes: node0, node1... node9 localNode: node0 pathList: root.sg0.s0, root.sg0.s1...
 * root.sg0.s9 (all double type)
 */
public class BaseQueryTest extends BaseMember {

  protected List<PartialPath> pathList;
  protected List<TSDataType> dataTypes;
  protected int defaultCompactionThread =
      IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread();

  protected static void checkAggregations(
      List<AggregateResult> aggregationResults, Object[] answer) {
    Assert.assertEquals(answer.length, aggregationResults.size());
    for (int i = 0; i < aggregationResults.size(); i++) {
      AggregateResult aggregateResult = aggregationResults.get(i);
      if (answer[i] != null) {
        Assert.assertEquals(
            (double) answer[i],
            Double.parseDouble(aggregateResult.getResult().toString()),
            0.00001);
      } else {
        assertNull(aggregateResult.getResult());
      }
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setConcurrentCompactionThread(0);
    super.setUp();
    pathList = new ArrayList<>();
    dataTypes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      pathList.add(new PartialPath(TestUtils.getTestSeries(i, 0)));
      dataTypes.add(TSDataType.DOUBLE);
    }
    NodeStatusManager.getINSTANCE().setMetaGroupMember(testMetaMember);
    TestUtils.prepareData();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    NodeStatusManager.getINSTANCE().setMetaGroupMember(null);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setConcurrentCompactionThread(defaultCompactionThread);
  }

  void checkSequentialDataset(QueryDataSet dataSet, int offset, int size) throws IOException {
    for (int i = offset; i < offset + size; i++) {
      assertTrue(dataSet.hasNext());
      RowRecord record = dataSet.next();
      assertEquals(i, record.getTimestamp());
      assertEquals(10, record.getFields().size());
      for (int j = 0; j < 10; j++) {
        assertEquals(i * 1.0, record.getFields().get(j).getDoubleV(), 0.00001);
      }
    }
    assertFalse(dataSet.hasNext());
  }

  protected void checkDoubleDataset(QueryDataSet queryDataSet, Object[] answers)
      throws IOException {
    Assert.assertTrue(queryDataSet.hasNext());
    RowRecord record = queryDataSet.next();
    List<Field> fields = record.getFields();
    Assert.assertEquals(answers.length, fields.size());
    for (int i = 0; i < answers.length; i++) {
      if (answers[i] != null) {
        Assert.assertEquals(
            (double) answers[i], Double.parseDouble(fields.get(i).getStringValue()), 0.000001);
      } else {
        assertNull(fields.get(i));
      }
    }
  }
}
