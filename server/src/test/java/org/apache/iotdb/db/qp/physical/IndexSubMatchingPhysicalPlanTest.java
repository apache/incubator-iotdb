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
package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.DropIndexPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;
import static org.junit.Assert.assertEquals;

/** refer to org.apache.iotdb.db.qp.plan.PhysicalPlanTest */
public class IndexSubMatchingPhysicalPlanTest {

  private Planner processor = new Planner();

  @Before
  public void before() throws MetadataException {
    MManager.getInstance().init();
    MManager.getInstance().setStorageGroup(new PartialPath("root.Wind"));
    MManager.getInstance()
        .createTimeseries(
            new PartialPath("root.Wind.AZQ02.Speed"),
            TSDataType.FLOAT,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null);
  }

  @After
  public void clean() throws IOException {
    MManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testCreateIndex() throws QueryProcessException {
    String sqlStr = "CREATE INDEX ON root.Wind.AZQ02.Speed WITH INDEX=ELB_INDEX, BLOCK_SIZE=5";

    Planner processor = new Planner();
    CreateIndexPlan plan = (CreateIndexPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    System.out.println(plan);
    assertEquals(
        "paths: [PartialPath{fullPath=root.Wind.AZQ02.Speed, majorVersion=0, minorVersion=0}], index type: ELB_INDEX, start time: 0, props: {BLOCK_SIZE=5}",
        plan.toString());
  }

  @Test
  public void testDropIndex() throws QueryProcessException {
    String sqlStr = "DROP INDEX ELB_INDEX ON root.Wind.AZQ02.Speed";
    Planner processor = new Planner();
    DropIndexPlan plan = (DropIndexPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals(
        "paths: [PartialPath{fullPath=root.Wind.AZQ02.Speed, majorVersion=0, minorVersion=0}], index type: ELB_INDEX",
        plan.toString());
  }

  @Test
  public void testQueryIndex() throws QueryProcessException {
    String sqlStr =
        "SELECT Speed.* FROM root.Wind.AZQ02 WHERE Speed "
            + "CONTAIN (15, 14, 12, 12, 12, 11) WITH TOLERANCE 1 "
            + "CONCAT (10, 20, 25, 24, 14, 8) WITH TOLERANCE 2 "
            + "CONCAT  (8, 9, 10, 14, 15, 15) WITH TOLERANCE 1";

    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    Assert.assertEquals(QueryIndexPlan.class, plan.getClass());
    QueryIndexPlan queryIndexPlan = (QueryIndexPlan) plan;
    Assert.assertEquals(OperatorType.QUERY_INDEX, queryIndexPlan.getOperatorType());
    Assert.assertEquals(IndexType.ELB_INDEX, queryIndexPlan.getIndexType());
    Assert.assertEquals(1, queryIndexPlan.getPaths().size());
    Assert.assertEquals("root.Wind.AZQ02.Speed", queryIndexPlan.getPaths().get(0).getFullPath());
    Assert.assertEquals(2, queryIndexPlan.getProps().size());
    Assert.assertEquals("[1.0, 2.0, 1.0]", queryIndexPlan.getProps().get(THRESHOLD).toString());
    Assert.assertTrue(queryIndexPlan.getProps().get(PATTERN) instanceof List);
    List pattern = (List) queryIndexPlan.getProps().get(PATTERN);
    Assert.assertEquals(
        "[15.0, 14.0, 12.0, 12.0, 12.0, 11.0]", Arrays.toString((double[]) pattern.get(0)));
    Assert.assertEquals(
        "[10.0, 20.0, 25.0, 24.0, 14.0, 8.0]", Arrays.toString((double[]) pattern.get(1)));
    Assert.assertEquals(
        "[8.0, 9.0, 10.0, 14.0, 15.0, 15.0]", Arrays.toString((double[]) pattern.get(2)));
  }

  @Test
  public void testCreateIndexSerialize()
      throws QueryProcessException, IOException, IllegalPathException {
    String sqlStr = "CREATE INDEX ON root.Wind.AZQ02.Speed WITH INDEX=ELB_INDEX, BLOCK_SIZE=5";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      plan.serialize(dataOutputStream);
      ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
      PhysicalPlan planB = PhysicalPlan.Factory.create(buffer);
      assertEquals(plan, planB);
    }

    ByteBuffer buffer = ByteBuffer.allocate(4096);
    plan.serialize(buffer);
    buffer.flip();
    PhysicalPlan planB = PhysicalPlan.Factory.create(buffer);
    assertEquals(plan, planB);
  }

  @Test
  public void testDropIndexSerialize()
      throws QueryProcessException, IOException, IllegalPathException {
    String sqlStr = "DROP INDEX ELB_INDEX ON root.Wind.AZQ02.Speed";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      plan.serialize(dataOutputStream);
      ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
      PhysicalPlan planB = PhysicalPlan.Factory.create(buffer);
      assertEquals(plan, planB);
    }

    ByteBuffer buffer = ByteBuffer.allocate(4096);
    plan.serialize(buffer);
    buffer.flip();
    PhysicalPlan planB = PhysicalPlan.Factory.create(buffer);
    assertEquals(plan, planB);
  }
}
