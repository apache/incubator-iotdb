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
package org.apache.iotdb.db.qp.logical;

import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.sys.CreateIndexOperator;
import org.apache.iotdb.db.qp.logical.sys.DropIndexOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.index.common.IndexConstant.BLOCK_SIZE;
import static org.apache.iotdb.db.index.common.IndexConstant.PAA_DIM;
import static org.apache.iotdb.db.index.common.IndexConstant.PATTERN;
import static org.apache.iotdb.db.index.common.IndexConstant.THRESHOLD;
import static org.apache.iotdb.db.index.common.IndexConstant.TOP_K;

public class IndexLogicalPlanTest {

  @Before
  public void before() {}

  @Test
  public void testParseCreateIndexWholeMatching() {
    String sqlStr =
        "CREATE INDEX ON root.Ery.*.Glu WHERE time > 50 WITH INDEX=RTREE_PAA, PAA_dim=8";
    Operator op = LogicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(CreateIndexOperator.class, op.getClass());
    CreateIndexOperator createOperator = (CreateIndexOperator) op;
    Assert.assertEquals(OperatorType.CREATE_INDEX, createOperator.getType());
    Assert.assertNotNull(createOperator.getPaths());
    Assert.assertEquals(
        "PartialPath{fullPath=root.Ery.*.Glu, majorVersion=0, minorVersion=0}",
        createOperator.getPaths().get(0).toString());
    Assert.assertEquals(IndexType.RTREE_PAA, createOperator.getIndexType());
    Assert.assertEquals(50, createOperator.getTime());
    Assert.assertEquals(1, createOperator.getProps().size());
    Assert.assertEquals("8", createOperator.getProps().get(PAA_DIM));
  }

  @Test
  public void testParseCreateIndexSubMatching() {
    String sqlStr = "CREATE INDEX ON root.Wind.AZQ02.Speed WITH INDEX=ELB_INDEX, BLOCK_SIZE=5";
    Operator op = LogicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(CreateIndexOperator.class, op.getClass());
    CreateIndexOperator createOperator = (CreateIndexOperator) op;
    Assert.assertEquals(OperatorType.CREATE_INDEX, createOperator.getType());
    Assert.assertNotNull(createOperator.getPaths());
    Assert.assertEquals(
        "PartialPath{fullPath=root.Wind.AZQ02.Speed, majorVersion=0, minorVersion=0}",
        createOperator.getPaths().get(0).toString());
    Assert.assertEquals(IndexType.ELB_INDEX, createOperator.getIndexType());
    Assert.assertEquals(1, createOperator.getProps().size());
    Assert.assertEquals("5", createOperator.getProps().get(BLOCK_SIZE));
  }

  @Test
  public void testParseDropIndexWholeMatching() {
    String sqlStr = "DROP INDEX RTREE_PAA ON root.Ery.*.Glu";
    Operator op = LogicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(DropIndexOperator.class, op.getClass());
    DropIndexOperator dropIndexOperator = (DropIndexOperator) op;
    Assert.assertEquals(OperatorType.DROP_INDEX, dropIndexOperator.getType());
    Assert.assertNotNull(dropIndexOperator.getPaths());
    Assert.assertEquals(
        "PartialPath{fullPath=root.Ery.*.Glu, majorVersion=0, minorVersion=0}",
        dropIndexOperator.getPaths().get(0).toString());
    Assert.assertEquals(IndexType.RTREE_PAA, dropIndexOperator.getIndexType());
  }

  @Test
  public void testParseDropIndexSubMatching() {
    String sqlStr = "DROP INDEX ELB_INDEX ON root.Wind.AZQ02.Speed";
    Operator op = LogicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(DropIndexOperator.class, op.getClass());
    DropIndexOperator dropIndexOperator = (DropIndexOperator) op;
    Assert.assertEquals(OperatorType.DROP_INDEX, dropIndexOperator.getType());
    Assert.assertNotNull(dropIndexOperator.getPaths());
    Assert.assertEquals(
        "PartialPath{fullPath=root.Wind.AZQ02.Speed, majorVersion=0, minorVersion=0}",
        dropIndexOperator.getPaths().get(0).toString());
    Assert.assertEquals(IndexType.ELB_INDEX, dropIndexOperator.getIndexType());
  }

  @Test
  public void testParseQueryIndexWholeMatching() {
    String sqlStr =
        "SELECT TOP 2 Glu FROM root.Ery.* WHERE Glu LIKE (0, 120, 20, 80, 120, 100, 80, 0)";
    Operator op = LogicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, op.getClass());
    QueryOperator queryOperator = (QueryOperator) op;
    Assert.assertEquals(OperatorType.QUERY, queryOperator.getType());
    Assert.assertEquals(
        "Glu",
        queryOperator.getSelectComponent().getResultColumns().get(0).getExpression().toString());
    Assert.assertEquals(
        "Glu",
        queryOperator.getSelectComponent().getResultColumns().get(0).getExpression().toString());
    Assert.assertEquals(
        "root.Ery.*", queryOperator.getFromComponent().getPrefixPaths().get(0).getFullPath());
    Assert.assertEquals(IndexType.RTREE_PAA, queryOperator.getIndexType());
    Assert.assertEquals(2, queryOperator.getProps().size());
    Assert.assertEquals(2, (int) queryOperator.getProps().get(TOP_K));
    Assert.assertEquals(
        "[0.0, 120.0, 20.0, 80.0, 120.0, 100.0, 80.0, 0.0]",
        Arrays.toString((double[]) queryOperator.getProps().get(PATTERN)));
  }

  @Test
  public void testParseQueryIndexSubMatching() {
    String sqlStr =
        "SELECT Speed.* FROM root.Wind.AZQ02 WHERE Speed "
            + "CONTAIN (15, 14, 12, 12, 12, 11) WITH TOLERANCE 1 "
            + "CONCAT (10, 20, 25, 24, 14, 8) WITH TOLERANCE 2 "
            + "CONCAT  (8, 9, 10, 14, 15, 15) WITH TOLERANCE 1";
    Operator op = LogicalGenerator.generate(sqlStr, ZoneId.systemDefault());
    Assert.assertEquals(QueryOperator.class, op.getClass());
    QueryOperator queryOperator = (QueryOperator) op;
    Assert.assertEquals(OperatorType.QUERY, queryOperator.getType());
    Assert.assertEquals(
        "Speed",
        queryOperator.getSelectComponent().getResultColumns().get(0).getExpression().toString());
    Assert.assertEquals(
        "Speed",
        queryOperator.getSelectComponent().getResultColumns().get(0).getExpression().toString());
    Assert.assertEquals(
        "root.Wind.AZQ02", queryOperator.getFromComponent().getPrefixPaths().get(0).getFullPath());
    Assert.assertEquals(IndexType.ELB_INDEX, queryOperator.getIndexType());
    Assert.assertEquals(2, queryOperator.getProps().size());
    Assert.assertEquals("[1.0, 2.0, 1.0]", queryOperator.getProps().get(THRESHOLD).toString());
    Assert.assertTrue(queryOperator.getProps().get(PATTERN) instanceof List);
    List pattern = (List) queryOperator.getProps().get(PATTERN);
    Assert.assertEquals(
        "[15.0, 14.0, 12.0, 12.0, 12.0, 11.0]", Arrays.toString((double[]) pattern.get(0)));
    Assert.assertEquals(
        "[10.0, 20.0, 25.0, 24.0, 14.0, 8.0]", Arrays.toString((double[]) pattern.get(1)));
    Assert.assertEquals(
        "[8.0, 9.0, 10.0, 14.0, 15.0, 15.0]", Arrays.toString((double[]) pattern.get(2)));
  }
}
