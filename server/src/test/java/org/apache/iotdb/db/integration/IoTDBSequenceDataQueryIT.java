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
package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.QueryRouter;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test. In this test case, no unseq insert data.
 */
public class IoTDBSequenceDataQueryIT {

  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int maxNumberOfPointsInPage;
  private static int pageSizeInByte;
  private static int groupSizeInByte;

  // count : d0s0 >= 14
  private static int count = 0;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();

    // use small page setting
    // origin value
    maxNumberOfPointsInPage = tsFileConfig.getMaxNumberOfPointsInPage();
    pageSizeInByte = tsFileConfig.getPageSizeInByte();
    groupSizeInByte = tsFileConfig.getGroupSizeInByte();

    // new value
    tsFileConfig.setMaxNumberOfPointsInPage(100);
    tsFileConfig.setPageSizeInByte(1024 * 1024 * 150);
    tsFileConfig.setGroupSizeInByte(1024 * 1024 * 100);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1024 * 1024 * 100);

    EnvironmentUtils.envSetUp();

    insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    // recovery value
    tsFileConfig.setMaxNumberOfPointsInPage(maxNumberOfPointsInPage);
    tsFileConfig.setPageSizeInByte(pageSizeInByte);
    tsFileConfig.setGroupSizeInByte(groupSizeInByte);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(groupSizeInByte);

    EnvironmentUtils.cleanEnv();
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      // create storage group and measurement
      for (String sql : TestConstant.create_sql) {
        statement.execute(sql);
      }

      // insert data (time from 300-999)
      for (long time = 300; time < 1000; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[(int) time % 5]);
        statement.execute(sql);

        if (time % 17 >= 14) {
          count++;
        }
      }

      statement.execute("flush");

      // insert data (time from 1200-1499)
      for (long time = 1200; time < 1500; time++) {
        String sql;
        if (time % 2 == 0) {
          sql =
              String.format(
                  "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 17);
          statement.execute(sql);
          sql =
              String.format(
                  "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 29);
          statement.execute(sql);
          if (time % 17 >= 14) {
            count++;
          }
        }
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 31);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[(int) time % 5]);
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void readWithoutFilterTest()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {

    QueryRouter queryRouter = new QueryRouter();
    List<PartialPath> pathList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0));
    dataTypes.add(TSDataType.INT32);
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s1));
    dataTypes.add(TSDataType.INT64);
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s2));
    dataTypes.add(TSDataType.FLOAT);
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s3));
    dataTypes.add(TSDataType.TEXT);
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s4));
    dataTypes.add(TSDataType.BOOLEAN);
    pathList.add(
        new PartialPath(TestConstant.d1 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0));
    dataTypes.add(TSDataType.INT32);
    pathList.add(
        new PartialPath(TestConstant.d1 + TsFileConstant.PATH_SEPARATOR + TestConstant.s1));
    dataTypes.add(TSDataType.INT64);

    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId(true);
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);
    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPaths(pathList);
    QueryDataSet queryDataSet = queryRouter.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      queryDataSet.next();
      cnt++;
    }
    assertEquals(1000, cnt);

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }

  @Test
  public void readWithTimeFilterTest()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {
    QueryRouter queryRouter = new QueryRouter();
    List<PartialPath> pathList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0));
    dataTypes.add(TSDataType.INT32);
    pathList.add(
        new PartialPath(TestConstant.d1 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0));
    dataTypes.add(TSDataType.INT32);
    pathList.add(
        new PartialPath(TestConstant.d1 + TsFileConstant.PATH_SEPARATOR + TestConstant.s1));
    dataTypes.add(TSDataType.INT64);

    GlobalTimeExpression globalTimeExpression = new GlobalTimeExpression(TimeFilter.gtEq(800L));
    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId(true);
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPaths(pathList);
    queryPlan.setExpression(globalTimeExpression);
    QueryDataSet queryDataSet = queryRouter.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      RowRecord rowRecord = queryDataSet.next();
      String value = rowRecord.getFields().get(0).getStringValue();
      long time = rowRecord.getTimestamp();
      assertEquals("" + time % 17, value);
      cnt++;
    }
    assertEquals(350, cnt);

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }

  @Test
  public void readWithValueFilterTest()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {
    // select * from root where root.vehicle.d0.s0 >=14
    QueryRouter queryRouter = new QueryRouter();
    List<PartialPath> pathList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0));
    dataTypes.add(TSDataType.INT32);
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s1));
    dataTypes.add(TSDataType.INT64);
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s2));
    dataTypes.add(TSDataType.FLOAT);
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s3));
    dataTypes.add(TSDataType.TEXT);
    pathList.add(
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s4));
    dataTypes.add(TSDataType.BOOLEAN);
    pathList.add(
        new PartialPath(TestConstant.d1 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0));
    dataTypes.add(TSDataType.INT32);
    pathList.add(
        new PartialPath(TestConstant.d1 + TsFileConstant.PATH_SEPARATOR + TestConstant.s1));
    dataTypes.add(TSDataType.INT64);

    Path queryPath =
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0);
    SingleSeriesExpression singleSeriesExpression =
        new SingleSeriesExpression(queryPath, ValueFilter.gtEq(14));

    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId(true);
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPaths(pathList);
    queryPlan.setExpression(singleSeriesExpression);
    QueryDataSet queryDataSet = queryRouter.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      queryDataSet.next();
      cnt++;
    }
    assertEquals(count, cnt);

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }
}
