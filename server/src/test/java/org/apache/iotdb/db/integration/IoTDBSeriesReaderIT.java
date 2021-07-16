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
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
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
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
public class IoTDBSeriesReaderIT {

  private static TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
  private static int pageSizeInByte;
  private static int groupSizeInByte;
  private static long prevPartitionInterval;
  private static int prevChunkMergePointThreshold;

  private static Connection connection;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();

    // use small page setting
    // origin value
    pageSizeInByte = tsFileConfig.getPageSizeInByte();
    groupSizeInByte = tsFileConfig.getGroupSizeInByte();

    // new value
    tsFileConfig.setMaxNumberOfPointsInPage(1000);
    tsFileConfig.setPageSizeInByte(1024 * 1024 * 150);
    tsFileConfig.setGroupSizeInByte(1024 * 1024 * 150);
    prevChunkMergePointThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(Integer.MAX_VALUE);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(1024 * 16);

    // test result of IBatchReader should not cross partition
    prevPartitionInterval = IoTDBDescriptor.getInstance().getConfig().getPartitionInterval();
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(2);

    EnvironmentUtils.envSetUp();

    insertData();
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    connection.close();
    // recovery value
    tsFileConfig.setMaxNumberOfPointsInPage(1024 * 1024 * 150);
    tsFileConfig.setPageSizeInByte(pageSizeInByte);
    tsFileConfig.setGroupSizeInByte(groupSizeInByte);

    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.LEVEL_COMPACTION);
    IoTDBDescriptor.getInstance().getConfig().setMemtableSizeThreshold(groupSizeInByte);
    IoTDBDescriptor.getInstance().getConfig().setPartitionInterval(prevPartitionInterval);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevChunkMergePointThreshold);
  }

  private static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {

      for (String sql : TestConstant.create_sql) {
        statement.execute(sql);
      }

      // insert large amount of data time range : 3000 ~ 13600
      for (int time = 3000; time < 13600; time++) {
        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 100);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 17);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 22);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[time % 5]);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s4) values(%s, %s)",
                time, TestConstant.booleanValue[time % 2]);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, time);
        statement.execute(sql);
      }

      // statement.execute("flush");

      // insert large amount of data time range : 13700 ~ 24000
      for (int time = 13700; time < 24000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 70);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 40);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 123);
        statement.execute(sql);
      }

      statement.execute("merge");

      // buffwrite data, unsealed file
      for (int time = 100000; time < 101000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time % 20);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time % 30);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time % 77);
        statement.execute(sql);
      }

      // sequential data, memory data
      for (int time = 200000; time < 201000; time++) {

        String sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, -time % 20);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, -time % 30);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, -time % 77);
        statement.execute(sql);
      }

      statement.execute("flush");
      // unsequence insert, time < 3000
      for (int time = 2000; time < 2500; time++) {

        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, time);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, time + 1);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, time + 2);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')",
                time, TestConstant.stringValue[time % 5]);
        statement.execute(sql);
      }

      for (int time = 100000; time < 100500; time++) {
        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 666);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 777);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 888);
        statement.execute(sql);
      }

      statement.execute("flush");
      // unsequence insert, time > 200000
      for (int time = 200900; time < 201000; time++) {

        String sql =
            String.format("insert into root.vehicle.d0(timestamp,s0) values(%s,%s)", time, 6666);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s1) values(%s,%s)", time, 7777);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s2) values(%s,%s)", time, 8888);
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s3) values(%s,'%s')", time, "goodman");
        statement.execute(sql);
        sql =
            String.format(
                "insert into root.vehicle.d0(timestamp,s4) values(%s, %s)",
                time, TestConstant.booleanValue[time % 2]);
        statement.execute(sql);
        sql = String.format("insert into root.vehicle.d0(timestamp,s5) values(%s, %s)", time, 9999);
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllTest()
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
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s5));
    dataTypes.add(TSDataType.DOUBLE);
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
    assertEquals(23400, cnt);

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }

  @Test
  public void selectOneSeriesWithValueFilterTest()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {
    QueryRouter queryRouter = new QueryRouter();
    List<PartialPath> pathList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    PartialPath p =
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0);
    pathList.add(p);
    dataTypes.add(TSDataType.INT32);
    SingleSeriesExpression singleSeriesExpression =
        new SingleSeriesExpression(p, ValueFilter.gtEq(20));

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
    assertEquals(16940, cnt);

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }

  @Test
  public void seriesTimeDigestReadTest()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {
    QueryRouter queryRouter = new QueryRouter();
    PartialPath path =
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0);
    List<TSDataType> dataTypes = Collections.singletonList(TSDataType.INT32);
    SingleSeriesExpression expression = new SingleSeriesExpression(path, TimeFilter.gt(22987L));

    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId(true);
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPaths(Collections.singletonList(path));
    queryPlan.setExpression(expression);
    QueryDataSet queryDataSet = queryRouter.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      queryDataSet.next();
      cnt++;
    }
    assertEquals(3012, cnt);

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }

  @Test
  public void crossSeriesReadUpdateTest()
      throws IOException, StorageEngineException, QueryProcessException, IllegalPathException {
    QueryRouter queryRouter = new QueryRouter();
    PartialPath path1 =
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s0);
    PartialPath path2 =
        new PartialPath(TestConstant.d0 + TsFileConstant.PATH_SEPARATOR + TestConstant.s1);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();

    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(path1);
    pathList.add(path2);
    queryPlan.setDeduplicatedPaths(pathList);

    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.INT64);
    queryPlan.setDeduplicatedDataTypes(dataTypes);

    TEST_QUERY_JOB_ID = QueryResourceManager.getInstance().assignQueryId(true);
    TEST_QUERY_CONTEXT = new QueryContext(TEST_QUERY_JOB_ID);

    SingleSeriesExpression singleSeriesExpression =
        new SingleSeriesExpression(path1, ValueFilter.lt(111));
    queryPlan.setExpression(singleSeriesExpression);

    QueryDataSet queryDataSet = queryRouter.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int cnt = 0;
    while (queryDataSet.hasNext()) {
      queryDataSet.next();
      cnt++;
    }

    assertEquals(22300, cnt);

    QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
  }

  @Test
  public void queryEmptySeriesTest() throws SQLException {
    Statement statement = connection.createStatement();
    statement.execute(
        "CREATE TIMESERIES root.vehicle.d_empty.s1 WITH DATATYPE=INT64, ENCODING=RLE");
    ResultSet resultSet = statement.executeQuery("select * from root.vehicle.d_empty");
    try {
      assertFalse(resultSet.next());
    } finally {
      resultSet.close();
    }
  }

  /** Test when one un-sequenced file may cover a long time range. */
  @Test
  public void queryWithLongRangeUnSeqTest() throws SQLException {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // make up data
      final String INSERT_TEMPLATE = "insert into root.sg.d1(time, s1) values(%d, %d)";
      final String FLUSH_CMD = "flush";
      for (int i = 1; i <= 10; i++) {
        statement.execute(String.format(INSERT_TEMPLATE, i, i));
      }
      statement.execute(FLUSH_CMD);
      for (int i = 12; i <= 20; i++) {
        statement.execute(String.format(INSERT_TEMPLATE, i, i));
      }
      statement.execute(FLUSH_CMD);
      for (int i = 21; i <= 110; i++) {
        statement.execute(String.format(INSERT_TEMPLATE, i, i));
        if (i % 10 == 0) {
          statement.execute(FLUSH_CMD);
        }
      }
      // unSeq from here
      for (int i = 11; i <= 101; i += 10) {
        statement.execute(String.format(INSERT_TEMPLATE, i, i));
      }
      statement.execute(FLUSH_CMD);

      // query from here
      ResultSet resultSet = statement.executeQuery("select s1 from root.sg.d1 where time > 10");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(100, cnt);
    }
  }
}
