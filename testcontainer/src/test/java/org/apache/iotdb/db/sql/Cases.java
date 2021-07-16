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
package org.apache.iotdb.db.sql;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.DropFunctionPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowFunctionsPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class Cases {

  protected Statement writeStatement;
  protected Connection writeConnection;
  protected Statement[] readStatements;
  protected Connection[] readConnections;
  protected Session session;
  private final Planner processor = new Planner();

  /** initialize the writeStatement,writeConnection, readStatements and the readConnections. */
  public abstract void init() throws Exception;

  public void clean() throws Exception {
    writeStatement.close();
    writeConnection.close();
    for (Statement statement : readStatements) {
      statement.close();
    }
    for (Connection connection : readConnections) {
      connection.close();
    }
    session.close();
  }

  // if we seperate the test into multiply test() methods, then the docker container have to be
  // built
  // several times. So, if the test cases are not conflict, we can put them into one method.
  // but if you want to avoid other cases' impact, use a seperate test() method.
  @Test
  public void multiCasesTest() throws SQLException {

    String[] timeSeriesArray = {"root.sg1.aa.bb", "root.sg1.aa.bb.cc", "root.sg1.aa"};

    for (String timeSeries : timeSeriesArray) {
      writeStatement.execute(
          String.format(
              "create timeseries %s with datatype=INT64, encoding=PLAIN, compression=SNAPPY",
              timeSeries));
    }
    ResultSet resultSet = null;
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("show timeseries");
      Set<String> result = new HashSet<>();
      while (resultSet.next()) {
        result.add(resultSet.getString(1));
      }
      Assert.assertEquals(3, result.size());
      for (String timeseries : timeSeriesArray) {
        Assert.assertTrue(result.contains(timeseries));
      }
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1331
    writeStatement.execute(
        "create timeseries root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE");
    String[] initDataArray = {
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) values(200,20.71)",
      "INSERT INTO root.ln.wf01.wt01(timestamp,temperature) values(220,50.71)"
    };
    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select avg(temperature) from root.ln.wf01.wt01");
      if (resultSet.next()) {
        Assert.assertEquals(35.71, resultSet.getDouble(1), 0.01);
      } else {
        Assert.fail("expect 1 result, but get an empty resultSet.");
      }
      Assert.assertFalse(resultSet.next());
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1348
    initDataArray =
        new String[] {
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(250, 10.0)",
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(300, 20.0)",
          "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) values(350, 25.0)"
        };

    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }
    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select last * from root.ln.wf01.wt01;");
      Assert.assertTrue(resultSet.next());
      double last = Double.parseDouble(resultSet.getString(3));
      Assert.assertEquals(25.0, last, 0.1);
      resultSet.close();
    }

    // test https://issues.apache.org/jira/browse/IOTDB-1457
    initDataArray =
        new String[] {
          "INSERT INTO root.ln.wf011.wt0110(timestamp, temperature) values(250, 10.0)",
          "INSERT INTO root.ln.wf011.wt0111(timestamp, temperature) values(300, 20.0)",
          "INSERT INTO root.ln.wf011.wt0112(timestamp, temperature) values(350, 25.0)"
        };

    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }
    try {
      session.executeNonQueryStatement(" delete from root.ln.wf011.*");
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      Assert.assertFalse(e instanceof BatchExecutionException);
    }

    // test dictionary encoding
    writeStatement.execute(
        "create timeseries root.ln.wf01.wt02.city WITH DATATYPE=TEXT, ENCODING=DICTIONARY");
    initDataArray =
        new String[] {
          "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(250, \"Nanjing\")",
          "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(300, \"Nanjing\")",
          "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(350, \"Singapore\")",
          "INSERT INTO root.ln.wf01.wt02(timestamp, city) values(400, \"Shanghai\")"
        };
    for (String initData : initDataArray) {
      writeStatement.execute(initData);
    }

    String[] results = new String[] {"Nanjing", "Nanjing", "Singapore", "Shanghai"};
    for (Statement readStatement : readStatements) {
      resultSet = readStatement.executeQuery("select * from root.ln.wf01.wt02");
      int i = 0;
      while (resultSet.next()) {
        Assert.assertEquals(results[i++], resultSet.getString("root.ln.wf01.wt02.city"));
      }
      Assert.assertFalse(resultSet.next());
      resultSet.close();
    }
  }

  // test https://issues.apache.org/jira/browse/IOTDB-1266
  @Test
  public void showTimeseriesRowsTest() throws SQLException {

    int n = 3000;
    String timeSeriesPrefix = "root.ln.wf01.wt";
    String timeSeriesSuffix = ".temperature WITH DATATYPE=DOUBLE, ENCODING=RLE";
    String timeSeries;
    for (int i = 0; i < n; i++) {
      timeSeries = timeSeriesPrefix + String.valueOf(i) + timeSeriesSuffix;
      writeStatement.execute(String.format("create timeseries %s ", timeSeries));
    }

    // try to read data on each node.
    for (Statement readStatement : readStatements) {
      ResultSet resultSet = readStatement.executeQuery("SHOW TIMESERIES");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(n, cnt);
      resultSet.close();
    }
  }

  //  @Test
  //  public void vectorCountTest() throws IoTDBConnectionException, StatementExecutionException {
  //    List<List<String>> measurementList = new ArrayList<>();
  //    List<String> schemaNames = new ArrayList<>();
  //    List<List<TSEncoding>> encodingList = new ArrayList<>();
  //    List<List<TSDataType>> dataTypeList = new ArrayList<>();
  //    List<CompressionType> compressionTypes = new ArrayList<>();
  //    List<TSDataType> dataTypes = new ArrayList<>();
  //    List<TSEncoding> encodings = new ArrayList<>();
  //    String[] vectorMeasurements = new String[10];
  //
  //    Stream.iterate(0, i -> i + 1)
  //        .limit(10)
  //        .forEach(
  //            i -> {
  //              dataTypes.add(TSDataType.DOUBLE);
  //              vectorMeasurements[i] = "vm" + i;
  //              encodings.add(TSEncoding.RLE);
  //              compressionTypes.add(CompressionType.SNAPPY);
  //            });
  //    schemaNames.add("schema");
  //    encodingList.add(encodings);
  //    dataTypeList.add(dataTypes);
  //    measurementList.add(Arrays.asList(vectorMeasurements));
  //
  //    session.createSchemaTemplate(
  //        "testcontainer",
  //        schemaNames,
  //        measurementList,
  //        dataTypeList,
  //        encodingList,
  //        compressionTypes);
  //    session.setStorageGroup("root.template");
  //    session.setSchemaTemplate("testcontainer", "root.template");
  //
  //    VectorMeasurementSchema vectorMeasurementSchema =
  //        new VectorMeasurementSchema(
  //            "vector", vectorMeasurements, dataTypes.toArray(new TSDataType[0]));
  //
  //    Tablet tablet = new Tablet("root.template.device1.vector",
  //    Arrays.asList(vectorMeasurementSchema));
  //    tablet.setAligned(true);
  //    for (int i = 0; i < 10; i++) {
  //      tablet.addTimestamp(i, i);
  //      for (int j = 0; j < 10; j++) {
  //        tablet.addValue("vm" + j, i, (double) i);
  //        tablet.rowSize++;
  //      }
  //    }
  //    session.insertTablet(tablet);
  //
  //    SessionDataSet sessionDataSet =
  //        session.executeQueryStatement("select count(*) from root.template.device1");
  //    Assert.assertTrue(sessionDataSet.hasNext());
  //    RowRecord next = sessionDataSet.next();
  //    Assert.assertEquals(10, next.getFields().get(0).getLongV());
  //
  //    sessionDataSet = session.executeQueryStatement("select count(vm1) from
  // root.template.device1");
  //    Assert.assertTrue(sessionDataSet.hasNext());
  //    next = sessionDataSet.next();
  //    Assert.assertEquals(10, next.getFields().get(0).getLongV());
  //
  //    sessionDataSet =
  //        session.executeQueryStatement("select count(vm1),count(vm2) from
  // root.template.device1");
  //    Assert.assertTrue(sessionDataSet.hasNext());
  //    next = sessionDataSet.next();
  //    Assert.assertEquals(2, next.getFields().size());
  //    Assert.assertEquals(10, next.getFields().get(0).getLongV());
  //    Assert.assertEquals(10, next.getFields().get(1).getLongV());
  //  }

  @Test
  public void clusterLastQueryTest() throws IoTDBConnectionException, StatementExecutionException {

    session.setStorageGroup("root.sg1");
    session.createTimeseries(
        "root.sg1.d1.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    session.createTimeseries(
        "root.sg1.d2.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);

    insertRecords();

    List<String> paths = new ArrayList<>();

    paths.add("root.sg1.d1.s1");
    paths.add("root.sg1.d2.s1");

    SessionDataSet sessionDataSet = session.executeLastDataQuery(paths);
    sessionDataSet.setFetchSize(1024);

    int count = 0;
    while (sessionDataSet.hasNext()) {
      count++;
      List<Field> fields = sessionDataSet.next().getFields();
      Assert.assertEquals("[root.sg1.d1.s1, 1]", fields.toString());
    }
    Assert.assertEquals(1, count);
    sessionDataSet.closeOperationHandle();
  }

  private void insertRecords() throws IoTDBConnectionException, StatementExecutionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    List<String> deviceIds = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 500; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      deviceIds.add(deviceId);
      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);
      if (time != 0 && time % 100 == 0) {
        session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
        deviceIds.clear();
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
      }
    }

    session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
  }

  @Test
  public void testCreateFunctionPlan1() {
    try {
      PhysicalPlan plan =
          processor.parseSQLToPhysicalPlan(
              "create function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      if (plan.isQuery() || !(plan instanceof CreateFunctionPlan)) {
        Assert.fail();
      }
      CreateFunctionPlan createFunctionPlan = (CreateFunctionPlan) plan;
      Assert.assertEquals("udf", createFunctionPlan.getUdfName());
      Assert.assertEquals(
          "org.apache.iotdb.db.query.udf.example.Adder", createFunctionPlan.getClassName());
      Assert.assertFalse(createFunctionPlan.isTemporary());
    } catch (QueryProcessException e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testCreateFunctionPlan2() { // create temporary function
    try {
      PhysicalPlan plan =
          processor.parseSQLToPhysicalPlan(
              "create temporary function udf as \"org.apache.iotdb.db.query.udf.example.Adder\"");
      if (plan.isQuery() || !(plan instanceof CreateFunctionPlan)) {
        Assert.fail();
      }
      CreateFunctionPlan createFunctionPlan = (CreateFunctionPlan) plan;
      Assert.assertEquals("udf", createFunctionPlan.getUdfName());
      Assert.assertEquals(
          "org.apache.iotdb.db.query.udf.example.Adder", createFunctionPlan.getClassName());
      Assert.assertTrue(createFunctionPlan.isTemporary());
    } catch (QueryProcessException e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testDropFunctionPlan() { // drop function
    try {
      DropFunctionPlan dropFunctionPlan =
          (DropFunctionPlan) processor.parseSQLToPhysicalPlan("drop function udf");
      Assert.assertEquals("udf", dropFunctionPlan.getUdfName());
    } catch (QueryProcessException e) {
      Assert.fail(e.toString());
    }
  }

  @Test
  public void testShowFunction() throws QueryProcessException {
    String sql = "SHOW FUNCTIONS";

    ShowFunctionsPlan plan = (ShowFunctionsPlan) processor.parseSQLToPhysicalPlan(sql);
    Assert.assertTrue(plan.isQuery());
    Assert.assertEquals(ShowPlan.ShowContentType.FUNCTIONS, plan.getShowContentType());
  }

  // test https://issues.apache.org/jira/browse/IOTDB-1407
  @Test
  public void showTimeseriesTagsTest() throws SQLException {
    String createTimeSeries1 =
        "create timeseries root.ln.wf01.wt1 WITH DATATYPE=DOUBLE, ENCODING=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2)";
    String createTimeSeries2 =
        "create timeseries root.ln.wf01.wt2 WITH DATATYPE=DOUBLE, ENCODING=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2)";
    writeStatement.execute(createTimeSeries1);
    writeStatement.execute(createTimeSeries2);
    // try to read data on each node. select .*
    for (Statement readStatement : readStatements) {
      ResultSet resultSet =
          readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.* where tag1=v1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      resultSet.close();
    }

    // try to read data on each node. select from parent series
    for (Statement readStatement : readStatements) {
      ResultSet resultSet =
          readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01 where tag1=v1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      resultSet.close();
    }

    // try to read data on each node. select from one series
    for (Statement readStatement : readStatements) {
      ResultSet resultSet =
          readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.wt1 where tag1=v1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(1, cnt);
      resultSet.close();
    }

    // try to read data on each node. select from root
    for (Statement readStatement : readStatements) {
      ResultSet resultSet = readStatement.executeQuery("SHOW TIMESERIES root where tag1=v1");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(2, cnt);
      resultSet.close();
    }

    // try to read data on each node. SHOW TIMESERIES root.ln.wf01.* where tag1=v3"
    for (Statement readStatement : readStatements) {
      ResultSet resultSet =
          readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.* where tag1=v3");
      int cnt = 0;
      while (resultSet.next()) {
        cnt++;
      }
      Assert.assertEquals(0, cnt);
      resultSet.close();
    }

    // try to read data on each node. SHOW TIMESERIES root.ln.wf01.* where tag3=v1"
    for (Statement readStatement : readStatements) {
      ResultSet resultSet = null;
      try {
        resultSet = readStatement.executeQuery("SHOW TIMESERIES root.ln.wf01.* where tag3=v1");
      } catch (Exception e) {
        Assert.assertTrue(e.getMessage().contains("The key tag3 is not a tag"));
      } finally {
        if (resultSet != null) {
          resultSet.close();
        }
      }
    }
  }

  @Test
  public void testApplyClearCache() throws InterruptedException {
    String sql = "CLEAR CACHE";
    try {
      // Wait for 3S so that the leader can be elected
      Thread.sleep(3000);
      writeStatement.execute(sql);
    } catch (SQLException e) {
      Assert.assertNull(e);
    }
  }

  @Test
  public void testApplyMerge() throws InterruptedException {
    String sql = "MERGE";
    try {
      // Wait for 3S so that the leader can be elected
      Thread.sleep(3000);
      writeStatement.execute(sql);
    } catch (SQLException e) {
      Assert.assertNull(e);
    }
  }

  @Test
  public void testCreateSnapshot() throws InterruptedException {
    String sql = "CREATE SNAPSHOT FOR SCHEMA";
    try {
      // Wait for 3S so that the leader can be elected
      Thread.sleep(3000);
      writeStatement.execute(sql);
    } catch (SQLException e) {
      Assert.assertNull(e);
    }
  }
}
