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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** IoTDBTests are tests that need a IoTDB daemon to support the tests. */
public abstract class IoTDBTest {

  private PlanExecutor planExecutor;
  private boolean prevEnableAutoSchema;
  private boolean prevUseAsyncServer;

  @Before
  public void setUp() throws StartupException, QueryProcessException, IllegalPathException {
    prevUseAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    prevEnableAutoSchema = IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    planExecutor = new PlanExecutor();
    prepareSchema();
    prepareData(0, 0, 100);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(prevEnableAutoSchema);
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(prevUseAsyncServer);
  }

  private void prepareSchema() {
    for (int i = 0; i < 4; i++) {
      // storage groups that has timeseries schema locally
      setStorageGroup(TestUtils.getTestSg(i));
      for (int j = 0; j < 10; j++) {
        createTimeSeries(i, j);
      }
    }
    // storage groups that has timeseries schema remotely
    setStorageGroup(TestUtils.getTestSg(4));
    // storage groups that does not have timeseries schema remotely or locally
    for (int i = 5; i < 10; i++) {
      setStorageGroup(TestUtils.getTestSg(i));
    }
  }

  protected void prepareData(int sgNum, int timeOffset, int size)
      throws QueryProcessException, IllegalPathException {
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setDeviceId(new PartialPath(TestUtils.getTestSg(sgNum)));
    String[] measurements = new String[10];
    for (int i = 0; i < measurements.length; i++) {
      measurements[i] = TestUtils.getTestMeasurement(i);
    }
    MeasurementMNode[] schemas = new MeasurementMNode[10];
    for (int i = 0; i < measurements.length; i++) {
      schemas[i] = TestUtils.getTestMeasurementMNode(i);
    }
    insertPlan.setMeasurements(measurements);
    insertPlan.setNeedInferType(true);
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);

    Object[] values = new Object[10];
    for (int i = timeOffset; i < timeOffset + size; i++) {
      insertPlan.setTime(i);
      Arrays.fill(values, String.valueOf(i * 1.0));
      insertPlan.setValues(values);
      insertPlan.setMeasurementMNodes(schemas);
      planExecutor.insert(insertPlan);
    }
  }

  protected void setStorageGroup(String storageGroupName) {
    try {
      planExecutor.setStorageGroup(new SetStorageGroupPlan(new PartialPath(storageGroupName)));
    } catch (QueryProcessException | IllegalPathException e) {
      // ignore
    }
  }

  private void createTimeSeries(int sgNum, int seriesNum) {
    try {
      MeasurementSchema schema = TestUtils.getTestMeasurementSchema(seriesNum);
      planExecutor.processNonQuery(
          new CreateTimeSeriesPlan(
              new PartialPath(
                  TestUtils.getTestSg(sgNum)
                      + IoTDBConstant.PATH_SEPARATOR
                      + schema.getMeasurementId()),
              schema.getType(),
              schema.getEncodingType(),
              schema.getCompressor(),
              schema.getProps(),
              Collections.emptyMap(),
              Collections.emptyMap(),
              null));
    } catch (QueryProcessException
        | StorageGroupNotSetException
        | StorageEngineException
        | IllegalPathException e) {
      // ignore
    }
  }

  protected QueryDataSet query(List<String> pathStrs, IExpression expression)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException,
          IOException, MetadataException, InterruptedException {
    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setExpression(expression);
    List<PartialPath> paths = new ArrayList<>();
    for (String pathStr : pathStrs) {
      paths.add(new PartialPath(pathStr));
    }
    queryPlan.setDeduplicatedPaths(paths);
    queryPlan.setPaths(paths);
    List<TSDataType> dataTypes = new ArrayList<>();
    for (PartialPath path : paths) {
      dataTypes.add(IoTDB.metaManager.getSeriesType(path));
    }
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDataTypes(dataTypes);
    queryPlan.setExpression(expression);

    return planExecutor.processQuery(queryPlan, context);
  }
}
