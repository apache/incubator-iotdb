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

package org.apache.iotdb.db.engine.modification;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.executor.QueryRouter;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.junit.Assert.assertEquals;

public class DeletionQueryTest {

  private String processorName = "root.test";

  private static String[] measurements = new String[10];
  private TSDataType dataType = TSDataType.DOUBLE;
  private TSEncoding encoding = TSEncoding.PLAIN;
  private QueryRouter router = new QueryRouter();

  private int prevUnseqLevelNum = 0;

  static {
    for (int i = 0; i < 10; i++) {
      measurements[i] = "m" + i;
    }
  }

  @Before
  public void setup() throws MetadataException {
    prevUnseqLevelNum = IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum();
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(2);
    EnvironmentUtils.envSetUp();
    IoTDB.metaManager.setStorageGroup(new PartialPath(processorName));
    for (int i = 0; i < 10; i++) {
      IoTDB.metaManager.createTimeseries(
          new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[i]),
          dataType,
          encoding,
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    }
  }

  @After
  public void teardown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setUnseqLevelNum(prevUnseqLevelNum);
  }

  private void insertToStorageEngine(TSRecord record)
      throws StorageEngineException, IllegalPathException {
    InsertRowPlan insertRowPlan = new InsertRowPlan(record);
    StorageEngine.getInstance().insert(insertRowPlan);
  }

  @Test
  public void testDeleteInBufferWriteCache()
      throws StorageEngineException, IOException, QueryProcessException, IllegalPathException {

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }

    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[3]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[4]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 0, 30, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 30, 50, -1, null);

    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[3]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[4]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(dataType);
    dataTypes.add(dataType);
    dataTypes.add(dataType);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPathsAndUpdate(pathList);
    QueryDataSet dataSet = router.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(50, count);
  }

  @Test
  public void testDeleteInBufferWriteFile()
      throws StorageEngineException, IOException, QueryProcessException, IllegalPathException {
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[4]), 0, 40, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[3]), 0, 30, -1, null);

    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[3]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[4]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]));

    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(dataType);
    dataTypes.add(dataType);
    dataTypes.add(dataType);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPathsAndUpdate(pathList);
    QueryDataSet dataSet = router.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(70, count);
  }

  @Test
  public void testDeleteInOverflowCache()
      throws StorageEngineException, IOException, QueryProcessException, IllegalPathException {
    // insert into BufferWrite
    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    // insert into Overflow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }

    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[3]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[4]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 0, 30, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 30, 50, -1, null);

    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[3]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[4]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(dataType);
    dataTypes.add(dataType);
    dataTypes.add(dataType);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPathsAndUpdate(pathList);
    QueryDataSet dataSet = router.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(150, count);
  }

  @Test
  public void testDeleteInOverflowFile()
      throws StorageEngineException, IOException, QueryProcessException, IllegalPathException {
    // insert into BufferWrite
    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    // insert into Overflow
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[4]), 0, 40, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[3]), 0, 30, -1, null);

    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[3]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[4]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]));

    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(dataType);
    dataTypes.add(dataType);
    dataTypes.add(dataType);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPathsAndUpdate(pathList);
    QueryDataSet dataSet = router.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(170, count);
  }

  @Test
  public void testSuccessiveDeletion()
      throws StorageEngineException, IOException, QueryProcessException, IllegalPathException {
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }

    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[3]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[4]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 0, 30, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 30, 50, -1, null);

    StorageEngine.getInstance().syncCloseAllProcessor();

    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }

    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[3]), 0, 250, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[4]), 0, 250, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 0, 230, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 230, 250, -1, null);

    StorageEngine.getInstance().syncCloseAllProcessor();

    for (int i = 201; i <= 300; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }

    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[3]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[4]), 0, 50, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 0, 30, -1, null);
    StorageEngine.getInstance()
        .delete(new PartialPath(processorName, measurements[5]), 30, 50, -1, null);

    StorageEngine.getInstance().syncCloseAllProcessor();

    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[3]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[4]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(dataType);
    dataTypes.add(dataType);
    dataTypes.add(dataType);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPathsAndUpdate(pathList);
    QueryDataSet dataSet = router.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);

    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(100, count);
  }
}
