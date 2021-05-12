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
import org.apache.iotdb.db.engine.modification.io.LocalTextModificationAccessor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_JOB_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DeletionFileNodeTest {

  private static String[] measurements = new String[10];

  static {
    for (int i = 0; i < 10; i++) {
      measurements[i] = "m" + i;
    }
  }

  private String processorName = "root.test";
  private TSDataType dataType = TSDataType.DOUBLE;
  private TSEncoding encoding = TSEncoding.PLAIN;
  private int prevUnseqLevelNum = 0;

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
      throws StorageEngineException, QueryProcessException, IOException, IllegalPathException {

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }

    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[3]), 0, 50, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[4]), 0, 50, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[5]), 0, 30, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[5]), 30, 50, -1);

    SingleSeriesExpression expression =
        new SingleSeriesExpression(
            new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]), null);
    List<StorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLock(Collections.singletonList((PartialPath) expression.getSeriesPath()));
    try {
      QueryDataSource dataSource =
          QueryResourceManager.getInstance()
              .getQueryDataSource(
                  (PartialPath) expression.getSeriesPath(), TEST_QUERY_CONTEXT, null);
      List<ReadOnlyMemChunk> timeValuePairs =
          dataSource.getSeqResources().get(0).getReadOnlyMemChunk();
      int count = 0;
      for (ReadOnlyMemChunk chunk : timeValuePairs) {
        IPointReader iterator = chunk.getPointReader();
        while (iterator.hasNextTimeValuePair()) {
          iterator.nextTimeValuePair();
          count++;
        }
      }
      assertEquals(50, count);
      QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  @Test
  public void testDeleteInBufferWriteFile()
      throws StorageEngineException, IOException, IllegalPathException {
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[5]), 0, 50, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[4]), 0, 40, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[3]), 0, 30, -1);

    Modification[] realModifications =
        new Modification[] {
          new Deletion(
              new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]),
              201,
              50),
          new Deletion(
              new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[4]),
              202,
              40),
          new Deletion(
              new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[3]),
              203,
              30),
        };

    File fileNodeDir =
        TierManager.getInstance().getAllSequenceFileFolders().get(0).getChildFile(processorName);
    List<File> modFiles = new ArrayList<>();
    for (File directory : fileNodeDir.listFiles()) {
      assertTrue(directory.isDirectory());
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.isDirectory()) {
            for (File tsfile : file.listFiles()) {
              if (tsfile.getPath().endsWith(ModificationFile.FILE_SUFFIX)) {
                modFiles.add(tsfile);
              }
            }
          }
        }
      }
    }

    assertEquals(1, modFiles.size());

    LocalTextModificationAccessor accessor = new LocalTextModificationAccessor(modFiles.get(0));
    try {
      Collection<Modification> modifications = accessor.read();
      assertEquals(3, modifications.size());
      int i = 0;
      for (Modification modification : modifications) {
        assertEquals(modification.path, realModifications[i].path);
        assertEquals(modification.type, realModifications[i].type);
        i++;
      }
    } finally {
      accessor.close();
    }
  }

  @Test
  public void testDeleteInOverflowCache()
      throws StorageEngineException, QueryProcessException, IOException, IllegalPathException {
    // insert sequence data
    for (int i = 101; i <= 200; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }
    StorageEngine.getInstance().syncCloseAllProcessor();

    // insert unsequence data
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      StorageEngine.getInstance().insert(new InsertRowPlan(record));
    }

    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[3]), 0, 50, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[4]), 0, 50, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[5]), 0, 30, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[5]), 30, 50, -1);

    SingleSeriesExpression expression =
        new SingleSeriesExpression(
            new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]), null);

    List<StorageGroupProcessor> list =
        StorageEngine.getInstance()
            .mergeLock(Collections.singletonList((PartialPath) expression.getSeriesPath()));

    try {
      QueryDataSource dataSource =
          QueryResourceManager.getInstance()
              .getQueryDataSource(
                  (PartialPath) expression.getSeriesPath(), TEST_QUERY_CONTEXT, null);

      List<ReadOnlyMemChunk> timeValuePairs =
          dataSource.getUnseqResources().get(0).getReadOnlyMemChunk();
      int count = 0;
      for (ReadOnlyMemChunk chunk : timeValuePairs) {
        IPointReader iterator = chunk.getPointReader();
        while (iterator.hasNextTimeValuePair()) {
          iterator.nextTimeValuePair();
          count++;
        }
      }
      assertEquals(50, count);

      QueryResourceManager.getInstance().endQuery(TEST_QUERY_JOB_ID);
    } finally {
      StorageEngine.getInstance().mergeUnLock(list);
    }
  }

  @Test
  public void testDeleteInOverflowFile() throws StorageEngineException, IllegalPathException {
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

    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[5]), 0, 50, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[4]), 0, 40, -1);
    StorageEngine.getInstance().delete(new PartialPath(processorName, measurements[3]), 0, 30, -1);

    Modification[] realModifications =
        new Modification[] {
          new Deletion(
              new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]),
              301,
              50),
          new Deletion(
              new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[4]),
              302,
              40),
          new Deletion(
              new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[3]),
              303,
              30),
        };

    File fileNodeDir =
        TierManager.getInstance().getAllUnSequenceFileFolders().get(0).getChildFile(processorName);
    List<File> modFiles = new ArrayList<>();
    for (File directory : fileNodeDir.listFiles()) {
      assertTrue(directory.isDirectory());
      if (directory.isDirectory()) {
        for (File file : directory.listFiles()) {
          if (file.isDirectory()) {
            for (File tsfile : file.listFiles()) {
              if (tsfile.getPath().endsWith(ModificationFile.FILE_SUFFIX)) {
                modFiles.add(tsfile);
              }
            }
          }
        }
      }
    }
    assertEquals(1, modFiles.size());

    LocalTextModificationAccessor accessor = new LocalTextModificationAccessor(modFiles.get(0));
    Collection<Modification> modifications = accessor.read();
    assertEquals(3, modifications.size());
    int i = 0;
    for (Modification modification : modifications) {
      assertEquals(modification.path, realModifications[i].path);
      assertEquals(modification.type, realModifications[i].type);
      i++;
    }
  }
}
