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
package org.apache.iotdb.db.engine.storagegroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.flush.TsFileFlushPolicy;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StorageGroupProcessorTest {

  private String storageGroup = "root.vehicle.d0";
  private String systemDir = TestConstant.OUTPUT_DATA_DIR.concat("info");
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private StorageGroupProcessor processor;
  private QueryContext context = EnvironmentUtils.TEST_QUERY_CONTEXT;

  @Before
  public void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig()
        .setCompactionStrategy(CompactionStrategy.NO_COMPACTION);
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
    processor = new DummySGP(systemDir, storageGroup);
    MergeManager.getINSTANCE().start();
  }

  @After
  public void tearDown() throws Exception {
    processor.syncDeleteDataFiles();
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(TestConstant.OUTPUT_DATA_DIR);
    MergeManager.getINSTANCE().stop();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig()
        .setCompactionStrategy(CompactionStrategy.LEVEL_COMPACTION);
  }

  private void insertToStorageGroupProcessor(TSRecord record)
      throws WriteProcessException, IllegalPathException {
    InsertRowPlan insertRowPlan = new InsertRowPlan(record);
    processor.insert(insertRowPlan);
  }

  @Test
  public void testUnseqUnsealedDelete()
      throws WriteProcessException, IOException, MetadataException {
    System.out.println("testUnseqUnsealedDelete");
    TSRecord record = new TSRecord(10000, deviceId);
    record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(1000)));
    processor.insert(new InsertRowPlan(record));
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 1; j <= 10; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    for (int j = 11; j <= 20; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }

    processor.delete(new PartialPath(deviceId, measurementId), 0, 15L, -1);

    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor
          .query(deviceId, measurementId, TSDataType.INT32, TSEncoding.RLE, Collections.emptyMap(),
              new QueryContext(), tsfileResourcesForQuery);
    }

    Assert.assertEquals(1, tsfileResourcesForQuery.size());
    Assert.assertEquals(0, tsfileResourcesForQuery.get(0).getChunkMetadataList().size());
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk();
    long time = 16;
    for (ReadOnlyMemChunk memChunk : memChunks) {
      IPointReader iterator = memChunk.getPointReader();
      while (iterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        Assert.assertEquals(time++, timeValuePair.getTimestamp());
      }
    }
  }

  @Test
  public void testSlidingMemTableJDBC()
      throws WriteProcessException, QueryProcessException, MetadataException {
    IoTDBDescriptor.getInstance().getConfig().setFlushWaitTime(30000);

    TSRecord record;
    for (int j = 5; j <= 10; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }

    for (TsFileProcessor tsfileProcessor : processor.getWorkSequenceTsFileProcessors()) {
      tsfileProcessor.asyncFlush();
    }

    for (int j = 1; j <= 3; j++) {
      record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
    }
    processor.syncCloseAllWorkingTsFileProcessors();
    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    IoTDBDescriptor.getInstance().getConfig().setFlushWaitTime(0);
  }

  @Test
  public void testSlidingMemTableNativeApi() throws QueryProcessException, IllegalPathException {
    IoTDBDescriptor.getInstance().getConfig().setFlushWaitTime(30000);
    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    MeasurementMNode[] measurementMNodes = new MeasurementMNode[2];
    measurementMNodes[0] = new MeasurementMNode(null, "s0",
        new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] = new MeasurementMNode(null, "s1",
        new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements, dataTypes);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    long[] times = new long[5];
    Object[] columns = new Object[2];
    columns[0] = new int[5];
    columns[1] = new long[5];

    for (int r = 5; r < 10; r++) {
      times[r - 5] = r;
      ((int[]) columns[0])[r - 5] = 1;
      ((long[]) columns[1])[r - 5] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);

    processor.insertTablet(insertTabletPlan1);
    for (TsFileProcessor tsfileProcessor : processor.getWorkSequenceTsFileProcessors()) {
      tsfileProcessor.asyncFlush();
    }

    InsertTabletPlan insertTabletPlan2 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements, dataTypes);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    for (int r = 0; r < 5; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);

    Assert.assertEquals(1, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    IoTDBDescriptor.getInstance().getConfig().setFlushWaitTime(0);
  }

  @Test
  public void testSequenceSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    System.out.println("testSequenceSyncClose");
    for (int j = 1; j <= 10; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    processor.syncCloseAllWorkingTsFileProcessors();
    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testIoTDBTabletWriteAndSyncClose()
      throws QueryProcessException, IllegalPathException {
    System.out.println("testIoTDBTabletWriteAndSyncClose");
    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    MeasurementMNode[] measurementMNodes = new MeasurementMNode[2];
    measurementMNodes[0] = new MeasurementMNode(null, "s0",
        new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] = new MeasurementMNode(null, "s1",
        new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements,
        dataTypes);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new int[100];
    columns[1] = new long[100];

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements,
        dataTypes);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    for (int r = 50; r < 149; r++) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(1, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }


  @Test
  public void testSeqAndUnSeqSyncClose()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    System.out.println("testSeqAndUnSeqSyncClose");
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    processor.syncCloseAllWorkingTsFileProcessors();

    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(10, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertRowPlan()
      throws WriteProcessException, QueryProcessException, IllegalPathException, IOException {
    System.out.println("testEnableDiscardOutOfOrderDataForInsertRowPlan");
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    boolean defaultValue = config.isEnableDiscardOutOfOrderData();
    config.setEnableDiscardOutOfOrderData(true);

    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      insertToStorageGroupProcessor(record);
      processor.asyncCloseAllWorkingTsFileProcessors();
    }
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      insertToStorageGroupProcessor(record);
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    processor.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultValue);
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertTablet1()
      throws QueryProcessException, IllegalPathException, IOException {
    System.out.println("testEnableDiscardOutOfOrderDataForInsertTablet1");
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getPartitionInterval();
    boolean defaultEnablePartition = config.isEnablePartition();
    config.setEnableDiscardOutOfOrderData(true);
    config.setEnablePartition(true);
    config.setPartitionInterval(100);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    MeasurementMNode[] measurementMNodes = new MeasurementMNode[2];
    measurementMNodes[0] = new MeasurementMNode(null, "s0",
        new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] = new MeasurementMNode(null, "s1",
        new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements,
        dataTypes);

    long[] times = new long[100];
    Object[] columns = new Object[2];
    columns[0] = new int[100];
    columns[1] = new long[100];

    for (int r = 0; r < 100; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements,
        dataTypes);

    for (int r = 149; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setPartitionInterval(defaultTimePartition);
    config.setEnablePartition(defaultEnablePartition);
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertTablet2()
      throws QueryProcessException, IllegalPathException, IOException {
    System.out.println("testEnableDiscardOutOfOrderDataForInsertTablet2");
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getPartitionInterval();
    boolean defaultEnablePartition = config.isEnablePartition();
    config.setEnableDiscardOutOfOrderData(true);
    config.setEnablePartition(true);
    config.setPartitionInterval(1200);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    MeasurementMNode[] measurementMNodes = new MeasurementMNode[2];
    measurementMNodes[0] = new MeasurementMNode(null, "s0",
        new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] = new MeasurementMNode(null, "s1",
        new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements,
        dataTypes);

    long[] times = new long[1200];
    Object[] columns = new Object[2];
    columns[0] = new int[1200];
    columns[1] = new long[1200];

    for (int r = 0; r < 1200; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements,
        dataTypes);

    for (int r = 1249; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setPartitionInterval(defaultTimePartition);
    config.setEnablePartition(defaultEnablePartition);
  }

  @Test
  public void testEnableDiscardOutOfOrderDataForInsertTablet3()
      throws QueryProcessException, IllegalPathException, IOException {
    System.out.println("testEnableDiscardOutOfOrderDataForInsertTablet3");
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    boolean defaultEnableDiscard = config.isEnableDiscardOutOfOrderData();
    long defaultTimePartition = config.getPartitionInterval();
    boolean defaultEnablePartition = config.isEnablePartition();
    config.setEnableDiscardOutOfOrderData(true);
    config.setEnablePartition(true);
    config.setPartitionInterval(1000);

    String[] measurements = new String[2];
    measurements[0] = "s0";
    measurements[1] = "s1";
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    MeasurementMNode[] measurementMNodes = new MeasurementMNode[2];
    measurementMNodes[0] = new MeasurementMNode(null, "s0",
        new MeasurementSchema("s0", TSDataType.INT32, TSEncoding.PLAIN), null);
    measurementMNodes[1] = new MeasurementMNode(null, "s1",
        new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN), null);

    InsertTabletPlan insertTabletPlan1 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements,
        dataTypes);

    long[] times = new long[1200];
    Object[] columns = new Object[2];
    columns[0] = new int[1200];
    columns[1] = new long[1200];

    for (int r = 0; r < 1200; r++) {
      times[r] = r;
      ((int[]) columns[0])[r] = 1;
      ((long[]) columns[1])[r] = 1;
    }
    insertTabletPlan1.setTimes(times);
    insertTabletPlan1.setColumns(columns);
    insertTabletPlan1.setRowCount(times.length);
    insertTabletPlan1.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan1);
    processor.asyncCloseAllWorkingTsFileProcessors();

    InsertTabletPlan insertTabletPlan2 = new InsertTabletPlan(new PartialPath("root.vehicle.d0"),
        measurements,
        dataTypes);

    for (int r = 1249; r >= 50; r--) {
      times[r - 50] = r;
      ((int[]) columns[0])[r - 50] = 1;
      ((long[]) columns[1])[r - 50] = 1;
    }
    insertTabletPlan2.setTimes(times);
    insertTabletPlan2.setColumns(columns);
    insertTabletPlan2.setRowCount(times.length);
    insertTabletPlan2.setMeasurementMNodes(measurementMNodes);

    processor.insertTablet(insertTabletPlan2);
    processor.asyncCloseAllWorkingTsFileProcessors();
    processor.syncCloseAllWorkingTsFileProcessors();

    for (TsFileProcessor tsfileProcessor : processor.getWorkUnsequenceTsFileProcessors()) {
      tsfileProcessor.syncFlush();
    }

    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);

    Assert.assertEquals(2, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }

    config.setEnableDiscardOutOfOrderData(defaultEnableDiscard);
    config.setPartitionInterval(defaultTimePartition);
    config.setEnablePartition(defaultEnablePartition);
  }

  @Test
  public void testMerge()
      throws WriteProcessException, QueryProcessException, IllegalPathException {
    System.out.println("testMerge");
    for (int j = 21; j <= 30; j++) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }
    processor.syncCloseAllWorkingTsFileProcessors();

    for (int j = 10; j >= 1; j--) {
      TSRecord record = new TSRecord(j, deviceId);
      record.addTuple(DataPoint.getDataPoint(TSDataType.INT32, measurementId, String.valueOf(j)));
      processor.insert(new InsertRowPlan(record));
      processor.asyncCloseAllWorkingTsFileProcessors();
    }

    processor.syncCloseAllWorkingTsFileProcessors();
    processor.merge(true);
    System.out.println("waiting for isUnseqMerging");
    while (processor.getTsFileManagement().isUnseqMerging) {
      // wait
    }
    System.out.println("isUnseqMerging is false");

    QueryDataSource queryDataSource = processor
        .query(new PartialPath(deviceId), measurementId, context,
            null, null);
    Assert.assertEquals(10, queryDataSource.getSeqResources().size());
    Assert.assertEquals(0, queryDataSource.getUnseqResources().size());
    for (TsFileResource resource : queryDataSource.getSeqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
    for (TsFileResource resource : queryDataSource.getUnseqResources()) {
      Assert.assertTrue(resource.isClosed());
    }
  }

  class DummySGP extends StorageGroupProcessor {

    DummySGP(String systemInfoDir, String storageGroupName) throws StorageGroupProcessorException {
      super(systemInfoDir, storageGroupName, new TsFileFlushPolicy.DirectFlushPolicy());
    }

  }
}