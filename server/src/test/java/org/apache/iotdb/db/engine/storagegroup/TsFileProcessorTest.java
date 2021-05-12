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

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TsFileProcessorTest {

  private TsFileProcessor processor;
  private String storageGroup = "storage_group1";
  private StorageGroupInfo sgInfo = new StorageGroupInfo(null);
  private String filePath =
      TestConstant.OUTPUT_DATA_DIR.concat("testUnsealedTsFileProcessor.tsfile");
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private TSDataType dataType = TSDataType.INT32;
  private TSEncoding encoding = TSEncoding.RLE;
  private Map<String, String> props = Collections.emptyMap();
  private QueryContext context;
  private static Logger logger = LoggerFactory.getLogger(TsFileProcessorTest.class);

  protected static final int INIT_ARRAY_SIZE = 64;

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
    MetadataManagerHelper.initMetadata();
    context = EnvironmentUtils.TEST_QUERY_CONTEXT;
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(new File(TestConstant.OUTPUT_DATA_DIR));
  }

  @Test
  public void testWriteAndFlush() throws IOException, WriteProcessException, MetadataException {
    logger.info("testWriteAndFlush begin..");
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor) -> true,
            true,
            INIT_ARRAY_SIZE);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertRowPlan(record));
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertFalse(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk().isEmpty());
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk();
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (int num = 1; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }

    // flush synchronously
    processor.syncFlush();

    tsfileResourcesForQuery.clear();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk().isEmpty());
    assertEquals(1, tsfileResourcesForQuery.get(0).getChunkMetadataList().size());
    assertEquals(
        measurementId,
        tsfileResourcesForQuery.get(0).getChunkMetadataList().get(0).getMeasurementUid());
    assertEquals(
        dataType, tsfileResourcesForQuery.get(0).getChunkMetadataList().get(0).getDataType());
    processor.syncClose();
  }

  @Test
  public void testWriteAndRestoreMetadata()
      throws IOException, WriteProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor) -> true,
            true,
            INIT_ARRAY_SIZE);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertRowPlan(record));
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertFalse(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk().isEmpty());
    int num = 1;
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk();
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }
    logger.info("syncFlush..");
    // flush synchronously
    processor.syncFlush();

    tsfileResourcesForQuery.clear();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk().isEmpty());
    assertEquals(1, tsfileResourcesForQuery.get(0).getChunkMetadataList().size());
    assertEquals(
        measurementId,
        tsfileResourcesForQuery.get(0).getChunkMetadataList().get(0).getMeasurementUid());
    assertEquals(
        dataType, tsfileResourcesForQuery.get(0).getChunkMetadataList().get(0).getDataType());

    RestorableTsFileIOWriter tsFileIOWriter = processor.getWriter();
    Map<String, List<ChunkMetadata>> chunkMetaDataListInChunkGroups =
        tsFileIOWriter.getDeviceChunkMetadataMap();
    RestorableTsFileIOWriter restorableTsFileIOWriter =
        new RestorableTsFileIOWriter(SystemFileFactory.INSTANCE.getFile(filePath));
    Map<String, List<ChunkMetadata>> restoredChunkMetaDataListInChunkGroups =
        restorableTsFileIOWriter.getDeviceChunkMetadataMap();
    assertEquals(
        chunkMetaDataListInChunkGroups.size(), restoredChunkMetaDataListInChunkGroups.size());
    for (Map.Entry<String, List<ChunkMetadata>> entry1 :
        chunkMetaDataListInChunkGroups.entrySet()) {
      for (Map.Entry<String, List<ChunkMetadata>> entry2 :
          restoredChunkMetaDataListInChunkGroups.entrySet()) {
        assertEquals(entry1.getKey(), entry2.getKey());
        assertEquals(entry1.getValue().size(), entry2.getValue().size());
        for (int i = 0; i < entry1.getValue().size(); i++) {
          ChunkMetadata chunkMetaData = entry1.getValue().get(i);
          ChunkMetadata chunkMetadataRestore = entry2.getValue().get(i);
          assertEquals(chunkMetaData, chunkMetadataRestore);
        }
      }
    }
    restorableTsFileIOWriter.close();
    logger.info("syncClose..");
    processor.syncClose();
    // we need to close the tsfile writer first and then reopen it.
  }

  @Test
  public void testMultiFlush() throws IOException, WriteProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor) -> true,
            true,
            INIT_ARRAY_SIZE);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int flushId = 0; flushId < 10; flushId++) {
      for (int i = 1; i <= 10; i++) {
        TSRecord record = new TSRecord(i, deviceId);
        record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
        processor.insert(new InsertRowPlan(record));
      }
      processor.asyncFlush();
    }
    processor.syncFlush();

    tsfileResourcesForQuery.clear();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertFalse(tsfileResourcesForQuery.isEmpty());
    assertTrue(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk().isEmpty());
    assertEquals(10, tsfileResourcesForQuery.get(0).getChunkMetadataList().size());
    assertEquals(
        measurementId,
        tsfileResourcesForQuery.get(0).getChunkMetadataList().get(0).getMeasurementUid());
    assertEquals(
        dataType, tsfileResourcesForQuery.get(0).getChunkMetadataList().get(0).getDataType());
    processor.syncClose();
  }

  @Test
  public void testWriteAndClose() throws IOException, WriteProcessException, MetadataException {
    logger.info("testWriteAndRestoreMetadata begin..");
    processor =
        new TsFileProcessor(
            storageGroup,
            SystemFileFactory.INSTANCE.getFile(filePath),
            sgInfo,
            this::closeTsFileProcessor,
            (tsFileProcessor) -> true,
            true,
            INIT_ARRAY_SIZE);

    TsFileProcessorInfo tsFileProcessorInfo = new TsFileProcessorInfo(sgInfo);
    processor.setTsFileProcessorInfo(tsFileProcessorInfo);
    this.sgInfo.initTsFileProcessorInfo(processor);
    SystemInfo.getInstance().reportStorageGroupStatus(sgInfo, processor);
    List<TsFileResource> tsfileResourcesForQuery = new ArrayList<>();

    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertTrue(tsfileResourcesForQuery.isEmpty());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertRowPlan(record));
    }

    // query data in memory
    tsfileResourcesForQuery.clear();
    processor.query(
        deviceId,
        measurementId,
        new MeasurementSchema(
            measurementId, dataType, encoding, CompressionType.UNCOMPRESSED, props),
        context,
        tsfileResourcesForQuery);
    assertFalse(tsfileResourcesForQuery.isEmpty());
    assertFalse(tsfileResourcesForQuery.get(0).getReadOnlyMemChunk().isEmpty());
    List<ReadOnlyMemChunk> memChunks = tsfileResourcesForQuery.get(0).getReadOnlyMemChunk();
    for (ReadOnlyMemChunk chunk : memChunks) {
      IPointReader iterator = chunk.getPointReader();
      for (int num = 1; num <= 100; num++) {
        iterator.hasNextTimeValuePair();
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        assertEquals(num, timeValuePair.getTimestamp());
        assertEquals(num, timeValuePair.getValue().getInt());
      }
    }

    // close synchronously
    processor.syncClose();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    assertTrue(processor.getTsFileResource().isClosed());
  }

  private void closeTsFileProcessor(TsFileProcessor unsealedTsFileProcessor)
      throws TsFileProcessorException {
    TsFileResource resource = unsealedTsFileProcessor.getTsFileResource();
    synchronized (resource) {
      for (String deviceId : resource.getDevices()) {
        resource.updateEndTime(deviceId, resource.getStartTime(deviceId));
      }
      try {
        resource.close();
      } catch (IOException e) {
        throw new TsFileProcessorException(e);
      }
    }
  }
}
