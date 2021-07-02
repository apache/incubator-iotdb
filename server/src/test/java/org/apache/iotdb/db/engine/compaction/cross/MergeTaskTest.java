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

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.CrossSpaceMergeTask;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MergeTaskTest extends MergeTest {

  private File tempSGDir;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testMerge() throws Exception {
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testMergeEndTime() throws Exception {
    List<TsFileResource> testSeqResources = seqResources.subList(0, 3);
    List<TsFileResource> testUnseqResource = unseqResources.subList(5, 6);
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(testSeqResources, testUnseqResource),
            tempSGDir.getPath(),
            (k, v, l) -> {
              assertEquals(499, k.get(2).getEndTime("root.mergeTest.device1"));
            },
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();
  }

  @Test
  public void testMergeEndTimeAfterDeletion() throws Exception {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                10
                    + "unseq"
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource smallUnseqTsFileResource = new TsFileResource(file);
    smallUnseqTsFileResource.setClosed(true);
    smallUnseqTsFileResource.setMinPlanIndex(10);
    smallUnseqTsFileResource.setMaxPlanIndex(10);
    smallUnseqTsFileResource.setVersion(10);
    prepareFile(smallUnseqTsFileResource, 0, 50, 0);
    unseqResources.add(smallUnseqTsFileResource);

    // remove all data of first file
    for (String deviceId : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        PartialPath device = new PartialPath(deviceId);
        seqResources
            .get(0)
            .getModFile()
            .write(
                new Deletion(
                    device.concatNode(measurementSchema.getMeasurementId()),
                    seqResources.get(0).getTsFileSize(),
                    Long.MIN_VALUE,
                    Long.MAX_VALUE));
      }
    }
    List<TsFileResource> testSeqResources = seqResources.subList(0, 1);
    List<TsFileResource> testUnseqResources = new ArrayList<>();
    testUnseqResources.add(smallUnseqTsFileResource);
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(testSeqResources, testUnseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {
              assertEquals(49, k.get(0).getEndTime("root.mergeTest.device1"));
            },
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();
  }

  @Test
  public void testFullMerge() throws Exception {
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            true,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[9].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[9].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    long count = 0L;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int t = 0; t < batchData.length(); t++) {
        assertEquals(batchData.getTimeByIndex(t) + 20000.0, batchData.getDoubleByIndex(t), 0.001);
        count++;
      }
    }
    assertEquals(100, count);
    tsFilesReader.close();
  }

  @Test
  public void testChunkNumThreshold() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(Integer.MAX_VALUE);
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources, unseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testPartialMerge1() throws Exception {
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources, unseqResources.subList(0, 1)),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) < 20) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i) + 0.0, batchData.getDoubleByIndex(i), 0.001);
        }
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testPartialMerge2() throws Exception {
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources, unseqResources.subList(5, 6)),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 20000.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testPartialMerge3() throws Exception {
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources, unseqResources.subList(0, 5)),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> list = new ArrayList<>();
    list.add(seqResources.get(2));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            list,
            new ArrayList<>(),
            null,
            null,
            true);
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) < 260) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i) + 0.0, batchData.getDoubleByIndex(i), 0.001);
        }
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void mergeWithDeletionTest() throws Exception {
    try {
      PartialPath device = new PartialPath(deviceIds[0]);
      seqResources
          .get(0)
          .getModFile()
          .write(
              new Deletion(
                  device.concatNode(measurementSchemas[0].getMeasurementId()),
                  seqResources.get(0).getTsFileSize(),
                  0,
                  49));
    } finally {
      seqResources.get(0).getModFile().close();
    }

    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(seqResources, unseqResources.subList(0, 1)),
            tempSGDir.getPath(),
            (k, v, l) -> {
              try {
                seqResources.get(0).removeModFile();
              } catch (IOException e) {
                e.printStackTrace();
              }
            },
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(seqResources.get(0));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        if (batchData.getTimeByIndex(i) <= 20) {
          assertEquals(batchData.getTimeByIndex(i) + 10000.0, batchData.getDoubleByIndex(i), 0.001);
        } else {
          assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
        }
        count++;
      }
    }
    assertEquals(70, count);
    tsFilesReader.close();
  }

  @Test
  public void testOnlyUnseqMerge() throws Exception {
    // unseq and no seq merge
    List<TsFileResource> testSeqResources = new ArrayList<>();
    List<TsFileResource> testUnseqResource = unseqResources.subList(5, 6);
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(testSeqResources, testUnseqResource),
            tempSGDir.getPath(),
            (k, v, l) -> {},
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();

    QueryContext context = new QueryContext();
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    List<TsFileResource> resources = new ArrayList<>();
    resources.add(seqResources.get(2));
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            resources,
            new ArrayList<>(),
            null,
            null,
            true);
    int count = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        assertEquals(batchData.getTimeByIndex(i) + 0.0, batchData.getDoubleByIndex(i), 0.001);
      }
    }
    tsFilesReader.close();
  }

  @Test
  public void testMergeWithFileWithoutSomeSensor() throws Exception {
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                10
                    + "unseq"
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 10
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource unseqTsFileResourceWithoutSomeSensor = new TsFileResource(file);
    unseqTsFileResourceWithoutSomeSensor.setClosed(true);
    unseqTsFileResourceWithoutSomeSensor.setMinPlanIndex(10);
    unseqTsFileResourceWithoutSomeSensor.setMaxPlanIndex(10);
    unseqTsFileResourceWithoutSomeSensor.setVersion(10);
    prepareFileWithLastSensor(unseqTsFileResourceWithoutSomeSensor, 0, 50, 0);
    unseqResources.add(unseqTsFileResourceWithoutSomeSensor);

    List<TsFileResource> testSeqResources = seqResources.subList(0, 1);
    List<TsFileResource> testUnseqResources = new ArrayList<>();
    testUnseqResources.add(unseqTsFileResourceWithoutSomeSensor);
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            new CrossSpaceMergeResource(testSeqResources, testUnseqResources),
            tempSGDir.getPath(),
            (k, v, l) -> {
              assertEquals(99, k.get(0).getEndTime("root.mergeTest.device1"));
            },
            "test",
            false,
            1,
            MERGE_TEST_SG);
    mergeTask.call();
  }

  private void prepareFileWithLastSensor(
      TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    for (int i = 0; i < deviceIds.length - 1; i++) {
      for (int j = 0; j < measurementSchemas.length - 1; j++) {
        fileWriter.registerTimeseries(
            new Path(deviceIds[i], measurementSchemas[j].getMeasurementId()),
            measurementSchemas[j]);
      }
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (int j = 0; j < deviceNum - 1; j++) {
        TSRecord record = new TSRecord(i, deviceIds[j]);
        for (int k = 0; k < measurementNum - 1; k++) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchemas[k].getType(),
                  measurementSchemas[k].getMeasurementId(),
                  String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceIds[j], i);
        tsFileResource.updateEndTime(deviceIds[j], i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();
  }
}
