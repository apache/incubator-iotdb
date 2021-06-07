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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.TsFileManagement.CompactionMergeTask;
import org.apache.iotdb.db.engine.compaction.heavyhitter.HitterLevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.compaction.heavyhitter.QueryHeavyHitters;
import org.apache.iotdb.db.engine.compaction.heavyhitter.QueryHitterManager;
import org.apache.iotdb.db.engine.compaction.heavyhitter.hitters.HashMapHitter;
import org.apache.iotdb.db.engine.compaction.heavyhitter.hitters.SpaceSavingHitter;
import org.apache.iotdb.db.engine.compaction.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.SOURCE_NAME;
import static org.apache.iotdb.db.engine.compaction.utils.CompactionLogger.TARGET_NAME;
import static org.junit.Assert.assertEquals;

public class HitterCompactionTest extends HitterTest {

  private CompactionStrategy prevCompactionStrategy;
  File tempSGDir;
  boolean compactionMergeWorking = false;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    prevCompactionStrategy = IoTDBDescriptor.getInstance().getConfig().getCompactionStrategy();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setCompactionStrategy(CompactionStrategy.HITTER_LEVEL_COMPACTION);
    prepareSeqFiles(seqFileNum);
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
    IoTDBDescriptor.getInstance().getConfig().setCompactionStrategy(prevCompactionStrategy);
  }

  void prepareHitter() throws StartupException {
    QueryHitterManager.getInstance().start();
    QueryHitterManager.getInstance()
        .submitTask(QueryHitterManager.getInstance().new HitterTask(queryPaths));
  }

  @Test
  public void testHashMapHitter() throws MetadataException {
    QueryHeavyHitters queryHeavyHitters = new HashMapHitter(maxHitterNum);
    List<PartialPath> compSeriesBef =
        queryHeavyHitters.getTopCompactionSeries(new PartialPath(HITTER_TEST_SG));
    Assert.assertEquals(compSeriesBef.size(), 0);
    queryHeavyHitters.acceptQuerySeriesList(queryPaths);
    List<PartialPath> compSeriesAft =
        queryHeavyHitters.getTopCompactionSeries(new PartialPath(HITTER_TEST_SG));
    Assert.assertEquals(compSeriesAft.size(), maxHitterNum);
  }

  @Test
  public void testSpaceSavingHitter() throws MetadataException {
    QueryHeavyHitters queryHeavyHitters = new SpaceSavingHitter(maxHitterNum);
    List<PartialPath> compSeriesBef =
        queryHeavyHitters.getTopCompactionSeries(new PartialPath(HITTER_TEST_SG));
    Assert.assertEquals(compSeriesBef.size(), 0);
    queryHeavyHitters.acceptQuerySeriesList(queryPaths);
    List<PartialPath> compSeriesAft =
        queryHeavyHitters.getTopCompactionSeries(new PartialPath(HITTER_TEST_SG));
    Assert.assertEquals(compSeriesAft.size(), maxHitterNum);
  }

  @Test
  public void testHitterCompaction() throws IllegalPathException, IOException, StartupException {
    prepareHitter();
    HitterLevelCompactionTsFileManagement hitterLevelCompactionTsFileManagement =
        new HitterLevelCompactionTsFileManagement(HITTER_TEST_SG, tempSGDir.getPath());
    hitterLevelCompactionTsFileManagement.addAll(seqResources, true);
    hitterLevelCompactionTsFileManagement.forkCurrentFileList(0);
    CompactionMergeTask compactionMergeTask =
        hitterLevelCompactionTsFileManagement
        .new CompactionMergeTask(this::closeCompactionMergeCallBack, 0);
    compactionMergeWorking = true;
    compactionMergeTask.call();
    while (compactionMergeWorking) {
      // wait
    }
    QueryContext context = new QueryContext();
    // hitter merged path
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            hitterLevelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int countMerged = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        countMerged++;
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
      }
    }
    assertEquals(600, countMerged);
    context = new QueryContext();
    // hitter other path
    path =
        new PartialPath(
            deviceIds[1]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[1].getMeasurementId());
    tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[1].getType(),
            context,
            hitterLevelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int countOthers = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        countOthers++;
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
      }
    }
    assertEquals(600, countOthers);
  }

  @Test
  public void testHitterCompactionRecover()
      throws IllegalPathException, IOException, StartupException {
    prepareHitter();
    HitterLevelCompactionTsFileManagement hitterLevelCompactionTsFileManagement =
        new HitterLevelCompactionTsFileManagement(HITTER_TEST_SG, tempSGDir.getPath());
    hitterLevelCompactionTsFileManagement.addAll(seqResources, true);
    CompactionLogger compactionLogger = new CompactionLogger(tempSGDir.getPath(), HITTER_TEST_SG);
    for (TsFileResource tsFileResource : seqResources) {
      compactionLogger.logFile(SOURCE_NAME, tsFileResource.getTsFile());
    }
    compactionLogger.logSequence(true);
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    compactionLogger.logFile(TARGET_NAME, targetTsFileResource.getTsFile());
    CompactionUtils.hitterMerge(
        targetTsFileResource,
        new ArrayList<>(seqResources),
        HITTER_TEST_SG,
        compactionLogger,
        new HashSet<>(),
        true,
        new ArrayList<>(),
        queryPaths);
    compactionLogger.close();
    hitterLevelCompactionTsFileManagement.addRecover(targetTsFileResource, true);
    hitterLevelCompactionTsFileManagement.recover();
    QueryContext context = new QueryContext();
    // hitter merged path
    PartialPath path =
        new PartialPath(
            deviceIds[0]
                + TsFileConstant.PATH_SEPARATOR
                + measurementSchemas[0].getMeasurementId());
    IBatchReader tsFilesReader =
        new SeriesRawDataBatchReader(
            path,
            measurementSchemas[0].getType(),
            context,
            hitterLevelCompactionTsFileManagement.getTsFileList(true),
            new ArrayList<>(),
            null,
            null,
            true);
    int countMerged = 0;
    while (tsFilesReader.hasNextBatch()) {
      BatchData batchData = tsFilesReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        countMerged++;
        assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
      }
    }
    assertEquals(600, countMerged);
  }

  /** close compaction merge callback, to release some locks */
  private void closeCompactionMergeCallBack(
      boolean isMergeExecutedInCurrentTask, long timePartitionId) {
    this.compactionMergeWorking = false;
  }

  void prepareSeqFiles(int seqFileNum) throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.updatePlanIndexes((long) i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
  }
}
