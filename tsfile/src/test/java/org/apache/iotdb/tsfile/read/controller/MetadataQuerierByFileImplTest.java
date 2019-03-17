/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier.LoadMode;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetadataQuerierByFileImplTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private TsFileSequenceReader fileReader;

  @Before
  public void before() throws InterruptedException, WriteProcessException, IOException {
    TsFileGeneratorForTest.generateFile(1000000, 1024 * 1024, 10000);
  }

  @After
  public void after() throws IOException {
    fileReader.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void test_NoPartition() throws IOException {
    fileReader = new TsFileSequenceReader(FILE_PATH);
    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
    List<ChunkMetaData> chunkMetaDataList = metadataQuerierByFile
        .getChunkMetaDataList(new Path("d2.s1"));
    for (ChunkMetaData chunkMetaData : chunkMetaDataList) {
      Assert.assertEquals("s1", chunkMetaData.getMeasurementUid());
    }
  }

  @Test
  public void test_InPartition() throws IOException {
    fileReader = new TsFileSequenceReader(FILE_PATH);

    HashMap<String, Long> params = new HashMap<>();
    params.put(QueryConstant.PARTITION_START_OFFSET, 3621649L);
    params.put(QueryConstant.PARTITION_END_OFFSET, 5010264L);

    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader,
        params);
    List<ChunkMetaData> chunkMetaDataList = metadataQuerierByFile
        .getChunkMetaDataList(new Path("d2.s1"));
    Assert.assertEquals(2, chunkMetaDataList.size());
    Assert.assertEquals(1480562757700L, chunkMetaDataList.get(0).getStartTime());
    Assert.assertEquals(1480562803465L, chunkMetaDataList.get(0).getEndTime());

    Assert.assertEquals(1480562803470L, chunkMetaDataList.get(1).getStartTime());
    Assert.assertEquals(1480562849220L, chunkMetaDataList.get(1).getEndTime());
  }

  @Test
  public void test_getTimeRangeInPartition() throws IOException {
    fileReader = new TsFileSequenceReader(FILE_PATH);

    HashMap<String, Long> params = new HashMap<>();
    params.put(QueryConstant.PARTITION_START_OFFSET, 1608255L);
    params.put(QueryConstant.PARTITION_END_OFFSET, 3006837L);

    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader,
        params);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    ArrayList<TimeRange> timeRanges = metadataQuerierByFile
        .getTimeRangeInOrPrev(paths, LoadMode.InPartition);
    Assert.assertEquals(2, timeRanges.size());
    Assert.assertEquals(1480562664770L, timeRanges.get(0).getMin());
    Assert.assertEquals(1480562711450L, timeRanges.get(0).getMax());

    Assert.assertEquals(1480562711455L, timeRanges.get(1).getMin());
    Assert.assertEquals(1480562757695L, timeRanges.get(1).getMax());
  }

  @Test
  public void test_getTimeRangePrePartition() throws IOException {
    fileReader = new TsFileSequenceReader(FILE_PATH);

    HashMap<String, Long> params = new HashMap<>();
    params.put(QueryConstant.PARTITION_START_OFFSET, 1608255L);
    params.put(QueryConstant.PARTITION_END_OFFSET, 3006837L);

    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader,
        params);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1.s6"));
    paths.add(new Path("d2.s1"));
    ArrayList<TimeRange> timeRanges = metadataQuerierByFile
        .getTimeRangeInOrPrev(paths, LoadMode.PrevPartition);
    Assert.assertEquals(2, timeRanges.size());
    Assert.assertEquals(1480562618000L, timeRanges.get(0).getMin());
    Assert.assertEquals(1480562664765L, timeRanges.get(0).getMax());

    Assert.assertEquals(1480562664770L, timeRanges.get(1).getMin());
    Assert.assertEquals(1480562711450L, timeRanges.get(1).getMax());
  }

}
