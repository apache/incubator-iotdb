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
package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IMetadataQuerierByFileImplTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private TsFileSequenceReader reader;
  private ArrayList<TimeRange> d1s6timeRangeList = new ArrayList<>();
  private ArrayList<TimeRange> d2s1timeRangeList = new ArrayList<>();
  private ArrayList<long[]> d1chunkGroupMetaDataOffsetList = new ArrayList<>();
  private ArrayList<long[]> d2chunkGroupMetaDataOffsetList = new ArrayList<>();

  @Before
  public void before() throws IOException {
    TsFileGeneratorForTest.generateFile(10000, 1024, 100);
    reader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH));
    List<ChunkMetadata> d1s6List = reader.getChunkMetadataList(new Path("d1", "s6"));
    for (ChunkMetadata chunkMetaData : d1s6List) {
      // get a series of [startTime, endTime] of d1.s6 from the chunkGroupMetaData of
      // d1
      d1s6timeRangeList.add(
          new TimeRange(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()));
      long[] startEndOffsets = new long[2];
      startEndOffsets[0] = chunkMetaData.getOffsetOfChunkHeader();
      startEndOffsets[1] =
          chunkMetaData.getOffsetOfChunkHeader()
              + chunkMetaData.getMeasurementUid().getBytes().length
              + Long.BYTES
              + Short.BYTES
              + chunkMetaData.getStatistics().getSerializedSize();
      d1chunkGroupMetaDataOffsetList.add(startEndOffsets);
    }

    List<ChunkMetadata> d2s1List = reader.getChunkMetadataList(new Path("d2", "s1"));
    for (ChunkMetadata chunkMetaData : d2s1List) {
      d2s1timeRangeList.add(
          new TimeRange(chunkMetaData.getStartTime(), chunkMetaData.getEndTime()));
      long[] startEndOffsets = new long[2];
      startEndOffsets[0] = chunkMetaData.getOffsetOfChunkHeader();
      startEndOffsets[1] =
          chunkMetaData.getOffsetOfChunkHeader()
              + chunkMetaData.getMeasurementUid().getBytes().length
              + Long.BYTES
              + Short.BYTES
              + chunkMetaData.getStatistics().getSerializedSize();
      d2chunkGroupMetaDataOffsetList.add(startEndOffsets);
    }
  }

  @After
  public void after() throws IOException {
    reader.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void testEmpty() throws IOException {
    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(reader);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s6"));
    paths.add(new Path("d2", "s1"));

    ArrayList<TimeRange> resTimeRanges =
        new ArrayList<>(metadataQuerierByFile.convertSpace2TimePartition(paths, 0L, 0L));

    Assert.assertEquals(0, resTimeRanges.size());
  }

  @Test
  public void testConvert1() throws IOException {
    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(reader);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s6"));
    paths.add(new Path("d2", "s1"));

    long spacePartitionStartPos = d1chunkGroupMetaDataOffsetList.get(0)[0];
    long spacePartitionEndPos = d1chunkGroupMetaDataOffsetList.get(1)[1];
    ArrayList<TimeRange> resTimeRanges =
        new ArrayList<>(
            metadataQuerierByFile.convertSpace2TimePartition(
                paths, spacePartitionStartPos, spacePartitionEndPos));

    ArrayList<TimeRange> unionCandidates = new ArrayList<>();
    unionCandidates.add(d1s6timeRangeList.get(0));
    unionCandidates.add(d2s1timeRangeList.get(0));
    unionCandidates.add(d1s6timeRangeList.get(1));
    ArrayList<TimeRange> expectedRanges = new ArrayList<>(TimeRange.sortAndMerge(unionCandidates));
    Assert.assertEquals(expectedRanges.toString(), resTimeRanges.toString());
  }

  @Test
  public void testConvert2() throws IOException {
    MetadataQuerierByFileImpl metadataQuerierByFile = new MetadataQuerierByFileImpl(reader);

    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s6"));
    paths.add(new Path("d2", "s1"));

    long spacePartitionStartPos = d2chunkGroupMetaDataOffsetList.get(0)[0];
    long spacePartitionEndPos = d2chunkGroupMetaDataOffsetList.get(0)[1];
    ArrayList<TimeRange> inCandidates = new ArrayList<>();
    ArrayList<TimeRange> beforeCandidates = new ArrayList<>();
    ArrayList<TimeRange> resTimeRanges =
        new ArrayList<>(
            metadataQuerierByFile.convertSpace2TimePartition(
                paths, spacePartitionStartPos, spacePartitionEndPos));
    inCandidates.add(d2s1timeRangeList.get(0));
    beforeCandidates.add(d1s6timeRangeList.get(0));
    ArrayList<TimeRange> expectedRanges = new ArrayList<>();
    for (TimeRange in : inCandidates) {
      ArrayList<TimeRange> remains = new ArrayList<>(in.getRemains(beforeCandidates));
      expectedRanges.addAll(remains);
    }
    Assert.assertEquals(expectedRanges.toString(), resTimeRanges.toString());
  }
}
