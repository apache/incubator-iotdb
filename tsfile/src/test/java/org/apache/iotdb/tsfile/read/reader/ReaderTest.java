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
package org.apache.iotdb.tsfile.read.reader;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ReaderTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private TsFileSequenceReader fileReader;
  private MetadataQuerierByFileImpl metadataQuerierByFile;
  private int maxDegreeOfIndexNode;
  private final int rowCount = 1000000;

  @Before
  public void before() throws IOException {
    maxDegreeOfIndexNode = TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode();
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
    TsFileGeneratorForTest.generateFile(rowCount, 10 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH));
    metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
  }

  @After
  public void after() throws IOException {
    fileReader.close();
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(maxDegreeOfIndexNode);
    TsFileGeneratorForTest.after();
  }

  @Test
  public void readTest() throws IOException {
    int count = 0;
    CachedChunkLoaderImpl seriesChunkLoader = new CachedChunkLoaderImpl(fileReader);
    List<IChunkMetadata> chunkMetadataList =
        metadataQuerierByFile.getChunkMetaDataList(new Path("d1", "s1"));

    AbstractFileSeriesReader seriesReader =
        new FileSeriesReader(seriesChunkLoader, chunkMetadataList, null);
    long startTime = TsFileGeneratorForTest.START_TIMESTAMP;
    BatchData data = null;

    while (seriesReader.hasNextBatch()) {
      data = seriesReader.nextBatch();
      while (data.hasCurrent()) {
        Assert.assertEquals(startTime, data.currentTime());
        data.next();
        startTime++;
        count++;
      }
    }
    Assert.assertEquals(rowCount, count);

    chunkMetadataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d1", "s4"));
    seriesReader = new FileSeriesReader(seriesChunkLoader, chunkMetadataList, null);
    count = 0;

    while (seriesReader.hasNextBatch()) {
      data = seriesReader.nextBatch();
      while (data.hasCurrent()) {
        data.next();
        startTime++;
        count++;
      }
    }
  }

  @Test
  public void readWithFilterTest() throws IOException {
    CachedChunkLoaderImpl seriesChunkLoader = new CachedChunkLoaderImpl(fileReader);
    List<IChunkMetadata> chunkMetadataList =
        metadataQuerierByFile.getChunkMetaDataList(new Path("d1", "s1"));

    Filter filter =
        new FilterFactory()
            .or(
                FilterFactory.and(TimeFilter.gt(1480563570029L), TimeFilter.lt(1480563570033L)),
                FilterFactory.and(ValueFilter.gtEq(9520331), ValueFilter.ltEq(9520361)));
    SingleSeriesExpression singleSeriesExp =
        new SingleSeriesExpression(new Path("d1", "s1"), filter);
    AbstractFileSeriesReader seriesReader =
        new FileSeriesReader(seriesChunkLoader, chunkMetadataList, singleSeriesExp.getFilter());

    BatchData data;

    long aimedTimestamp = 1480563570030L;

    while (seriesReader.hasNextBatch()) {
      data = seriesReader.nextBatch();
      while (data.hasCurrent()) {
        Assert.assertEquals(aimedTimestamp++, data.currentTime());
        data.next();
      }
    }
  }
}
