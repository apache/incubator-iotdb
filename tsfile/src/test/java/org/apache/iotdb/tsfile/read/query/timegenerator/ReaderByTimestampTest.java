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
package org.apache.iotdb.tsfile.read.query.timegenerator;

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
import org.apache.iotdb.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReaderByTimestampTest {

  private static final String FILE_PATH = TsFileGeneratorForSeriesReaderByTimestamp.outputDataFile;
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private TsFileSequenceReader fileReader;
  private MetadataQuerierByFileImpl metadataQuerierByFile;
  private int rowCount = 1000000;

  @Before
  public void before() throws IOException {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
    TsFileGeneratorForSeriesReaderByTimestamp.generateFile(rowCount, 10 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH)); // TODO remove this class
    metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
  }

  @After
  public void after() throws IOException {
    fileReader.close();
    TsFileGeneratorForSeriesReaderByTimestamp.after();
  }

  @Test
  public void readByTimestamp() throws IOException {
    CachedChunkLoaderImpl seriesChunkLoader = new CachedChunkLoaderImpl(fileReader);
    List<IChunkMetadata> chunkMetadataList =
        metadataQuerierByFile.getChunkMetaDataList(new Path("d1", "s1"));
    AbstractFileSeriesReader seriesReader =
        new FileSeriesReader(seriesChunkLoader, chunkMetadataList, null);

    List<Long> timeList = new ArrayList<>();
    List<Object> valueList = new ArrayList<>();
    int count = 0;
    BatchData data = null;

    while (seriesReader.hasNextBatch()) {
      data = seriesReader.nextBatch();
      while (data.hasCurrent()) {
        timeList.add(data.currentTime() - 1);
        valueList.add(null);
        timeList.add(data.currentTime());
        valueList.add(data.currentValue());
        data.next();
        count++;
      }
    }

    long startTimestamp = System.currentTimeMillis();
    count = 0;

    FileSeriesReaderByTimestamp seriesReaderFromSingleFileByTimestamp =
        new FileSeriesReaderByTimestamp(seriesChunkLoader, chunkMetadataList);

    for (long time : timeList) {
      Object value = seriesReaderFromSingleFileByTimestamp.getValueInTimestamp(time);
      if (value == null) {
        Assert.assertNull(valueList.get(count));
      } else {
        Assert.assertEquals(valueList.get(count), value);
      }
      count++;
    }
    long endTimestamp = System.currentTimeMillis();
    System.out.println(
        "SeriesReadWithFilterTest. [Time used]: "
            + (endTimestamp - startTimestamp)
            + " ms. [Read Count]: "
            + count);
  }
}
