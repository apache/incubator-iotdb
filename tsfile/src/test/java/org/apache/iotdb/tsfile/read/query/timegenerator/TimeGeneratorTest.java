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
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TimeGeneratorTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private TsFileSequenceReader fileReader;
  private MetadataQuerierByFileImpl metadataQuerierByFile;
  private IChunkLoader chunkLoader;

  @Before
  public void before() throws IOException {
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder("TS_2DIFF");
    TsFileGeneratorForTest.generateFile(1000, 10 * 1024 * 1024, 10000);
    fileReader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH));
    metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
    chunkLoader = new CachedChunkLoaderImpl(fileReader);
  }

  @After
  public void after() throws IOException {
    fileReader.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void testTimeGenerator() throws IOException {
    long startTimestamp = 1480562618000L;
    Filter filter = TimeFilter.lt(1480562618100L);
    Filter filter2 = ValueFilter.gt(new Binary("dog"));
    Filter filter3 =
        FilterFactory.and(TimeFilter.gtEq(1480562618000L), TimeFilter.ltEq(1480562618100L));

    IExpression IExpression =
        BinaryExpression.or(
            BinaryExpression.and(
                new SingleSeriesExpression(new Path("d1", "s1"), filter),
                new SingleSeriesExpression(new Path("d1", "s4"), filter2)),
            new SingleSeriesExpression(new Path("d1", "s1"), filter3));

    TsFileTimeGenerator timestampGenerator =
        new TsFileTimeGenerator(IExpression, chunkLoader, metadataQuerierByFile);
    while (timestampGenerator.hasNext()) {
      // System.out.println(timestampGenerator.next());
      Assert.assertEquals(startTimestamp, timestampGenerator.next());
      startTimestamp += 1;
    }
    Assert.assertEquals(1480562618101L, startTimestamp);
  }
}
