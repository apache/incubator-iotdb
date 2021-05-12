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
package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.expression.ExpressionType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
 This test is designed for the TsFileExecutor's execute(queryExpression, params) function.

 The test target here is the logic of converting the query partition constraint to an additional time filter.

 Note that the correctness of the constructed additional time filter, which is guaranteed and tested in
 IMetadataQuerierByFileImplTest and TimeRangeTest, is not the test focus here.

*/
public class ReadInPartitionTest {

  private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private static ReadOnlyTsFile roTsFile = null;
  private ArrayList<TimeRange> d1s6timeRangeList = new ArrayList<>();
  private ArrayList<TimeRange> d2s1timeRangeList = new ArrayList<>();
  private ArrayList<long[]> d1chunkGroupMetaDataOffsetList = new ArrayList<>();

  @Before
  public void before() throws IOException {
    TsFileGeneratorForTest.generateFile(10000, 1024, 100);
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH));
    roTsFile = new ReadOnlyTsFile(reader);

    // Because the size of the generated chunkGroupMetaData may differ under
    // different test environments,
    // we get metadata from the real-time generated TsFile instead of using a fixed
    // parameter setting.
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
    }
  }

  @After
  public void after() throws IOException {
    roTsFile.close();
    TsFileGeneratorForTest.after();
  }

  @Test
  public void test0() throws IOException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s6"));
    paths.add(new Path("d2", "s1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = roTsFile.query(queryExpression, 0L, 0L);

    // test the transformed expression
    Assert.assertNull(queryExpression.getExpression());

    // test the equivalence of the query result
    Assert.assertFalse(queryDataSet.hasNext());
  }

  @Test
  public void test1() throws IOException, QueryFilterOptimizationException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s6"));
    paths.add(new Path("d2", "s1"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet =
        roTsFile.query(
            queryExpression,
            d1chunkGroupMetaDataOffsetList.get(0)[0],
            d1chunkGroupMetaDataOffsetList.get(0)[1]);
    // get the transformed expression
    IExpression transformedExpression = queryExpression.getExpression();

    // test the transformed expression
    Assert.assertEquals(ExpressionType.GLOBAL_TIME, transformedExpression.getType());

    IExpression expectedTimeExpression = d1s6timeRangeList.get(0).getExpression();
    String expected =
        ExpressionOptimizer.getInstance()
            .optimize(expectedTimeExpression, queryExpression.getSelectedSeries())
            .toString();
    Assert.assertEquals(expected, transformedExpression.toString());

    // test the equivalence of the query result:
    QueryDataSet queryDataSet_eq = roTsFile.query(queryExpression);
    while (queryDataSet.hasNext() && queryDataSet_eq.hasNext()) {
      RowRecord r = queryDataSet.next();
      RowRecord r2 = queryDataSet_eq.next();
      Assert.assertEquals(r2.toString(), r.toString());
    }
    Assert.assertEquals(queryDataSet_eq.hasNext(), queryDataSet.hasNext());
  }

  @Test
  public void test2() throws IOException, QueryFilterOptimizationException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s6"));
    paths.add(new Path("d2", "s1"));
    IExpression expression = new GlobalTimeExpression(TimeFilter.gt(50L));
    QueryExpression queryExpression = QueryExpression.create(paths, expression);

    QueryDataSet queryDataSet =
        roTsFile.query(
            queryExpression,
            d1chunkGroupMetaDataOffsetList.get(0)[0],
            d1chunkGroupMetaDataOffsetList.get(0)[1]);
    // get the transformed expression
    IExpression transformedExpression = queryExpression.getExpression();

    // test the transformed expression
    Assert.assertEquals(ExpressionType.GLOBAL_TIME, transformedExpression.getType());

    IExpression expectedTimeExpression =
        BinaryExpression.and(expression, d1s6timeRangeList.get(0).getExpression());
    String expected =
        ExpressionOptimizer.getInstance()
            .optimize(expectedTimeExpression, queryExpression.getSelectedSeries())
            .toString();
    Assert.assertEquals(expected, transformedExpression.toString());

    // test the equivalence of the query result:
    QueryDataSet queryDataSet_eq = roTsFile.query(queryExpression);
    while (queryDataSet.hasNext() && queryDataSet_eq.hasNext()) {
      RowRecord r = queryDataSet.next();
      RowRecord r2 = queryDataSet_eq.next();
      Assert.assertEquals(r2.toString(), r.toString());
    }
    Assert.assertEquals(queryDataSet_eq.hasNext(), queryDataSet.hasNext());
  }

  @Test
  public void test3() throws IOException, QueryFilterOptimizationException {
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("d1", "s6"));
    paths.add(new Path("d2", "s1"));
    Filter filter = ValueFilter.gt(10L);
    IExpression expression = new SingleSeriesExpression(new Path("d1", "s3"), filter);
    QueryExpression queryExpression = QueryExpression.create(paths, expression);

    QueryDataSet queryDataSet =
        roTsFile.query(
            queryExpression,
            d1chunkGroupMetaDataOffsetList.get(0)[0],
            d1chunkGroupMetaDataOffsetList.get(0)[1]);
    // get the transformed expression
    IExpression transformedExpression = queryExpression.getExpression();

    // test the transformed expression
    Assert.assertEquals(ExpressionType.SERIES, transformedExpression.getType());

    IExpression expectedTimeExpression =
        BinaryExpression.and(expression, d1s6timeRangeList.get(0).getExpression());
    String expected =
        ExpressionOptimizer.getInstance()
            .optimize(expectedTimeExpression, queryExpression.getSelectedSeries())
            .toString();
    Assert.assertEquals(expected, transformedExpression.toString());

    // test the equivalence of the query result:
    QueryDataSet queryDataSet_eq = roTsFile.query(queryExpression);
    while (queryDataSet.hasNext() && queryDataSet_eq.hasNext()) {
      RowRecord r = queryDataSet.next();
      RowRecord r2 = queryDataSet_eq.next();
      Assert.assertEquals(r2.toString(), r.toString());
    }
    Assert.assertEquals(queryDataSet_eq.hasNext(), queryDataSet.hasNext());
  }
}
