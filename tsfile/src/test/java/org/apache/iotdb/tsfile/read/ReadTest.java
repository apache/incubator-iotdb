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
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.FileGenerator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ReadTest {

  private static String fileName = TestConstant.BASE_OUTPUT_PATH.concat("perTestOutputData.tsfile");
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private static ReadOnlyTsFile roTsFile = null;

  @Before
  public void prepare() throws IOException {
    FileGenerator.generateFile(1000, 100);
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(fileName));
    roTsFile = new ReadOnlyTsFile(reader);
  }

  @After
  public void after() throws IOException {
    if (roTsFile != null) {
      roTsFile.close();
    }
    FileGenerator.after();
  }

  @Test
  public void queryOneMeasurementWithoutFilterTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s1"));
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (count == 0) {
        assertEquals(1480562618010L, r.getTimestamp());
      }
      if (count == 499) {
        assertEquals(1480562618999L, r.getTimestamp());
      }
      count++;
    }
    assertEquals(500, count);
  }

  @Test
  public void queryTwoMeasurementsWithoutFilterTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s1"));
    pathList.add(new Path("d2", "s2"));
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int count = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (count == 0) {
        assertEquals(1480562618005L, r.getTimestamp());
      }
      count++;
    }
    assertEquals(750, count);
  }

  @Test
  public void queryTwoMeasurementsWithSingleFilterTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d2", "s1"));
    pathList.add(new Path("d2", "s4"));
    IExpression valFilter = new SingleSeriesExpression(new Path("d2", "s2"), ValueFilter.gt(9722L));
    IExpression tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
            new GlobalTimeExpression(TimeFilter.lt(1480562618977L)));
    IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
    QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int cnt = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      cnt++;
    }
  }

  @Test
  public void queryOneMeasurementsWithSameFilterTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d2", "s2"));
    IExpression valFilter = new SingleSeriesExpression(new Path("d2", "s2"), ValueFilter.gt(9722L));
    QueryExpression queryExpression = QueryExpression.create(pathList, valFilter);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int cnt = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Field value = record.getFields().get(0);
      if (cnt == 0) {
        assertEquals(1480562618973L, record.getTimestamp());
        assertEquals(9732, value.getLongV());
      } else if (cnt == 1) {
        assertEquals(1480562618974L, record.getTimestamp());
        assertEquals(9742, value.getLongV());
      } else if (cnt == 7) {
        assertEquals(1480562618985L, record.getTimestamp());
        assertEquals(9852, value.getLongV());
      }

      cnt++;
    }
  }

  @Test
  public void queryWithTwoSeriesTimeValueFilterCrossTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s1"));
    pathList.add(new Path("d2", "s2"));
    IExpression valFilter =
        new SingleSeriesExpression(new Path("d2", "s2"), ValueFilter.notEq(9722L));
    IExpression tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
            new GlobalTimeExpression(TimeFilter.lt(1480562618977L)));
    IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
    QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    // time filter & value filter
    // verify d1.s1, d2.s1
    int cnt = 1;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (cnt == 1) {
        assertEquals(1480562618970L, r.getTimestamp());
      } else if (cnt == 2) {
        assertEquals(1480562618971L, r.getTimestamp());
      } else if (cnt == 3) {
        assertEquals(1480562618973L, r.getTimestamp());
      }
      cnt++;
    }
    assertEquals(7, cnt);
  }

  @Test
  public void queryWithCrossSeriesTimeValueFilterTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s1"));
    pathList.add(new Path("d2", "s2"));
    IExpression valFilter =
        new SingleSeriesExpression(new Path("d2", "s2"), ValueFilter.notEq(9722L));
    IExpression tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
            new GlobalTimeExpression(TimeFilter.lt(1480562618975L)));
    IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
    QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    // time filter & value filter
    // verify d1.s1, d2.s1
    /**
     * 1480562618950 9501 9502 1480562618954 9541 9542 1480562618955 9551 9552 1480562618956 9561
     * 9562
     */
    int cnt = 1;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (cnt == 1) {
        assertEquals(1480562618970L, r.getTimestamp());
      } else if (cnt == 2) {
        assertEquals(1480562618971L, r.getTimestamp());
      } else if (cnt == 3) {
        assertEquals(1480562618973L, r.getTimestamp());
      } else if (cnt == 4) {
        assertEquals(1480562618974L, r.getTimestamp());
      }
      cnt++;
    }
    assertEquals(5, cnt);

    pathList.clear();
    pathList.add(new Path("d1", "s1"));
    pathList.add(new Path("d2", "s2"));
    valFilter = new SingleSeriesExpression(new Path("d2", "s2"), ValueFilter.ltEq(9082L));
    tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618906L)),
            new GlobalTimeExpression(TimeFilter.ltEq(1480562618915L)));
    tFilter =
        BinaryExpression.or(
            tFilter,
            BinaryExpression.and(
                new GlobalTimeExpression(TimeFilter.gtEq(1480562618928L)),
                new GlobalTimeExpression(TimeFilter.ltEq(1480562618933L))));
    finalFilter = BinaryExpression.and(valFilter, tFilter);
    queryExpression = QueryExpression.create(pathList, finalFilter);
    dataSet = roTsFile.query(queryExpression);

    // time filter & value filter
    // verify d1.s1, d2.s1
    cnt = 1;
    while (dataSet.hasNext()) {
      dataSet.next();
      cnt++;
    }
    assertEquals(4, cnt);
  }

  @Test
  public void queryBooleanTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s5"));
    IExpression valFilter = new SingleSeriesExpression(new Path("d1", "s5"), ValueFilter.eq(false));
    IExpression tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
            new GlobalTimeExpression(TimeFilter.lt(1480562618981L)));
    IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
    QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int cnt = 1;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (cnt == 1) {
        assertEquals(1480562618972L, r.getTimestamp());
        Field f1 = r.getFields().get(0);
        assertFalse(f1.getBoolV());
      }
      if (cnt == 2) {
        assertEquals(1480562618981L, r.getTimestamp());
        Field f2 = r.getFields().get(0);
        assertFalse(f2.getBoolV());
      }
      cnt++;
    }
  }

  @Test
  public void queryStringTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s4"));
    IExpression tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
            new GlobalTimeExpression(TimeFilter.ltEq(1480562618981L)));
    QueryExpression queryExpression = QueryExpression.create(pathList, tFilter);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int cnt = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (cnt == 0) {
        assertEquals(1480562618976L, r.getTimestamp());
        Field f1 = r.getFields().get(0);
        assertEquals("dog976", f1.toString());
      }
      cnt++;
    }
    Assert.assertEquals(1, cnt);

    pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s4"));
    tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
            new GlobalTimeExpression(TimeFilter.ltEq(1480562618981L)));
    queryExpression = QueryExpression.create(pathList, tFilter);
    dataSet = roTsFile.query(queryExpression);
    cnt = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (cnt == 1) {
        assertEquals(1480562618976L, r.getTimestamp());
        Field f1 = r.getFields().get(0);
        assertEquals("dog976", f1.getBinaryV().getStringValue());
      }
      cnt++;
    }
    Assert.assertEquals(1, cnt);
  }

  @Test
  public void queryFloatTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s6"));
    IExpression valFilter =
        new SingleSeriesExpression(new Path("d1", "s6"), ValueFilter.gt(103.0f));
    IExpression tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618970L)),
            new GlobalTimeExpression(TimeFilter.ltEq(1480562618981L)));
    IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
    QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int cnt = 0;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (cnt == 1) {
        assertEquals(1480562618980L, r.getTimestamp());
        Field f1 = r.getFields().get(0);
        assertEquals(108.0, f1.getFloatV(), 0.0);
      }
      if (cnt == 2) {
        assertEquals(1480562618990L, r.getTimestamp());
        Field f2 = r.getFields().get(0);
        assertEquals(110.0, f2.getFloatV(), 0.0);
      }
      cnt++;
    }
  }

  @Test
  public void queryDoubleTest() throws IOException {
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s7"));
    IExpression valFilter = new SingleSeriesExpression(new Path("d1", "s7"), ValueFilter.gt(1.0));
    IExpression tFilter =
        BinaryExpression.and(
            new GlobalTimeExpression(TimeFilter.gtEq(1480562618011L)),
            new GlobalTimeExpression(TimeFilter.ltEq(1480562618033L)));
    IExpression finalFilter = BinaryExpression.and(valFilter, tFilter);
    QueryExpression queryExpression = QueryExpression.create(pathList, finalFilter);
    QueryDataSet dataSet = roTsFile.query(queryExpression);

    int cnt = 1;
    while (dataSet.hasNext()) {
      RowRecord r = dataSet.next();
      if (cnt == 1) {
        assertEquals(1480562618022L, r.getTimestamp());
        Field f1 = r.getFields().get(0);
        assertEquals(2.0, f1.getDoubleV(), 0.0);
      }
      if (cnt == 2) {
        assertEquals(1480562618033L, r.getTimestamp());
        Field f1 = r.getFields().get(0);
        assertEquals(3.0, f1.getDoubleV(), 0.0);
      }
      cnt++;
    }
  }
}
