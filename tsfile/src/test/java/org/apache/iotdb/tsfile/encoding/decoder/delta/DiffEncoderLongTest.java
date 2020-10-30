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
package org.apache.iotdb.tsfile.encoding.decoder.delta;

import org.apache.iotdb.tsfile.encoding.decoder.DiffDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DiffEncoder;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class DiffEncoderLongTest {

  private static int ROW_NUM = 100_000_00;
  private final long BASIC_FACTOR = 1l << 32;
  ByteArrayOutputStream out;
  private DiffEncoder writer;
  private DiffDecoder reader;
  private Random ran = new Random();
  private ByteBuffer buffer;

  @Before
  public void test() {
    writer = new DiffEncoder.LongDeltaEncoder();
    reader = new DiffDecoder.LongDeltaDecoder();
  }

  @Test
  public void testBasic() throws IOException {
    reader.reset();
    long data[] = new long[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = i * i * BASIC_FACTOR;
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testBoundInt() throws IOException {
    reader.reset();
    long data[] = new long[ROW_NUM];
    for (int i = 2; i < 21; i++) {
      boundInt(i, data);
    }
  }

  private void boundInt(int power, long[] data) throws IOException {
    reader.reset();
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = ran.nextInt((int) Math.pow(2, power)) * BASIC_FACTOR;
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRandom() throws IOException {
    reader.reset();
    long data[] = new long[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = ran.nextLong();
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testMaxMin() throws IOException {
    reader.reset();
    long data[] = new long[ROW_NUM];
    for (int i = 0; i < ROW_NUM; i++) {
      data[i] = (i & 1) == 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
    }
    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularEncoding() throws IOException {
    reader.reset();
    List<String> dates = getBetweenDate("1970-01-08", "2004-03-29");

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    ROW_NUM = dates.size();
    System.out.println("ROW_NUM: "+ROW_NUM);

    long[] data = new long[ROW_NUM];
    for (int i = 0; i < dates.size(); i++) {
      try {
        Date date = dateFormat.parse(dates.get(i));
        data[i] = date.getTime();
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }

    shouldReadAndWrite(data, ROW_NUM);
  }

  @Test
  public void testRegularWithMissingPoints() throws IOException {
    reader.reset();
    List<String> dates = getBetweenDate("1970-01-08", "2004-04-24");

    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    int kong = 0;
    for (int i = 0; i < dates.size(); i++) {
      if (i % 500 == 0) {
        kong++;
      }
    }

    ROW_NUM = dates.size() - kong;

    long[] data = new long[ROW_NUM];
    int j = 0;
    for (int i = 0; i < dates.size(); i++) {
      if (i % 500 == 0) {
        continue;
      }

      try {
        Date date = dateFormat.parse(dates.get(i));
        data[j++] = date.getTime();
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }

    shouldReadAndWrite(data, ROW_NUM);
  }

  private List<String> getBetweenDate(String start, String end) {
    List<String> list = new ArrayList<>();
    LocalDate startDate = LocalDate.parse(start);
    LocalDate endDate = LocalDate.parse(end);

    long distance = ChronoUnit.DAYS.between(startDate, endDate);
    if (distance < 1) {
      return list;
    }
    Stream.iterate(startDate, d -> {
      return d.plusDays(1);
    }).limit(distance + 1).forEach(f -> {
      list.add(f.toString());
    });
    return list;
  }

  private void writeData(long[] data, int length) throws IOException {
    for (int i = 0; i < length; i++) {
      writer.encode(data[i], out);
    }
    writer.flush(out);
  }

  private void shouldReadAndWrite(long[] data, int length) throws IOException {
    System.out.println("source data size:" + 8 * length + " byte");
    out = new ByteArrayOutputStream();

    long encodeStart=System.nanoTime();
    writeData(data, length);
    Long encodeEnd=System.nanoTime();
    System.out.println("encode take(ns): "+(encodeEnd-encodeStart));

    byte[] page = out.toByteArray();
    System.out.println("encoding data size:" + page.length + " byte");
    buffer = ByteBuffer.wrap(page);
    int i = 0;

    Long decodeStart=System.nanoTime();
    while (reader.hasNext(buffer)) {
      assertEquals(data[i++], reader.readLong(buffer));
    }
    Long decodeEnd=System.nanoTime();
    System.out.println("decode take(ns): "+(decodeEnd-decodeStart));
  }

}
