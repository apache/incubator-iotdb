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
package org.apache.iotdb.db.index.usable;

import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class SubMatchIndexUsabilityTest {

  @Test
  public void addUsableRange1() throws IOException {
    SubMatchIndexUsability usability = new SubMatchIndexUsability(4, false);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());
    // split range
    usability.addUsableRange(null, 1, 9);
    Assert.assertEquals("size:2,[MIN,0],[10,MAX],", usability.toString());
    System.out.println(usability);
    usability.addUsableRange(null, 21, 29);
    Assert.assertEquals("size:3,[MIN,0],[10,20],[30,MAX],", usability.toString());
    System.out.println(usability);
    usability.addUsableRange(null, 41, 49);
    Assert.assertEquals("size:4,[MIN,0],[10,20],[30,40],[50,MAX],", usability.toString());
    System.out.println(usability);
    // cover a range
    usability.addUsableRange(null, 29, 45);
    Assert.assertEquals("size:3,[MIN,0],[10,20],[50,MAX],", usability.toString());
    System.out.println(usability);

    // split a range
    usability.addUsableRange(null, 14, 17);
    Assert.assertEquals("size:4,[MIN,0],[10,13],[18,20],[50,MAX],", usability.toString());
    System.out.println(usability);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    usability.serialize(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    SubMatchIndexUsability usability2 = new SubMatchIndexUsability();
    usability2.deserialize(in);
    Assert.assertEquals("size:4,[MIN,0],[10,13],[18,20],[50,MAX],", usability2.toString());
  }

  @Test
  public void addUsableRange2() throws IOException {
    SubMatchIndexUsability usability = new SubMatchIndexUsability(4, false);
    usability.addUsableRange(null, 1, 19);
    usability.addUsableRange(null, 51, 59);
    usability.addUsableRange(null, 81, 99);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[20,50],[60,80],[100,MAX],", usability.toString());
    // left cover
    usability.addUsableRange(null, 10, 29);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[30,50],[60,80],[100,MAX],", usability.toString());
    // right cover
    usability.addUsableRange(null, 71, 99);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,0],[30,50],[60,70],[100,MAX],", usability.toString());
    // left cover multiple
    usability.addUsableRange(null, 20, 80);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,0],[100,MAX],", usability.toString());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    usability.serialize(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    SubMatchIndexUsability usability2 = new SubMatchIndexUsability();
    usability2.deserialize(in);
    Assert.assertEquals("size:2,[MIN,0],[100,MAX],", usability2.toString());
  }

  @Test
  public void minusUsableRange() throws IOException {
    SubMatchIndexUsability usability = new SubMatchIndexUsability(4, false);

    // merge with MIN, MAX
    usability.minusUsableRange(null, 1, 19);
    System.out.println(usability);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());

    // new range is covered by the first range
    usability.addUsableRange(null, 51, 89);
    usability.minusUsableRange(null, 1, 19);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,50],[90,MAX],", usability.toString());

    usability.addUsableRange(null, 101, 259);
    // new range extend node's end time
    usability.minusUsableRange(null, 51, 60);
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,60],[90,100],[260,MAX],", usability.toString());

    // new range extend node's start time
    usability.minusUsableRange(null, 80, 89);
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,60],[80,100],[260,MAX],", usability.toString());

    // new range is inserted as a individual node [120, 140]
    usability.minusUsableRange(null, 120, 140);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[120,140],[260,MAX],", usability.toString());

    // re-insert: new range is totally same as an exist node
    usability.minusUsableRange(null, 120, 140);
    System.out.println(usability);
    Assert.assertTrue(usability.hasUnusableRange());
    Assert.assertEquals("size:4,[MIN,60],[80,100],[120,140],[260,MAX],", usability.toString());

    // re-insert: new range extend the both sides of an exist node
    usability.minusUsableRange(null, 110, 150);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[110,150],[260,MAX],", usability.toString());

    // an isolate range but the segmentation number reaches the upper bound, thus merge the range
    // with a closer neighbor.
    usability.minusUsableRange(null, 200, 220);
    System.out.println(usability);
    Assert.assertEquals("size:4,[MIN,60],[80,100],[110,150],[200,MAX],", usability.toString());

    // a range covers several node.
    usability.minusUsableRange(null, 50, 90);
    System.out.println(usability);
    Assert.assertEquals("size:3,[MIN,100],[110,150],[200,MAX],", usability.toString());

    // a range covers several node.
    usability.minusUsableRange(null, 105, 200);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,100],[105,MAX],", usability.toString());
    Assert.assertTrue(usability.hasUnusableRange());
    // the end
    usability.minusUsableRange(null, 101, 107);
    System.out.println(usability);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());
    Assert.assertFalse(usability.hasUnusableRange());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    usability.serialize(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    SubMatchIndexUsability usability2 = new SubMatchIndexUsability();
    usability2.deserialize(in);
    Assert.assertEquals("size:1,[MIN,MAX],", usability2.toString());
  }

  @Test
  public void subMatchingToFilter() {
    SubMatchIndexUsability a = new SubMatchIndexUsability(10, false);
    Assert.assertFalse(a.hasUnusableRange());
    a.addUsableRange(null, 31, 39);
    a.addUsableRange(null, 61, 89);
    System.out.println(a);

    Assert.assertTrue(a.hasUnusableRange());
    List<Filter> res = a.getUnusableRange();
    Assert.assertEquals(3, res.size());
    System.out.println(res);
    Assert.assertEquals(
        "[(time >= -9223372036854775808 && time <= 30), (time >= 40 && time <= 60), (time >= 90 && time <= 9223372036854775807)]",
        res.toString());
  }

  @Test
  public void minusUsableRange2() {
    SubMatchIndexUsability usability = new SubMatchIndexUsability(4, false);
    Assert.assertFalse(usability.hasUnusableRange());
    usability.addUsableRange(null, 0, 15);
    usability.minusUsableRange(null, 20, 24);
    Assert.assertEquals("size:2,[MIN,-1],[16,MAX],", usability.toString());
    Assert.assertTrue(usability.hasUnusableRange());
    System.out.println(usability.toString());
  }

  @Test
  public void reachUpBoundAndStopSplit() {
    SubMatchIndexUsability usability = new SubMatchIndexUsability(2, false);
    System.out.println(usability);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());

    usability.addUsableRange(null, 5, 7);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,4],[8,MAX],", usability.toString());
    usability.addUsableRange(null, 2, 3);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,4],[8,MAX],", usability.toString());
    usability.minusUsableRange(null, 6, 6);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,4],[6,MAX],", usability.toString());
    usability.addUsableRange(null, 6, 9);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,4],[10,MAX],", usability.toString());
    usability.minusUsableRange(null, 6, 6);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,6],[10,MAX],", usability.toString());
    usability.addUsableRange(null, 2, 6);
    System.out.println(usability);
    Assert.assertEquals("size:2,[MIN,1],[10,MAX],", usability.toString());
  }

  @Test
  public void testBoundCase() {
    SubMatchIndexUsability usability = new SubMatchIndexUsability(2, true);
    usability.addUsableRange(null, Long.MIN_VALUE, Long.MAX_VALUE);
    System.out.println(usability);
    Assert.assertEquals(
        "size:2,[MIN,-9223372036854775808],[9223372036854775807,MAX],", usability.toString());
    usability.minusUsableRange(null, Long.MIN_VALUE, Long.MAX_VALUE);
    System.out.println(usability);
    Assert.assertEquals("size:1,[MIN,MAX],", usability.toString());
    Assert.assertTrue(usability.hasUnusableRange());
  }
}
