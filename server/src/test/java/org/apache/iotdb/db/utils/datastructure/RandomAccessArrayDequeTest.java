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

package org.apache.iotdb.db.utils.datastructure;

import static org.junit.Assert.*;

import org.junit.Test;

public class RandomAccessArrayDequeTest {

  @Test
  public void get() {
    int eleNum = 10;
    RandomAccessArrayDeque<Long> deque = new RandomAccessArrayDeque<>(eleNum);

    // empty deque
    for (int i = -1; i <= eleNum; i++) {
      try {
        deque.get(i);
        fail("Exception expected");
      } catch (ArrayIndexOutOfBoundsException e) {
        assertEquals("Array index out of range: " + i, e.getMessage());
      }
    }

    // half-empty deque
    for (int i = 0; i < eleNum / 2; i++) {
      deque.add((long) i);
    }
    for (int i = 0; i < eleNum / 2; i++) {
      assertEquals(i, deque.get(i).intValue());
    }
    for (int i = eleNum / 2; i <= eleNum; i++) {
      try {
        deque.get(i);
        fail("Exception expected");
      } catch (ArrayIndexOutOfBoundsException e) {
        assertEquals("Array index out of range: " + i, e.getMessage());
      }
    }

    // full-deque
    deque.clear();
    for (int i = 0; i < eleNum; i++) {
      deque.add((long) i);
    }
    for (int i = 0; i < eleNum; i++) {
      assertEquals(i, deque.get(i).intValue());
    }
    try {
      deque.get(eleNum);
      fail("Exception expected");
    } catch (ArrayIndexOutOfBoundsException e) {
      assertEquals("Array index out of range: " + eleNum, e.getMessage());
    }

    // after 3 remove first
    for (int i = 0; i < 3; i++) {
      deque.removeFirst();
    }
    for (int i = 3; i < eleNum; i++) {
      assertEquals(i, deque.get(i - 3).intValue());
    }
    try {
      deque.get(eleNum);
      fail("Exception expected");
    } catch (ArrayIndexOutOfBoundsException e) {
      assertEquals("Array index out of range: " + eleNum, e.getMessage());
    }

    // after 3 remove last
    for (int i = 0; i < 3; i++) {
      deque.removeLast();
    }
    for (int i = 3; i < eleNum - 3; i++) {
      assertEquals(i, deque.get(i - 3).intValue());
    }
    try {
      deque.get(eleNum - 3);
      fail("Exception expected");
    } catch (ArrayIndexOutOfBoundsException e) {
      assertEquals("Array index out of range: " + (eleNum - 3), e.getMessage());
    }

    // after clear
    deque.clear();
    for (int i = -1; i <= eleNum; i++) {
      try {
        deque.get(i);
        fail("Exception expected");
      } catch (ArrayIndexOutOfBoundsException e) {
        assertEquals("Array index out of range: " + i, e.getMessage());
      }
    }
  }
}