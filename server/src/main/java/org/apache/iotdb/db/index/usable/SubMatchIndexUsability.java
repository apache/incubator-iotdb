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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is to record the index usable range for a single long series, which corresponds to the
 * subsequence matching scenario.
 *
 * <p>The series path in the initialized parameters must be a full path (without wildcard
 * characters).
 *
 * <p>It's not thread-safe.
 */
public class SubMatchIndexUsability implements IIndexUsable {

  private int maxSizeOfUsableSegments;
  private final boolean initAllUsable;
  private RangeNode unusableRanges;
  private int size;

  SubMatchIndexUsability() {
    this(IoTDBDescriptor.getInstance().getConfig().getDefaultMaxSizeOfUnusableSegments(), true);
  }

  /**
   * constructor.
   *
   * @param maxSizeOfUsableSegments the max size of usable segments
   * @param initAllUsable true if the entire time range is set to "usable".
   */
  SubMatchIndexUsability(int maxSizeOfUsableSegments, boolean initAllUsable) {
    this.maxSizeOfUsableSegments = maxSizeOfUsableSegments;
    this.initAllUsable = initAllUsable;
    unusableRanges = new RangeNode(Long.MIN_VALUE, Long.MAX_VALUE, null);
    size = 1;
    if (initAllUsable) {
      addUsableRange(null, 0, Long.MAX_VALUE - 1);
    }
  }

  @Override
  public void addUsableRange(PartialPath fullPath, long startTime, long endTime) {
    // simplify the problem
    if (startTime == Long.MIN_VALUE) {
      startTime = startTime + 1;
    }
    if (endTime == Long.MAX_VALUE) {
      endTime = endTime - 1;
    }
    RangeNode node = locateIdxByTime(startTime);
    RangeNode prevNode = node;
    while (node != null && node.start <= endTime) {
      if (node.start > node.end) {
        throw new IllegalArgumentException(
            String.format("Meet error. node.start=%d, node.end=%d.", node.start, node.end));
      }
      if (startTime > endTime) {
        throw new IllegalArgumentException(
            String.format("Meet error. startTime=%d, endTime=%d.", startTime, endTime));
      }
      if (node.end < startTime) {
        prevNode = node;
        node = node.next;
      } else if (startTime <= node.start && node.end <= endTime) {
        // the unusable range is covered.
        prevNode.next = node.next;
        node = node.next;
        size--;
      } else if (node.start <= startTime && endTime <= node.end) {
        if (node.start == startTime) {
          // left aligned
          node.start = endTime + 1;
          prevNode = node;
          node = node.next;
        } else if (node.end == endTime) {
          // right aligned
          node.end = startTime - 1;
          prevNode = node;
          node = node.next;
        }
        // the unusable range is split. If it reaches the upper bound, not split it
        else if (size < maxSizeOfUsableSegments) {
          RangeNode newNode = new RangeNode(endTime + 1, node.end, node.next);
          node.end = startTime - 1;
          newNode.next = node.next;
          node.next = newNode;
          prevNode = newNode;
          node = newNode.next;
          size++;
        } else {
          // don't split, thus do nothing.
          prevNode = node;
          node = node.next;
        }
      } else if (startTime < node.start) {
        node.start = endTime + 1;
        prevNode = node;
        node = node.next;
      } else {
        node.end = startTime - 1;
        prevNode = node;
        node = node.next;
      }
    }
  }

  @Override
  public void minusUsableRange(PartialPath fullPath, long startTime, long endTime) {
    // simplify the problem
    if (startTime == Long.MIN_VALUE) {
      startTime = startTime + 1;
    }
    if (endTime == Long.MAX_VALUE) {
      endTime = endTime - 1;
    }
    RangeNode node = locateIdxByTime(startTime);
    if (node == null || endTime <= node.end) {
      return;
    }
    // add the unusable range into the list
    RangeNode newNode;
    if (startTime <= node.end + 1) {
      // overlapping this node
      node.end = endTime;
      newNode = node;
      node = newNode.next;
    } else if (node.next.start <= endTime + 1) {
      newNode = node.next;
      if (startTime < newNode.start) {
        newNode.start = startTime;
      }
      if (newNode.end < endTime) {
        newNode.end = endTime;
      }
      node = newNode.next;
    } else {
      // non-overlap with former and latter nodes
      if (size < maxSizeOfUsableSegments) {
        // it doesn't overlap with latter node, new node is inserted
        newNode = new RangeNode(startTime, endTime, node.next);
        node.next = newNode;
        size++;
      } else {
        // we cannot add more range, merge it with closer neighbor.
        if (startTime - node.end < node.next.start - endTime) {
          node.end = endTime;
        } else {
          node.next.start = startTime;
        }
      }
      return;
    }

    // merge the overlapped list
    while (node != null && node.start <= endTime + 1) {
      if (newNode.end < node.end) {
        newNode.end = node.end;
      }
      // delete the redundant node
      newNode.next = node.next;
      node = node.next;
      size--;
    }
  }

  @Override
  public List<Filter> getUnusableRange() {
    List<Filter> res = new ArrayList<>();
    RangeNode p = this.unusableRanges;
    while (p != null) {
      res.add(toFilter(p.start, p.end));
      p = p.next;
    }
    return res;
  }

  /**
   * Find the latest node whose start time is less than {@code timestamp} A naive scanning search
   * for the linked list. Further optimization is still going on. It returns a node and there is
   * {@code node.start <= timestamp}
   */
  private RangeNode locateIdxByTime(long timestamp) {
    if (unusableRanges.next == null) {
      return unusableRanges;
    }
    RangeNode res = null;
    RangeNode node = unusableRanges;
    while (node.next != null) {
      if (node.start >= timestamp) {
        break;
      }
      res = node;
      node = node.next;
    }
    return res;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(size, outputStream);
    ReadWriteIOUtils.write(maxSizeOfUsableSegments, outputStream);
    RangeNode.serialize(unusableRanges, outputStream);
  }

  @Override
  public void deserialize(InputStream inputStream) throws IOException {
    size = ReadWriteIOUtils.readInt(inputStream);
    maxSizeOfUsableSegments = ReadWriteIOUtils.readInt(inputStream);
    unusableRanges = RangeNode.deserialize(inputStream);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(String.format("size:%d,", size));
    RangeNode node = unusableRanges;
    while (node != null) {
      sb.append(node.toString());
      node = node.next;
    }
    return sb.toString();
  }

  private static Filter toFilter(long startTime, long endTime) {
    return FilterFactory.and(TimeFilter.gtEq(startTime), TimeFilter.ltEq(endTime));
  }

  public boolean hasUnusableRange() {
    if (initAllUsable) {
      return !(unusableRanges.end == 0
          && unusableRanges.next != null
          && unusableRanges.next.start == Long.MAX_VALUE - 1
          && unusableRanges.next.next == null);
    } else {
      return size > 1;
    }
  }

  private static class RangeNode {

    long start;
    long end;
    RangeNode next;

    RangeNode(long start, long end, RangeNode next) {
      this.start = start;
      this.end = end;
      this.next = next;
    }

    @Override
    public String toString() {
      return "["
          + (start == Long.MIN_VALUE ? "MIN" : start)
          + ","
          + (end == Long.MAX_VALUE ? "MAX" : end)
          + "],";
    }

    public static void serialize(RangeNode head, OutputStream outputStream) throws IOException {
      int listLength = 0;
      RangeNode tmp = head;
      while (tmp != null) {
        listLength++;
        tmp = tmp.next;
      }
      ReadWriteIOUtils.write(listLength, outputStream);
      while (head != null) {
        ReadWriteIOUtils.write(head.start, outputStream);
        ReadWriteIOUtils.write(head.end, outputStream);
        head = head.next;
      }
    }

    public static RangeNode deserialize(InputStream inputStream) throws IOException {
      int listLength = ReadWriteIOUtils.readInt(inputStream);
      RangeNode fakeHead = new RangeNode(-1, -1, null);
      RangeNode node = fakeHead;
      for (int i = 0; i < listLength; i++) {
        long start = ReadWriteIOUtils.readLong(inputStream);
        long end = ReadWriteIOUtils.readLong(inputStream);
        RangeNode nextNode = new RangeNode(start, end, null);
        node.next = nextNode;
        node = nextNode;
      }
      return fakeHead.next;
    }
  }
}
