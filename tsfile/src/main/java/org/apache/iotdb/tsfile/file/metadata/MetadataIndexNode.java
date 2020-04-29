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

package org.apache.iotdb.tsfile.file.metadata;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class MetadataIndexNode {

  private static final int MAX_DEGREE_OF_INDEX_NODE = TSFileDescriptor.getInstance().getConfig()
      .getMaxDegreeOfIndexNode();
  private List<MetadataIndexEntry> children;
  private long endOffset;

  public MetadataIndexNode() {
    children = new ArrayList<>();
    endOffset = -1L;
  }

  public MetadataIndexNode(List<MetadataIndexEntry> children, long endOffset) {
    this.children = children;
    this.endOffset = endOffset;
  }

  public List<MetadataIndexEntry> getChildren() {
    return children;
  }

  public long getEndOffset() {
    return endOffset;
  }

  public void setEndOffset(long endOffset) {
    this.endOffset = endOffset;
  }

  public void addEntry(MetadataIndexEntry metadataIndexEntry) {
    this.children.add(metadataIndexEntry);
  }

  boolean isFull() {
    return children.size() == MAX_DEGREE_OF_INDEX_NODE;
  }

  MetadataIndexEntry peek() {
    if (children.isEmpty()) {
      return null;
    }
    return children.get(0);
  }

  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.write(children.size(), outputStream);
    for (MetadataIndexEntry metadataIndexEntry : children) {
      byteLen += metadataIndexEntry.serializeTo(outputStream);
    }
    byteLen += ReadWriteIOUtils.write(endOffset, outputStream);
    return byteLen;
  }

  public static MetadataIndexNode deserializeFrom(ByteBuffer buffer) {
    List<MetadataIndexEntry> children = new ArrayList<>();
    int size = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < size; i++) {
      children.add(MetadataIndexEntry.deserializeFrom(buffer));
    }
    long offset = ReadWriteIOUtils.readLong(buffer);
    return new MetadataIndexNode(children, offset);
  }

  public Pair<MetadataIndexEntry, Long> getChildIndexEntry(String key) {
    int index = binarySearchInChildren(key);
    long endOffset;
    if (index != children.size() - 1) {
      endOffset = children.get(index + 1).getOffset();
    } else {
      endOffset = this.endOffset;
    }
    return new Pair<>(children.get(index), endOffset);
  }

  public int binarySearchInChildren(String key) {
    int low = 0;
    int high = children.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      MetadataIndexEntry midVal = children.get(mid);
      int cmp = midVal.getName().compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return low - 1;  // key not found
  }
}
