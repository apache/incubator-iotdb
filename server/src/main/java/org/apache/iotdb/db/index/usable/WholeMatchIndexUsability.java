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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is to record the index usable range for a list of short series, which corresponds to
 * the whole matching scenario.
 *
 * <p>The series path involves wildcard characters. One series is marked as "index-usable" or
 * "index-unusable".
 *
 * <p>It's not thread-safe.
 */
public class WholeMatchIndexUsability implements IIndexUsable {

  private final Set<PartialPath> unusableSeriesSet;

  WholeMatchIndexUsability() {
    this.unusableSeriesSet = new HashSet<>();
  }

  @Override
  public void addUsableRange(PartialPath fullPath, long start, long end) {
    // do nothing temporarily
  }

  @Override
  public void minusUsableRange(PartialPath fullPath, long start, long end) {
    unusableSeriesSet.add(fullPath);
  }

  @Override
  public Set<PartialPath> getUnusableRange() {
    return Collections.unmodifiableSet(this.unusableSeriesSet);
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(unusableSeriesSet.size(), outputStream);
    for (PartialPath s : unusableSeriesSet) {
      ReadWriteIOUtils.write(s.getFullPath(), outputStream);
    }
  }

  @Override
  public void deserialize(InputStream inputStream) throws IllegalPathException, IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      unusableSeriesSet.add(new PartialPath(ReadWriteIOUtils.readString(inputStream)));
    }
  }
}
