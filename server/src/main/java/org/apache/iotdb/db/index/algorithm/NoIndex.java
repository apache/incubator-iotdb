/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.algorithm;

import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.read.IndexQueryDataSet;
import org.apache.iotdb.db.index.read.optimize.IIndexCandidateOrderOptimize;
import org.apache.iotdb.db.index.usable.IIndexUsable;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * NoIndex do nothing on feature extracting and data pruning. Its index-available range is always
 * empty.
 */
public class NoIndex extends IoTDBIndex {

  public NoIndex(PartialPath path, TSDataType tsDataType, IndexInfo indexInfo) {
    super(path, tsDataType, indexInfo);
  }

  @Override
  public void initFeatureExtractor(ByteBuffer previous, boolean inQueryMode) {
    // NoIndex does nothing
  }

  @Override
  public boolean buildNext() {
    return true;
  }

  @Override
  protected void flushIndex() {
    // NoIndex does nothing
  }

  @Override
  public QueryDataSet query(
      Map<String, Object> queryProps,
      IIndexUsable indexUsable,
      QueryContext context,
      IIndexCandidateOrderOptimize candidateOrderOptimize,
      boolean alignedByTime) {
    return new IndexQueryDataSet(
        Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());
  }
}
