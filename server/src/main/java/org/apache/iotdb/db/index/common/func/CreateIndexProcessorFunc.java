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
package org.apache.iotdb.db.index.common.func;

import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.PartialPath;

import java.util.Map;

/**
 * Do something without input and output.
 *
 * <p>This is a <a href="package-summary.html">functional interface</a> whose functional method is
 * {@link #act(PartialPath indexSeries, Map indexInfoMap)}.
 */
@FunctionalInterface
public interface CreateIndexProcessorFunc {

  /** Do something. */
  IndexProcessor act(PartialPath indexSeries, Map<IndexType, IndexInfo> indexInfoMap);
}
