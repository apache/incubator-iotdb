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
package org.apache.iotdb.db.index.read;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.index.QueryIndexException;
import org.apache.iotdb.db.index.IndexManager;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.index.common.IndexUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.util.List;
import java.util.Map;

/** the executor for index query. */
public class QueryIndexExecutor {

  private final Map<String, Object> queryProps;
  private final IndexType indexType;
  private final QueryContext context;
  private final List<PartialPath> paths;
  private final boolean alignedByTime;

  public QueryIndexExecutor(QueryIndexPlan queryIndexPlan, QueryContext context) {
    this.indexType = queryIndexPlan.getIndexType();
    this.queryProps = IndexUtils.toUpperCaseProps(queryIndexPlan.getProps());
    this.paths = queryIndexPlan.getPaths();
    this.alignedByTime = queryIndexPlan.isAlignedByTime();
    this.paths.forEach(PartialPath::toLowerCase);
    this.context = context;
  }

  public QueryDataSet executeIndexQuery() throws StorageEngineException, QueryIndexException {
    // get all related storage group
    return IndexManager.getInstance()
        .queryIndex(paths, indexType, queryProps, context, alignedByTime);
  }
}
