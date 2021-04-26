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
package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;

import java.util.Map;
import java.util.Objects;

public class QueryIndexPlan extends RawDataQueryPlan {

  private Map<String, Object> props;
  private IndexType indexType;
  private boolean alignedByTime = false;

  public QueryIndexPlan() {
    super();
    setOperatorType(OperatorType.QUERY_INDEX);
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public Map<String, Object> getProps() {
    return props;
  }

  public void setProps(Map<String, Object> props) {
    this.props = props;
  }

  public boolean isAlignedByTime() {
    return alignedByTime;
  }

  public void setAlignedByTime(boolean alignedByTime) {
    this.alignedByTime = alignedByTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryIndexPlan that = (QueryIndexPlan) o;
    return Objects.equals(paths, that.paths)
        && Objects.equals(props, that.props)
        && Objects.equals(indexType, that.indexType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(paths, props, indexType);
  }

  @Override
  public String toString() {
    return String.format("Query paths: %s, index type: %s, props: %s", paths, indexType, props);
  }

  @Override
  public boolean isRawQuery() {
    return false;
  }
}
