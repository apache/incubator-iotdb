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

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregationPlan extends RawDataQueryPlan {

  // e.g., for select count(s1), count(s1), count(s2), count(s2), sum (s1)
  // aggregations are count, count, count, count, sum
  // deduplicatedAggregations are count, count, sum

  private List<String> aggregations = new ArrayList<>();
  private List<String> deduplicatedAggregations = new ArrayList<>();

  private int level = -1;
  // group by level aggregation result path
  private final Map<String, AggregateResult> levelAggPaths = new LinkedHashMap<>();

  public AggregationPlan() {
    super();
    setOperatorType(Operator.OperatorType.AGGREGATION);
  }

  @Override
  public List<String> getAggregations() {
    return aggregations;
  }

  public void setAggregations(List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public List<String> getDeduplicatedAggregations() {
    return deduplicatedAggregations;
  }

  public void addDeduplicatedAggregations(String aggregations) {
    this.deduplicatedAggregations.add(aggregations);
  }

  public void setDeduplicatedAggregations(List<String> deduplicatedAggregations) {
    this.deduplicatedAggregations = deduplicatedAggregations;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public Map<String, AggregateResult> getAggPathByLevel() throws QueryProcessException {
    if (!levelAggPaths.isEmpty()) {
      return levelAggPaths;
    }
    List<PartialPath> seriesPaths = getPaths();
    List<TSDataType> dataTypes = getDataTypes();
    try {
      for (int i = 0; i < seriesPaths.size(); i++) {
        String transformedPath =
            FilePathUtils.generatePartialPathByLevel(seriesPaths.get(i).getFullPath(), getLevel());
        String key = getAggregations().get(i) + "(" + transformedPath + ")";
        if (!levelAggPaths.containsKey(key)) {
          AggregateResult aggRet =
              AggregateResultFactory.getAggrResultByName(
                  getAggregations().get(i), dataTypes.get(i));
          levelAggPaths.put(key, aggRet);
        }
      }
    } catch (IllegalPathException e) {
      throw new QueryProcessException(e.getMessage());
    }
    return levelAggPaths;
  }

  @Override
  public String getColumnForReaderFromPath(PartialPath path, int pathIndex) {
    return resultColumns.get(pathIndex).getResultColumnName();
  }

  @Override
  public String getColumnForDisplay(String columnForReader, int pathIndex)
      throws IllegalPathException {
    String columnForDisplay = columnForReader;
    if (level >= 0) {
      PartialPath path = paths.get(pathIndex);
      String aggregatePath =
          path.isMeasurementAliasExists()
              ? FilePathUtils.generatePartialPathByLevel(path.getFullPathWithAlias(), level)
              : FilePathUtils.generatePartialPathByLevel(path.getFullPath(), level);
      columnForDisplay = aggregations.get(pathIndex) + "(" + aggregatePath + ")";
    }
    return columnForDisplay;
  }

  @Override
  public boolean isRawQuery() {
    return false;
  }
}
