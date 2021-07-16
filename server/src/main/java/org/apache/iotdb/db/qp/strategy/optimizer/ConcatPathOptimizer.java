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
package org.apache.iotdb.db.qp.strategy.optimizer;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.PathNumOverLimitException;
import org.apache.iotdb.db.exception.runtime.SQLParserException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.FromComponent;
import org.apache.iotdb.db.qp.logical.crud.FunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.crud.SelectComponent;
import org.apache.iotdb.db.qp.logical.crud.WhereComponent;
import org.apache.iotdb.db.qp.utils.WildcardsRemover;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.service.IoTDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** concat paths in select and from clause. */
public class ConcatPathOptimizer implements ILogicalOptimizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConcatPathOptimizer.class);

  private static final String WARNING_NO_SUFFIX_PATHS =
      "failed to concat series paths because the given query operator didn't have suffix paths";
  private static final String WARNING_NO_PREFIX_PATHS =
      "failed to concat series paths because the given query operator didn't have prefix paths";

  @Override
  public Operator transform(Operator operator)
      throws LogicalOptimizeException, PathNumOverLimitException {
    QueryOperator queryOperator = (QueryOperator) operator;
    if (!optimizable(queryOperator)) {
      return queryOperator;
    }
    concatSelect(queryOperator);
    removeWildcardsInSelectPaths(queryOperator);
    concatFilterAndRemoveWildcards(queryOperator);
    return queryOperator;
  }

  private boolean optimizable(QueryOperator queryOperator) {
    if (queryOperator.isAlignByDevice()) {
      return false;
    }

    SelectComponent select = queryOperator.getSelectComponent();
    if (select == null || select.getResultColumns().isEmpty()) {
      LOGGER.warn(WARNING_NO_SUFFIX_PATHS);
      return false;
    }

    FromComponent from = queryOperator.getFromComponent();
    if (from == null || from.getPrefixPaths().isEmpty()) {
      LOGGER.warn(WARNING_NO_PREFIX_PATHS);
      return false;
    }

    return true;
  }

  private void concatSelect(QueryOperator queryOperator) throws LogicalOptimizeException {
    List<PartialPath> prefixPaths = queryOperator.getFromComponent().getPrefixPaths();
    List<ResultColumn> resultColumns = new ArrayList<>();
    for (ResultColumn suffixColumn : queryOperator.getSelectComponent().getResultColumns()) {
      suffixColumn.concat(prefixPaths, resultColumns);
    }
    queryOperator.getSelectComponent().setResultColumns(resultColumns);
  }

  private void removeWildcardsInSelectPaths(QueryOperator queryOperator)
      throws LogicalOptimizeException, PathNumOverLimitException {
    if (queryOperator.getIndexType() != null) {
      return;
    }

    WildcardsRemover wildcardsRemover = new WildcardsRemover(queryOperator);
    List<ResultColumn> resultColumns = new ArrayList<>();
    for (ResultColumn resultColumn : queryOperator.getSelectComponent().getResultColumns()) {
      resultColumn.removeWildcards(wildcardsRemover, resultColumns);
      if (wildcardsRemover.checkIfPathNumberIsOverLimit(resultColumns)) {
        break;
      }
    }
    wildcardsRemover.checkIfSoffsetIsExceeded(resultColumns);
    queryOperator.getSelectComponent().setResultColumns(resultColumns);
  }

  private void concatFilterAndRemoveWildcards(QueryOperator queryOperator)
      throws LogicalOptimizeException {
    WhereComponent whereComponent = queryOperator.getWhereComponent();
    if (whereComponent == null) {
      return;
    }

    Set<PartialPath> filterPaths = new HashSet<>();
    whereComponent.setFilterOperator(
        concatFilterAndRemoveWildcards(
            queryOperator.getFromComponent().getPrefixPaths(),
            whereComponent.getFilterOperator(),
            filterPaths));
    whereComponent.getFilterOperator().setPathSet(filterPaths);
  }

  private FilterOperator concatFilterAndRemoveWildcards(
      List<PartialPath> fromPaths, FilterOperator operator, Set<PartialPath> filterPaths)
      throws LogicalOptimizeException {
    if (!operator.isLeaf()) {
      List<FilterOperator> newFilterList = new ArrayList<>();
      for (FilterOperator child : operator.getChildren()) {
        newFilterList.add(concatFilterAndRemoveWildcards(fromPaths, child, filterPaths));
      }
      operator.setChildren(newFilterList);
      return operator;
    }
    FunctionOperator functionOperator = (FunctionOperator) operator;
    PartialPath filterPath = functionOperator.getSinglePath();
    // do nothing in the cases of "where time > 5" or "where root.d1.s1 > 5"
    if (SQLConstant.isReservedPath(filterPath)
        || filterPath.getFirstNode().startsWith(SQLConstant.ROOT)) {
      filterPaths.add(filterPath);
      return operator;
    }
    List<PartialPath> concatPaths = new ArrayList<>();
    fromPaths.forEach(fromPath -> concatPaths.add(fromPath.concatPath(filterPath)));
    List<PartialPath> noStarPaths = removeWildcardsInConcatPaths(concatPaths);
    filterPaths.addAll(noStarPaths);
    if (noStarPaths.size() == 1) {
      // Transform "select s1 from root.car.* where s1 > 10" to
      // "select s1 from root.car.* where root.car.*.s1 > 10"
      functionOperator.setSinglePath(noStarPaths.get(0));
      return operator;
    } else {
      // Transform "select s1 from root.car.d1, root.car.d2 where s1 > 10" to
      // "select s1 from root.car.d1, root.car.d2 where root.car.d1.s1 > 10 and root.car.d2.s1 > 10"
      // Note that,
      // two fork tree has to be maintained while removing stars in paths for DnfFilterOptimizer
      // requirement.
      return constructBinaryFilterTreeWithAnd(noStarPaths, operator);
    }
  }

  private FilterOperator constructBinaryFilterTreeWithAnd(
      List<PartialPath> noStarPaths, FilterOperator operator) throws LogicalOptimizeException {
    FilterOperator filterBinaryTree = new FilterOperator(FilterType.KW_AND);
    FilterOperator currentNode = filterBinaryTree;
    for (int i = 0; i < noStarPaths.size(); i++) {
      if (i > 0 && i < noStarPaths.size() - 1) {
        FilterOperator newInnerNode = new FilterOperator(FilterType.KW_AND);
        currentNode.addChildOperator(newInnerNode);
        currentNode = newInnerNode;
      }
      try {
        currentNode.addChildOperator(
            new BasicFunctionOperator(
                operator.getFilterType(),
                noStarPaths.get(i),
                ((BasicFunctionOperator) operator).getValue()));
      } catch (SQLParserException e) {
        throw new LogicalOptimizeException(e.getMessage());
      }
    }
    return filterBinaryTree;
  }

  private List<PartialPath> removeWildcardsInConcatPaths(List<PartialPath> originalPaths)
      throws LogicalOptimizeException {
    HashSet<PartialPath> actualPaths = new HashSet<>();
    try {
      for (PartialPath originalPath : originalPaths) {
        List<PartialPath> all =
            IoTDB.metaManager.getAllTimeseriesPathWithAlias(originalPath, 0, 0).left;
        if (all.isEmpty()) {
          throw new LogicalOptimizeException(
              String.format("Unknown time series %s in `where clause`", originalPath));
        }
        actualPaths.addAll(all);
      }
    } catch (MetadataException e) {
      throw new LogicalOptimizeException("error when remove star: " + e.getMessage());
    }
    return new ArrayList<>(actualPaths);
  }

  public static <T> void cartesianProduct(
      List<List<T>> dimensionValue, List<List<T>> resultList, int layer, List<T> currentList) {
    if (layer < dimensionValue.size() - 1) {
      if (dimensionValue.get(layer).isEmpty()) {
        cartesianProduct(dimensionValue, resultList, layer + 1, currentList);
      } else {
        for (int i = 0; i < dimensionValue.get(layer).size(); i++) {
          List<T> list = new ArrayList<>(currentList);
          list.add(dimensionValue.get(layer).get(i));
          cartesianProduct(dimensionValue, resultList, layer + 1, list);
        }
      }
    } else if (layer == dimensionValue.size() - 1) {
      if (dimensionValue.get(layer).isEmpty()) {
        resultList.add(currentList);
      } else {
        for (int i = 0; i < dimensionValue.get(layer).size(); i++) {
          List<T> list = new ArrayList<>(currentList);
          list.add(dimensionValue.get(layer).get(i));
          resultList.add(list);
        }
      }
    }
  }
}
