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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.crud.BasicFunctionOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;

public class MergeSingleFilterOptimizer implements IFilterOptimizer {

  @Override
  public FilterOperator optimize(FilterOperator filter) throws LogicalOptimizeException {
    mergeSamePathFilter(filter);
    return filter;
  }

  private void checkInnerFilterLen(List<FilterOperator> children) throws LogicalOptimizeException {
    if (children.isEmpty()) {
      throw new LogicalOptimizeException("this inner filter has no children!");
    }
    if (children.size() == 1) {
      throw new LogicalOptimizeException("this inner filter has just one child!");
    }
  }

  /**
   * mergeUnseq and extract node with same Path recursively. <br> If a node has more than two children
   * and some children has same paths, remove them from this node and mergeUnseq them to a new single
   * node, then add the new node to this children list.<br> if all recursive children of this node
   * have same seriesPath, set this node to single node, and return the same seriesPath, otherwise,
   * throw exception;
   *
   * @param filter - children is not empty.
   * @return - if all recursive children of this node have same seriesPath, set this node to single
   * node, and return the same seriesPath, otherwise, throw exception;
   */
  private PartialPath mergeSamePathFilter(FilterOperator filter) throws LogicalOptimizeException {
    if (filter.isLeaf()) {
      return filter.getSinglePath();
    }
    List<FilterOperator> children = filter.getChildren();
    checkInnerFilterLen(children);
    PartialPath childPath = mergeSamePathFilter(children.get(0));
    PartialPath tempPath;
    for (int i = 1; i < children.size(); i++) {
      tempPath = mergeSamePathFilter(children.get(i));
      // if one of children differs from others or is not single node(seriesPath = null), filter's
      // seriesPath is null
      if (tempPath == null || !tempPath.equals(childPath)) {
        childPath = null;
      }
    }
    if (childPath != null) {
      filter.setIsSingle(true);
      filter.setSinglePath(childPath);
      return childPath;
    }

    // sort paths of BasicFunction by their single seriesPath. We don't sort children on non-leaf
    // layer.
    if (!children.isEmpty() && allIsBasic(children)) {
      children.sort(Comparator.comparing(o -> o.getSinglePath().getFullPath()));
    }
    List<FilterOperator> ret = new ArrayList<>();
    int firstNonSingleIndex = mergeSingleFilters(ret, filter);

    // add last null child
    return addLastNullChild(ret, filter, firstNonSingleIndex, childPath);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private int mergeSingleFilters(List<FilterOperator> ret, FilterOperator filter) {
    List<FilterOperator> children = filter.getChildren();
    List<FilterOperator> tempExtrNode = null;
    PartialPath tempPath;
    PartialPath childPath = null;
    int firstNonSingleIndex;
    for (firstNonSingleIndex = 0; firstNonSingleIndex < children.size(); firstNonSingleIndex++) {
      tempPath = children.get(firstNonSingleIndex).getSinglePath();
      // sorted by seriesPath, all non-single filters are in the end
      if (tempPath == null) {
        break;
      }
      if (childPath == null) {
        // first child to be added
        childPath = tempPath;
        tempExtrNode = new ArrayList<>();
        tempExtrNode.add(children.get(firstNonSingleIndex));
      } else if (childPath.equals(tempPath)) {
        // successive next single child with same seriesPath,mergeUnseq it with previous children
        // if not duplicate
        FilterOperator child = children.get(firstNonSingleIndex);
        if(!tempExtrNode.contains(child)){
          tempExtrNode.add(child);
        }
      } else {
        // not more same, add existing nodes in tempExtrNode into a new node
        // prevent make a node which has only one child.
        if (tempExtrNode.size() == 1) {
          ret.add(tempExtrNode.get(0));
          // use exist Object directly for efficiency
          tempExtrNode.set(0, children.get(firstNonSingleIndex));
          childPath = tempPath;
        } else {
          // add a new inner node
          FilterOperator newFilter = new FilterOperator(filter.getTokenIntType(), true);
          newFilter.setSinglePath(childPath);
          newFilter.setChildren(tempExtrNode);
          ret.add(newFilter);
          tempExtrNode = new ArrayList<>();
          tempExtrNode.add(children.get(firstNonSingleIndex));
          childPath = tempPath;
        }
      }
    }
    // the last several children before "not single paths" has not been added to ret list.
    if (childPath != null) {
      if (tempExtrNode.size() == 1) {
        ret.add(tempExtrNode.get(0));
      } else {
        // add a new inner node
        FilterOperator newFil = new FilterOperator(filter.getTokenIntType(), true);
        newFil.setSinglePath(childPath);
        newFil.setChildren(tempExtrNode);
        ret.add(newFil);
      }
    }
    return firstNonSingleIndex;
  }

  private PartialPath addLastNullChild(List<FilterOperator> ret,
                                FilterOperator filter, int i, PartialPath childPath){
    List<FilterOperator> children = filter.getChildren();
    for (; i < children.size(); i++) {
      ret.add(children.get(i));
    }
    if (ret.size() == 1) {
      // all children have same seriesPath, which means this filter node is a single node
      filter.setIsSingle(true);
      filter.setSinglePath(childPath);
      filter.setChildren(ret.get(0).getChildren());
      return childPath;
    } else {
      filter.setIsSingle(false);
      filter.setChildren(ret);
      return null;
    }
  }

  private boolean allIsBasic(List<FilterOperator> children) {
    for (FilterOperator child : children) {
      if (!(child instanceof BasicFunctionOperator)) {
        return false;
      }
    }
    return true;
  }
}
