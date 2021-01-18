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

import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_AND;
import static org.apache.iotdb.db.qp.constant.SQLConstant.KW_OR;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;

public class DnfFilterOptimizer implements IFilterOptimizer {

  /**
   * get DNF(disjunctive normal form) for this filter operator tree. Before getDnf, this op tree
   * must be binary, in another word, each non-leaf node has exactly two children.
   *
   * @return FilterOperator optimized operator
   * @throws LogicalOptimizeException exception in DNF optimize
   */
  @Override
  public FilterOperator optimize(FilterOperator filter) throws LogicalOptimizeException {
    return getDnf(filter);
  }

  private void dealWithLeftAndRightAndChildren(
      List<FilterOperator> leftAndChildren, List<FilterOperator> rightAndChildren,
      List<FilterOperator> newChildrenList) throws LogicalOptimizeException {
    for (FilterOperator leftAndChild : leftAndChildren) {
      for (FilterOperator rightAndChild : rightAndChildren) {
        FilterOperator r = mergeToConjunction(leftAndChild.copy(), rightAndChild.copy());
        newChildrenList.add(r);
      }
    }
  }

  private FilterOperator getDnf(FilterOperator filter) throws LogicalOptimizeException {
    if (filter.isLeaf()) {
      return filter;
    }
    List<FilterOperator> childOperators = filter.getChildren();
    if (childOperators.size() != 2) {
      throw new LogicalOptimizeException(
          "node :" + filter.getTokenName() + " has " + childOperators.size() + " children");

    }
    FilterOperator left = getDnf(childOperators.get(0));
    FilterOperator right = getDnf(childOperators.get(1));
    List<FilterOperator> newChildrenList = new ArrayList<>();
    switch (filter.getTokenIntType()) {
      case KW_OR:
        addChildOpInOr(left, newChildrenList);
        addChildOpInOr(right, newChildrenList);
        break;
      case KW_AND:
        if (left.getTokenIntType() != KW_OR && right.getTokenIntType() != KW_OR) {
          addChildOpInAnd(left, newChildrenList);
          addChildOpInAnd(right, newChildrenList);
        } else {
          dealWithLeftAndRightAndChildren(getAndChild(left), getAndChild(right), newChildrenList);
          filter.setTokenIntType(KW_OR);
        }
        break;
      default:
        throw new LogicalOptimizeException(
            "get DNF failed, this tokenType is:" + filter.getTokenIntType());
    }
    filter.setChildren(newChildrenList);
    return filter;
  }

  /**
   * used by getDnf. mergeUnseq two conjunction filter operators into a conjunction.<br> conjunction
   * operator consists of {@code FunctionOperator} and inner operator which token is KW_AND.<br>
   * e.g. (a and b) mergeUnseq (c) is (a and b and c)
   *
   * @param operator1 To be merged operator
   * @param operator2 To be merged operator
   * @return merged operator
   * @throws LogicalOptimizeException exception in DNF optimizing
   */
  private FilterOperator mergeToConjunction(FilterOperator operator1, FilterOperator operator2)
      throws LogicalOptimizeException {
    List<FilterOperator> retChildrenList = new ArrayList<>();
    addChildOpInAnd(operator1, retChildrenList);
    addChildOpInAnd(operator2, retChildrenList);
    FilterOperator ret = new FilterOperator(KW_AND, false);
    ret.setChildren(retChildrenList);
    return ret;
  }

  /**
   * used by getDnf. get conjunction node. <br> If child is basic function or AND node, return a
   * list just containing this. <br> If this child is OR, return children of OR.
   *
   * @param child operator
   * @return children operator
   */
  private List<FilterOperator> getAndChild(FilterOperator child) {
    if (child.getTokenIntType() == KW_OR) {
      return child.getChildren();
    } else {
      // other token type means leaf node or and
      List<FilterOperator> ret = new ArrayList<>();
      ret.add(child);
      return ret;
    }
  }

  /**
   * If operator is leaf, add it in newChildrenList. If operator is And, add its children to
   * newChildrenList.
   *
   * @param operator which children should be added in new children list
   * @param newChildrenList new children list
   * @throws LogicalOptimizeException exception in DNF optimizing
   */
  private void addChildOpInAnd(FilterOperator operator, List<FilterOperator> newChildrenList)
      throws LogicalOptimizeException {
    if (operator.isLeaf()) {
      newChildrenList.add(operator);
    } else if (operator.getTokenIntType() == KW_AND) {
      newChildrenList.addAll(operator.getChildren());
    } else {
      throw new LogicalOptimizeException(
          "add all children of an OR operator to newChildrenList in AND");
    }
  }

  /**
   * used by getDnf. If operator is leaf or And, add operator to newChildrenList. Else add
   * operator's children to newChildrenList
   *
   * @param operator to be added in new children list
   * @param newChildrenList new children list
   */
  private void addChildOpInOr(FilterOperator operator, List<FilterOperator> newChildrenList) {
    if (operator.isLeaf() || operator.getTokenIntType() == KW_AND) {
      newChildrenList.add(operator);
    } else {
      newChildrenList.addAll(operator.getChildren());
    }
  }

}
