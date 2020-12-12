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
package org.apache.iotdb.calcite;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;

/**
 * Rules and relational operators for {@link IoTDBRel#CONVENTION} calling convention.
 */
public class IoTDBRules {

  private IoTDBRules() {
  }

  public static final RelOptRule[] RULES = {
      IoTDBFilterRule.INSTANCE,
      IoTDBProjectRule.INSTANCE,
      IoTDBLimitRule.INSTANCE
  };

  static List<String> IoTDBFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(rowType.getFieldNames(),
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  /**
   * Translator from {@link RexNode} to strings in IoTDB's expression language.
   */
  static class RexToIoTDBTranslator extends RexVisitorImpl<String> {

    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    protected RexToIoTDBTranslator(JavaTypeFactory typeFactory,
        List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override
    public String visitInputRef(RexInputRef inputRef) {
      return inFields.get(inputRef.getIndex());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a {@link
   * IoTDBFilter}.
   */
  private static class IoTDBFilterRule extends ConverterRule {

    private static final IoTDBFilterRule INSTANCE = new IoTDBFilterRule();

    private IoTDBFilterRule() {
      super(LogicalFilter.class, Convention.NONE, IoTDBRel.CONVENTION, "IoTDBFilterRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      RexNode condition = filter.getCondition();

      return compareFieldWithLiteral(condition);
    }

    /**
     * Check if all binary operations in filter are comparing a variable with a literal or just two
     * literals.
     *
     * @param node Condition node to check
     */
    private boolean compareFieldWithLiteral(RexNode node) {
      RexCall call = (RexCall) node;
      switch (call.getKind()) {
        case AND:
        case OR:
          for (RexNode childOperand : call.getOperands()) {
            if (!compareFieldWithLiteral(childOperand)) {
              return false;
            }
          }
          return true;
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          return call.getOperands().get(0).isA(SqlKind.LITERAL) || call.getOperands().get(1)
              .isA(SqlKind.LITERAL);
        default:
          return false;
      }
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(IoTDBRel.CONVENTION);
      return new IoTDBFilter(filter.getCluster(), traitSet,
          convert(filter.getInput(), IoTDBRel.CONVENTION), filter.getCondition());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject} to a {@link
   * IoTDBProject}.
   */
  private static class IoTDBProjectRule extends ConverterRule {

    private static final IoTDBProjectRule INSTANCE = new IoTDBProjectRule();

    private IoTDBProjectRule() {
      super(LogicalProject.class, Convention.NONE, IoTDBRel.CONVENTION, "IoTDBProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(IoTDBRel.CONVENTION);
      return new IoTDBProject(project.getCluster(), traitSet,
          convert(project.getInput(), IoTDBRel.CONVENTION), project.getProjects(),
          project.getRowType());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.adapter.enumerable.EnumerableLimit} to a {@link
   * IoTDBLimit}.
   */
  private static class IoTDBLimitRule extends RelOptRule {

    private static final IoTDBLimitRule INSTANCE = new IoTDBLimitRule();

    private IoTDBLimitRule() {
      super(operand(EnumerableLimit.class, operand(IoTDBToEnumerableConverter.class, any())),
          "IoTDBLimitRule");
    }

    public RelNode convert(EnumerableLimit limit) {
      final RelTraitSet traitSet = limit.getTraitSet().replace(IoTDBRel.CONVENTION);
      return new IoTDBLimit(limit.getCluster(), traitSet,
          convert(limit.getInput(), IoTDBRel.CONVENTION), limit.offset, limit.fetch);
    }

    /**
     * @see org.apache.calcite.rel.convert.ConverterRule
     */
    public void onMatch(RelOptRuleCall call) {
      final EnumerableLimit limit = call.rel(0);
      final RelNode converted = convert(limit);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }
}

// End IoTDBRules.java