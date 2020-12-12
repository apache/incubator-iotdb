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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

/**
 * Implementation of {@link Project} relational expression in IoTDB.
 */
public class IoTDBProject extends Project implements IoTDBRel {

  public IoTDBProject(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
    assert getConvention() == IoTDBRel.CONVENTION;
    assert getConvention() == input.getConvention();
  }

  @Override
  public Project copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new IoTDBProject(getCluster(), traitSet, input, projects,
        rowType);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    final IoTDBRules.RexToIoTDBTranslator translator =
        new IoTDBRules.RexToIoTDBTranslator(
            (JavaTypeFactory) getCluster().getTypeFactory(),
            IoTDBRules.IoTDBFieldNames(getInput().getRowType()));
    final List<String> selectFields = new ArrayList<>();
    for (Pair<RexNode, String> pair : getNamedProjects()) {
      final String originalName = pair.left.accept(translator);
      if (originalName == null){
        selectFields.add(pair.left.toString());
      } else {
        selectFields.add(originalName);
      }
    }

    implementor.addFields(selectFields);
  }
}

// End IoTDBProject.java