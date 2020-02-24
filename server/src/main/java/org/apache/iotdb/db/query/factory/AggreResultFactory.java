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

package org.apache.iotdb.db.query.factory;

import org.apache.iotdb.db.exception.query.PathException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.impl.AvgAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.CountAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.FirstValueAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.LastValueAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.MaxValueAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.MinTimeAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.MinValueAggrResult;
import org.apache.iotdb.db.query.aggregation.impl.SumAggrResult;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

/**
 * Easy factory pattern to build AggregateFunction.
 */
public class AggreResultFactory {

  private AggreResultFactory() {
  }

  /**
   * construct AggregateFunction using factory pattern.
   *
   * @param aggrFuncName function name.
   * @param dataType data type.
   */
  public static AggregateResult getAggrResultByName(String aggrFuncName, TSDataType dataType)
      throws PathException {
    if (aggrFuncName == null) {
      throw new PathException("AggregateFunction Name must not be null");
    }

    switch (aggrFuncName.toLowerCase()) {
      case SQLConstant.MIN_TIME:
        return new MinTimeAggrResult();
      case SQLConstant.MAX_TIME:
        return new MaxTimeAggrResult();
      case SQLConstant.MIN_VALUE:
        return new MinValueAggrResult(dataType);
      case SQLConstant.MAX_VALUE:
        return new MaxValueAggrResult(dataType);
      case SQLConstant.COUNT:
        return new CountAggrResult();
      case SQLConstant.AVG:
        return new AvgAggrResult(dataType);
      case SQLConstant.FIRST_VALUE:
        return new FirstValueAggrResult(dataType);
      case SQLConstant.SUM:
        return new SumAggrResult(dataType);
      case SQLConstant.LAST_VALUE:
        return new LastValueAggrResult(dataType);
      default:
        throw new PathException(
            "aggregate does not support " + aggrFuncName + " function.");
    }
  }
}
