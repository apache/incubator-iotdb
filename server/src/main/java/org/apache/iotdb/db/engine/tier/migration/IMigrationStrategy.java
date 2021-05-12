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

package org.apache.iotdb.db.engine.tier.migration;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

/** This interface is functional and aims to help judging whether a TsFile needs migrating. */
@FunctionalInterface
public interface IMigrationStrategy {
  String PINNED_STRATEGY_CLASS_NAME = PinnedStrategy.class.getName();
  String TIME2LIVE_STRATEGY_CLASS_NAME = Time2LiveStrategy.class.getName();

  /**
   * Judges whether the TsFile needs migrating.
   *
   * @param tsFileResource the TsFile to be judged
   * @return true if the TsFile needs migrating
   */
  boolean shouldMigrate(TsFileResource tsFileResource);

  /**
   * Parses the class name and parameters string into {@code IMigrationStrategy} object.
   *
   * @param classNameWithParams a string contains strategy class name and probably contains
   *     parameters
   * @return the migration strategy object represented by the string
   */
  static IMigrationStrategy parse(String classNameWithParams) {
    int leftParenthesis = classNameWithParams.indexOf('(');
    int rightParenthesis = classNameWithParams.indexOf(')');
    // parse strategies without params
    if (leftParenthesis == -1) {
      if (PINNED_STRATEGY_CLASS_NAME.equals(classNameWithParams)) {
        return PinnedStrategy.getInstance();
      } else {
        throw new IllegalArgumentException(classNameWithParams);
      }
    }
    // parse strategies with params
    if (rightParenthesis != classNameWithParams.length() - 1) {
      throw new IllegalArgumentException(classNameWithParams);
    }
    String className = classNameWithParams.substring(0, leftParenthesis);
    String[] params =
        classNameWithParams
            .substring(leftParenthesis + 1, rightParenthesis)
            .replaceAll("\\s", "")
            .split(",");
    if (TIME2LIVE_STRATEGY_CLASS_NAME.equals(className) && params.length == 1) {
      return new Time2LiveStrategy(Long.parseLong(params[0]));
    } else {
      throw new IllegalArgumentException(classNameWithParams);
    }
  }
}
