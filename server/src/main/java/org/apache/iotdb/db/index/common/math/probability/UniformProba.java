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
package org.apache.iotdb.db.index.common.math.probability;

import java.security.SecureRandom;

public class UniformProba extends Probability {

  private final double downBound;
  private final double range;
  private SecureRandom random;

  public UniformProba(double upBound, double downBound, SecureRandom random) {
    if (upBound < downBound) {
      throw new IllegalArgumentException(
          String.format("Invalid up bound and down bound: %f, %f: ", upBound, downBound));
    }
    this.downBound = downBound;
    this.range = upBound - downBound;
    this.random = random;
  }

  @Override
  public double getNextRandom() {
    return random.nextDouble() * range + downBound;
  }
}
