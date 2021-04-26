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
package org.apache.iotdb.db.index.common.math;

import org.apache.iotdb.db.index.common.math.probability.UniformProba;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.google.common.primitives.Longs;

import java.security.SecureRandom;

public class Randomwalk {

  public static TVList generateRanWalkTVList(long length, long seed, float R, float miu) {
    TVList res = TVListAllocator.getInstance().allocate(TSDataType.DOUBLE);
    double lastPoint = R;
    SecureRandom r = new SecureRandom(Longs.toByteArray(seed));
    UniformProba uniform = new UniformProba(miu / 2, -miu / 2, r);
    for (int i = 0; i < length; i++) {
      lastPoint = lastPoint + uniform.getNextRandom();
      res.putDouble(i, lastPoint);
    }
    return res;
  }

  public static TVList generateRanWalkTVList(long length) {
    return generateRanWalkTVList(length, 0, 0, 1);
  }
}
