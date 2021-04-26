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
package org.apache.iotdb.db.index.read.optimize;

/**
 * In the indexing mechanism, it refers to the optimization of the access order of candidate series
 * after pruning phase. Due to the mismatch between the candidate set order given by the index and
 * the file organization of TsFile, the access of the refinement phase advised by the index may be
 * inefficient. We can use this query optimizerto rearrange the candidate set access order.
 */
public interface IIndexCandidateOrderOptimize {
  class Factory {

    private Factory() {
      // hidden initializer
    }

    public static IIndexCandidateOrderOptimize getOptimize() {
      return new NoCandidateOrderOptimizer();
    }
  }
}
