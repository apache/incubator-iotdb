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

package org.apache.iotdb.db.engine.compaction.cross.inplace.task;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import java.io.File;
import java.util.List;

@FunctionalInterface
public interface MergeCallback {

  /**
   * On calling this method, the callee should: 1. replace the modification files of seqFiles with
   * merging modifications since the old modifications have been merged into the new files. 2.
   * remove the unseqFiles since they have been merged into new files. 3. remove the merge log file
   * 4. exit merging status
   *
   * @param seqFiles
   * @param unseqFiles
   */
  void call(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, File logFile);
}
