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

class Time2LiveStrategy implements IMigrationStrategy {
  /** time to live, in ms */
  private final long ttl;

  Time2LiveStrategy(long ttl) {
    this.ttl = ttl;
  }

  /**
   * Judges whether the TsFile needs migrating.
   *
   * @param tsFileResource the TsFile to be judged
   * @return true if none device in this TsFile lives over the given time
   *     bound(System.currentTimeMillis() - ttl)
   */
  @Override
  public boolean shouldMigrate(TsFileResource tsFileResource) {
    return !tsFileResource.stillLives(System.currentTimeMillis() - ttl);
  }
}
