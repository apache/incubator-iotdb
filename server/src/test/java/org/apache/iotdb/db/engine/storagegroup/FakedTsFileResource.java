/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.storagegroup;

public class FakedTsFileResource extends TsFileResource {

  private long tsFileSize;

  public FakedTsFileResource(long tsFileSize) {
    this.tsFileSize = tsFileSize;
    super.closed = true;
    super.isMerging = false;
  }

  public FakedTsFileResource(long tsFileSize, boolean isClosed, boolean isMerging) {
    this.tsFileSize = tsFileSize;
    super.closed = isClosed;
    super.isMerging = isMerging;
  }

  public void setTsFileSize(long tsFileSize) {
    this.tsFileSize = tsFileSize;
  }

  @Override
  public long getTsFileSize() {
    return tsFileSize;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(tsFileSize).append(",");
    builder.append(closed).append(",");
    builder.append(isMerging);
    return builder.toString();
  }
}
