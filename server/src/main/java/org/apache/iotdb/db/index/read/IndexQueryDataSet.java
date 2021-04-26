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
package org.apache.iotdb.db.index.read;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;
import java.util.Map;

public class IndexQueryDataSet extends ListDataSet {

  private Map<String, Integer> pathToIndex;

  public IndexQueryDataSet(
      List<PartialPath> paths, List<TSDataType> dataTypes, Map<String, Integer> pathToIndex) {
    super(paths, dataTypes);
    this.pathToIndex = pathToIndex;
  }

  public Map<String, Integer> getPathToIndex() {
    return pathToIndex;
  }
}
