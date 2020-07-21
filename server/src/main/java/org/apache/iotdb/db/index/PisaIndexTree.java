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
package org.apache.iotdb.db.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.exception.index.IndexException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public class PisaIndexTree {

  private Map<Long, Statistics> nodeNumberStatisticMap;

  public PisaIndexTree(Map<Long, Statistics> nodeNumberStatisticMap) {
    this.nodeNumberStatisticMap = nodeNumberStatisticMap;
  }

  public Map<Long, Statistics> getNodeNumberStatisticMap() {
    return nodeNumberStatisticMap;
  }

  public int serialize(OutputStream outputStream) throws IOException, IndexException {
    int byteLen = ReadWriteIOUtils.write(nodeNumberStatisticMap.size(), outputStream);
    for (Map.Entry<Long, Statistics> entry : nodeNumberStatisticMap.entrySet()) {
      long nodeNumber = entry.getKey();
      byteLen += ReadWriteIOUtils.write(nodeNumber, outputStream);
      byteLen += entry.getValue().serialize(outputStream);
    }
    return byteLen;
  }

  public static PisaIndexTree deserialize(InputStream inputStream, TSDataType dataType)
      throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    Map<Long, Statistics> nodeNumberStatisticMap = new HashMap<>();
    for (int i = 0; i < size; i++) {
      nodeNumberStatisticMap.put(ReadWriteIOUtils.readLong(inputStream),
          Statistics.deserialize(inputStream, dataType));
    }
    return new PisaIndexTree(nodeNumberStatisticMap);
  }
}
