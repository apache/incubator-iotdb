/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.query.dataset;

import java.io.IOException;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;

public class AggreResultDataPointReader implements IPointReader {

  private AggreResultData aggreResultData;

  public AggreResultDataPointReader(AggreResultData aggreResultData) {
    this.aggreResultData = aggreResultData;
  }

  @Override
  public boolean hasNext() throws IOException {
    return aggreResultData.isSetValue();
  }

  @Override
  public TimeValuePair next() throws IOException {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(aggreResultData);
    aggreResultData.reSet();
    return timeValuePair;
  }

  @Override
  public TimeValuePair current() throws IOException {
    TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(aggreResultData);
    return timeValuePair;
  }

  @Override
  public void close() throws IOException {
    //batch data doesn't need to close.
  }
}
