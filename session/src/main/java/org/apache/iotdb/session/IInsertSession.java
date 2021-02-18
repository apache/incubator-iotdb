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
package org.apache.iotdb.session;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;

public interface IInsertSession {
  void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecords(List<String> deviceIds, List<Long> times,
                     List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,
                     List<List<TSDataType>> typesList, List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  @FunctionalInterface
  interface FiveInputConsumer<First, Second, Third, Fourth, Fifth> {
    void apply(First one, Second two, Third three, Fourth four, Fifth five);
  }

  @FunctionalInterface
  interface SixInputConsumer<First, Second, Third, Fourth, Fifth, Sixth> {
    void apply(First one, Second two, Third three, Fourth four, Fifth five, Sixth six);
  }
}
