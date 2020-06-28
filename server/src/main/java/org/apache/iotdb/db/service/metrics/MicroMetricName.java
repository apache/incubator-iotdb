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
package org.apache.iotdb.db.service.metrics;

public class MicroMetricName {
  public static final String GROUP_TAG = "_group";
  public static final String OPEN_SESSION_REQUEST_COUNTER = "iotdb.open.session.request";
  public static String INSERT_OPERATION_COUNT = "iotdb.storage.insert.count";
  public static String INSERT_OPERATION_LATENCY = "iotdb.storage.insert.latency";
  public static String WAL_SYNC_COUNT = "iotdb.wal.sync.count";
  public static String QUERY_SG_LATENCY = "iotdb.processor.query.latency";
  public static String QUERY_SG_ACTIVE = "iotdb.processor.query.active";
  public static String QUERY_EXECUTION_PLAN = "iotdb.query.execution";
  public static String INSERT_BATCH_SIZE = "iotdb.storage.insert.batch.size";
  public static String INSERT_BATCH_LATENCY = "iotdb.storage.insert.batch.latency";
}
