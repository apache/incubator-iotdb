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

package org.apache.iotdb.db.service;

import org.apache.iotdb.db.conf.IoTDBConstant;

public enum ServiceType {
  STORAGE_ENGINE_SERVICE("Storage Engine ServerService", ""),
  JMX_SERVICE("JMX ServerService", "JMX ServerService"),
  METRICS_SERVICE("Metrics ServerService", "MetricsService"),
  RPC_SERVICE("RPC ServerService", "RPCService"),
  MQTT_SERVICE("MQTTService", ""),
  MONITOR_SERVICE("Monitor ServerService", "Monitor"),
  STAT_MONITOR_SERVICE("Statistics ServerService", ""),
  WAL_SERVICE("WAL ServerService", ""),
  CLOSE_MERGE_SERVICE("Close&Merge ServerService", ""),
  JVM_MEM_CONTROL_SERVICE("Memory Controller", ""),
  AUTHORIZATION_SERVICE("Authorization ServerService", ""),
  FILE_READER_MANAGER_SERVICE("File reader manager ServerService", ""),
  SYNC_SERVICE("SYNC ServerService", ""),
  UPGRADE_SERVICE("UPGRADE DataService", ""),
  MERGE_SERVICE("Merge Manager", "Merge Manager"),
  COMPACTION_SERVICE("Compaction Manager", "Compaction Manager"),
  HITTER_SERVICE("Hitter Manager", "Hitter Manager"),
  PERFORMANCE_STATISTIC_SERVICE("PERFORMANCE_STATISTIC_SERVICE", "PERFORMANCE_STATISTIC_SERVICE"),
  TVLIST_ALLOCATOR_SERVICE("TVList Allocator", ""),
  UDF_CLASSLOADER_MANAGER_SERVICE("UDF Classloader Manager Service", ""),
  UDF_REGISTRATION_SERVICE("UDF Registration Service", ""),
  TEMPORARY_QUERY_DATA_FILE_SERVICE("Temporary Query Data File Service", ""),
  TRIGGER_REGISTRATION_SERVICE("Trigger Registration Service", ""),
  CACHE_HIT_RATIO_DISPLAY_SERVICE(
      "CACHE_HIT_RATIO_DISPLAY_SERVICE",
      generateJmxName(IoTDBConstant.IOTDB_PACKAGE, "Cache Hit Ratio")),
  QUERY_TIME_MANAGER("Query time manager", "Query time"),

  FLUSH_SERVICE(
      "Flush ServerService", generateJmxName("org.apache.iotdb.db.engine.pool", "Flush Manager")),
  CLUSTER_MONITOR_SERVICE("Cluster Monitor ServerService", "Cluster Monitor"),
  SYSTEMINFO_SERVICE("MemTable Monitor Service", "MemTable, Monitor"),

  CLUSTER_INFO_SERVICE("Cluster Monitor Service (thrift-based)", "Cluster Monitor-Thrift"),
  ;

  private final String name;
  private final String jmxName;

  ServiceType(String name, String jmxName) {
    this.name = name;
    this.jmxName = jmxName;
  }

  public String getName() {
    return name;
  }

  public String getJmxName() {
    return jmxName;
  }

  private static String generateJmxName(String packageName, String jmxName) {
    return String.format("%s:type=%s", packageName, jmxName);
  }
}
