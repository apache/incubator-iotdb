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
package org.apache.iotdb.db.conf;

public class IoTDBConstant {

  private IoTDBConstant() {}

  public static final String ENV_FILE_NAME = "iotdb-env";
  public static final String IOTDB_CONF = "IOTDB_CONF";
  public static final String GLOBAL_DB_NAME = "IoTDB";
  // when running the program in IDE, we can not get the version info using
  // getImplementationVersion()
  public static final String VERSION =
      IoTDBConstant.class.getPackage().getImplementationVersion() != null
          ? IoTDBConstant.class.getPackage().getImplementationVersion()
          : "UNKNOWN";

  public static final String AUDIT_LOGGER_NAME = "IoTDB_AUDIT_LOGGER";

  public static final String IOTDB_JMX_PORT = "iotdb.jmx.port";

  public static final String IOTDB_PACKAGE = "org.apache.iotdb.service";
  public static final String JMX_TYPE = "type";

  public static final long GB = 1024 * 1024 * 1024L;
  public static final long MB = 1024 * 1024L;
  public static final long KB = 1024L;

  public static final String IOTDB_HOME = "IOTDB_HOME";

  public static final String SEQFILE_LOG_NODE_SUFFIX = "-seq";
  public static final String UNSEQFILE_LOG_NODE_SUFFIX = "-unseq";

  public static final String PATH_ROOT = "root";
  public static final char PATH_SEPARATOR = '.';
  public static final String ADMIN_NAME = "root";
  public static final String ADMIN_PW = "root";
  public static final String PROFILE_SUFFIX = ".profile";
  public static final String MAX_TIME = "max_time";
  public static final String MIN_TIME = "min_time";
  public static final String LAST_VALUE = "last_value";
  public static final int MIN_SUPPORTED_JDK_VERSION = 8;

  // show info
  public static final String COLUMN_ITEM = "                             item";
  public static final String COLUMN_VALUE = "value";
  public static final String COLUMN_VERSION = "        version";
  public static final String COLUMN_TIMESERIES = "timeseries";
  public static final String COLUMN_TIMESERIES_ALIAS = "alias";
  public static final String COLUMN_TIMESERIES_DATATYPE = "dataType";
  public static final String COLUMN_TIMESERIES_ENCODING = "encoding";
  public static final String COLUMN_TIMESERIES_COMPRESSION = "compression";
  public static final String COLUMN_CHILD_PATHS = "child paths";
  public static final String COLUMN_CHILD_NODES = "child nodes";
  public static final String COLUMN_DEVICES = "devices";
  public static final String COLUMN_COLUMN = "column";
  public static final String COLUMN_COUNT = "count";
  public static final String COLUMN_TAGS = "tags";
  public static final String COLUMN_ATTRIBUTES = "attributes";
  public static final String QUERY_ID = "queryId";
  public static final String STATEMENT = "statement";

  public static final String COLUMN_ROLE = "role";
  public static final String COLUMN_USER = "user";
  public static final String COLUMN_PRIVILEGE = "privilege";

  public static final String COLUMN_STORAGE_GROUP = "storage group";
  public static final String COLUMN_TTL = "ttl";

  public static final String COLUMN_TASK_NAME = "task name";
  public static final String COLUMN_CREATED_TIME = "created time";
  public static final String COLUMN_PROGRESS = "progress";
  public static final String COLUMN_CANCELLED = "cancelled";
  public static final String COLUMN_DONE = "done";

  public static final String COLUMN_FUNCTION_NAME = "function name";
  public static final String COLUMN_FUNCTION_TYPE = "function type";
  public static final String COLUMN_FUNCTION_CLASS = "class name (UDF)";

  public static final String FUNCTION_TYPE_NATIVE = "native";
  public static final String FUNCTION_TYPE_BUILTIN_UDAF = "built-in UDAF";
  public static final String FUNCTION_TYPE_BUILTIN_UDTF = "built-in UDTF";
  public static final String FUNCTION_TYPE_EXTERNAL_UDAF = "external UDAF";
  public static final String FUNCTION_TYPE_EXTERNAL_UDTF = "external UDTF";

  public static final String COLUMN_TRIGGER_NAME = "trigger name";
  public static final String COLUMN_TRIGGER_STATUS = "status";
  public static final String COLUMN_TRIGGER_EVENT = "event";
  public static final String COLUMN_TRIGGER_PATH = "path";
  public static final String COLUMN_TRIGGER_CLASS = "class name";
  public static final String COLUMN_TRIGGER_ATTRIBUTES = "attributes";

  public static final String COLUMN_TRIGGER_STATUS_STARTED = "started";
  public static final String COLUMN_TRIGGER_STATUS_STOPPED = "stopped";

  public static final String PATH_WILDCARD = "*";
  public static final String TIME = "time";
  public static final String ALIGN_TIMESERIES_PREFIX = "$#$";

  // sdt parameters
  public static final String LOSS = "loss";
  public static final String SDT = "sdt";
  public static final String SDT_COMP_DEV = "compdev";
  public static final String SDT_COMP_MIN_TIME = "compmintime";
  public static final String SDT_COMP_MAX_TIME = "compmaxtime";

  // data folder name
  public static final String SEQUENCE_FLODER_NAME = "sequence";
  public static final String UNSEQUENCE_FLODER_NAME = "unsequence";
  public static final String FILE_NAME_SEPARATOR = "-";
  public static final String UPGRADE_FOLDER_NAME = "upgrade";

  // system folder name
  public static final String SYSTEM_FOLDER_NAME = "system";
  public static final String SCHEMA_FOLDER_NAME = "schema";
  public static final String SYNC_FOLDER_NAME = "sync";
  public static final String QUERY_FOLDER_NAME = "query";
  public static final String TRACING_FOLDER_NAME = "tracing";
  public static final String TRACING_LOG = "tracing.txt";
  public static final String EXT_FOLDER_NAME = "ext";
  public static final String UDF_FOLDER_NAME = "udf";
  public static final String TRIGGER_FOLDER_NAME = "trigger";
  public static final String WAL_FOLDER_NAME = "wal";
  public static final String INDEX_FOLDER_NAME = "index";

  // mqtt
  public static final String ENABLE_MQTT = "enable_mqtt_service";
  public static final String MQTT_HOST_NAME = "mqtt_host";
  public static final String MQTT_PORT_NAME = "mqtt_port";
  public static final String MQTT_HANDLER_POOL_SIZE_NAME = "mqtt_handler_pool_size";
  public static final String MQTT_PAYLOAD_FORMATTER_NAME = "mqtt_payload_formatter";
  public static final String MQTT_MAX_MESSAGE_SIZE = "mqtt_max_message_size";

  // thrift
  public static final int LEFT_SIZE_IN_REQUEST = 4 * 1024 * 1024;

  // change tsFile name
  public static final int FILE_NAME_SUFFIX_INDEX = 0;
  public static final int FILE_NAME_SUFFIX_TIME_INDEX = 0;
  public static final int FILE_NAME_SUFFIX_VERSION_INDEX = 1;
  public static final int FILE_NAME_SUFFIX_MERGECNT_INDEX = 2;
  public static final int FILE_NAME_SUFFIX_UNSEQMERGECNT_INDEX = 3;
  public static final String FILE_NAME_SUFFIX_SEPARATOR = "\\.";
}
