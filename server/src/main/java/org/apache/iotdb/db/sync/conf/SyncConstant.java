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
package org.apache.iotdb.db.sync.conf;

public class SyncConstant {

  private SyncConstant() {}

  public static final String CONFIG_NAME = "iotdb-sync-client.properties";

  public static final String SYNC_NAME = "sync";

  public static final String SYNC_SENDER = "sync-sender";

  public static final String SYNC_RECEIVER = "sync-receiver";

  public static final String MESSAGE_DIGIT_NAME = "SHA-256";

  public static final String SYNC_DIR_NAME_SEPARATOR = "_";

  /** Split data file, block size at each transmission */
  public static final int DATA_CHUNK_SIZE = 64 * 1024 * 1024;

  // sender section
  public static final String TMP_FILE_SUFFIX = ".tmp";

  public static final String LOCK_FILE_NAME = "sync_lock";

  public static final String UUID_FILE_NAME = "uuid.txt";

  public static final String SCHEMA_POS_FILE_NAME = "sync_schema_pos";

  public static final String LAST_LOCAL_FILE_NAME = "last_local_files.txt";

  public static final String CURRENT_LOCAL_FILE_NAME = "current_local_files.txt";

  public static final String DELETED_BLACKLIST_FILE_NAME = "deleted_files_blacklist.txt";

  public static final String TO_BE_SYNCED_BLACKLIST_FILE_NAME = "to_be_synced_files_blacklist.txt";

  public static final String DATA_SNAPSHOT_NAME = "snapshot";

  public static final String SYNC_LOG_NAME = "sync.log";

  private static final SyncSenderConfig CONFIG = SyncSenderDescriptor.getInstance().getConfig();

  public static final long SYNC_PROCESS_DELAY = 0;

  public static final long SYNC_MONITOR_DELAY = CONFIG.getSyncPeriodInSecond();

  public static final long SYNC_PROCESS_PERIOD = CONFIG.getSyncPeriodInSecond();

  public static final long SYNC_MONITOR_PERIOD = CONFIG.getSyncPeriodInSecond();

  // receiver section

  public static final String RECEIVER_DATA_FOLDER_NAME = "data";

  public static final String LOAD_LOG_NAME = "load.log";

  public static final String DEVICE_OWNER_FILE_NAME = "device_owner";

  public static final String DEVICE_OWNER_TMP_FILE_NAME = "device_owner.tmp";

  public static final int SUCCESS_CODE = 1;

  public static final int ERROR_CODE = -1;

  public static final int CONFLICT_CODE = -2;
}
