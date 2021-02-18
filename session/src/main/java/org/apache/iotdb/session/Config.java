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
package org.apache.iotdb.session;

public class Config {

  public static final String DEFAULT_USER = "root";
  public static final String DEFAULT_PASSWORD = "root";
  public static final int DEFAULT_FETCH_SIZE = 5000;
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 0;
  public static final boolean DEFAULT_CACHE_LEADER_MODE = false;
  public static final int DEFAULT_THREAD_POOL_SIZE = 100;
  public static final int DEFAULT_BLOCKING_QUEUE_SIZE = 100000;
  public static final int DEFAULT_THREAD_WAIT_TIME_MS = 10000;

  public static final int RETRY_NUM = 3;
  public static final long RETRY_INTERVAL_MS = 1000;

  /**
   * thrift init buffer size, 1KB by default
   */
  public static final int DEFAULT_INITIAL_BUFFER_CAPACITY = 1024;

  /**
   * thrift max frame size (16384000 bytes by default), we change it to 64MB
   */
  public static final int DEFAULT_MAX_FRAME_SIZE = 67108864;
}
