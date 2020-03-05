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
package org.apache.iotdb.db.exception.storageGroup;

import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.rpc.TSStatusCode;

public class StorageGroupProcessorException extends IoTDBException {

  private static final long serialVersionUID = 7373978140952977661L;

  public StorageGroupProcessorException(Exception exception) {
    super(exception, TSStatusCode.STORAGE_GROUP_PROCESSOR_ERROR.getStatusCode());
  }

  public StorageGroupProcessorException(String message) {
    super(message, TSStatusCode.STORAGE_GROUP_PROCESSOR_ERROR.getStatusCode());
  }
}
