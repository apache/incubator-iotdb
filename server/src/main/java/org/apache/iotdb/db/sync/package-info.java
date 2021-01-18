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

/**
 * <p>
 * Package Sync is a suite tool that periodically uploads persistent tsfiles from the sender disk to
 * the receiver and loads them. With mergeUnseq module, synchronous update of write, update and delete
 * operations can be synced.
 *
 * On the sender side of the sync, the sync module is a separate process, independent of the IoTDB
 * process. It can be started and closed through separate scripts.
 *
 * On the receiver side of the sync, the sync module is embedded in the engine of IoTDB and is in
 * the same process with IoTDB. The receiver module listens to a separate port. Before using it, it
 * needs to set up a whitelist at the sync receiver, which is expressed as a network segment. The
 * receiver only accepts the data transferred from the sender located in the whitelist segment.
 *
 * Due to the IoTDB system supports multiple directories of data files, it will perform sub-tasks
 * according to disks in every complete synchronization task, because hard links are needed in the
 * execution process. Hard links can not be operated across disk partitions, and a synchronization
 * task will be performed in turn according to disks.
 */
package org.apache.iotdb.db.sync;