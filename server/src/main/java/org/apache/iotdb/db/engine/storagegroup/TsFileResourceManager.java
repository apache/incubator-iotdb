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

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.db.exception.WriteLockFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

public class TsFileResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileResourceManager.class);
  private String storageGroupName;
  private String virtualStorageGroup;
  private String storageGroupDir;

  /** Serialize queries, delete resource files, compaction cleanup files */
  private final ReadWriteLock resourceListLock = new ReentrantReadWriteLock();

  private String writeLockHolder;
  // time partition -> double linked list of tsfiles
  private Map<Long, TsFileResourceList> sequenceFiles = new TreeMap<>();
  private Map<Long, TsFileResourceList> unsequenceFiles = new TreeMap<>();

  private List<TsFileResource> sequenceRecoverTsFileResources = new ArrayList<>();
  private List<TsFileResource> unsequenceRecoverTsFileResources = new ArrayList<>();

  public TsFileResourceManager(
      String storageGroupName, String virtualStorageGroup, String storageGroupDir) {
    this.storageGroupName = storageGroupName;
    this.storageGroupDir = storageGroupDir;
    this.virtualStorageGroup = virtualStorageGroup;
  }

  public List<TsFileResource> getTsFileList(boolean sequence) {
    readLock();
    try {
      List<TsFileResource> allResources = new ArrayList<>();
      Map<Long, TsFileResourceList> chosenMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : chosenMap.entrySet()) {
        allResources.addAll(entry.getValue().getArrayList());
      }
      return allResources;
    } finally {
      readUnlock();
    }
  }

  public TsFileResourceList getSequenceListByTimePartition(long timePartition) {
    return sequenceFiles.getOrDefault(timePartition, new TsFileResourceList());
  }

  public TsFileResourceList getUnsequenceListByTimePartition(long timePartition) {
    return unsequenceFiles.getOrDefault(timePartition, new TsFileResourceList());
  }

  public Iterator<TsFileResource> getIterator(boolean sequence) {
    readLock();
    try {
      return getTsFileList(sequence).iterator();
    } finally {
      readUnlock();
    }
  }

  public void remove(TsFileResource tsFileResource, boolean sequence) {
    writeLock("remove");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : selectedMap.entrySet()) {
        if (entry.getValue().contains(tsFileResource)) {
          entry.getValue().remove(tsFileResource);
          break;
        }
      }
    } finally {
      writeUnlock();
    }
  }

  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    for (TsFileResource resource : tsFileResourceList) {
      remove(resource, sequence);
    }
  }

  /**
   * insert tsFileResource to a target pos(targetPos = insertPos + 1) e.g. if insertPos = 0, then to
   * the first, if insert Pos = 1, then to the second.
   */
  public void insert(TsFileResource tsFileResource, boolean sequence, int insertPos) {
    writeLock("add");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      TsFileResourceList tsFileResources =
          selectedMap.computeIfAbsent(
              tsFileResource.getTimePartition(), o -> new TsFileResourceList());
      tsFileResources.set(insertPos, tsFileResource);
    } finally {
      writeUnlock();
    }
  }

  public void add(TsFileResource tsFileResource, boolean sequence) {
    writeLock("add");
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      selectedMap
          .computeIfAbsent(tsFileResource.getTimePartition(), o -> new TsFileResourceList())
          .add(tsFileResource);
    } finally {
      writeUnlock();
    }
  }

  public void addRecover(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      sequenceRecoverTsFileResources.add(tsFileResource);
    } else {
      unsequenceRecoverTsFileResources.add(tsFileResource);
    }
  }

  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    writeLock("add");
    try {
      for (TsFileResource resource : tsFileResourceList) {
        add(resource, sequence);
      }
    } finally {
      writeUnlock();
    }
  }

  public boolean contains(TsFileResource tsFileResource, boolean sequence) {
    readLock();
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      TsFileResourceList list = selectedMap.getOrDefault(tsFileResource.getTimePartition(), null);
      return list != null && list.contains(tsFileResource);
    } finally {
      readUnlock();
    }
  }

  public void clear() {
    writeLock("clear");
    try {
      sequenceFiles.clear();
      unsequenceFiles.clear();
    } finally {
      writeUnlock();
    }
  }

  public boolean isEmpty(boolean sequence) {
    readLock();
    try {
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : selectedMap.entrySet()) {
        if (!entry.getValue().isEmpty()) {
          return false;
        }
      }
      return true;
    } finally {
      readUnlock();
    }
  }

  public int size(boolean sequence) {
    readLock();
    try {
      int totalSize = 0;
      Map<Long, TsFileResourceList> selectedMap = sequence ? sequenceFiles : unsequenceFiles;
      for (Map.Entry<Long, TsFileResourceList> entry : selectedMap.entrySet()) {
        totalSize += entry.getValue().size();
      }
      return totalSize;
    } finally {
      readUnlock();
    }
  }

  public void readLock() {
    resourceListLock.readLock().lock();
  }

  public void readUnlock() {
    resourceListLock.readLock().unlock();
  }

  public void writeLock(String holder) {
    resourceListLock.writeLock().lock();
    writeLockHolder = holder;
  }

  /**
   * Acquire write lock with timeout, {@link WriteLockFailedException} will be thrown after timeout.
   * The unit of timeout is ms.
   */
  public void writeLockWithTimeout(String holder, long timeout) throws WriteLockFailedException {
    try {
      if (resourceListLock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
        writeLockHolder = holder;
      } else {
        throw new WriteLockFailedException(
            String.format("cannot get write lock in %d ms", timeout));
      }
    } catch (InterruptedException e) {
      LOGGER.warn(e.getMessage(), e);
      Thread.interrupted();
      throw new WriteLockFailedException("thread is interrupted");
    }
  }

  public void writeUnlock() {
    resourceListLock.writeLock().unlock();
    writeLockHolder = "";
  }

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public String getStorageGroupDir() {
    return storageGroupDir;
  }

  public void setStorageGroupDir(String storageGroupDir) {
    this.storageGroupDir = storageGroupDir;
  }

  public Set<Long> getTimePartitions() {
    readLock();
    try {
      Set<Long> timePartitions = new HashSet<>(sequenceFiles.keySet());
      return timePartitions;
    } finally {
      readUnlock();
    }
  }

  public String getVirtualStorageGroup() {
    return virtualStorageGroup;
  }

  public void setVirtualStorageGroup(String virtualStorageGroup) {
    this.virtualStorageGroup = virtualStorageGroup;
  }

  public List<TsFileResource> getSequenceRecoverTsFileResources() {
    return sequenceRecoverTsFileResources;
  }

  public List<TsFileResource> getUnsequenceRecoverTsFileResources() {
    return unsequenceRecoverTsFileResources;
  }

  // ({systemTime}-{versionNum}-{compactionNum}-{mergeNum}.tsfile)
  public static int compareFileName(File o1, File o2) {
    String[] items1 = o1.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    String[] items2 = o2.getName().replace(TSFILE_SUFFIX, "").split(FILE_NAME_SEPARATOR);
    long ver1 = Long.parseLong(items1[0]);
    long ver2 = Long.parseLong(items2[0]);
    int cmp = Long.compare(ver1, ver2);
    if (cmp == 0) {
      int cmpVersion = Long.compare(Long.parseLong(items1[1]), Long.parseLong(items2[1]));
      if (cmpVersion == 0) {
        return Long.compare(Long.parseLong(items1[2]), Long.parseLong(items2[2]));
      }
      return cmpVersion;
    } else {
      return cmp;
    }
  }
}
