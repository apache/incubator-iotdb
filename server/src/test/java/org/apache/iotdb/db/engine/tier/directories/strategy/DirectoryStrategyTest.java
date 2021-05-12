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
package org.apache.iotdb.db.engine.tier.directories.strategy;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "org.w3c.*"})
@PrepareForTest(CommonUtils.class)
public class DirectoryStrategyTest {

  List<FSPath> dataDirList;
  Set<Integer> fullDirIndexSet;

  @Before
  public void setUp() throws IOException {
    dataDirList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      dataDirList.add(new FSPath(TestConstant.DEFAULT_TEST_FS, TestConstant.OUTPUT_DATA_DIR + i));
    }

    fullDirIndexSet = new HashSet<>();
    fullDirIndexSet.add(1);
    fullDirIndexSet.add(3);

    PowerMockito.mockStatic(CommonUtils.class);
    for (int i = 0; i < dataDirList.size(); i++) {
      boolean res = !fullDirIndexSet.contains(i);
      PowerMockito.when(CommonUtils.hasSpace(dataDirList.get(i))).thenReturn(res);
      PowerMockito.when(CommonUtils.getUsableSpace(dataDirList.get(i)))
          .thenReturn(res ? (long) (i + 1) : 0L);
      PowerMockito.when(CommonUtils.getOccupiedSpace(dataDirList.get(i)))
          .thenReturn(res ? (long) (i + 1) : Long.MAX_VALUE);
    }
  }

  @After
  public void tearDown() {}

  @Test
  public void testSequenceStrategy() throws DiskSpaceInsufficientException {
    SequenceStrategy sequenceStrategy = new SequenceStrategy();
    sequenceStrategy.setFolders(dataDirList);

    // loop two times of data dir size to fully loop
    int index = 0;
    for (int i = 0; i < dataDirList.size() * 2; i++, index++) {
      index = index % dataDirList.size();
      while (fullDirIndexSet.contains(index)) {
        index = (index + 1) % dataDirList.size();
      }
      assertEquals(index, sequenceStrategy.nextFolderIndex());
    }
  }

  @Test
  public void testMaxDiskUsableSpaceFirstStrategy() throws DiskSpaceInsufficientException {
    MaxDiskUsableSpaceFirstStrategy maxDiskUsableSpaceFirstStrategy =
        new MaxDiskUsableSpaceFirstStrategy();
    maxDiskUsableSpaceFirstStrategy.setFolders(dataDirList);

    int maxIndex = getIndexOfMaxSpace();
    for (int i = 0; i < dataDirList.size(); i++) {
      assertEquals(maxIndex, maxDiskUsableSpaceFirstStrategy.nextFolderIndex());
    }

    PowerMockito.when(CommonUtils.getUsableSpace(dataDirList.get(maxIndex))).thenReturn(0L);
    maxIndex = getIndexOfMaxSpace();
    for (int i = 0; i < dataDirList.size(); i++) {
      assertEquals(maxIndex, maxDiskUsableSpaceFirstStrategy.nextFolderIndex());
    }
  }

  private int getIndexOfMaxSpace() {
    int index = -1;
    long maxSpace = -1;
    for (int i = 0; i < dataDirList.size(); i++) {
      long space = CommonUtils.getUsableSpace(dataDirList.get(i));
      if (maxSpace < space) {
        index = i;
        maxSpace = space;
      }
    }
    return index;
  }

  @Test
  public void testMinFolderOccupiedSpaceFirstStrategy()
      throws DiskSpaceInsufficientException, IOException {
    MinFolderOccupiedSpaceFirstStrategy minFolderOccupiedSpaceFirstStrategy =
        new MinFolderOccupiedSpaceFirstStrategy();
    minFolderOccupiedSpaceFirstStrategy.setFolders(dataDirList);

    int minIndex = getIndexOfMinOccupiedSpace();
    for (int i = 0; i < dataDirList.size(); i++) {
      assertEquals(minIndex, minFolderOccupiedSpaceFirstStrategy.nextFolderIndex());
    }

    PowerMockito.when(CommonUtils.getOccupiedSpace(dataDirList.get(minIndex)))
        .thenReturn(Long.MAX_VALUE);
    minIndex = getIndexOfMinOccupiedSpace();
    for (int i = 0; i < dataDirList.size(); i++) {
      assertEquals(minIndex, minFolderOccupiedSpaceFirstStrategy.nextFolderIndex());
    }
  }

  private int getIndexOfMinOccupiedSpace() throws IOException {
    int index = -1;
    long minOccupied = Long.MAX_VALUE;
    for (int i = 0; i < dataDirList.size(); i++) {
      long space = CommonUtils.getOccupiedSpace(dataDirList.get(i));
      if (minOccupied > space) {
        index = i;
        minOccupied = space;
      }
    }
    return index;
  }

  @Test
  public void testRandomOnDiskUsableSpaceStrategy() throws DiskSpaceInsufficientException {
    RandomOnDiskUsableSpaceStrategy randomOnDiskUsableSpaceStrategy =
        new RandomOnDiskUsableSpaceStrategy();
    randomOnDiskUsableSpaceStrategy.setFolders(dataDirList);

    for (int i = 0; i < dataDirList.size(); i++) {
      assertFalse(fullDirIndexSet.contains(randomOnDiskUsableSpaceStrategy.nextFolderIndex()));
    }

    int newFullIndex = randomOnDiskUsableSpaceStrategy.nextFolderIndex();
    PowerMockito.when(CommonUtils.getUsableSpace(dataDirList.get(newFullIndex))).thenReturn(0L);
    for (int i = 0; i < dataDirList.size(); i++) {
      int index = randomOnDiskUsableSpaceStrategy.nextFolderIndex();
      assertFalse(fullDirIndexSet.contains(index));
      assertTrue(newFullIndex != index);
    }
  }

  @Test
  public void testAllDiskFull() {
    for (int i = 0; i < dataDirList.size(); i++) {
      PowerMockito.when(CommonUtils.hasSpace(dataDirList.get(i))).thenReturn(false);
    }

    SequenceStrategy sequenceStrategy = new SequenceStrategy();
    try {
      sequenceStrategy.setFolders(dataDirList);
      fail();
    } catch (DiskSpaceInsufficientException e) {
    }

    MaxDiskUsableSpaceFirstStrategy maxDiskUsableSpaceFirstStrategy =
        new MaxDiskUsableSpaceFirstStrategy();
    try {
      maxDiskUsableSpaceFirstStrategy.setFolders(dataDirList);
      fail();
    } catch (DiskSpaceInsufficientException e) {
    }

    MinFolderOccupiedSpaceFirstStrategy minFolderOccupiedSpaceFirstStrategy =
        new MinFolderOccupiedSpaceFirstStrategy();
    try {
      minFolderOccupiedSpaceFirstStrategy.setFolders(dataDirList);
      fail();
    } catch (DiskSpaceInsufficientException e) {
    }
  }
}
