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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.level.LevelCompactionTsFileManagement;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class LevelCompactionModsTest extends LevelCompactionTest {

  File tempSGDir;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = fsFactory.getFile(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testCompactionMods() throws IllegalPathException, IOException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    TsFileResource sourceTsFileResource = seqResources.get(0);
    TsFileResource targetTsFileResource = seqResources.get(1);
    List<Modification> filterModifications = new ArrayList<>();
    Modification modification1;
    Modification modification2;
    try (ModificationFile sourceModificationFile =
        new ModificationFile(
            fsFactory.getFile(
                sourceTsFileResource.getTsFilePath() + ModificationFile.FILE_SUFFIX))) {
      modification1 = new Deletion(new PartialPath(deviceIds[0], "sensor0"), 0, 0);
      modification2 = new Deletion(new PartialPath(deviceIds[0], "sensor1"), Long.MAX_VALUE, 0);
      sourceModificationFile.write(modification1);
      sourceModificationFile.write(modification2);
      filterModifications.add(modification1);
    }
    List<TsFileResource> sourceTsFileResources = new ArrayList<>();
    sourceTsFileResources.add(sourceTsFileResource);
    levelCompactionTsFileManagement.renameLevelFilesMods(
        filterModifications, sourceTsFileResources, targetTsFileResource);
    try (ModificationFile targetModificationFile =
        new ModificationFile(
            fsFactory.getFile(
                targetTsFileResource.getTsFilePath() + ModificationFile.FILE_SUFFIX))) {
      Collection<Modification> modifications = targetModificationFile.getModifications();
      assertEquals(1, modifications.size());
      assertEquals(modification2, modifications.stream().findFirst().get());
    }
  }

  /**
   * As we change the structure of mods file in 0.12, we have to check whether a modification record
   * is valid by its offset in tsfile
   */
  @Test
  public void testCompactionModsByOffset() throws IllegalPathException, IOException {
    LevelCompactionTsFileManagement levelCompactionTsFileManagement =
        new LevelCompactionTsFileManagement(COMPACTION_TEST_SG, tempSGDir.getPath());
    TsFileResource sourceTsFileResource = seqResources.get(0);
    TsFileResource targetTsFileResource = seqResources.get(1);
    List<Modification> filterModifications = new ArrayList<>();
    Modification modification1;
    Modification modification2;
    try (ModificationFile sourceModificationFile =
        new ModificationFile(
            fsFactory.getFile(
                sourceTsFileResource.getTsFilePath() + ModificationFile.FILE_SUFFIX))) {
      modification1 =
          new Deletion(
              new PartialPath(deviceIds[0], "sensor0"),
              sourceTsFileResource.getTsFileSize() / 2,
              0,
              100);
      modification2 =
          new Deletion(
              new PartialPath(deviceIds[0], "sensor1"),
              sourceTsFileResource.getTsFileSize() / 2,
              0,
              100);
      sourceModificationFile.write(modification1);
      sourceModificationFile.write(modification2);
      filterModifications.add(modification1);
    }
    List<TsFileResource> sourceTsFileResources = new ArrayList<>();
    sourceTsFileResources.add(sourceTsFileResource);
    levelCompactionTsFileManagement.renameLevelFilesMods(
        filterModifications, sourceTsFileResources, targetTsFileResource);
    try (ModificationFile targetModificationFile =
        new ModificationFile(
            fsFactory.getFile(
                targetTsFileResource.getTsFilePath() + ModificationFile.FILE_SUFFIX))) {
      Collection<Modification> modifications = targetModificationFile.getModifications();
      assertEquals(1, modifications.size());
      assertEquals(Long.MAX_VALUE, modifications.stream().findFirst().get().getFileOffset());
    }
  }
}
