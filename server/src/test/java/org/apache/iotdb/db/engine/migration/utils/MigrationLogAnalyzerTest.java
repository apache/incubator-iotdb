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

package org.apache.iotdb.db.engine.migration.utils;

import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.migration.task.IMigrationTask.FileMigrationStatus;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrationLogAnalyzerTest {
  private static final String testSgName = "root.migrationTest";
  private static final String testSgSysDir =
      TestConstant.OUTPUT_DATA_DIR.concat(testSgName).concat(File.separator);
  private final List<File> srcFiles =
      Arrays.asList(
          new File(testSgSysDir.concat("src1.tsfile")),
          new File(testSgSysDir.concat("src2.tsfile")));
  private final File targetDir = new File(TestConstant.OUTPUT_DATA_DIR);

  @AfterClass
  public static void afterClass() throws Exception {
    EnvironmentUtils.cleanDir(new File(testSgSysDir));
  }

  @Test
  public void testAnalyzeWithAllFilesEnd() throws IOException {
    Map<File, FileMigrationStatus> fileStatus = new HashMap<>();
    fileStatus.put(srcFiles.get(0), FileMigrationStatus.END);
    fileStatus.put(srcFiles.get(1), FileMigrationStatus.END);

    // generate log
    MigrationLogger logger = new MigrationLogger(testSgSysDir, 0);
    logger.logSourceFiles(srcFiles, true);
    logger.logTargetDir(targetDir);
    logger.startMigration();
    logger.startMigrateTsFile(srcFiles.get(0));
    logger.endCopyTsFile();
    logger.endMigrateTsFile();
    logger.startMigrateTsFile(srcFiles.get(1));
    logger.endMoveTsFile();
    logger.endMigrateTsFile();
    logger.endMigration();
    logger.close();

    // analyze log
    MigrationLogAnalyzer analyzer = new MigrationLogAnalyzer(logger.getLogFile());
    analyzer.analyze();

    Assert.assertTrue(analyzer.isSequence());
    Assert.assertEquals(
        FSPath.parse(targetDir).getAbsoluteFSPath().getRawFSPath(), analyzer.getTargetDir());
    List<String> srcRawPaths = analyzer.getFiles();
    for (int i = 0; i < this.srcFiles.size(); ++i) {
      Assert.assertEquals(
          FSPath.parse(srcFiles.get(i)).getAbsoluteFSPath().getRawFSPath(), srcRawPaths.get(i));
    }
    for (File src : this.srcFiles) {
      Assert.assertEquals(
          fileStatus.get(src),
          analyzer.getMigrationStatus(FSPath.parse(src).getAbsoluteFSPath().getRawFSPath()));
    }
  }

  @Test
  public void testAnalyzeWith1FileMoveEnd() throws IOException {
    Map<File, FileMigrationStatus> fileStatus = new HashMap<>();
    fileStatus.put(srcFiles.get(0), FileMigrationStatus.END);
    fileStatus.put(srcFiles.get(1), FileMigrationStatus.MOVE_END);

    // generate log
    MigrationLogger logger = new MigrationLogger(testSgSysDir, 0);
    logger.logSourceFiles(srcFiles, true);
    logger.logTargetDir(targetDir);
    logger.startMigration();
    logger.startMigrateTsFile(srcFiles.get(0));
    logger.endCopyTsFile();
    logger.endMigrateTsFile();
    logger.startMigrateTsFile(srcFiles.get(1));
    logger.endMoveTsFile();
    logger.close();

    // analyze log
    MigrationLogAnalyzer analyzer = new MigrationLogAnalyzer(logger.getLogFile());
    analyzer.analyze();

    Assert.assertTrue(analyzer.isSequence());
    Assert.assertEquals(
        FSPath.parse(targetDir).getAbsoluteFSPath().getRawFSPath(), analyzer.getTargetDir());
    List<String> srcRawPaths = analyzer.getFiles();
    for (int i = 0; i < this.srcFiles.size(); ++i) {
      Assert.assertEquals(
          FSPath.parse(srcFiles.get(i)).getAbsoluteFSPath().getRawFSPath(), srcRawPaths.get(i));
    }
    for (File src : this.srcFiles) {
      Assert.assertEquals(
          fileStatus.get(src),
          analyzer.getMigrationStatus(FSPath.parse(src).getAbsoluteFSPath().getRawFSPath()));
    }
  }

  @Test
  public void testAnalyzeWith1FileEnd() throws IOException {
    Map<File, FileMigrationStatus> fileStatus = new HashMap<>();
    fileStatus.put(srcFiles.get(0), FileMigrationStatus.END);
    fileStatus.put(srcFiles.get(1), FileMigrationStatus.NONE);

    // generate log
    MigrationLogger logger = new MigrationLogger(testSgSysDir, 0);
    logger.logSourceFiles(srcFiles, true);
    logger.logTargetDir(targetDir);
    logger.startMigration();
    logger.startMigrateTsFile(srcFiles.get(0));
    logger.endCopyTsFile();
    logger.endMigrateTsFile();
    logger.close();

    // analyze log
    MigrationLogAnalyzer analyzer = new MigrationLogAnalyzer(logger.getLogFile());
    analyzer.analyze();

    Assert.assertTrue(analyzer.isSequence());
    Assert.assertEquals(
        FSPath.parse(targetDir).getAbsoluteFSPath().getRawFSPath(), analyzer.getTargetDir());
    List<String> srcRawPaths = analyzer.getFiles();
    for (int i = 0; i < this.srcFiles.size(); ++i) {
      Assert.assertEquals(
          FSPath.parse(srcFiles.get(i)).getAbsoluteFSPath().getRawFSPath(), srcRawPaths.get(i));
    }
    for (File src : this.srcFiles) {
      Assert.assertEquals(
          fileStatus.get(src),
          analyzer.getMigrationStatus(FSPath.parse(src).getAbsoluteFSPath().getRawFSPath()));
    }
  }

  @Test
  public void testAnalyzeNotStart() throws IOException {
    Map<File, FileMigrationStatus> fileStatus = new HashMap<>();
    fileStatus.put(srcFiles.get(0), FileMigrationStatus.NONE);
    fileStatus.put(srcFiles.get(1), FileMigrationStatus.NONE);

    // generate log
    MigrationLogger logger = new MigrationLogger(testSgSysDir, 0);
    logger.logSourceFiles(srcFiles, false);
    logger.logTargetDir(targetDir);
    logger.startMigration();
    logger.close();

    // analyze log
    MigrationLogAnalyzer analyzer = new MigrationLogAnalyzer(logger.getLogFile());
    analyzer.analyze();

    Assert.assertFalse(analyzer.isSequence());
    Assert.assertEquals(
        FSPath.parse(targetDir).getAbsoluteFSPath().getRawFSPath(), analyzer.getTargetDir());
    List<String> srcRawPaths = analyzer.getFiles();
    for (int i = 0; i < this.srcFiles.size(); ++i) {
      Assert.assertEquals(
          FSPath.parse(srcFiles.get(i)).getAbsoluteFSPath().getRawFSPath(), srcRawPaths.get(i));
    }
    for (File src : this.srcFiles) {
      Assert.assertEquals(
          fileStatus.get(src),
          analyzer.getMigrationStatus(FSPath.parse(src).getAbsoluteFSPath().getRawFSPath()));
    }
  }
}
