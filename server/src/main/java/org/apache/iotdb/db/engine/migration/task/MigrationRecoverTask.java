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

package org.apache.iotdb.db.engine.migration.task;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.migration.utils.MigrationLogAnalyzer;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class MigrationRecoverTask implements IMigrationTask {

  private static final Logger logger = LoggerFactory.getLogger(MigrationRecoverTask.class);

  private File logFile;
  private MigrationCallBack callBack;
  /** deletes TsFile and its TsFileResource */
  private Consumer<File> sgTsFileDeleteFunc;
  /** accepts (sequence, time partition) -> return tsFileResources */
  private BiFunction<Boolean, Long, List<TsFileResource>> tsFileResourcesGetter;

  private String storageGroupName;
  private String storageGroupSysDir;

  public MigrationRecoverTask(
      File logFile,
      MigrationCallBack callBack,
      Consumer<File> sgTsFileDeleteFunc,
      BiFunction<Boolean, Long, List<TsFileResource>> tsFileResourcesGetter,
      String storageGroupName,
      String storageGroupSysDir) {
    this.logFile = logFile;
    this.callBack = callBack;
    this.sgTsFileDeleteFunc = sgTsFileDeleteFunc;
    this.tsFileResourcesGetter = tsFileResourcesGetter;
    this.storageGroupName = storageGroupName;
    this.storageGroupSysDir = storageGroupSysDir;
  }

  void recover() {
    logger.info(
        "[Migration Recover] Start analyzing {}'s recover log {}.", storageGroupName, logFile);
    // analyze log file
    MigrationLogAnalyzer analyzer = new MigrationLogAnalyzer(logFile);
    try {
      analyzer.analyze();
    } catch (IOException e) {
      logger.error(
          "[Migration Recover] Error occurred when analyzing migration log {}.",
          logFile.getAbsolutePath());
    }
    List<String> srcPaths = new ArrayList<>();
    File targetDir = FSPath.parse(analyzer.getTargetDir()).toFile();
    for (String src : analyzer.getFiles()) {
      switch (analyzer.getMigrationStatus(src)) {
        case START:
        case NONE:
          srcPaths.add(src);
          break;
        case COPY_END:
          sgTsFileDeleteFunc.accept(FSPath.parse(src).toFile());
          break;
        case MOVE_END:
        case END:
        default:
          break;
      }
    }
    logFile.delete();
    if (srcPaths.isEmpty()) {
      return;
    }

    // get files' TsFileResource object and then submit a new migration task
    long timePartitionId =
        Long.parseLong(logFile.getName().split(IoTDBConstant.FILE_NAME_SEPARATOR)[0]);
    Map<String, TsFileResource> tsFileResourcesByPath = new HashMap<>();
    for (TsFileResource tsFileResource :
        tsFileResourcesGetter.apply(analyzer.isSequence(), timePartitionId)) {
      tsFileResourcesByPath.put(
          FSPath.parse(tsFileResource.getTsFile()).getAbsoluteFSPath().getRawFSPath(),
          tsFileResource);
    }
    List<TsFileResource> srcTsFileResources = new ArrayList<>();
    try {
      for (String srcPath : srcPaths) {
        TsFileResource tsFileResource = tsFileResourcesByPath.get(srcPath);
        if (tsFileResource != null) {
          tsFileResource.setMigrating(true);
          srcTsFileResources.add(tsFileResource);
        } else {
          logger.info("File {} doesn't have corresponding TsFileResource.", srcPath);
        }
      }
      MigrationTask newTask =
          new MigrationTask(
              srcTsFileResources,
              targetDir,
              analyzer.isSequence(),
              timePartitionId,
              callBack,
              storageGroupName,
              storageGroupSysDir);
      newTask.run();
    } catch (Exception e) {
      logger.error("Fail to submit a new migration task");
      for (TsFileResource tsFileResource : srcTsFileResources) {
        tsFileResource.setMigrating(false);
      }
    }
  }

  @Override
  public void run() {
    recover();
  }
}
