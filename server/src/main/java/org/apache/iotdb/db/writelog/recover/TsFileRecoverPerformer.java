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

package org.apache.iotdb.db.writelog.recover;

import static org.apache.iotdb.db.engine.storagegroup.TsFileResource.RESOURCE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.flush.MemTableFlushTask;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.PrimitiveMemTable;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.tsfile.exception.NotCompatibleTsFileException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TsFileRecoverPerformer recovers a SeqTsFile to correct status, redoes the WALs since last crash
 * and removes the redone logs.
 */
public class TsFileRecoverPerformer {

  private static final Logger logger = LoggerFactory.getLogger(TsFileRecoverPerformer.class);

  private final String filePath;
  private final String logNodePrefix;
  private final VersionController versionController;
  private final TsFileResource tsFileResource;
  private final boolean sequence;

  /**
   * @param isLastFile whether this TsFile is the last file of its partition
   */
  public TsFileRecoverPerformer(String logNodePrefix, VersionController versionController,
      TsFileResource currentTsFileResource, boolean sequence, boolean isLastFile) {
    this.filePath = currentTsFileResource.getTsFilePath();
    this.logNodePrefix = logNodePrefix;
    this.versionController = versionController;
    this.tsFileResource = currentTsFileResource;
    this.sequence = sequence;
  }

  /**
   * 1. recover the TsFile by RestorableTsFileIOWriter and truncate the file to remaining corrected
   * data 2. redo the WALs to recover unpersisted data 3. flush and close the file 4. clean WALs
   *
   * @return a RestorableTsFileIOWriter and a list of RestorableTsFileIOWriter of vmfiles, if the
   * file and the vmfiles are not closed before crash, so these writers can be used to continue
   * writing
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public RestorableTsFileIOWriter recover(boolean needRedoWal)
      throws StorageGroupProcessorException {

    File file = FSFactoryProducer.getFSFactory().getFile(filePath);
    if (!file.exists()) {
      logger.error("TsFile {} is missing, will skip its recovery.", filePath);
      return null;
    }

    // remove corrupted part of the TsFile
    RestorableTsFileIOWriter restorableTsFileIOWriter;
    try {
      restorableTsFileIOWriter = new RestorableTsFileIOWriter(file);
    } catch (NotCompatibleTsFileException e) {
      boolean result = file.delete();
      logger.warn("TsFile {} is incompatible. Delete it successfully {}", filePath, result);
      throw new StorageGroupProcessorException(e);
    } catch (IOException e) {
      throw new StorageGroupProcessorException(e);
    }

    // judge whether tsfile is complete
    if (!restorableTsFileIOWriter.hasCrashed()) {
      try {
        recoverResource();
        return restorableTsFileIOWriter;
      } catch (IOException e) {
        throw new StorageGroupProcessorException(
            "recover the resource file failed: " + filePath + RESOURCE_SUFFIX + e);
      }
    }

    // tsfile has crashed
    // due to failure, the last ChunkGroup may contain the same data as the WALs, so the time
    // map must be updated first to avoid duplicated insertion
    recoverResourceFromWriter(restorableTsFileIOWriter);

    // redo logs
    if (needRedoWal) {
      long start = System.currentTimeMillis();
      redoLogs(restorableTsFileIOWriter);
      logger.debug("Redoing logs of {} costs {}ms", file, System.currentTimeMillis() - start);

      // clean logs
      try {
        MultiFileLogNodeManager.getInstance()
            .deleteNode(logNodePrefix + SystemFileFactory.INSTANCE.getFile(filePath).getName());
      } catch (IOException e) {
        throw new StorageGroupProcessorException(e);
      }
    }
    return restorableTsFileIOWriter;
  }

  private void recoverResource() throws IOException {
    if (tsFileResource.resourceFileExists()) {
      // .resource file exists, deserialize it
      recoverResourceFromFile();
    } else {
      // .resource file does not exist, read file metadata and recover tsfile resource
      try (TsFileSequenceReader reader = new TsFileSequenceReader(
          tsFileResource.getTsFile().getAbsolutePath())) {
        FileLoaderUtils.updateTsFileResource(reader, tsFileResource);
      }
      // write .resource file
      tsFileResource.serialize();
    }
  }

  private void recoverResourceFromFile() throws IOException {
    try {
      tsFileResource.deserialize();
    } catch (IOException e) {
      logger.warn("Cannot deserialize TsFileResource {}, construct it using "
          + "TsFileSequenceReader", tsFileResource.getTsFile(), e);
      recoverResourceFromReader();
    }
  }


  private void recoverResourceFromReader() throws IOException {
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(tsFileResource.getTsFile().getAbsolutePath(), true)) {
      for (Entry<String, List<TimeseriesMetadata>> entry : reader.getAllTimeseriesMetadata()
          .entrySet()) {
        for (TimeseriesMetadata timeseriesMetaData : entry.getValue()) {
          tsFileResource
              .updateStartTime(entry.getKey(), timeseriesMetaData.getStatistics().getStartTime());
          tsFileResource
              .updateEndTime(entry.getKey(), timeseriesMetaData.getStatistics().getEndTime());
        }
      }
    }
    // write .resource file
    tsFileResource.serialize();
  }


  private void recoverResourceFromWriter(RestorableTsFileIOWriter restorableTsFileIOWriter) {
    Map<String, List<ChunkMetadata>> deviceChunkMetaDataMap =
        restorableTsFileIOWriter.getDeviceChunkMetadataMap();
    for (Map.Entry<String, List<ChunkMetadata>> entry : deviceChunkMetaDataMap.entrySet()) {
      String deviceId = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      TSDataType dataType = entry.getValue().get(entry.getValue().size() - 1).getDataType();
      for (ChunkMetadata chunkMetaData : chunkMetadataList) {
        if (!chunkMetaData.getDataType().equals(dataType)) {
          continue;
        }
        tsFileResource.updateStartTime(deviceId, chunkMetaData.getStartTime());
        tsFileResource.updateEndTime(deviceId, chunkMetaData.getEndTime());
      }
    }
    tsFileResource.updatePlanIndexes(restorableTsFileIOWriter.getMinPlanIndex());
    tsFileResource.updatePlanIndexes(restorableTsFileIOWriter.getMaxPlanIndex());
  }

  private void redoLogs(RestorableTsFileIOWriter restorableTsFileIOWriter)
      throws StorageGroupProcessorException {
    IMemTable recoverMemTable = new PrimitiveMemTable();
    recoverMemTable.setVersion(versionController.nextVersion());
    LogReplayer logReplayer = new LogReplayer(logNodePrefix, filePath, tsFileResource.getModFile(),
        versionController, tsFileResource, recoverMemTable, sequence);
    logReplayer.replayLogs();
    try {
      if (!recoverMemTable.isEmpty()) {
        // flush logs
        MemTableFlushTask tableFlushTask = new MemTableFlushTask(recoverMemTable,
            restorableTsFileIOWriter,
            tsFileResource.getTsFile().getParentFile().getParentFile().getName());
        tableFlushTask.syncFlushMemTable();
        tsFileResource.updatePlanIndexes(recoverMemTable.getMinPlanIndex());
        tsFileResource.updatePlanIndexes(recoverMemTable.getMaxPlanIndex());
      }

      restorableTsFileIOWriter.endFile();
      tsFileResource.serialize();

      // otherwise this file is not closed before crush, do nothing so we can continue writing
      // into it
    } catch (IOException | ExecutionException e) {
      throw new StorageGroupProcessorException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StorageGroupProcessorException(e);
    }
  }

}
