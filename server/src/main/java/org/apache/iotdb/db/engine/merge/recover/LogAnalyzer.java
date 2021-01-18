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

package org.apache.iotdb.db.engine.merge.recover;

import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_ALL_TS_END;
import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_END;
import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_MERGE_END;
import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_MERGE_START;
import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_SEQ_FILES;
import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_START;
import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_TIMESERIES;
import static org.apache.iotdb.db.engine.merge.recover.MergeLogger.STR_UNSEQ_FILES;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.task.MergeTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LogAnalyzer scans the "mergeUnseq.log" file and recovers information such as files of last mergeUnseq,
 * the last available positions of each file and how many timeseries and files have been merged.
 * An example of merging 1 seqFile and 1 unseqFile containing 3 series is:
 * seqFiles
 * server/0seq.tsfile
 * unseqFiles
 * server/0unseq.tsfile
 * mergeUnseq start
 * start root.mergeTest.device0.sensor0
 * server/0seq.tsfile.mergeUnseq 338
 * end
 * start root.mergeTest.device0.sensor1
 * server/0seq.tsfile.mergeUnseq 664
 * end
 * all ts end
 * server/0seq.tsfile 145462
 * end
 * mergeUnseq end
 */
public class LogAnalyzer {

  private static final Logger logger = LoggerFactory.getLogger(LogAnalyzer.class);

  private MergeResource resource;
  private String taskName;
  private File logFile;
  private String storageGroupName;

  private Map<File, Long> fileLastPositions = new HashMap<>();
  private Map<File, Long> tempFileLastPositions = new HashMap<>();

  private List<PartialPath> mergedPaths = new ArrayList<>();
  private List<PartialPath> unmergedPaths;
  private List<TsFileResource> unmergedFiles;
  private String currLine;

  private Status status;

  public LogAnalyzer(MergeResource resource, String taskName, File logFile, String storageGroupName) {
    this.resource = resource;
    this.taskName = taskName;
    this.logFile = logFile;
    this.storageGroupName = storageGroupName;
  }

  /**
   * Scan through the logs to find out where the last mergeUnseq has stopped and store the information
   * about the progress in the fields.
   * @return a Status indicating the completed stage of the last mergeUnseq.
   * @throws IOException
   */
  public Status analyze() throws IOException, MetadataException {
    status = Status.NONE;
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(logFile))) {
      currLine = bufferedReader.readLine();
      if (currLine != null) {
        analyzeSeqFiles(bufferedReader);

        analyzeUnseqFiles(bufferedReader);

        List<PartialPath> storageGroupPaths = IoTDB.metaManager.getAllTimeseriesPath(new PartialPath(storageGroupName + ".*"));
        unmergedPaths = new ArrayList<>();
        unmergedPaths.addAll(storageGroupPaths);

        analyzeMergedSeries(bufferedReader, unmergedPaths);

        analyzeMergedFiles(bufferedReader);
      }
    }
    return status;
  }

  private void analyzeSeqFiles(BufferedReader bufferedReader) throws IOException {
    if (!STR_SEQ_FILES.equals(currLine)) {
      return;
    }
    long startTime = System.currentTimeMillis();
    List<TsFileResource> mergeSeqFiles = new ArrayList<>();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (STR_UNSEQ_FILES.equals(currLine)) {
        break;
      }
      Iterator<TsFileResource> iterator = resource.getSeqFiles().iterator();
      while (iterator.hasNext()) {
        TsFileResource seqFile = iterator.next();
        if (seqFile.getTsFile().getAbsolutePath().equals(currLine)) {
          mergeSeqFiles.add(seqFile);
          // remove to speed-up next iteration
          iterator.remove();
          break;
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} seq files after {}ms", taskName, mergeSeqFiles.size(),
          (System.currentTimeMillis() - startTime));
    }
    resource.setSeqFiles(mergeSeqFiles);
  }

  private void analyzeUnseqFiles(BufferedReader bufferedReader) throws IOException {
    if (!STR_UNSEQ_FILES.equals(currLine)) {
      return;
    }
    long startTime = System.currentTimeMillis();
    List<TsFileResource> mergeUnseqFiles = new ArrayList<>();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (currLine.equals(STR_TIMESERIES)) {
        break;
      }
      Iterator<TsFileResource> iterator = resource.getUnseqFiles().iterator();
      while (iterator.hasNext()) {
        TsFileResource unseqFile = iterator.next();
        if (unseqFile.getTsFile().getAbsolutePath().equals(currLine)) {
          mergeUnseqFiles.add(unseqFile);
          // remove to speed-up next iteration
          iterator.remove();
          break;
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} unseq files after {}ms", taskName, mergeUnseqFiles.size(),
          (System.currentTimeMillis() - startTime));
    }
    resource.setUnseqFiles(mergeUnseqFiles);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void analyzeMergedSeries(BufferedReader bufferedReader, List<PartialPath> unmergedPaths) throws IOException {
    if (!STR_MERGE_START.equals(currLine)) {
      return;
    }

    status = Status.MERGE_START;
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      File mergeFile = SystemFileFactory.INSTANCE.getFile(seqFile.getTsFilePath() + MergeTask.MERGE_SUFFIX);
      fileLastPositions.put(mergeFile, 0L);
    }

    List<PartialPath> currTSList = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    while ((currLine = bufferedReader.readLine()) != null) {
      if (STR_ALL_TS_END.equals(currLine)) {
        break;
      }
      if (currLine.contains(STR_START)) {
        // a TS starts to mergeUnseq
        String[] splits = currLine.split(" ");
        for (int i = 1; i < splits.length; i ++) {
          try {
            currTSList.add(new PartialPath(splits[i]));
          } catch (IllegalPathException e) {
            throw new IOException(e.getMessage());
          }
        }
        tempFileLastPositions.clear();
      } else if (!currLine.contains(STR_END)) {
        // file position
        String[] splits = currLine.split(" ");
        File file = SystemFileFactory.INSTANCE.getFile(splits[0]);
        Long position = Long.parseLong(splits[1]);
        tempFileLastPositions.put(file, position);
      } else {
        // a TS ends merging
        unmergedPaths.removeAll(currTSList);
        for (Entry<File, Long> entry : tempFileLastPositions.entrySet()) {
          fileLastPositions.put(entry.getKey(), entry.getValue());
        }
        mergedPaths.addAll(currTSList);
      }
    }
    tempFileLastPositions = null;
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} series have already been merged after {}ms", taskName,
          mergedPaths.size(), (System.currentTimeMillis() - startTime));
    }
  }

  @SuppressWarnings("squid:S3776")
  private void analyzeMergedFiles(BufferedReader bufferedReader) throws IOException {
    if (!STR_ALL_TS_END.equals(currLine)) {
      return;
    }

    status = Status.ALL_TS_MERGED;
    unmergedFiles = resource.getSeqFiles();

    File currFile = null;
    long startTime = System.currentTimeMillis();
    int mergedCnt = 0;
    while ((currLine = bufferedReader.readLine()) != null) {
      if (STR_MERGE_END.equals(currLine)) {
        status = Status.MERGE_END;
        break;
      }
      if (!currLine.contains(STR_END)) {
        String[] splits = currLine.split(" ");
        currFile = SystemFileFactory.INSTANCE.getFile(splits[0]);
        Long lastPost = Long.parseLong(splits[1]);
        fileLastPositions.put(currFile, lastPost);
      } else {
        if (currFile == null) {
          throw new IOException("Illegal mergeUnseq files");
        }
        fileLastPositions.remove(currFile);
        String seqFilePath = currFile.getAbsolutePath().replace(MergeTask.MERGE_SUFFIX, "");
        Iterator<TsFileResource> unmergedFileIter = unmergedFiles.iterator();
        while (unmergedFileIter.hasNext()) {
          TsFileResource seqFile = unmergedFileIter.next();
          if (seqFile.getTsFile().getAbsolutePath().equals(seqFilePath)) {
            mergedCnt ++;
            unmergedFileIter.remove();
            break;
          }
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{} found {} files have already been merged after {}ms", taskName,
          mergedCnt, (System.currentTimeMillis() - startTime));
    }
  }

  public List<PartialPath> getUnmergedPaths() {
    return unmergedPaths;
  }

  public void setUnmergedPaths(List<PartialPath> unmergedPaths) {
    this.unmergedPaths = unmergedPaths;
  }

  public List<TsFileResource> getUnmergedFiles() {
    return unmergedFiles;
  }

  public void setUnmergedFiles(
      List<TsFileResource> unmergedFiles) {
    this.unmergedFiles = unmergedFiles;
  }

  public List<PartialPath> getMergedPaths() {
    return mergedPaths;
  }

  public void setMergedPaths(List<PartialPath> mergedPaths) {
    this.mergedPaths = mergedPaths;
  }

  public Map<File, Long> getFileLastPositions() {
    return fileLastPositions;
  }

  public void setFileLastPositions(Map<File, Long> fileLastPositions) {
    this.fileLastPositions = fileLastPositions;
  }

  public enum Status {
    // almost nothing has been done
    NONE,
    // at least the files and timeseries to be merged are known
    MERGE_START,
    // all the timeseries have been merged(merged chunks are generated)
    ALL_TS_MERGED,
    // all the mergeUnseq files are merged with the origin files and the task is almost done
    MERGE_END
  }
}
