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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeUtils.class);

  private MergeUtils() {
    // util class
  }

  public static void writeTVPair(TimeValuePair timeValuePair, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(
            timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary(), false);
        break;
      case DOUBLE:
        chunkWriter.write(
            timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble(), false);
        break;
      case BOOLEAN:
        chunkWriter.write(
            timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean(), false);
        break;
      case INT64:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong(), false);
        break;
      case INT32:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt(), false);
        break;
      case FLOAT:
        chunkWriter.write(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat(), false);
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  private static List<Path> collectFileSeries(TsFileSequenceReader sequenceReader)
      throws IOException {
    return sequenceReader.getAllPaths();
  }

  public static long collectFileSizes(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    long totalSize = 0;
    for (TsFileResource tsFileResource : seqFiles) {
      totalSize += tsFileResource.getTsFileSize();
    }
    for (TsFileResource tsFileResource : unseqFiles) {
      totalSize += tsFileResource.getTsFileSize();
    }
    return totalSize;
  }

  public static int writeChunkWithoutUnseq(Chunk chunk, IChunkWriter chunkWriter)
      throws IOException {
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    int ptWritten = 0;
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      for (int i = 0; i < batchData.length(); i++) {
        writeBatchPoint(batchData, i, chunkWriter);
      }
      ptWritten += batchData.length();
    }
    return ptWritten;
  }

  public static void writeBatchPoint(BatchData batchData, int i, IChunkWriter chunkWriter) {
    switch (chunkWriter.getDataType()) {
      case TEXT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBinaryByIndex(i), false);
        break;
      case DOUBLE:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), false);
        break;
      case BOOLEAN:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getBooleanByIndex(i), false);
        break;
      case INT64:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getLongByIndex(i), false);
        break;
      case INT32:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getIntByIndex(i), false);
        break;
      case FLOAT:
        chunkWriter.write(batchData.getTimeByIndex(i), batchData.getFloatByIndex(i), false);
        break;
      default:
        throw new UnsupportedOperationException("Unknown data type " + chunkWriter.getDataType());
    }
  }

  // returns totalChunkNum of a file and the max number of chunks of a series
  public static long[] findTotalAndLargestSeriesChunkNum(
      TsFileResource tsFileResource, TsFileSequenceReader sequenceReader) throws IOException {
    long totalChunkNum = 0;
    long maxChunkNum = Long.MIN_VALUE;
    List<Path> paths = collectFileSeries(sequenceReader);

    for (Path path : paths) {
      List<ChunkMetadata> chunkMetadataList = sequenceReader.getChunkMetadataList(path, true);
      totalChunkNum += chunkMetadataList.size();
      maxChunkNum = chunkMetadataList.size() > maxChunkNum ? chunkMetadataList.size() : maxChunkNum;
    }
    logger.debug(
        "In file {}, total chunk num {}, series max chunk num {}",
        tsFileResource,
        totalChunkNum,
        maxChunkNum);
    return new long[] {totalChunkNum, maxChunkNum};
  }

  public static long getFileMetaSize(TsFileResource seqFile, TsFileSequenceReader sequenceReader) {
    return seqFile.getTsFileSize() - sequenceReader.getFileMetadataPos();
  }

  /**
   * Reads chunks of paths in unseqResources and put them in separated lists. When reading a file,
   * this method follows the order of positions of chunks instead of the order of timeseries, which
   * reduce disk seeks.
   *
   * @param paths names of the timeseries
   */
  public static List<Chunk>[] collectUnseqChunks(
      List<PartialPath> paths,
      List<TsFileResource> unseqResources,
      CrossSpaceMergeResource mergeResource)
      throws IOException {
    List<Chunk>[] ret = new List[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      ret[i] = new ArrayList<>();
    }
    PriorityQueue<MetaListEntry> chunkMetaHeap = new PriorityQueue<>();

    for (TsFileResource tsFileResource : unseqResources) {

      TsFileSequenceReader tsFileReader = mergeResource.getFileReader(tsFileResource);
      // prepare metaDataList
      buildMetaHeap(paths, tsFileReader, mergeResource, tsFileResource, chunkMetaHeap);

      // read chunks order by their position
      collectUnseqChunks(chunkMetaHeap, tsFileReader, ret);
    }
    return ret;
  }

  private static void buildMetaHeap(
      List<PartialPath> paths,
      TsFileSequenceReader tsFileReader,
      CrossSpaceMergeResource resource,
      TsFileResource tsFileResource,
      PriorityQueue<MetaListEntry> chunkMetaHeap)
      throws IOException {
    for (int i = 0; i < paths.size(); i++) {
      PartialPath path = paths.get(i);
      List<ChunkMetadata> metaDataList = tsFileReader.getChunkMetadataList(path, true);
      if (metaDataList.isEmpty()) {
        continue;
      }
      List<Modification> pathModifications = resource.getModifications(tsFileResource, path);
      if (!pathModifications.isEmpty()) {
        QueryUtils.modifyChunkMetaData(metaDataList, pathModifications);
      }
      MetaListEntry entry = new MetaListEntry(i, metaDataList);
      if (entry.hasNext()) {
        entry.next();
        chunkMetaHeap.add(entry);
      }
    }
  }

  private static void collectUnseqChunks(
      PriorityQueue<MetaListEntry> chunkMetaHeap,
      TsFileSequenceReader tsFileReader,
      List<Chunk>[] ret)
      throws IOException {
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      ChunkMetadata currMeta = metaListEntry.current();
      Chunk chunk = tsFileReader.readMemChunk(currMeta);
      ret[metaListEntry.pathId].add(chunk);
      if (metaListEntry.hasNext()) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      }
    }
  }

  public static boolean isChunkOverflowed(TimeValuePair timeValuePair, ChunkMetadata metaData) {
    return timeValuePair != null && timeValuePair.getTimestamp() <= metaData.getEndTime();
  }

  public static boolean isChunkTooSmall(
      int ptWritten, ChunkMetadata chunkMetaData, boolean isLastChunk, int minChunkPointNum) {
    return ptWritten > 0
        || (minChunkPointNum >= 0
            && chunkMetaData.getNumOfPoints() < minChunkPointNum
            && !isLastChunk);
  }

  public static List<List<PartialPath>> splitPathsByDevice(List<PartialPath> paths) {
    if (paths.isEmpty()) {
      return Collections.emptyList();
    }
    paths.sort(Comparator.comparing(PartialPath::getFullPath));

    String currDevice = null;
    List<PartialPath> currList = null;
    List<List<PartialPath>> ret = new ArrayList<>();
    for (PartialPath path : paths) {
      if (currDevice == null) {
        currDevice = path.getDevice();
        currList = new ArrayList<>();
        currList.add(path);
      } else if (path.getDevice().equals(currDevice)) {
        currList.add(path);
      } else {
        ret.add(currList);
        currDevice = path.getDevice();
        currList = new ArrayList<>();
        currList.add(path);
      }
    }
    ret.add(currList);
    return ret;
  }

  public static class MetaListEntry implements Comparable<MetaListEntry> {

    private int pathId;
    private int listIdx;
    private List<ChunkMetadata> chunkMetadataList;

    public MetaListEntry(int pathId, List<ChunkMetadata> chunkMetadataList) {
      this.pathId = pathId;
      this.listIdx = -1;
      this.chunkMetadataList = chunkMetadataList;
    }

    @Override
    public int compareTo(MetaListEntry o) {
      return Long.compare(
          this.current().getOffsetOfChunkHeader(), o.current().getOffsetOfChunkHeader());
    }

    public ChunkMetadata current() {
      return chunkMetadataList.get(listIdx);
    }

    public boolean hasNext() {
      return listIdx + 1 < chunkMetadataList.size();
    }

    public ChunkMetadata next() {
      return chunkMetadataList.get(++listIdx);
    }

    public int getPathId() {
      return pathId;
    }
  }
}
