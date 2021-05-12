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

package org.apache.iotdb.db.engine.merge.manage;

import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.reader.resource.CachedUnseqResourceMergeReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.FSUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.engine.merge.task.MergeTask.MERGE_SUFFIX;

/**
 * MergeResource manages files and caches of readers, writers, MeasurementSchemas and modifications
 * to avoid unnecessary object creations and file openings.
 */
public class MergeResource {

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;

  private Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();
  private Map<TsFileResource, RestorableTsFileIOWriter> fileWriterCache = new HashMap<>();
  private Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();
  private Map<TsFileResource, Map<String, Pair<Long, Long>>> startEndTimeCache =
      new HashMap<>(); // pair<startTime, endTime>
  private Map<PartialPath, IMeasurementSchema> measurementSchemaMap =
      new HashMap<>(); // is this too waste?
  private Map<IMeasurementSchema, IChunkWriter> chunkWriterCache = new ConcurrentHashMap<>();

  private long timeLowerBound = Long.MIN_VALUE;

  private boolean cacheDeviceMeta = false;

  public MergeResource(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles.stream().filter(this::filterResource).collect(Collectors.toList());
    this.unseqFiles = unseqFiles.stream().filter(this::filterResource).collect(Collectors.toList());
  }

  /** If returns true, it means to participate in the merge */
  private boolean filterResource(TsFileResource res) {
    return res.getTsFile().exists()
        && !res.isDeleted()
        && (!res.isClosed() || res.stillLives(timeLowerBound));
  }

  public MergeResource(
      Collection<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, long timeLowerBound) {
    this.timeLowerBound = timeLowerBound;
    this.seqFiles = seqFiles.stream().filter(this::filterResource).collect(Collectors.toList());
    this.unseqFiles = unseqFiles.stream().filter(this::filterResource).collect(Collectors.toList());
  }

  public void clear() throws IOException {
    for (TsFileSequenceReader sequenceReader : fileReaderCache.values()) {
      sequenceReader.close();
    }
    for (RestorableTsFileIOWriter writer : fileWriterCache.values()) {
      writer.close();
    }

    fileReaderCache.clear();
    fileWriterCache.clear();
    modificationCache.clear();
    measurementSchemaMap.clear();
    chunkWriterCache.clear();
  }

  public IMeasurementSchema getSchema(PartialPath path) {
    return measurementSchemaMap.get(path);
  }

  /**
   * Construct a new or get an existing RestorableTsFileIOWriter of a merge temp file for a SeqFile.
   * The path of the merge temp file will be the seqFile's + ".merge".
   *
   * @return A RestorableTsFileIOWriter of a merge temp file for a SeqFile.
   */
  public RestorableTsFileIOWriter getMergeFileWriter(TsFileResource resource) throws IOException {
    RestorableTsFileIOWriter writer = fileWriterCache.get(resource);
    if (writer == null) {
      writer =
          new RestorableTsFileIOWriter(
              FSFactoryProducer.getFSFactory(FSUtils.getFSType(resource.getTsFile()))
                  .getFile(resource.getTsFilePath() + MERGE_SUFFIX));
      fileWriterCache.put(resource, writer);
    }
    return writer;
  }

  /**
   * Query ChunkMetadata of a timeseries from the given TsFile (seq or unseq). The ChunkMetadata is
   * not cached since it is usually huge.
   *
   * @param path name of the time series
   */
  public List<ChunkMetadata> queryChunkMetadata(PartialPath path, TsFileResource seqFile)
      throws IOException {
    TsFileSequenceReader sequenceReader = getFileReader(seqFile);
    return sequenceReader.getChunkMetadataList(path, true);
  }

  /**
   * Construct the a new or get an existing TsFileSequenceReader of a TsFile.
   *
   * @return a TsFileSequenceReader
   */
  public TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getTsFile(), true, cacheDeviceMeta);
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  /**
   * Construct UnseqResourceMergeReaders of for each timeseries over all seqFiles. The readers are
   * not cached since the method is only called once for each timeseries.
   *
   * @param paths names of the timeseries
   * @return an array of UnseqResourceMergeReaders each corresponding to a timeseries in paths
   */
  public IPointReader[] getUnseqReaders(List<PartialPath> paths) throws IOException {
    List<Chunk>[] pathChunks = MergeUtils.collectUnseqChunks(paths, unseqFiles, this);
    IPointReader[] ret = new IPointReader[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      TSDataType dataType = getSchema(paths.get(i)).getType();
      ret[i] = new CachedUnseqResourceMergeReader(pathChunks[i], dataType);
    }
    return ret;
  }

  /**
   * Construct the a new or get an existing ChunkWriter of a measurement. Different timeseries of
   * the same measurement and data type shares the same instance.
   */
  public IChunkWriter getChunkWriter(IMeasurementSchema measurementSchema) {
    return chunkWriterCache.computeIfAbsent(measurementSchema, ChunkWriterImpl::new);
  }

  /**
   * Get the modifications of a timeseries in the ModificationFile of a TsFile.
   *
   * @param path name of the time series
   */
  public List<Modification> getModifications(TsFileResource tsFileResource, PartialPath path) {
    // copy from TsFileResource so queries are not affected
    List<Modification> modifications =
        modificationCache.computeIfAbsent(
            tsFileResource, resource -> new LinkedList<>(resource.getModFile().getModifications()));
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().matchFullPath(path)) {
        pathModifications.add(modification);
      }
    }
    return pathModifications;
  }

  /**
   * Remove and close the writer of the merge temp file of a SeqFile. The merge temp file is also
   * deleted.
   *
   * @param tsFileResource the SeqFile
   */
  public void removeFileAndWriter(TsFileResource tsFileResource) throws IOException {
    RestorableTsFileIOWriter newFileWriter = fileWriterCache.remove(tsFileResource);
    if (newFileWriter != null) {
      newFileWriter.close();
      newFileWriter.getFile().delete();
    }
  }

  /**
   * Remove and close the reader of the TsFile. The TsFile is NOT deleted.
   *
   * @param resource the SeqFile
   */
  public void removeFileReader(TsFileResource resource) throws IOException {
    TsFileSequenceReader sequenceReader = fileReaderCache.remove(resource);
    if (sequenceReader != null) {
      sequenceReader.close();
    }
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  public void setSeqFiles(List<TsFileResource> seqFiles) {
    this.seqFiles = seqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public void setUnseqFiles(List<TsFileResource> unseqFiles) {
    this.unseqFiles = unseqFiles;
  }

  public void removeOutdatedSeqReaders() throws IOException {
    Iterator<Entry<TsFileResource, TsFileSequenceReader>> entryIterator =
        fileReaderCache.entrySet().iterator();
    while (entryIterator.hasNext()) {
      Entry<TsFileResource, TsFileSequenceReader> entry = entryIterator.next();
      TsFileResource tsFile = entry.getKey();
      if (!seqFiles.contains(tsFile)) {
        TsFileSequenceReader reader = entry.getValue();
        reader.close();
        entryIterator.remove();
      }
    }
  }

  public void setCacheDeviceMeta(boolean cacheDeviceMeta) {
    this.cacheDeviceMeta = cacheDeviceMeta;
  }

  public void setMeasurementSchemaMap(Map<PartialPath, IMeasurementSchema> measurementSchemaMap) {
    this.measurementSchemaMap = measurementSchemaMap;
  }

  public void clearChunkWriterCache() {
    this.chunkWriterCache.clear();
  }

  public void updateStartTime(TsFileResource tsFileResource, String device, long startTime) {
    Map<String, Pair<Long, Long>> deviceStartEndTimePairMap =
        startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
    Pair<Long, Long> startEndTimePair =
        deviceStartEndTimePairMap.getOrDefault(device, new Pair<>(Long.MAX_VALUE, Long.MIN_VALUE));
    long newStartTime = startEndTimePair.left > startTime ? startTime : startEndTimePair.left;
    deviceStartEndTimePairMap.put(device, new Pair<>(newStartTime, startEndTimePair.right));
    startEndTimeCache.put(tsFileResource, deviceStartEndTimePairMap);
  }

  public void updateEndTime(TsFileResource tsFileResource, String device, long endTime) {
    Map<String, Pair<Long, Long>> deviceStartEndTimePairMap =
        startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
    Pair<Long, Long> startEndTimePair =
        deviceStartEndTimePairMap.getOrDefault(device, new Pair<>(Long.MAX_VALUE, Long.MIN_VALUE));
    long newEndTime = startEndTimePair.right < endTime ? endTime : startEndTimePair.right;
    deviceStartEndTimePairMap.put(device, new Pair<>(startEndTimePair.left, newEndTime));
    startEndTimeCache.put(tsFileResource, deviceStartEndTimePairMap);
  }

  public Map<String, Pair<Long, Long>> getStartEndTime(TsFileResource tsFileResource) {
    return startEndTimeCache.getOrDefault(tsFileResource, new HashMap<>());
  }
}
