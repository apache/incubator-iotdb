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

package org.apache.iotdb.db.engine.compaction.utils;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.BatchDataIterator;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.utils.FSUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

public class CompactionUtils {

  private static final Logger logger = LoggerFactory.getLogger(CompactionUtils.class);

  private CompactionUtils() {
    throw new IllegalStateException("Utility class");
  }

  private static Pair<ChunkMetadata, Chunk> readByAppendPageMerge(
      Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap) throws IOException {
    ChunkMetadata newChunkMetadata = null;
    Chunk newChunk = null;
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> entry :
        readerChunkMetadataMap.entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        Chunk chunk = reader.readMemChunk(chunkMetadata);
        if (newChunkMetadata == null) {
          newChunkMetadata = chunkMetadata;
          newChunk = chunk;
        } else {
          newChunk.mergeChunk(chunk);
          newChunkMetadata.mergeChunkMetadata(chunkMetadata);
        }
      }
    }
    return new Pair<>(newChunkMetadata, newChunk);
  }

  private static void readByDeserializePageMerge(
      Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap,
      Map<Long, TimeValuePair> timeValuePairMap,
      Map<String, List<Modification>> modificationCache,
      PartialPath seriesPath,
      List<Modification> modifications)
      throws IOException {
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> entry :
        readerChunkMetadataMap.entrySet()) {
      TsFileSequenceReader reader = entry.getKey();
      List<ChunkMetadata> chunkMetadataList = entry.getValue();
      modifyChunkMetaDataWithCache(
          reader, chunkMetadataList, modificationCache, seriesPath, modifications);
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        IChunkReader chunkReader = new ChunkReaderByTimestamp(reader.readMemChunk(chunkMetadata));
        while (chunkReader.hasNextSatisfiedPage()) {
          IPointReader iPointReader = new BatchDataIterator(chunkReader.nextPageData());
          while (iPointReader.hasNextTimeValuePair()) {
            TimeValuePair timeValuePair = iPointReader.nextTimeValuePair();
            timeValuePairMap.put(timeValuePair.getTimestamp(), timeValuePair);
          }
        }
      }
    }
  }

  /**
   * When chunk is large enough, we do not have to merge them any more. Just read chunks and write
   * them to the new file directly.
   */
  public static void writeByAppendChunkMerge(
      String device,
      RateLimiter compactionWriteRateLimiter,
      Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry,
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer)
      throws IOException {
    Map<TsFileSequenceReader, List<ChunkMetadata>> readerListMap = entry.getValue();
    for (Entry<TsFileSequenceReader, List<ChunkMetadata>> readerListEntry :
        readerListMap.entrySet()) {
      TsFileSequenceReader reader = readerListEntry.getKey();
      List<ChunkMetadata> chunkMetadataList = readerListEntry.getValue();
      // read chunk and write it to new file directly
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        Chunk chunk = reader.readMemChunk(chunkMetadata);
        MergeManager.mergeRateLimiterAcquire(
            compactionWriteRateLimiter,
            (long) chunk.getHeader().getDataSize() + chunk.getData().position());
        writer.writeChunk(chunk, chunkMetadata);
        targetResource.updateStartTime(device, chunkMetadata.getStartTime());
        targetResource.updateEndTime(device, chunkMetadata.getEndTime());
      }
    }
  }

  public static void writeByAppendPageMerge(
      String device,
      RateLimiter compactionWriteRateLimiter,
      Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry,
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer)
      throws IOException {
    Pair<ChunkMetadata, Chunk> chunkPair = readByAppendPageMerge(entry.getValue());
    ChunkMetadata newChunkMetadata = chunkPair.left;
    Chunk newChunk = chunkPair.right;
    if (newChunkMetadata != null && newChunk != null) {
      // wait for limit write
      MergeManager.mergeRateLimiterAcquire(
          compactionWriteRateLimiter,
          (long) newChunk.getHeader().getDataSize() + newChunk.getData().position());
      writer.writeChunk(newChunk, newChunkMetadata);
      targetResource.updateStartTime(device, newChunkMetadata.getStartTime());
      targetResource.updateEndTime(device, newChunkMetadata.getEndTime());
    }
  }

  public static void writeByDeserializePageMerge(
      String device,
      RateLimiter compactionRateLimiter,
      Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry,
      TsFileResource targetResource,
      RestorableTsFileIOWriter writer,
      Map<String, List<Modification>> modificationCache,
      List<Modification> modifications)
      throws IOException, IllegalPathException {
    Map<Long, TimeValuePair> timeValuePairMap = new TreeMap<>();
    Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap = entry.getValue();
    readByDeserializePageMerge(
        readerChunkMetadataMap,
        timeValuePairMap,
        modificationCache,
        new PartialPath(device, entry.getKey()),
        modifications);
    boolean isChunkMetadataEmpty = true;
    for (List<ChunkMetadata> chunkMetadataList : readerChunkMetadataMap.values()) {
      if (!chunkMetadataList.isEmpty()) {
        isChunkMetadataEmpty = false;
        break;
      }
    }
    if (isChunkMetadataEmpty) {
      return;
    }
    IChunkWriter chunkWriter;
    try {
      chunkWriter =
          new ChunkWriterImpl(
              IoTDB.metaManager.getSeriesSchema(new PartialPath(device), entry.getKey()), true);
    } catch (MetadataException e) {
      // this may caused in IT by restart
      logger.error("{} get schema {} error, skip this sensor", device, entry.getKey(), e);
      return;
    }
    for (TimeValuePair timeValuePair : timeValuePairMap.values()) {
      writeTVPair(timeValuePair, chunkWriter);
      targetResource.updateStartTime(device, timeValuePair.getTimestamp());
      targetResource.updateEndTime(device, timeValuePair.getTimestamp());
    }
    // wait for limit write
    MergeManager.mergeRateLimiterAcquire(compactionRateLimiter, chunkWriter.getCurrentChunkSize());
    chunkWriter.writeToFileWriter(writer);
  }

  private static Set<String> getTsFileDevicesSet(
      List<TsFileResource> subLevelResources,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap,
      String storageGroup)
      throws IOException {
    Set<String> tsFileDevicesSet = new HashSet<>();
    for (TsFileResource levelResource : subLevelResources) {
      TsFileSequenceReader reader =
          buildReaderFromTsFileResource(levelResource, tsFileSequenceReaderMap, storageGroup);
      if (reader == null) {
        continue;
      }
      tsFileDevicesSet.addAll(reader.getAllDevices());
    }
    return tsFileDevicesSet;
  }

  private static boolean hasNextChunkMetadataList(
      Collection<Iterator<Map<String, List<ChunkMetadata>>>> iteratorSet) {
    boolean hasNextChunkMetadataList = false;
    for (Iterator<Map<String, List<ChunkMetadata>>> iterator : iteratorSet) {
      hasNextChunkMetadataList = hasNextChunkMetadataList || iterator.hasNext();
    }
    return hasNextChunkMetadataList;
  }

  /**
   * @param targetResource the target resource to be merged to
   * @param tsFileResources the source resource to be merged
   * @param storageGroup the storage group name
   * @param compactionLogger the logger
   * @param devices the devices to be skipped(used by recover)
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static void merge(
      TsFileResource targetResource,
      List<TsFileResource> tsFileResources,
      String storageGroup,
      CompactionLogger compactionLogger,
      Set<String> devices,
      boolean sequence,
      List<Modification> modifications)
      throws IOException, IllegalPathException {
    RestorableTsFileIOWriter writer = new RestorableTsFileIOWriter(targetResource.getTsFile());
    Map<String, TsFileSequenceReader> tsFileSequenceReaderMap = new HashMap<>();
    Map<String, List<Modification>> modificationCache = new HashMap<>();
    RateLimiter compactionWriteRateLimiter = MergeManager.getINSTANCE().getMergeWriteRateLimiter();
    Set<String> tsFileDevicesMap =
        getTsFileDevicesSet(tsFileResources, tsFileSequenceReaderMap, storageGroup);
    for (String device : tsFileDevicesMap) {
      if (devices.contains(device)) {
        continue;
      }
      writer.startChunkGroup(device);
      Map<TsFileSequenceReader, Map<String, List<ChunkMetadata>>> chunkMetadataListCacheForMerge =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManagement.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));
      Map<TsFileSequenceReader, Iterator<Map<String, List<ChunkMetadata>>>>
          chunkMetadataListIteratorCache =
              new TreeMap<>(
                  (o1, o2) ->
                      TsFileManagement.compareFileName(
                          new File(o1.getFileName()), new File(o2.getFileName())));
      for (TsFileResource tsFileResource : tsFileResources) {
        TsFileSequenceReader reader =
            buildReaderFromTsFileResource(tsFileResource, tsFileSequenceReaderMap, storageGroup);
        if (reader == null) {
          throw new IOException();
        }
        Iterator<Map<String, List<ChunkMetadata>>> iterator =
            reader.getMeasurementChunkMetadataListMapIterator(device);
        chunkMetadataListIteratorCache.put(reader, iterator);
        chunkMetadataListCacheForMerge.put(reader, new TreeMap<>());
      }
      while (hasNextChunkMetadataList(chunkMetadataListIteratorCache.values())) {
        String lastSensor = null;
        Set<String> allSensors = new HashSet<>();
        for (Entry<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
            chunkMetadataListCacheForMergeEntry : chunkMetadataListCacheForMerge.entrySet()) {
          TsFileSequenceReader reader = chunkMetadataListCacheForMergeEntry.getKey();
          Map<String, List<ChunkMetadata>> sensorChunkMetadataListMap =
              chunkMetadataListCacheForMergeEntry.getValue();
          if (sensorChunkMetadataListMap.size() <= 0) {
            if (chunkMetadataListIteratorCache.get(reader).hasNext()) {
              sensorChunkMetadataListMap = chunkMetadataListIteratorCache.get(reader).next();
              chunkMetadataListCacheForMerge.put(reader, sensorChunkMetadataListMap);
            } else {
              continue;
            }
          }
          // get the min last sensor in the current chunkMetadata cache list for merge
          String maxSensor = Collections.max(sensorChunkMetadataListMap.keySet());
          if (lastSensor == null) {
            lastSensor = maxSensor;
          } else {
            if (maxSensor.compareTo(lastSensor) < 0) {
              lastSensor = maxSensor;
            }
          }
          // get all sensor used later
          allSensors.addAll(sensorChunkMetadataListMap.keySet());
        }

        for (String sensor : allSensors) {
          if (sensor.compareTo(lastSensor) <= 0) {
            Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataListMap =
                new TreeMap<>(
                    (o1, o2) ->
                        TsFileManagement.compareFileName(
                            new File(o1.getFileName()), new File(o2.getFileName())));
            // find all chunkMetadata of a sensor
            for (Entry<TsFileSequenceReader, Map<String, List<ChunkMetadata>>>
                chunkMetadataListCacheForMergeEntry : chunkMetadataListCacheForMerge.entrySet()) {
              TsFileSequenceReader reader = chunkMetadataListCacheForMergeEntry.getKey();
              Map<String, List<ChunkMetadata>> sensorChunkMetadataListMap =
                  chunkMetadataListCacheForMergeEntry.getValue();
              if (sensorChunkMetadataListMap.containsKey(sensor)) {
                readerChunkMetadataListMap.put(reader, sensorChunkMetadataListMap.get(sensor));
                sensorChunkMetadataListMap.remove(sensor);
              }
            }
            Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>>
                sensorReaderChunkMetadataListEntry =
                    new DefaultMapEntry<>(sensor, readerChunkMetadataListMap);
            if (!sequence) {
              writeByDeserializePageMerge(
                  device,
                  compactionWriteRateLimiter,
                  sensorReaderChunkMetadataListEntry,
                  targetResource,
                  writer,
                  modificationCache,
                  modifications);
            } else {
              boolean isChunkEnoughLarge = true;
              boolean isPageEnoughLarge = true;
              for (List<ChunkMetadata> chunkMetadatas : readerChunkMetadataListMap.values()) {
                for (ChunkMetadata chunkMetadata : chunkMetadatas) {
                  if (chunkMetadata.getNumOfPoints()
                      < IoTDBDescriptor.getInstance()
                          .getConfig()
                          .getMergePagePointNumberThreshold()) {
                    isPageEnoughLarge = false;
                  }
                  if (chunkMetadata.getNumOfPoints()
                      < IoTDBDescriptor.getInstance()
                          .getConfig()
                          .getMergeChunkPointNumberThreshold()) {
                    isChunkEnoughLarge = false;
                  }
                }
              }
              // if a chunk is large enough, it's page must be large enough too
              if (isChunkEnoughLarge) {
                logger.debug(
                    "{} [Compaction] chunk enough large, use append chunk merge", storageGroup);
                // append page in chunks, so we do not have to deserialize a chunk
                writeByAppendChunkMerge(
                    device,
                    compactionWriteRateLimiter,
                    sensorReaderChunkMetadataListEntry,
                    targetResource,
                    writer);
              } else if (isPageEnoughLarge) {
                logger.debug(
                    "{} [Compaction] page enough large, use append page merge", storageGroup);
                // append page in chunks, so we do not have to deserialize a chunk
                writeByAppendPageMerge(
                    device,
                    compactionWriteRateLimiter,
                    sensorReaderChunkMetadataListEntry,
                    targetResource,
                    writer);
              } else {
                logger.debug(
                    "{} [Compaction] page too small, use deserialize page merge", storageGroup);
                // we have to deserialize chunks to merge pages
                writeByDeserializePageMerge(
                    device,
                    compactionWriteRateLimiter,
                    sensorReaderChunkMetadataListEntry,
                    targetResource,
                    writer,
                    modificationCache,
                    modifications);
              }
            }
          }
        }
      }
      writer.endChunkGroup();
      if (compactionLogger != null) {
        compactionLogger.logDevice(device, writer.getPos());
      }
    }

    for (TsFileSequenceReader reader : tsFileSequenceReaderMap.values()) {
      reader.close();
    }

    for (TsFileResource tsFileResource : tsFileResources) {
      targetResource.updatePlanIndexes(tsFileResource);
    }
    targetResource.serialize();
    writer.endFile();
    targetResource.close();
  }

  private static TsFileSequenceReader buildReaderFromTsFileResource(
      TsFileResource levelResource,
      Map<String, TsFileSequenceReader> tsFileSequenceReaderMap,
      String storageGroup) {
    return tsFileSequenceReaderMap.computeIfAbsent(
        levelResource.getTsFile().getAbsolutePath(),
        path -> {
          try {
            if (levelResource.getTsFile().exists()) {
              return new TsFileSequenceReader(levelResource.getTsFile());
            } else {
              logger.info("{} tsfile does not exist", path);
              return null;
            }
          } catch (IOException e) {
            logger.error(
                "Storage group {}, flush recover meets error. reader create failed.",
                storageGroup,
                e);
            return null;
          }
        });
  }

  private static void modifyChunkMetaDataWithCache(
      TsFileSequenceReader reader,
      List<ChunkMetadata> chunkMetadataList,
      Map<String, List<Modification>> modificationCache,
      PartialPath seriesPath,
      List<Modification> usedModifications) {
    List<Modification> modifications =
        modificationCache.computeIfAbsent(
            reader.getFileName(),
            fileName ->
                new LinkedList<>(
                    new ModificationFile(
                            FSFactoryProducer.getFSFactory(FSUtils.getFSType(reader.getFile()))
                                .getFile(fileName + ModificationFile.FILE_SUFFIX))
                        .getModifications()));
    List<Modification> seriesModifications = new LinkedList<>();
    for (Modification modification : modifications) {
      if (modification.getPath().matchFullPath(seriesPath)) {
        seriesModifications.add(modification);
        usedModifications.add(modification);
      }
    }
    modifyChunkMetaData(chunkMetadataList, seriesModifications);
  }
}
