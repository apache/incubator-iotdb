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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.utils.CompactionUtils;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.junit.Assert.assertEquals;

public class CompactionChunkTest extends LevelCompactionTest {

  File tempSGDir;

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testAppendMerge() throws IOException, IllegalPathException {
    Map<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> measurementChunkMetadataMap =
        new HashMap<>();
    List<TsFileResource> sourceTsfileResources = seqResources.subList(0, 2);
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource targetTsfileResource = new TsFileResource(file);
    RateLimiter compactionWriteRateLimiter = MergeManager.getINSTANCE().getMergeWriteRateLimiter();
    String device = COMPACTION_TEST_SG + PATH_SEPARATOR + "device0";
    RestorableTsFileIOWriter writer =
        new RestorableTsFileIOWriter(targetTsfileResource.getTsFile());
    writer.startChunkGroup(device);
    for (TsFileResource tsFileResource : sourceTsfileResources) {
      TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFile());
      Map<String, List<ChunkMetadata>> chunkMetadataMap = reader.readChunkMetadataInDevice(device);
      for (Entry<String, List<ChunkMetadata>> entry : chunkMetadataMap.entrySet()) {
        for (ChunkMetadata chunkMetadata : entry.getValue()) {
          Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap;
          String measurementUid = chunkMetadata.getMeasurementUid();
          if (measurementChunkMetadataMap.containsKey(measurementUid)) {
            readerChunkMetadataMap = measurementChunkMetadataMap.get(measurementUid);
          } else {
            readerChunkMetadataMap = new LinkedHashMap<>();
          }
          List<ChunkMetadata> chunkMetadataList;
          if (readerChunkMetadataMap.containsKey(reader)) {
            chunkMetadataList = readerChunkMetadataMap.get(reader);
          } else {
            chunkMetadataList = new ArrayList<>();
          }
          chunkMetadataList.add(chunkMetadata);
          readerChunkMetadataMap.put(reader, chunkMetadataList);
          measurementChunkMetadataMap.put(
              chunkMetadata.getMeasurementUid(), readerChunkMetadataMap);
        }
      }
      for (Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry :
          measurementChunkMetadataMap.entrySet()) {
        CompactionUtils.writeByAppendPageMerge(
            device, compactionWriteRateLimiter, entry, targetTsfileResource, writer);
      }
      reader.close();
    }
    writer.endChunkGroup();
    targetTsfileResource.serialize();
    writer.endFile();
    targetTsfileResource.close();

    TsFileSequenceReader reader = new TsFileSequenceReader(file);
    List<Path> paths = reader.getAllPaths();
    for (Path path : paths) {
      List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        Chunk chunk = reader.readMemChunk(chunkMetadata);
        IChunkReader chunkReader = new ChunkReaderByTimestamp(chunk);
        long totalPointCount = 0;
        while (chunkReader.hasNextSatisfiedPage()) {
          BatchData batchData = chunkReader.nextPageData();
          for (int i = 0; i < batchData.length(); i++) {
            assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
          }
          totalPointCount += batchData.length();
        }
        assertEquals(totalPointCount, chunkMetadata.getNumOfPoints());
      }
    }
    reader.close();
  }

  @Test
  public void testDeserializeMerge() throws IOException, IllegalPathException {
    Map<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> measurementChunkMetadataMap =
        new HashMap<>();
    List<TsFileResource> sourceTsfileResources = seqResources.subList(0, 2);
    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 1
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource targetTsfileResource = new TsFileResource(file);
    RateLimiter compactionWriteRateLimiter = MergeManager.getINSTANCE().getMergeWriteRateLimiter();
    String device = COMPACTION_TEST_SG + PATH_SEPARATOR + "device0";
    RestorableTsFileIOWriter writer =
        new RestorableTsFileIOWriter(targetTsfileResource.getTsFile());
    writer.startChunkGroup(device);
    for (TsFileResource tsFileResource : sourceTsfileResources) {
      TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFile());
      Map<String, List<ChunkMetadata>> chunkMetadataMap = reader.readChunkMetadataInDevice(device);
      for (Entry<String, List<ChunkMetadata>> entry : chunkMetadataMap.entrySet()) {
        for (ChunkMetadata chunkMetadata : entry.getValue()) {
          Map<TsFileSequenceReader, List<ChunkMetadata>> readerChunkMetadataMap;
          String measurementUid = chunkMetadata.getMeasurementUid();
          if (measurementChunkMetadataMap.containsKey(measurementUid)) {
            readerChunkMetadataMap = measurementChunkMetadataMap.get(measurementUid);
          } else {
            readerChunkMetadataMap = new LinkedHashMap<>();
          }
          List<ChunkMetadata> chunkMetadataList;
          if (readerChunkMetadataMap.containsKey(reader)) {
            chunkMetadataList = readerChunkMetadataMap.get(reader);
          } else {
            chunkMetadataList = new ArrayList<>();
          }
          chunkMetadataList.add(chunkMetadata);
          readerChunkMetadataMap.put(reader, chunkMetadataList);
          measurementChunkMetadataMap.put(
              chunkMetadata.getMeasurementUid(), readerChunkMetadataMap);
        }
      }
      for (Entry<String, Map<TsFileSequenceReader, List<ChunkMetadata>>> entry :
          measurementChunkMetadataMap.entrySet()) {
        CompactionUtils.writeByDeserializePageMerge(
            device,
            compactionWriteRateLimiter,
            entry,
            targetTsfileResource,
            writer,
            new HashMap<>(),
            new ArrayList<>());
      }
      reader.close();
    }
    writer.endChunkGroup();
    targetTsfileResource.serialize();
    writer.endFile();
    targetTsfileResource.close();

    TsFileSequenceReader reader = new TsFileSequenceReader(file);
    List<Path> paths = reader.getAllPaths();
    for (Path path : paths) {
      List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(path);
      for (ChunkMetadata chunkMetadata : chunkMetadataList) {
        Chunk chunk = reader.readMemChunk(chunkMetadata);
        IChunkReader chunkReader = new ChunkReaderByTimestamp(chunk);
        long totalPointCount = 0;
        while (chunkReader.hasNextSatisfiedPage()) {
          BatchData batchData = chunkReader.nextPageData();
          for (int i = 0; i < batchData.length(); i++) {
            assertEquals(batchData.getTimeByIndex(i), batchData.getDoubleByIndex(i), 0.001);
          }
          totalPointCount += batchData.length();
        }
        assertEquals(totalPointCount, chunkMetadata.getNumOfPoints());
      }
    }
    reader.close();
  }
}
