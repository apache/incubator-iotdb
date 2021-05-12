/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.FileGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsFileSequenceReaderTest {

  private static final String FILE_PATH = FileGenerator.outputDataFile;
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private ReadOnlyTsFile tsFile;

  @Before
  public void before() throws IOException {
    int rowCount = 100;
    FileGenerator.generateFile(rowCount, 10000);
    TsFileSequenceReader fileReader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH));
    tsFile = new ReadOnlyTsFile(fileReader);
  }

  @After
  public void after() throws IOException {
    tsFile.close();
    FileGenerator.after();
  }

  @Test
  public void testReadTsFileSequentially() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH));
    reader.position(TSFileConfig.MAGIC_STRING.getBytes().length + 1);
    Map<String, List<Pair<Long, Long>>> deviceChunkGroupMetadataOffsets = new HashMap<>();

    long startOffset = reader.position();
    byte marker;
    while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
      switch (marker) {
        case MetaMarker.CHUNK_HEADER:
        case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          ChunkHeader header = reader.readChunkHeader(marker);
          int dataSize = header.getDataSize();
          while (dataSize > 0) {
            PageHeader pageHeader =
                reader.readPageHeader(
                    header.getDataType(), header.getChunkType() == MetaMarker.CHUNK_HEADER);
            ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
            dataSize -= pageHeader.getSerializedPageSize();
          }
          break;
        case MetaMarker.CHUNK_GROUP_HEADER:
          ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
          long endOffset = reader.position();
          Pair<Long, Long> pair = new Pair<>(startOffset, endOffset);
          deviceChunkGroupMetadataOffsets.putIfAbsent(
              chunkGroupHeader.getDeviceID(), new ArrayList<>());
          List<Pair<Long, Long>> metadatas =
              deviceChunkGroupMetadataOffsets.get(chunkGroupHeader.getDeviceID());
          metadatas.add(pair);
          startOffset = endOffset;
          break;
        case MetaMarker.OPERATION_INDEX_RANGE:
          reader.readPlanIndex();
          break;
        default:
          MetaMarker.handleUnexpectedMarker(marker);
      }
    }
    reader.close();
  }

  @Test
  public void testReadChunkMetadataInDevice() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_PATH));

    // test for exist device "d2"
    Map<String, List<ChunkMetadata>> chunkMetadataMap = reader.readChunkMetadataInDevice("d2");
    int[] res = new int[] {20, 75, 100, 13};

    Assert.assertEquals(4, chunkMetadataMap.size());
    for (int i = 0; i < chunkMetadataMap.size(); i++) {
      int id = i + 1;
      List<ChunkMetadata> metadataList = chunkMetadataMap.get("s" + id);
      int numOfPoints = 0;
      for (ChunkMetadata metadata : metadataList) {
        numOfPoints += metadata.getNumOfPoints();
      }
      Assert.assertEquals(res[i], numOfPoints);
    }

    // test for non-exist device "d3"
    Assert.assertTrue(reader.readChunkMetadataInDevice("d3").isEmpty());
    reader.close();
  }
}
