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
package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.TimeSeriesMetadataTest;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.utils.TestHelper;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TsFileIOWriterTest {

  private static String tsfile = TestConstant.BASE_OUTPUT_PATH.concat("tsfileIOWriterTest.tsfile");
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);
  private static String deviceId = "device1";

  @Before
  public void before() throws IOException {
    TsFileIOWriter writer = new TsFileIOWriter(fsFactory.getFile(tsfile));

    // file schema
    MeasurementSchema measurementSchema = TestHelper.createSimpleMeasurementSchema("sensor01");
    Schema schema = new Schema();
    schema.registerTimeseries(new Path(deviceId, "sensor01"), measurementSchema);

    // chunk statistics
    Statistics statistics = Statistics.getStatsByType(measurementSchema.getType());
    statistics.updateStats(0L, 0L);

    // chunk group 1
    writer.startChunkGroup(deviceId);
    writer.startFlushChunk(
        measurementSchema.getMeasurementId(),
        measurementSchema.getCompressor(),
        measurementSchema.getType(),
        measurementSchema.getEncodingType(),
        statistics,
        0,
        0,
        0);
    writer.endCurrentChunk();
    writer.endChunkGroup();

    writer.setMinPlanIndex(100);
    writer.setMaxPlanIndex(10000);
    writer.writePlanIndices();
    // end file
    writer.endFile();
  }

  @After
  public void after() {
    File file = fsFactory.getFile(tsfile);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void endFileTest() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(tsfile));

    // magic_string
    Assert.assertEquals(TSFileConfig.MAGIC_STRING, reader.readHeadMagic());
    Assert.assertEquals(TSFileConfig.VERSION_NUMBER, reader.readVersionNumber());
    Assert.assertEquals(TSFileConfig.MAGIC_STRING, reader.readTailMagic());

    reader.position(TSFileConfig.MAGIC_STRING.getBytes().length + 1);

    // chunk group header
    Assert.assertEquals(MetaMarker.CHUNK_GROUP_HEADER, reader.readMarker());
    ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
    Assert.assertEquals(deviceId, chunkGroupHeader.getDeviceID());

    // chunk header
    Assert.assertEquals(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, reader.readMarker());
    ChunkHeader header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
    Assert.assertEquals(TimeSeriesMetadataTest.measurementUID, header.getMeasurementID());

    Assert.assertEquals(MetaMarker.OPERATION_INDEX_RANGE, reader.readMarker());
    reader.readPlanIndex();
    Assert.assertEquals(100, reader.getMinPlanIndex());
    Assert.assertEquals(10000, reader.getMaxPlanIndex());

    Assert.assertEquals(MetaMarker.SEPARATOR, reader.readMarker());

    // FileMetaData
    TsFileMetadata metaData = reader.readFileMetadata();
    Assert.assertEquals(1, metaData.getMetadataIndex().getChildren().size());
  }
}
