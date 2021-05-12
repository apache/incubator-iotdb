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

package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.exception.NotCompatibleTsFileException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileCheckStatus;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.TsFileGeneratorForTest;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("squid:S4042") // Suppress use java.nio.Files#delete warning
public class RestorableTsFileIOWriterTest {

  private static final String FILE_NAME = TestConstant.BASE_OUTPUT_PATH.concat("test.ts");
  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  @Test(expected = NotCompatibleTsFileException.class)
  public void testBadHeadMagic() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    FileWriter fWriter = new FileWriter(file);
    try {
      fWriter.write("Tsfile");
    } finally {
      fWriter.close();
    }
    try {
      new RestorableTsFileIOWriter(file);
    } finally {
      assertTrue(file.delete());
    }
  }

  @Test
  public void testOnlyHeadMagic() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.getIOWriter().close();

    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TSFileConfig.MAGIC_STRING.getBytes().length + 1, rWriter.getTruncatedSize());

    rWriter = new RestorableTsFileIOWriter(file);
    assertEquals(TsFileCheckStatus.COMPLETE_FILE, rWriter.getTruncatedSize());
    assertFalse(rWriter.canWrite());
    rWriter.close();
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyFirstMask() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    // we have to flush using inner API.
    writer.getIOWriter().out.write(new byte[] {MetaMarker.CHUNK_HEADER});
    writer.getIOWriter().close();
    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TsFileIOWriter.MAGIC_STRING_BYTES.length + 1, rWriter.getTruncatedSize());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneIncompleteChunkHeader() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);

    TsFileGeneratorForTest.writeFileWithOneIncompleteChunkHeader(file);

    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    TsFileWriter writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TsFileIOWriter.MAGIC_STRING_BYTES.length + 1, rWriter.getTruncatedSize());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneChunkHeader() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.getIOWriter().startChunkGroup("root.sg1.d1");
    writer
        .getIOWriter()
        .startFlushChunk(
            new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.PLAIN).getMeasurementId(),
            CompressionType.SNAPPY,
            TSDataType.FLOAT,
            TSEncoding.PLAIN,
            new FloatStatistics(),
            100,
            10,
            0);
    writer.getIOWriter().close();

    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertEquals(TsFileIOWriter.MAGIC_STRING_BYTES.length + 1, rWriter.getTruncatedSize());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneChunkHeaderAndSomePage() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos1 = writer.getIOWriter().getPos();
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    long pos2 = writer.getIOWriter().getPos();
    // let's delete one byte. the version is broken
    writer.getIOWriter().out.truncate(pos2 - 1);
    writer.getIOWriter().close();
    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    // truncate version marker and version
    assertEquals(pos1, rWriter.getTruncatedSize());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneChunkGroup() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    writer.getIOWriter().writePlanIndices();
    writer.getIOWriter().close();
    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();

    ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(new TsFileSequenceReader(file));
    List<Path> pathList = new ArrayList<>();
    pathList.add(new Path("d1", "s1"));
    pathList.add(new Path("d1", "s2"));
    QueryExpression queryExpression = QueryExpression.create(pathList, null);
    QueryDataSet dataSet = readOnlyTsFile.query(queryExpression);
    RowRecord record = dataSet.next();
    assertEquals(1, record.getTimestamp());
    assertEquals(5.0f, record.getFields().get(0).getFloatV(), 0.001);
    assertEquals(4.0f, record.getFields().get(1).getFloatV(), 0.001);
    record = dataSet.next();
    assertEquals(2, record.getTimestamp());
    assertEquals(5.0f, record.getFields().get(0).getFloatV(), 0.001);
    assertEquals(4.0f, record.getFields().get(1).getFloatV(), 0.001);
    readOnlyTsFile.close();
    assertFalse(dataSet.hasNext());
    assertTrue(file.delete());
  }

  @Test
  public void testOnlyOneChunkGroupAndOneMarker() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    writer.getIOWriter().writeChunkGroupMarkerForTest();
    writer.getIOWriter().close();
    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    assertNotEquals(TsFileIOWriter.MAGIC_STRING_BYTES.length, rWriter.getTruncatedSize());
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_NAME));
    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s2"));
    assertNotNull(chunkMetadataList);
    reader.close();
    assertTrue(file.delete());
  }

  @Test
  public void testTwoChunkGroupAndMore() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    writer.getIOWriter().writeChunkGroupMarkerForTest();
    writer.getIOWriter().close();
    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_NAME));
    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s2"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d2", "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d2", "s2"));
    assertNotNull(chunkMetadataList);
    reader.close();
    assertTrue(file.delete());
  }

  @Test
  public void testNoSeperatorMask() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().close();
    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_NAME));
    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s2"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d2", "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d2", "s2"));
    assertNotNull(chunkMetadataList);
    reader.close();
    assertTrue(file.delete());
  }

  @Test
  public void testHavingSomeFileMetadata() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d2", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));

    writer.write(
        new TSRecord(1, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d2")
            .addTuple(new FloatDataPoint("s1", 6))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.flushAllChunkGroups();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().writeSeparatorMaskForTest();
    writer.getIOWriter().close();
    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    writer = new TsFileWriter(rWriter);
    writer.close();
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_NAME));
    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s2"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d2", "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d2", "s2"));
    assertNotNull(chunkMetadataList);
    reader.close();
    assertTrue(file.delete());
  }

  @Test
  public void testOpenCompleteFile() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.close();

    RestorableTsFileIOWriter rWriter = new RestorableTsFileIOWriter(file);
    assertFalse(rWriter.canWrite());
    rWriter.close();

    rWriter = new RestorableTsFileIOWriter(file);
    assertFalse(rWriter.canWrite());
    TsFileSequenceReader reader = new TsFileSequenceReader(fsFactory.getFile(FILE_NAME));
    List<ChunkMetadata> chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s1"));
    assertNotNull(chunkMetadataList);
    chunkMetadataList = reader.getChunkMetadataList(new Path("d1", "s2"));
    assertNotNull(chunkMetadataList);
    reader.close();
    assertTrue(file.delete());
  }

  @Test
  public void testAppendDataOnCompletedFile() throws Exception {
    File file = fsFactory.getFile(FILE_NAME);
    TsFileWriter writer = new TsFileWriter(file);
    writer.registerTimeseries(
        new Path("d1", "s1"), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    writer.registerTimeseries(
        new Path("d1", "s2"), new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    writer.write(
        new TSRecord(1, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.write(
        new TSRecord(2, "d1")
            .addTuple(new FloatDataPoint("s1", 5))
            .addTuple(new FloatDataPoint("s2", 4)));
    writer.close();

    long size = file.length();
    RestorableTsFileIOWriter rWriter =
        RestorableTsFileIOWriter.getWriterForAppendingDataOnCompletedTsFile(file);
    TsFileWriter write = new TsFileWriter(rWriter);
    write.close();
    assertEquals(size, file.length());
    assertTrue(file.delete());
  }
}
