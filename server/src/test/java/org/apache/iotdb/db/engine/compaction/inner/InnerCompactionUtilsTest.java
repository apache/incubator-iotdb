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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

import static org.apache.iotdb.db.engine.compaction.inner.utils.CompactionLogger.SOURCE_NAME;
import static org.junit.Assert.assertEquals;

public class InnerCompactionUtilsTest extends InnerCompactionTest {

  File tempSGDir;

  @Override
  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    super.setUp();
    tempSGDir = new File(TestConstant.BASE_OUTPUT_PATH.concat("tempSG"));
    tempSGDir.mkdirs();
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    FileUtils.deleteDirectory(tempSGDir);
  }

  @Test
  public void testCompact() throws IOException, IllegalPathException {
    TsFileResource targetTsFileResource =
        new TsFileResource(
            new File(
                TestConstant.BASE_OUTPUT_PATH.concat(
                    0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 1
                        + IoTDBConstant.FILE_NAME_SEPARATOR
                        + 0
                        + ".tsfile")));
    CompactionLogger compactionLogger =
        new CompactionLogger(tempSGDir.getPath(), COMPACTION_TEST_SG);
    for (TsFileResource resource : seqResources) {
      compactionLogger.logFile(SOURCE_NAME, resource.getTsFile());
    }
    compactionLogger.logSequence(true);
    CompactionUtils.compact(
        targetTsFileResource,
        seqResources,
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
        true);
    compactionLogger.close();
    Path path = new Path(deviceIds[0], measurementSchemas[0].getMeasurementId());
    try (TsFileSequenceReader reader =
            new TsFileSequenceReader(targetTsFileResource.getTsFilePath());
        ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)) {
      QueryExpression queryExpression =
          QueryExpression.create(Collections.singletonList(path), null);
      QueryDataSet queryDataSet = readTsFile.query(queryExpression);
      int cut = 0;
      RowRecord record;
      while (queryDataSet.hasNext()) {
        record = queryDataSet.next();
        assertEquals(record.getTimestamp(), record.getFields().get(0).getDoubleV(), 0.001);
        cut++;
      }
      assertEquals(500, cut);
    }
  }
}
