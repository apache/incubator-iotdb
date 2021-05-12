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

package org.apache.iotdb.db.engine.merge;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.selector.MaxFileMergeFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MergeUpgradeTest {
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  private List<TsFileResource> seqResources = new ArrayList<>();
  private List<TsFileResource> unseqResources = new ArrayList<>();

  private int seqFileNum = 2;
  private TSEncoding encoding = TSEncoding.RLE;
  private MeasurementSchema[] measurementSchemas;
  private int timeseriesNum = 5;
  private long ptNum = 10;
  private boolean changeVersion = true;
  private String deviceName = "root.MergeUpgrade.device0";

  @Before
  public void setUp() throws IOException, WriteProcessException {
    prepareSeries();
    prepareFiles();
  }

  @After
  public void tearDown() {
    removeFiles();
    seqResources.clear();
    unseqResources.clear();
  }

  @Test
  public void testMergeUpgradeSelect() throws MergeException {
    MergeResource resource = new MergeResource(seqResources, unseqResources);
    MaxFileMergeFileSelector mergeFileSelector =
        new MaxFileMergeFileSelector(resource, Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    assertEquals(0, result.length);
  }

  private void prepareFiles() throws IOException, WriteProcessException {
    // prepare seqFiles
    for (int i = 0; i < seqFileNum; i++) {
      File seqfile =
          fsFactory.getFile(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  "seq"
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource seqTsFileResource = new TsFileResource(seqfile);
      seqResources.add(seqTsFileResource);
      prepareOldFile(seqTsFileResource, i * ptNum, ptNum, 0);
    }
    // prepare unseqFile
    File unseqfile =
        fsFactory.getFile(
            TestConstant.BASE_OUTPUT_PATH.concat(
                "unseq"
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource unseqTsFileResource = new TsFileResource(unseqfile);
    unseqResources.add(unseqTsFileResource);
    prepareFile(unseqTsFileResource, 0, 2 * ptNum, 10);
  }

  private void prepareSeries() {
    measurementSchemas = new MeasurementSchema[timeseriesNum];
    for (int i = 0; i < timeseriesNum; i++) {
      measurementSchemas[i] =
          new MeasurementSchema(
              "sensor" + i, TSDataType.DOUBLE, encoding, CompressionType.UNCOMPRESSED);
    }
  }

  private void prepareOldFile(
      TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    prepareData(tsFileResource, fileWriter, timeOffset, ptNum, valueOffset);
    fileWriter.close();
    if (changeVersion) {
      try (RandomAccessFile oldTsfile = new RandomAccessFile(tsFileResource.getTsFile(), "rw")) {
        oldTsfile.seek(TSFileConfig.MAGIC_STRING.length());
        oldTsfile.write(TSFileConfig.VERSION_NUMBER_V1.getBytes());
      }
      changeVersion = false;
    }
  }

  private void prepareFile(
      TsFileResource tsFileResource, long timeOffset, long ptNum, long valueOffset)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    prepareData(tsFileResource, fileWriter, timeOffset, ptNum, valueOffset);
    fileWriter.close();
  }

  private void removeFiles() {
    for (TsFileResource tsFileResource : seqResources) {
      tsFileResource.remove();
    }
    for (TsFileResource tsFileResource : unseqResources) {
      tsFileResource.remove();
    }
  }

  private void prepareData(
      TsFileResource tsFileResource,
      TsFileWriter fileWriter,
      long timeOffset,
      long ptNum,
      long valueOffset)
      throws WriteProcessException, IOException {
    for (MeasurementSchema MeasurementSchema : measurementSchemas) {
      fileWriter.registerTimeseries(
          new Path(deviceName, MeasurementSchema.getMeasurementId()), MeasurementSchema);
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      TSRecord record = new TSRecord(i, deviceName);
      for (int k = 0; k < timeseriesNum; k++) {
        record.addTuple(
            DataPoint.getDataPoint(
                measurementSchemas[k].getType(),
                measurementSchemas[k].getMeasurementId(),
                String.valueOf(i + valueOffset)));
      }
      fileWriter.write(record);
      tsFileResource.updateStartTime(deviceName, i);
      tsFileResource.updateEndTime(deviceName, i);
    }
  }
}
