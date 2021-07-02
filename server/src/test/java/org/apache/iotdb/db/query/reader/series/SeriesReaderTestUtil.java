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

package org.apache.iotdb.db.query.reader.series;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;

public class SeriesReaderTestUtil {

  private static int seqFileNum = 5;
  private static int unseqFileNum = 5;
  private static int measurementNum = 10;
  private static int deviceNum = 10;
  private static long ptNum = 100;
  private static long flushInterval = 20;
  private static TSEncoding encoding = TSEncoding.PLAIN;
  private static final String SERIES_READER_TEST_SG = "root.seriesReaderTest";

  public static void setUp(
      List<MeasurementSchema> measurementSchemas,
      List<String> deviceIds,
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources)
      throws MetadataException, IOException, WriteProcessException {
    IoTDB.metaManager.init();
    prepareSeries(measurementSchemas, deviceIds);
    prepareFiles(seqResources, unseqResources, measurementSchemas, deviceIds);
  }

  public static void tearDown(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws IOException {
    removeFiles(seqResources, unseqResources);
    seqResources.clear();
    unseqResources.clear();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
    MergeManager.getINSTANCE().stop();
  }

  private static void prepareFiles(
      List<TsFileResource> seqResources,
      List<TsFileResource> unseqResources,
      List<MeasurementSchema> measurementSchemas,
      List<String> deviceIds)
      throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + "seq"
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setMinPlanIndex(i);
      tsFileResource.setMaxPlanIndex(i);
      tsFileResource.setVersion(i);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0, measurementSchemas, deviceIds);
    }
    for (int i = 0; i < unseqFileNum; i++) {
      File file =
          new File(
              TestConstant.BASE_OUTPUT_PATH.concat(
                  i
                      + "unseq"
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + i
                      + IoTDBConstant.FILE_NAME_SEPARATOR
                      + 0
                      + ".tsfile"));
      TsFileResource tsFileResource = new TsFileResource(file);
      tsFileResource.setClosed(true);
      tsFileResource.setMinPlanIndex(i + seqFileNum);
      tsFileResource.setMaxPlanIndex(i + seqFileNum);
      tsFileResource.setVersion(i + seqFileNum);
      unseqResources.add(tsFileResource);
      prepareFile(
          tsFileResource,
          i * ptNum,
          ptNum * (i + 1) / unseqFileNum,
          10000,
          measurementSchemas,
          deviceIds);
    }

    File file =
        new File(
            TestConstant.BASE_OUTPUT_PATH.concat(
                unseqFileNum
                    + "unseq"
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + unseqFileNum
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + unseqFileNum
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.setMinPlanIndex(seqFileNum + unseqFileNum);
    tsFileResource.setMaxPlanIndex(seqFileNum + unseqFileNum);
    tsFileResource.setVersion(seqFileNum + unseqFileNum);
    unseqResources.add(tsFileResource);
    prepareFile(tsFileResource, 0, ptNum * 2, 20000, measurementSchemas, deviceIds);
  }

  private static void prepareFile(
      TsFileResource tsFileResource,
      long timeOffset,
      long ptNum,
      long valueOffset,
      List<MeasurementSchema> measurementSchemas,
      List<String> deviceIds)
      throws IOException, WriteProcessException {
    TsFileWriter fileWriter = new TsFileWriter(tsFileResource.getTsFile());
    Map<String, IMeasurementSchema> template = new HashMap<>();
    for (MeasurementSchema measurementSchema : measurementSchemas) {
      template.put(measurementSchema.getMeasurementId(), measurementSchema);
    }
    fileWriter.registerDeviceTemplate("template0", template);
    for (String deviceId : deviceIds) {
      fileWriter.registerDevice(deviceId, "template0");
    }
    for (long i = timeOffset; i < timeOffset + ptNum; i++) {
      for (String deviceId : deviceIds) {
        TSRecord record = new TSRecord(i, deviceId);
        for (MeasurementSchema measurementSchema : measurementSchemas) {
          record.addTuple(
              DataPoint.getDataPoint(
                  measurementSchema.getType(),
                  measurementSchema.getMeasurementId(),
                  String.valueOf(i + valueOffset)));
        }
        fileWriter.write(record);
        tsFileResource.updateStartTime(deviceId, i);
        tsFileResource.updateEndTime(deviceId, i);
      }
      if ((i + 1) % flushInterval == 0) {
        fileWriter.flushAllChunkGroups();
      }
    }
    fileWriter.close();
  }

  private static void prepareSeries(
      List<MeasurementSchema> measurementSchemas, List<String> deviceIds) throws MetadataException {
    for (int i = 0; i < measurementNum; i++) {
      measurementSchemas.add(
          new MeasurementSchema(
              "sensor" + i, TSDataType.INT32, encoding, CompressionType.UNCOMPRESSED));
    }
    for (int i = 0; i < deviceNum; i++) {
      deviceIds.add(SERIES_READER_TEST_SG + PATH_SEPARATOR + "device" + i);
    }
    IoTDB.metaManager.setStorageGroup(new PartialPath(SERIES_READER_TEST_SG));
    for (String device : deviceIds) {
      for (MeasurementSchema measurementSchema : measurementSchemas) {
        IoTDB.metaManager.createTimeseries(
            new PartialPath(device + PATH_SEPARATOR + measurementSchema.getMeasurementId()),
            measurementSchema.getType(),
            measurementSchema.getEncodingType(),
            measurementSchema.getCompressor(),
            Collections.emptyMap());
      }
    }
  }

  private static void removeFiles(
      List<TsFileResource> seqResources, List<TsFileResource> unseqResources) throws IOException {
    for (TsFileResource tsFileResource : seqResources) {
      tsFileResource.remove();
    }
    for (TsFileResource tsFileResource : unseqResources) {
      tsFileResource.remove();
    }

    FileReaderManager.getInstance().closeAndRemoveAllOpenedReaders();
    FileReaderManager.getInstance().stop();
  }
}
