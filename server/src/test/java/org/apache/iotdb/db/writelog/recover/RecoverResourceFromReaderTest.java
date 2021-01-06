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

package org.apache.iotdb.db.writelog.recover;

import static org.junit.Assert.assertEquals;

import java.util.*;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.StorageGroupProcessorException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoverResourceFromReaderTest {

  private File tsF;
  private TsFileWriter writer;
  private WriteLogNode node;
  private String logNodePrefix = TestConstant.OUTPUT_DATA_DIR.concat("testNode/0");
  private Schema schema;
  private TsFileResource resource;
  private VersionController versionController = new VersionController() {
    private int i;

    @Override
    public long nextVersion() {
      return ++i;
    }

    @Override
    public long currVersion() {
      return i;
    }
  };

  @Before
  public void setup() throws IOException, WriteProcessException, MetadataException {
    EnvironmentUtils.envSetUp();
    tsF = SystemFileFactory.INSTANCE.getFile(logNodePrefix, "1-1-1.tsfile");
    tsF.getParentFile().mkdirs();

    schema = new Schema();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        PartialPath path = new PartialPath("root.sg.device" + i + IoTDBConstant.PATH_SEPARATOR + "sensor" + j);
        MeasurementSchema measurementSchema = new MeasurementSchema("sensor" + j, TSDataType.INT64,
            TSEncoding.PLAIN);
        schema.registerTimeseries(path.toTSFilePath(), measurementSchema);
        IoTDB.metaManager.createTimeseries(path, measurementSchema.getType(),
            measurementSchema.getEncodingType(), measurementSchema.getCompressor(),
            measurementSchema.getProps());
      }
    }
    schema.registerTimeseries(new Path(("root.sg.device99"), ("sensor4")),
        new MeasurementSchema("sensor4", TSDataType.INT64, TSEncoding.PLAIN));
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.device99.sensor4"), TSDataType.INT64, TSEncoding.PLAIN,
            TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    schema.registerTimeseries(new Path(("root.sg.device99"), ("sensor2")),
        new MeasurementSchema("sensor2", TSDataType.INT64, TSEncoding.PLAIN));
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.device99.sensor2"), TSDataType.INT64, TSEncoding.PLAIN,
            TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    schema.registerTimeseries(new Path(("root.sg.device99"), ("sensor1")),
        new MeasurementSchema("sensor1", TSDataType.INT64, TSEncoding.PLAIN));
    IoTDB.metaManager
        .createTimeseries(new PartialPath("root.sg.device99.sensor1"), TSDataType.INT64, TSEncoding.PLAIN,
            TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    writer = new TsFileWriter(tsF, schema);

    TSRecord tsRecord = new TSRecord(100, "root.sg.device99");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor4", String.valueOf(0)));
    writer.write(tsRecord);
    tsRecord = new TSRecord(2, "root.sg.device99");
    tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor1", String.valueOf(0)));
    writer.write(tsRecord);

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        tsRecord = new TSRecord(i, "root.sg.device" + j);
        for (int k = 0; k < 10; k++) {
          tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.INT64, "sensor" + k,
              String.valueOf(k)));
        }
        writer.write(tsRecord);
      }
    }

    writer.flushAllChunkGroups();
    writer.getIOWriter().close();

    node = MultiFileLogNodeManager.getInstance().getNode(logNodePrefix + tsF.getName());
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        String[] measurements = new String[10];
        String[] values = new String[10];
        TSDataType[] types = new TSDataType[10];
        for (int k = 0; k < 10; k++) {
          measurements[k] = "sensor" + k;
          types[k] = TSDataType.INT64;
          values[k] = String.valueOf(k + 10);
        }
        InsertRowPlan insertRowPlan = new InsertRowPlan(new PartialPath("root.sg.device" + j), i, measurements,
            types, values);
        node.write(insertRowPlan, false);
      }
      node.notifyStartFlush();
    }
    InsertRowPlan insertRowPlan = new InsertRowPlan(new PartialPath("root.sg.device99"), 1, new String[]{"sensor4"},
        new TSDataType[]{TSDataType.INT64}, new String[]{"4"});
    node.write(insertRowPlan, false);
    insertRowPlan = new InsertRowPlan(new PartialPath("root.sg.device99"), 300, new String[]{"sensor2"},
        new TSDataType[]{TSDataType.INT64}, new String[]{"2"});
    node.write(insertRowPlan, false);
    node.close();

    resource = new TsFileResource(tsF);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
    FileUtils.deleteDirectory(tsF.getParentFile());
    resource.close();
    node.delete();
  }

  @Test
  public void testResourceRecovery() throws StorageGroupProcessorException, IOException {
    // write a broken resourceFile
    File resourceFile = FSFactoryProducer.getFSFactory()
        .getFile(resource.getTsFile() + TsFileResource.RESOURCE_SUFFIX);
    FileUtils.deleteQuietly(resourceFile);
    try (OutputStream outputStream = FSFactoryProducer.getFSFactory()
        .getBufferedOutputStream(resourceFile.getPath())) {
      ReadWriteIOUtils.write(123, outputStream);
    }

    TsFileRecoverPerformer performer = new TsFileRecoverPerformer(logNodePrefix, versionController,
        resource, false, false);
    performer.recover(true).close();
    assertEquals(1, resource.getStartTime("root.sg.device99"));
    assertEquals(300, resource.getEndTime("root.sg.device99"));
    for (int i = 0; i < 10; i++) {
      assertEquals(0, resource.getStartTime("root.sg.device" + i));
      assertEquals(9, resource.getEndTime("root.sg.device" + i));
    }
  }
}
