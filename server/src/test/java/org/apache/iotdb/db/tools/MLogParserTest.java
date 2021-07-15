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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.SetDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.tools.mlog.MLogParser;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MLogParserTest {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    File file = new File("target" + File.separator + "tmp" + File.separator + "text.mlog");
    file.deleteOnExit();
    file = new File("target" + File.separator + "tmp" + File.separator + "text.snapshot");
    file.deleteOnExit();
  }

  public void prepareData() {
    // prepare data
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 5; j++) {
        for (int k = 0; k < 10; k++) {
          CreateTimeSeriesPlan plan = new CreateTimeSeriesPlan();
          try {
            plan.setPath(new PartialPath("root.sg" + i + "." + "device" + j + "." + "s" + k));
            plan.setDataType(TSDataType.INT32);
            plan.setEncoding(TSEncoding.PLAIN);
            plan.setCompressor(CompressionType.GZIP);
            IoTDB.metaManager.createTimeseries(plan);
          } catch (MetadataException e) {
            e.printStackTrace();
          }
        }
      }
    }

    try {
      IoTDB.metaManager.setStorageGroup(new PartialPath("root.ln.cc"));
      IoTDB.metaManager.setStorageGroup(new PartialPath("root.sgcc"));
      IoTDB.metaManager.setTTL(new PartialPath("root.sgcc"), 1234L);
      IoTDB.metaManager.deleteTimeseries(new PartialPath("root.sg1.device1.s1"));
      List<PartialPath> paths = new ArrayList<>();
      paths.add(new PartialPath("root.ln.cc"));
      IoTDB.metaManager.deleteStorageGroups(paths);
      Map<String, String> tags = new HashMap<String, String>();
      tags.put("tag1", "value1");
      IoTDB.metaManager.addTags(tags, new PartialPath("root.sg1.device1.s2"));
      IoTDB.metaManager.changeAlias(new PartialPath("root.sg1.device1.s3"), "hello");
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
    }

    try {
      IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));
      IoTDB.metaManager.createDeviceTemplate(genCreateTemplatePlan());
      SetDeviceTemplatePlan setDeviceTemplatePlan =
          new SetDeviceTemplatePlan("template1", "root.sg");
      IoTDB.metaManager.setDeviceTemplate(setDeviceTemplatePlan);
      IoTDB.metaManager.getDeviceNodeWithAutoCreate(new PartialPath("root.sg.d1"), true, true, 1);
    } catch (MetadataException | IOException e) {
      e.printStackTrace();
    }
  }

  private CreateTemplatePlan genCreateTemplatePlan() {
    List<List<String>> measurementList = new ArrayList<>();
    measurementList.add(Collections.singletonList("s11"));
    measurementList.add(Collections.singletonList("s12"));

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    dataTypeList.add(Collections.singletonList(TSDataType.INT64));
    dataTypeList.add(Collections.singletonList(TSDataType.DOUBLE));

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    encodingList.add(Collections.singletonList(TSEncoding.RLE));
    encodingList.add(Collections.singletonList(TSEncoding.GORILLA));

    List<CompressionType> compressionTypes = new ArrayList<>();
    compressionTypes.add(CompressionType.SNAPPY);
    compressionTypes.add(CompressionType.SNAPPY);

    List<String> schemaNames = new ArrayList<>();
    schemaNames.add("s11");
    schemaNames.add("s12");

    return new CreateTemplatePlan(
        "template1", schemaNames, measurementList, dataTypeList, encodingList, compressionTypes);
  }

  @Test
  public void testParseMLog() throws IOException {
    prepareData();
    IoTDB.metaManager.flushAllMlogForTest();

    try {
      MLogParser.parseFromFile(
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
              + File.separator
              + MetadataConstant.METADATA_LOG,
          "target" + File.separator + "tmp" + File.separator + "text.mlog");
    } catch (IOException e) {
      e.printStackTrace();
    }

    try (BufferedReader reader =
        new BufferedReader(
            new FileReader("target" + File.separator + "tmp" + File.separator + "text.mlog"))) {
      int lineNum = 0;
      List<String> lines = new ArrayList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        lineNum++;
        lines.add(line);
      }
      if (lineNum != 113) {
        // First, we prepare 2 storage groups, each one has 5 devices, and every device has 10
        // measurements.
        // So, mlog records 2 * 5 * 10 = 100 CreateTimeSeriesPlan, and 2 SetStorageGroupPlan.
        // Next, we do 6 operations which will be written into mlog, include set 2 sgs, set ttl,
        // delete timeseries, delete sg, add tags.
        // The final operation changeAlias only change the mtree in memory, so it will not write
        // record to mlog.
        // Then, we set 1 more storage group, create a template with 2 measurements, set
        // the template to this storage group and create 1 device using template.
        // Finally, the mlog should have 100 + 2 + 6 + 1 + 2 + 1 + 1 = 113 records
        for (String content : lines) {
          System.out.println(content);
        }
      }
      Assert.assertEquals(113, lineNum);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void testParseSnapshot() {
    prepareData();
    IoTDB.metaManager.createMTreeSnapshot();

    try {
      MLogParser.parseFromFile(
          IoTDBDescriptor.getInstance().getConfig().getSchemaDir()
              + File.separator
              + MetadataConstant.MTREE_SNAPSHOT,
          "target" + File.separator + "tmp" + File.separator + "text.snapshot");
    } catch (IOException e) {
      e.printStackTrace();
    }

    try (BufferedReader reader =
        new BufferedReader(
            new FileReader("target" + File.separator + "tmp" + File.separator + "text.snapshot"))) {
      int lineNum = 0;
      List<String> lines = new ArrayList<>();
      String line;
      while ((line = reader.readLine()) != null) {
        lineNum++;
        lines.add(line);
      }
      if (lineNum != 115) {
        // First, we prepare 2 storage groups, each one has 5 devices, and every device has 10
        // measurements.
        // So, mtree records 2 * 5 * 10 = 100 TimeSeries, and 2 SetStorageGroup, 2 * 5 devices.
        // Next, we do 4 operations which will be record in mtree, include set 2 sgs, delete
        // timeseries, delete sg.
        // Then, we set 1 more storage group, create a template with 2 measurements and set
        // the template to this storage group and create 1 device using template.
        // The snapshot should have 100 + 2 + 5 * 2 + 2 - 1 - 1 + 1 + 1 = 114 records,
        // and we have root record,
        // so we have 114 + 1 = 115 records finally.
        for (String content : lines) {
          System.out.println(content);
        }
      }
      Assert.assertEquals(115, lineNum);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
