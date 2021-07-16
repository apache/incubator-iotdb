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
 *
 */

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({IoTDBDescriptor.class, ClusterDescriptor.class})
public class LoadConfigurationTest {

  private static FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  private static final String ENGINE_PROPERTIES_FILE =
      TestConstant.BASE_OUTPUT_PATH.concat("LoadConfigurationTestEngineProperties");
  private static final String CLUSTER_PROPERTIES_FILE =
      TestConstant.BASE_OUTPUT_PATH.concat("LoadConfigurationTestClusterProperties");

  @Mock private IoTDBDescriptor ioTDBDescriptor;

  @Mock private ClusterDescriptor clusterDescriptor;

  @Before
  public void setUp() throws Exception {
    // init engine properties
    File engineFile = fsFactory.getFile(ENGINE_PROPERTIES_FILE);
    if (engineFile.exists()) {
      Assert.assertTrue(engineFile.delete());
    }
    try (FileWriter fw = new FileWriter(engineFile)) {
      fw.write("enable_metric_service=false");
    }
    ioTDBDescriptor = PowerMockito.mock(IoTDBDescriptor.class);
    PowerMockito.mockStatic(IoTDBDescriptor.class);
    PowerMockito.doReturn(ioTDBDescriptor).when(IoTDBDescriptor.class, "getInstance");
    when(ioTDBDescriptor.getPropsUrl()).thenReturn(new URL("file:" + ENGINE_PROPERTIES_FILE));

    // init cluster properties
    File clusterFile = fsFactory.getFile(CLUSTER_PROPERTIES_FILE);
    if (clusterFile.exists()) {
      Assert.assertTrue(clusterFile.delete());
    }
    try (FileWriter fw = new FileWriter(clusterFile)) {
      fw.write("cluster_rpc_ip=127.0.0.1");
    }
    clusterDescriptor = PowerMockito.mock(ClusterDescriptor.class);
    PowerMockito.mockStatic(ClusterDescriptor.class);
    PowerMockito.doReturn(clusterDescriptor).when(ClusterDescriptor.class, "getInstance");
    when(clusterDescriptor.getPropsUrl()).thenReturn(CLUSTER_PROPERTIES_FILE);
  }

  @After
  public void tearDown() {
    File engineFile = fsFactory.getFile(ENGINE_PROPERTIES_FILE);
    if (engineFile.exists()) {
      Assert.assertTrue(engineFile.delete());
    }
    File clusterFile = fsFactory.getFile(CLUSTER_PROPERTIES_FILE);
    if (clusterFile.exists()) {
      Assert.assertTrue(clusterFile.delete());
    }
  }

  @Test
  public void testLoadConfigurationGlobal() throws QueryProcessException {
    PhysicalGenerator physicalGenerator = new ClusterPhysicalGenerator();
    LoadConfigurationOperator loadConfigurationOperator =
        new LoadConfigurationOperator(
            LoadConfigurationOperator.LoadConfigurationOperatorType.GLOBAL);

    LoadConfigurationPlan loadConfigurationPlan =
        (LoadConfigurationPlan)
            physicalGenerator.transformToPhysicalPlan(loadConfigurationOperator);
    String metricProperties =
        (String) loadConfigurationPlan.getIoTDBProperties().get("enable_metric_service");
    assertEquals("false", metricProperties);
    String clusterIp = (String) loadConfigurationPlan.getClusterProperties().get("cluster_rpc_ip");
    assertEquals("127.0.0.1", clusterIp);
  }
}
