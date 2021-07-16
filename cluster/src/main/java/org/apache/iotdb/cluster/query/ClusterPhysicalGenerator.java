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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.logical.sys.LoadConfigurationOperator.LoadConfigurationOperatorType;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan;
import org.apache.iotdb.db.qp.physical.sys.LoadConfigurationPlan.LoadConfigurationPlanType;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ClusterPhysicalGenerator extends PhysicalGenerator {

  private static final Logger logger = LoggerFactory.getLogger(ClusterPhysicalGenerator.class);

  private CMManager getCMManager() {
    return ((CMManager) IoTDB.metaManager);
  }

  @Override
  public Pair<List<TSDataType>, List<TSDataType>> getSeriesTypes(
      List<PartialPath> paths, String aggregation) throws MetadataException {
    return getCMManager().getSeriesTypesByPaths(paths, aggregation);
  }

  @Override
  public List<TSDataType> getSeriesTypes(List<PartialPath> paths) throws MetadataException {
    return getCMManager().getSeriesTypesByPaths(paths, null).left;
  }

  @Override
  public Pair<List<PartialPath>, Map<String, Integer>> getSeriesSchema(List<PartialPath> paths)
      throws MetadataException {
    return getCMManager().getSeriesSchemas(paths);
  }

  @Override
  public PhysicalPlan transformToPhysicalPlan(Operator operator) throws QueryProcessException {
    // update storage groups before parsing query plans
    if (operator instanceof QueryOperator) {
      try {
        getCMManager().syncMetaLeader();
      } catch (MetadataException e) {
        throw new QueryProcessException(e);
      }
    }
    return super.transformToPhysicalPlan(operator);
  }

  @Override
  public PhysicalPlan generateLoadConfigurationPlan(LoadConfigurationOperatorType type)
      throws QueryProcessException {
    if (type == LoadConfigurationOperatorType.GLOBAL) {
      Properties[] properties = new Properties[2];
      properties[0] = new Properties();
      URL iotdbEnginePropertiesUrl = IoTDBDescriptor.getInstance().getPropsUrl();
      if (iotdbEnginePropertiesUrl == null) {
        logger.error("Fail to find the engine config file");
        throw new QueryProcessException("Fail to find config file");
      }
      try (InputStream inputStream = iotdbEnginePropertiesUrl.openStream()) {
        properties[0].load(inputStream);
      } catch (IOException e) {
        logger.error("Fail to read iotdb-engine config file {}", iotdbEnginePropertiesUrl, e);
        throw new QueryProcessException("Fail to read iotdb-engine config file.");
      }
      String clusterPropertiesUrl = ClusterDescriptor.getInstance().getPropsUrl();
      properties[1] = new Properties();
      try (InputStream inputStream = new FileInputStream(new File(clusterPropertiesUrl))) {
        properties[1].load(inputStream);
      } catch (IOException e) {
        logger.error("Fail to read iotdb-cluster config file {}", clusterPropertiesUrl, e);
        throw new QueryProcessException("Fail to read iotdb-cluster config file.");
      }

      return new LoadConfigurationPlan(LoadConfigurationPlanType.GLOBAL, properties);
    } else {
      return new LoadConfigurationPlan(LoadConfigurationPlanType.LOCAL);
    }
  }
}
