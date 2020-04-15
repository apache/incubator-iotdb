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

import static org.apache.iotdb.cluster.server.RaftServer.connectionTimeoutInMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GetChildNodeNextLevelPathHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GetNodesListHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTTLPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterPlanExecutor extends PlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ClusterPlanExecutor.class);
  private MetaGroupMember metaGroupMember;

  private static final int THREAD_POOL_SIZE = 6;
  private static final int WAIT_GET_NODES_LIST_TIME = 5;
  private static final TimeUnit WAIT_GET_NODES_LIST_TIME_UNIT = TimeUnit.MINUTES;

  public ClusterPlanExecutor(MetaGroupMember metaGroupMember) throws QueryProcessException {
    super();
    this.metaGroupMember = metaGroupMember;
    this.queryRouter = new ClusterQueryRouter(metaGroupMember);
  }

  @Override
  public QueryDataSet processQuery(PhysicalPlan queryPlan, QueryContext context)
      throws IOException, StorageEngineException, QueryFilterOptimizationException, QueryProcessException,
      MetadataException, TException, InterruptedException {
    if (queryPlan instanceof QueryPlan) {
      logger.debug("Executing a query: {}", queryPlan);
      return processDataQuery((QueryPlan) queryPlan, context);
    } else if (queryPlan instanceof ShowPlan) {
      return processShowQuery((ShowPlan) queryPlan);
    } else {
      //TODO-Cluster: support more queries
      throw new QueryProcessException(String.format("Unrecognized query plan %s", queryPlan));
    }
  }

  @Override
  protected List<String> getPaths(String path) throws MetadataException {
    return metaGroupMember.getMatchedPaths(path);
  }

  @Override
  protected Set<String> getDevices(String path) throws MetadataException {
    return metaGroupMember.getMatchedDevices(path);
  }

  @Override
  protected List<String> getNodesList(String schemaPattern, int level)
      throws IOException, TException, InterruptedException, MetadataException {
    Set<String> nodeListSet = new HashSet<>(
        MManager.getInstance().getNodesList(schemaPattern, level));

    GetNodesListHandler handler = new GetNodesListHandler();

    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

    for (Node node : metaGroupMember.getAllNodes()) {
      pool.submit(() -> {
        DataClient client = null;
        try {
          client = metaGroupMember.getDataClient(node);
        } catch (IOException e) {
          logger.error("Failed to connect to node: {}", node, e);
        }
        AtomicReference<List<String>> response = new AtomicReference<>(null);
        handler.setResponse(response);
        handler.setContact(node);
        synchronized (response) {
          try {
            if (client != null) {
              client.getNodeList(null, schemaPattern, level, handler);
              response.wait(connectionTimeoutInMS);
            }
          } catch (TException e) {
            logger.error("Error occurs when getting node lists in node {}.", node, e);
          } catch (InterruptedException e) {
            logger.error("Interrupted when getting node lists in node {}.", node, e);
          }
        }
        List<String> paths = response.get();
        if (paths != null) {
          nodeListSet.addAll(response.get());
        }
      });
    }
    pool.shutdown();
    pool.awaitTermination(WAIT_GET_NODES_LIST_TIME, WAIT_GET_NODES_LIST_TIME_UNIT);
    return new ArrayList<>(nodeListSet);
  }

  @Override
  protected Set<String> getPathNextChildren(String path)
      throws MetadataException, InterruptedException {
    Set<String> nodeListSet = new HashSet<>(
        MManager.getInstance().getChildNodePathInNextLevel(path));

    GetChildNodeNextLevelPathHandler handler = new GetChildNodeNextLevelPathHandler();

    ExecutorService pool = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);

    for (Node node : metaGroupMember.getAllNodes()) {
      pool.submit(() -> {
        DataClient client = null;
        try {
          client = metaGroupMember.getDataClient(node);
        } catch (IOException e) {
          logger.error("Failed to connect to node: {}", node, e);
        }
        AtomicReference<List<String>> response = new AtomicReference<>(null);
        handler.setResponse(response);
        handler.setContact(node);
        synchronized (response) {
          try {
            if (client != null) {
              client.getChildNodePathInNextLevel(null, path, handler);
              response.wait(connectionTimeoutInMS);
            }
          } catch (TException e) {
            logger.error("Error occurs when getting node lists in node {}.", node, e);
          } catch (InterruptedException e) {
            logger.error("Interrupted when getting node lists in node {}.", node, e);
          }
        }
        List<String> paths = response.get();
        if (paths != null) {
          nodeListSet.addAll(response.get());
        }
      });
    }
    pool.shutdown();
    pool.awaitTermination(WAIT_GET_NODES_LIST_TIME, WAIT_GET_NODES_LIST_TIME_UNIT);
    return nodeListSet;
  }

  @Override
  protected List<String[]> getTimeseriesSchemas(String path) {
    // TODO-Cluster: enable meta queries
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  protected MeasurementSchema[] getSeriesSchemas(String[] measurementList, String deviceId,
      String[] strValues) throws MetadataException {

    MNode node = MManager.getInstance().getDeviceNodeWithAutoCreateStorageGroup(deviceId);
    boolean allSeriesExists = true;
    for (String measurement : measurementList) {
      if (!node.hasChild(measurement)) {
        allSeriesExists = false;
        break;
      }
    }

    if (allSeriesExists) {
      return super.getSeriesSchemas(measurementList, deviceId, strValues);
    }

    // some schemas does not exist locally, fetch them from the remote side
    List<String> schemasToPull = new ArrayList<>();
    for (String s : measurementList) {
      schemasToPull.add(deviceId + IoTDBConstant.PATH_SEPARATOR + s);
    }
    List<MeasurementSchema> schemas = metaGroupMember.pullTimeSeriesSchemas(schemasToPull);
    for (MeasurementSchema schema : schemas) {
      SchemaUtils.registerTimeseries(schema);
    }

    if (schemas.size() == measurementList.length) {
      // all schemas can be fetched from the remote side
      return schemas.toArray(new MeasurementSchema[0]);
    } else {
      // some schemas does not exist in the remote side, check if we can use auto-creation
      return super.getSeriesSchemas(measurementList, deviceId, strValues);
    }
  }

  @Override
  protected List<String> getAllStorageGroupNames() {
    return metaGroupMember.getAllStorageGroupNames();
  }

  @Override
  protected List<StorageGroupMNode> getAllStorageGroupNodes() {
    return metaGroupMember.getAllStorageGroupNodes();
  }
}
