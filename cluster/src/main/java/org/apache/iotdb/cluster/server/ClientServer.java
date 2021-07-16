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

package org.apache.iotdb.cluster.server;

import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.query.ClusterPlanExecutor;
import org.apache.iotdb.cluster.query.ClusterPlanner;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSIService.Processor;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ClientServer is the cluster version of TSServiceImpl, which is responsible for the processing of
 * the user requests (sqls and session api). It inherits the basic procedures from TSServiceImpl,
 * but redirect the queries of data and metadata to a MetaGroupMember of the local node.
 */
public class ClientServer extends TSServiceImpl {

  private static final Logger logger = LoggerFactory.getLogger(ClientServer.class);
  /**
   * The Coordinator of the local node. Through this node ClientServer queries data and meta from
   * the cluster and performs data manipulations to the cluster.
   */
  private Coordinator coordinator;

  public void setCoordinator(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  /** The single thread pool that runs poolServer to unblock the main thread. */
  private ExecutorService serverService;

  /**
   * Using the poolServer, ClientServer will listen to a socket to accept thrift requests like an
   * HttpServer.
   */
  private TServer poolServer;

  /** The socket poolServer will listen to. Async service requires nonblocking socket */
  private TServerTransport serverTransport;

  /**
   * queryId -> queryContext map. When a query ends either normally or accidentally, the resources
   * used by the query can be found in the context and then released.
   */
  private Map<Long, RemoteQueryContext> queryContextMap = new ConcurrentHashMap<>();

  public ClientServer(MetaGroupMember metaGroupMember) throws QueryProcessException {
    super();
    this.processor = new ClusterPlanner();
    this.executor = new ClusterPlanExecutor(metaGroupMember);
  }

  /**
   * Create a thrift server to listen to the client port and accept requests from clients. This
   * server is run in a separate thread. Calling the method twice does not induce side effects.
   *
   * @throws TTransportException
   */
  public void start() throws TTransportException {
    if (serverService != null) {
      return;
    }

    serverService = Executors.newSingleThreadExecutor(r -> new Thread(r, "ClusterClientServer"));
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();

    // this defines how thrift parse the requests bytes to a request
    TProtocolFactory protocolFactory;
    if (IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()) {
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      protocolFactory = new TBinaryProtocol.Factory();
    }
    serverTransport =
        new TServerSocket(
            new InetSocketAddress(
                IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
                config.getClusterRpcPort()));
    // async service also requires nonblocking server, and HsHaServer is basically more efficient a
    // nonblocking server
    int maxConcurrentClientNum =
        Math.max(CommonUtils.getCpuCores(), config.getMaxConcurrentClientNum());
    TThreadPoolServer.Args poolArgs =
        new TThreadPoolServer.Args(serverTransport)
            .maxWorkerThreads(maxConcurrentClientNum)
            .minWorkerThreads(CommonUtils.getCpuCores());
    poolArgs.executorService(
        new ThreadPoolExecutor(
            poolArgs.minWorkerThreads,
            poolArgs.maxWorkerThreads,
            poolArgs.stopTimeoutVal,
            poolArgs.stopTimeoutUnit,
            new SynchronousQueue<>(),
            new ThreadFactory() {
              private AtomicLong threadIndex = new AtomicLong(0);

              @Override
              public Thread newThread(Runnable r) {
                return new Thread(r, "ClusterClient-" + threadIndex.incrementAndGet());
              }
            }));
    // ClientServer will do the following processing when the HsHaServer has parsed a request
    poolArgs.processor(new Processor<>(this));
    poolArgs.protocolFactory(protocolFactory);
    // nonblocking server requests FramedTransport
    poolArgs.transportFactory(RpcTransportFactory.INSTANCE);

    poolServer = new TThreadPoolServer(poolArgs);
    // mainly for handling client exit events
    poolServer.setServerEventHandler(new EventHandler());

    serverService.submit(() -> poolServer.serve());
    logger.info("Client service is set up");
  }

  /**
   * Stop the thrift server, close the socket and shutdown the thread pool. Calling the method twice
   * does not induce side effects.
   */
  public void stop() {
    if (serverService == null) {
      return;
    }

    poolServer.stop();
    serverService.shutdownNow();
    serverTransport.close();
  }

  /**
   * Redirect the plan to the local Coordinator so that it will be processed cluster-wide.
   *
   * @param plan
   * @return
   */
  @Override
  protected TSStatus executeNonQueryPlan(PhysicalPlan plan) {
    try {
      plan.checkIntegrity();
    } catch (QueryProcessException e) {
      logger.warn("Illegal plan detected： {}", plan);
      return RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR, e.getMessage());
    }
    return coordinator.executeNonQueryPlan(plan);
  }

  /**
   * EventHandler handles the preprocess and postprocess of the thrift requests, but it currently
   * only release resources when a client disconnects.
   */
  class EventHandler implements TServerEventHandler {

    @Override
    public void preServe() {
      // do nothing
    }

    @Override
    public ServerContext createContext(TProtocol input, TProtocol output) {
      return null;
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
      ClientServer.this.handleClientExit();
    }

    @Override
    public void processContext(
        ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
      // do nothing
    }
  }

  /**
   * Get the data types of each path in “paths”. If "aggregations" is not null, then it should be
   * corresponding to "paths" one to one and the data type will be the type of the aggregation over
   * the corresponding path.
   *
   * @param paths full timeseries paths
   * @param aggregations if not null, it should be the same size as "paths"
   * @return the data types of "paths" (using the aggregations)
   * @throws MetadataException
   */
  @Override
  protected List<TSDataType> getSeriesTypesByPaths(
      List<PartialPath> paths, List<String> aggregations) throws MetadataException {
    return ((CMManager) IoTDB.metaManager).getSeriesTypesByPath(paths, aggregations).left;
  }

  /**
   * Get the data types of each path in “paths”. If "aggregation" is not null, all "paths" will use
   * this aggregation.
   *
   * @param paths full timeseries paths
   * @param aggregation if not null, it means "paths" all use this aggregation
   * @return the data types of "paths" (using the aggregation)
   * @throws MetadataException
   */
  protected List<TSDataType> getSeriesTypesByString(List<PartialPath> paths, String aggregation)
      throws MetadataException {
    return ((CMManager) IoTDB.metaManager).getSeriesTypesByPaths(paths, aggregation).left;
  }

  /**
   * Generate and cache a QueryContext using "queryId". In the distributed version, the QueryContext
   * is a RemoteQueryContext.
   *
   * @param queryId
   * @return a RemoteQueryContext using queryId
   */
  @Override
  protected QueryContext genQueryContext(long queryId, boolean debug) {
    RemoteQueryContext context = new RemoteQueryContext(queryId, debug);
    queryContextMap.put(queryId, context);
    return context;
  }

  /**
   * Release the local and remote resources used by a query.
   *
   * @param queryId
   * @throws StorageEngineException
   */
  @Override
  protected void releaseQueryResource(long queryId) throws StorageEngineException {
    // release resources locally
    super.releaseQueryResource(queryId);
    // release resources remotely
    RemoteQueryContext context = queryContextMap.remove(queryId);
    if (context != null) {
      // release the resources in every queried node
      for (Entry<Node, Set<Node>> headerEntry : context.getQueriedNodesMap().entrySet()) {
        Node header = headerEntry.getKey();
        Set<Node> queriedNodes = headerEntry.getValue();

        for (Node queriedNode : queriedNodes) {
          GenericHandler<Void> handler = new GenericHandler<>(queriedNode, new AtomicReference<>());
          try {
            if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
              AsyncDataClient client =
                  coordinator.getAsyncDataClient(
                      queriedNode, RaftServer.getReadOperationTimeoutMS());
              client.endQuery(header, coordinator.getThisNode(), queryId, handler);
            } else {
              try (SyncDataClient syncDataClient =
                  coordinator.getSyncDataClient(
                      queriedNode, RaftServer.getReadOperationTimeoutMS())) {
                try {
                  syncDataClient.endQuery(header, coordinator.getThisNode(), queryId);
                } catch (TException e) {
                  // the connection may be broken, close it to avoid it being reused
                  syncDataClient.getInputProtocol().getTransport().close();
                  throw e;
                }
              }
            }
          } catch (IOException | TException e) {
            logger.error("Cannot end query {} in {}", queryId, queriedNode);
          }
        }
      }
    }
  }
}
