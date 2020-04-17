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

package org.apache.iotdb.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSIService.Client;
import org.apache.iotdb.service.rpc.thrift.TSIService.Client.Factory;
import org.apache.iotdb.service.rpc.thrift.TSInsertReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientMain {
  
  private static final Logger logger = LoggerFactory.getLogger(ClientMain.class);

  private static Map<String, TSStatus> failedQueries;

  private static final String[] STORAGE_GROUPS = new String[] {
      "root.beijing",
      "root.shanghai",
      "root.guangzhou",
      "root.shenzhen",
  };

  private static final String[] DEVICES = new String[] {
      "root.beijing.d1",
      "root.shanghai.d1",
      "root.guangzhou.d1",
      "root.shenzhen.d1",
  };

  private static final String[] MEASUREMENTS = new String[] {
      "s1"
  };

  private static final TSDataType[] DATA_TYPES = new TSDataType[] {
      TSDataType.DOUBLE
  };

  private static List<MeasurementSchema> schemas;

  private static final String[] DATA_QUERIES = new String[] {
      // raw data multi series
      "SELECT * FROM root",
      "SELECT * FROM root WHERE time <= 691200000",
      "SELECT * FROM root WHERE time >= 391200000 and time <= 691200000",
      "SELECT * FROM root.*.* WHERE s1 <= 0.7",
      // raw data single series
      "SELECT s1 FROM root.beijing.d1",
      "SELECT s1 FROM root.shanghai.d1",
      "SELECT s1 FROM root.guangzhou.d1",
      "SELECT s1 FROM root.shenzhen.d1",
      // aggregation
      "SELECT count(s1) FROM root.*.*",
      "SELECT avg(s1) FROM root.*.*",
      "SELECT sum(s1) FROM root.*.*",
      "SELECT max_value(s1) FROM root.*.*",
      "SELECT count(s1) FROM root.*.* where time <= 691200000",
      "SELECT count(s1) FROM root.*.* where s1 <= 0.7",
      // group by device
      "SELECT * FROM root GROUP BY DEVICE",
      // fill
      "SELECT s1 FROM root.beijing.d1 WHERE time = 86400000 FILL (DOUBLE[PREVIOUS,1d])",
      "SELECT s1 FROM root.shanghai.d1 WHERE time = 86400000 FILL (DOUBLE[LINEAR,1d,1d])",
      "SELECT s1 FROM root.guangzhou.d1 WHERE time = 126400000 FILL (DOUBLE[PREVIOUS,1d])",
      "SELECT s1 FROM root.shenzhen.d1 WHERE time = 126400000 FILL (DOUBLE[LINEAR,1d,1d])",
      // group by
      "SELECT COUNT(*) FROM root.*.* GROUP BY ([0, 864000000), 3d, 3d)",
      "SELECT AVG(*) FROM root.*.* WHERE s1 <= 0.7 GROUP BY ([0, 864000000), 3d, 3d)"
  };

  private static String[] META_QUERY = new String[] {
      "SHOW STORAGE GROUP",
      "SHOW TIMESERIES root",
      "COUNT TIMESERIES root",
      "COUNT TIMESERIES root GROUP BY LEVEL=10",
      "SHOW DEVICES",
  };

  public static void main(String[] args)
      throws TException, StatementExecutionException, IoTDBConnectionException {
    failedQueries = new HashMap<>();
    prepareSchema();

    String ip = "127.0.0.1";
    int port = 55560;
    TSIService.Client.Factory factory = new Factory();
    TTransport transport = new TFramedTransport(new TSocket(ip, port));
    transport.open();

    Client client = factory.getClient(new TCompactProtocol(transport));

    TSOpenSessionReq openReq = new TSOpenSessionReq(TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V2);

    openReq.setUsername("root");
    openReq.setPassword("root");
    TSOpenSessionResp openResp = client.openSession(openReq);
    long sessionId = openResp.getSessionId();

    System.out.println("Test insertion");
    testInsertion(client, sessionId);

    System.out.println("Test data queries");
    testQuery(client, sessionId, DATA_QUERIES);

    System.out.println("Test metadata queries");
    testQuery(client, sessionId, META_QUERY);

    client.closeSession(new TSCloseSessionReq(openResp.getSessionId()));

    logger.info("Failed queries: {}", failedQueries);
  }

  private static void prepareSchema() {
    schemas = new ArrayList<>();
    for (String device : DEVICES) {
      for (int i = 0; i < MEASUREMENTS.length; i++) {
        String measurement = MEASUREMENTS[i];
        schemas.add(new MeasurementSchema(device + IoTDBConstant.PATH_SEPARATOR + measurement,
            DATA_TYPES[i]));
      }
    }
  }

  private static void testQuery(Client client, long sessionId, String[] queries)
      throws TException, StatementExecutionException, IoTDBConnectionException {
    long statementId = client.requestStatementId(sessionId);
    for (String dataQuery : queries) {
      executeQuery(client, sessionId, dataQuery, statementId);
    }

    TSCloseOperationReq tsCloseOperationReq = new TSCloseOperationReq(sessionId);
    tsCloseOperationReq.setStatementId(statementId);
    client.closeOperation(tsCloseOperationReq);
  }

  private static void executeQuery(Client client, long sessionId, String query, long statementId)
      throws TException, StatementExecutionException, IoTDBConnectionException {
    logger.info("{" + query + "}");
    TSExecuteStatementResp resp = client
        .executeQueryStatement(new TSExecuteStatementReq(sessionId, query, statementId));
    if (resp.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      failedQueries.put(query, resp.status);
      return;
    }

    long queryId = resp.getQueryId();
    logger.info(resp.columns.toString());

    SessionDataSet dataSet = new SessionDataSet(query, resp.getColumns(),
        resp.getDataTypeList(), queryId, client, sessionId, resp.queryDataSet);

    while (dataSet.hasNext()) {
      logger.info(dataSet.next().toString());
    }
    System.out.println();

    TSCloseOperationReq tsCloseOperationReq = new TSCloseOperationReq(sessionId);
    tsCloseOperationReq.setQueryId(queryId);
    client.closeOperation(tsCloseOperationReq);
  }



  private static void testInsertion(Client client, long sessionId) throws TException {
    for (String storageGroup : STORAGE_GROUPS) {
      logger.info(client.setStorageGroup(sessionId,storageGroup ).toString());
    }

    TSCreateTimeseriesReq req = new TSCreateTimeseriesReq();
    req.setSessionId(sessionId);
    for (MeasurementSchema schema : schemas) {
      req.setDataType(schema.getType().ordinal());
      req.setEncoding(schema.getEncodingType().ordinal());
      req.setCompressor(schema.getCompressor().ordinal());
      req.setPath(schema.getMeasurementId());
      logger.info(client.createTimeseries(req).toString());
    }

    TSInsertReq insertReq = new TSInsertReq();
    insertReq.setMeasurements(Arrays.asList(MEASUREMENTS));
    insertReq.setSessionId(sessionId);
    String[] values = new String[MEASUREMENTS.length];
    for (int i = 0; i < 10; i ++) {
      insertReq.setTimestamp(i * 24 * 3600 * 1000L);
      for (int i1 = 0; i1 < values.length; i1++) {
        switch (DATA_TYPES[i1]) {
          case DOUBLE:
            values[i1] = Double.toString(i * 0.1);
            break;
          case BOOLEAN:
            values[i1] = Boolean.toString(i % 2 == 0);
            break;
          case INT64:
            values[i1] = Long.toString(i);
            break;
          case INT32:
            values[i1] = Integer.toString(i);
            break;
          case FLOAT:
            values[i1] = Float.toString(i * 0.1f);
            break;
          case TEXT:
            values[i1] = "S" + i;
            break;
        }
      }
      insertReq.setValues(Arrays.asList(values));
      for (String device : DEVICES) {
        insertReq.setDeviceId(device);
        logger.info(insertReq.toString());
        logger.info(client.insert(insertReq).toString());
      }
    }
  }

}
