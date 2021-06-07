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
namespace java org.apache.iotdb.service.rpc.thrift
namespace py iotdb.thrift.rpc

struct EndPoint {
  1: required string ip
  2: required i32 port
}

// The return status code and message in each response.
struct TSStatus {
  1: required i32 code
  2: optional string message
  3: optional list<TSStatus> subStatus
  4: optional EndPoint redirectNode
}

struct TSQueryDataSet{
  // ByteBuffer for time column
  1: required binary time
  // ByteBuffer for each column values
  2: required list<binary> valueList
  // Bitmap for each column to indicate whether it is a null value
  3: required list<binary> bitmapList
}

struct TSQueryNonAlignDataSet{
  // ByteBuffer for each time column
  1: required list<binary> timeList
  // ByteBuffer for each column values
  2: required list<binary> valueList
}

struct TSExecuteStatementResp {
  1: required TSStatus status
  2: optional i64 queryId
  // Column names in select statement of SQL
  3: optional list<string> columns
  4: optional string operationType
  5: optional bool ignoreTimeStamp
  // Data type list of columns in select statement of SQL
  6: optional list<string> dataTypeList
  7: optional TSQueryDataSet queryDataSet
  // for disable align statements, queryDataSet is null and nonAlignQueryDataSet is not null
  8: optional TSQueryNonAlignDataSet nonAlignQueryDataSet
  9: optional map<string, i32> columnNameIndexMap
}

enum TSProtocolVersion {
  IOTDB_SERVICE_PROTOCOL_V1,
  IOTDB_SERVICE_PROTOCOL_V2,//V2 is the first version that we can check version compatibility
  IOTDB_SERVICE_PROTOCOL_V3,//V3 is incompatible with V2
}

struct TSOpenSessionResp {
  1: required TSStatus status

  // The protocol version that the server is using.
  2: required TSProtocolVersion serverProtocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1

  // Session id
  3: optional i64 sessionId

  // The configuration settings for this session.
  4: optional map<string, string> configuration
}

// OpenSession()
// Open a session (connection) on the server against which operations may be executed.
struct TSOpenSessionReq {
  1: required TSProtocolVersion client_protocol = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3
  2: required string zoneId
  3: optional string username
  4: optional string password
  5: optional map<string, string> configuration
}

// CloseSession()
// Closes the specified session and frees any resources currently allocated to that session.
// Any open operations in that session will be canceled.
struct TSCloseSessionReq {
  1: required i64 sessionId
}

// ExecuteStatement()
//
// Execute a statement.
// The returned OperationHandle can be used to check on the status of the statement, and to fetch results once the
// statement has finished executing.
struct TSExecuteStatementReq {
  // The session to execute the statement against
  1: required i64 sessionId

  // The statement to be executed (DML, DDL, SET, etc)
  2: required string statement

  // statementId
  3: required i64 statementId

  4: optional i32 fetchSize

  5: optional i64 timeout

  6: optional bool enableRedirectQuery;
}

struct TSExecuteBatchStatementReq{
  // The session to execute the statement against
  1: required i64 sessionId

  // The statements to be executed (DML, DDL, SET, etc)
  2: required list<string> statements
}

struct TSGetOperationStatusReq {
  1: required i64 sessionId
  // Session to run this request against
  2: required i64 queryId
}

// CancelOperation()
//
// Cancels processing on the specified operation handle and frees any resources which were allocated.
struct TSCancelOperationReq {
  1: required i64 sessionId
  // Operation to cancel
  2: required i64 queryId
}

// CloseOperation()
struct TSCloseOperationReq {
  1: required i64 sessionId
  2: optional i64 queryId
  3: optional i64 statementId
}

struct TSFetchResultsReq{
  1: required i64 sessionId
  2: required string statement
  3: required i32 fetchSize
  4: required i64 queryId
  5: required bool isAlign
  6: optional i64 timeout
}

struct TSFetchResultsResp{
  1: required TSStatus status
  2: required bool hasResultSet
  3: required bool isAlign
  4: optional TSQueryDataSet queryDataSet
  5: optional TSQueryNonAlignDataSet nonAlignQueryDataSet
}

struct TSFetchMetadataResp{
  1: required TSStatus status
  2: optional string metadataInJson
  3: optional list<string> columnsList
  4: optional string dataType
}

struct TSFetchMetadataReq{
  1: required i64 sessionId
  2: required string type
  3: optional string columnPath
}

struct TSGetTimeZoneResp {
  1: required TSStatus status
  2: required string timeZone
}

struct TSSetTimeZoneReq {
  1: required i64 sessionId
  2: required string timeZone
}

// for session
struct TSInsertRecordReq {
  1: required i64 sessionId
  2: required string prefixPath
  3: required list<string> measurements
  4: required binary values
  5: required i64 timestamp
  6: optional bool isAligned
}

struct TSInsertStringRecordReq {
  1: required i64 sessionId
  2: required string deviceId
  3: required list<string> measurements
  4: required list<string> values
  5: required i64 timestamp
}

struct TSInsertTabletReq {
  1: required i64 sessionId
  2: required string prefixPath
  3: required list<string> measurements
  4: required binary values
  5: required binary timestamps
  6: required list<i32> types
  7: required i32 size
  8: optional bool isAligned
}

struct TSInsertTabletsReq {
  1: required i64 sessionId
  2: required list<string> deviceIds
  3: required list<list<string>> measurementsList
  4: required list<binary> valuesList
  5: required list<binary> timestampsList
  6: required list<list<i32>> typesList
  7: required list<i32> sizeList
}

struct TSInsertRecordsReq {
  1: required i64 sessionId
  2: required list<string> deviceIds
  3: required list<list<string>> measurementsList
  4: required list<binary> valuesList
  5: required list<i64> timestamps
}

struct TSInsertRecordsOfOneDeviceReq {
    1: required i64 sessionId
    2: required string deviceId
    3: required list<list<string>> measurementsList
    4: required list<binary> valuesList
    5: required list<i64> timestamps
}

struct TSInsertSinglePointReq {
  1: required i64 sessionId
  2: required string deviceId
  3: required string measurement
  4: required binary value
  5: required i64 timestamp
}

struct TSInsertStringSinglePointReq {
  1: required i64 sessionId
  2: required string deviceId
  3: required string measurement
  4: required string value
  5: required i64 timestamp
}

struct TSInsertStringRecordsReq {
  1: required i64 sessionId
  2: required list<string> deviceIds
  3: required list<list<string>> measurementsList
  4: required list<list<string>> valuesList
  5: required list<i64> timestamps
}

struct TSDeleteDataReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: required i64 startTime
  4: required i64 endTime
}

struct TSCreateTimeseriesReq {
  1: required i64 sessionId
  2: required string path
  3: required i32 dataType
  4: required i32 encoding
  5: required i32 compressor
  6: optional map<string, string> props
  7: optional map<string, string> tags
  8: optional map<string, string> attributes
  9: optional string measurementAlias
}

struct TSCreateAlignedTimeseriesReq {
  1: required i64 sessionId
  2: required string prefixPath
  3: required list<string> measurements
  4: required list<i32> dataTypes
  5: required list<i32> encodings
  6: required i32 compressor
  7: optional list<string> measurementAlias
}

struct TSRawDataQueryReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: optional i32 fetchSize
  4: required i64 startTime
  5: required i64 endTime
  6: required i64 statementId
  7: optional bool enableRedirectQuery;
}

struct TSLastDataQueryReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: optional i32 fetchSize
  4: required i64 time
  5: required i64 statementId
  6: optional bool enableRedirectQuery;
}

struct TSCreateMultiTimeseriesReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: required list<i32> dataTypes
  4: required list<i32> encodings
  5: required list<i32> compressors
  6: optional list<map<string, string>> propsList
  7: optional list<map<string, string>> tagsList
  8: optional list<map<string, string>> attributesList
  9: optional list<string> measurementAliasList
}

struct ServerProperties {
  1: required string version;
  2: required list<string> supportedTimeAggregationOperations;
  3: required string timestampPrecision;
}

struct TSSetSchemaTemplateReq {
  1: required i64 sessionId
  2: required string templateName
  3: required string prefixPath
}

struct TSCreateSchemaTemplateReq {
  1: required i64 sessionId
  2: required string name
  3: required list<string> schemaNames
  4: required list<list<string>> measurements
  5: required list<list<i32>> dataTypes
  6: required list<list<i32>> encodings
  7: required list<i32> compressors
}

service TSIService {
  TSOpenSessionResp openSession(1:TSOpenSessionReq req);

  TSStatus closeSession(1:TSCloseSessionReq req);

  TSExecuteStatementResp executeStatement(1:TSExecuteStatementReq req);

  TSStatus executeBatchStatement(1:TSExecuteBatchStatementReq req);

  TSExecuteStatementResp executeQueryStatement(1:TSExecuteStatementReq req);

  TSExecuteStatementResp executeUpdateStatement(1:TSExecuteStatementReq req);

  TSFetchResultsResp fetchResults(1:TSFetchResultsReq req)

  TSFetchMetadataResp fetchMetadata(1:TSFetchMetadataReq req)

  TSStatus cancelOperation(1:TSCancelOperationReq req);

  TSStatus closeOperation(1:TSCloseOperationReq req);

  TSGetTimeZoneResp getTimeZone(1:i64 sessionId);

  TSStatus setTimeZone(1:TSSetTimeZoneReq req);

  ServerProperties getProperties();

  TSStatus setStorageGroup(1:i64 sessionId, 2:string storageGroup);

  TSStatus createTimeseries(1:TSCreateTimeseriesReq req);

  TSStatus createAlignedTimeseries(1:TSCreateAlignedTimeseriesReq req);

  TSStatus createMultiTimeseries(1:TSCreateMultiTimeseriesReq req);

  TSStatus deleteTimeseries(1:i64 sessionId, 2:list<string> path)

  TSStatus deleteStorageGroups(1:i64 sessionId, 2:list<string> storageGroup);

  TSStatus insertRecord(1:TSInsertRecordReq req);

  TSStatus insertStringRecord(1:TSInsertStringRecordReq req);

  TSStatus insertTablet(1:TSInsertTabletReq req);

  TSStatus insertTablets(1:TSInsertTabletsReq req);

  TSStatus insertRecords(1:TSInsertRecordsReq req);

  TSStatus insertSinglePoint(1:TSInsertSinglePointReq req);

  TSStatus insertStringSinglePoint(1:TSInsertStringSinglePointReq req);

  TSStatus insertRecordsOfOneDevice(1:TSInsertRecordsOfOneDeviceReq req);

  TSStatus insertStringRecords(1:TSInsertStringRecordsReq req);

  TSStatus testInsertTablet(1:TSInsertTabletReq req);

  TSStatus testInsertTablets(1:TSInsertTabletsReq req);

  TSStatus testInsertRecord(1:TSInsertRecordReq req);

  TSStatus testInsertStringRecord(1:TSInsertStringRecordReq req);

  TSStatus testInsertRecords(1:TSInsertRecordsReq req);

  TSStatus testInsertSinglePoint(1:TSInsertSinglePointReq req);

  TSStatus testInsertStringSinglePoint(1:TSInsertStringSinglePointReq req);

  TSStatus testInsertRecordsOfOneDevice(1:TSInsertRecordsOfOneDeviceReq req);

  TSStatus testInsertStringRecords(1:TSInsertStringRecordsReq req);

  TSStatus deleteData(1:TSDeleteDataReq req);

  TSExecuteStatementResp executeRawDataQuery(1:TSRawDataQueryReq req);

  TSExecuteStatementResp executeLastDataQuery(1:TSLastDataQueryReq req);

  i64 requestStatementId(1:i64 sessionId);

  TSStatus createSchemaTemplate(1:TSCreateSchemaTemplateReq req);

  TSStatus setSchemaTemplate(1:TSSetSchemaTemplateReq req);
}
