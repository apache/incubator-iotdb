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

// The return status code and message in each response.
struct TSStatusType {
  1: required i32 code
  2: required string message
}

// The return status of a remote request
struct TSStatus {
  1: required TSStatusType statusType
  2: optional list<string> infoMessages
  3: optional string sqlState  // as defined in the ISO/IEF CLIENT specification
}

struct TSHandleIdentifier {
  // 16 byte globally unique identifier This is the public ID of the handle and can be used for reporting.
  // In current version, it is not used.
  1: required binary guid,

  // 16 byte secret generated by the server and used to verify that the handle is not being hijacked by another user.
  // In current version, it is not used.
  2: required binary secret,

  // unique identifier in session. This is a ID to identify a query in one session.
  3: required i64 queryId,
}

// Client-side reference to a task running asynchronously on the server.
struct TSOperationHandle {
  1: required TSHandleIdentifier operationId

  // If hasResultSet = TRUE, then this operation
  // generates a result set that can be fetched.
  // Note that the result set may be empty.
  //
  // If hasResultSet = FALSE, then this operation
  // does not generate a result set, and calling
  // GetResultSetMetadata or FetchResults against
  // this OperationHandle will generate an error.
  2: required bool hasResultSet
}

struct TSExecuteStatementResp {
	1: required TSStatus status
	2: optional TSOperationHandle operationHandle
  // Column names in select statement of SQL
	3: optional list<string> columns
	4: optional string operationType
	5: optional bool ignoreTimeStamp
  // Data type list of columns in select statement of SQL
  6: optional list<string> dataTypeList
}

enum TSProtocolVersion {
  IOTDB_SERVICE_PROTOCOL_V1,
}

// Client-side handle to persistent session information on the server-side.
// In current version, it is not used.
struct TS_SessionHandle {
  1: required TSHandleIdentifier sessionId
}


struct TSOpenSessionResp {
  1: required TSStatus status

  // The protocol version that the server is using.
  2: required TSProtocolVersion serverProtocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1

  // Session Handle
  3: optional TS_SessionHandle sessionHandle

  // The configuration settings for this session.
  4: optional map<string, string> configuration
}

// OpenSession()
// Open a session (connection) on the server against which operations may be executed.
struct TSOpenSessionReq {
  1: required TSProtocolVersion client_protocol = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V1
  2: optional string username
  3: optional string password
  4: optional map<string, string> configuration
}

// CloseSession()
// Closes the specified session and frees any resources currently allocated to that session.
// Any open operations in that session will be canceled.
struct TSCloseSessionReq {
  1: required TS_SessionHandle sessionHandle
}

// ExecuteStatement()
//
// Execute a statement.
// The returned OperationHandle can be used to check on the status of the statement, and to fetch results once the
// statement has finished executing.
struct TSExecuteStatementReq {
  // The session to execute the statement against
  1: required TS_SessionHandle sessionHandle

  // The statement to be executed (DML, DDL, SET, etc)
  2: required string statement

  // statementId
  3: required i64 statementId
}

struct TSExecuteInsertRowInBatchResp{
	1: required list<TSStatus> statusList
}

struct TSExecuteBatchStatementResp{
	1: required TSStatus status
  // For each value in result, Statement.SUCCESS_NO_INFO represents success, Statement.EXECUTE_FAILED represents fail otherwise.
	2: optional list<i32> result
}

struct TSExecuteBatchStatementReq{
  // The session to execute the statement against
  1: required TS_SessionHandle sessionHandle

  // The statements to be executed (DML, DDL, SET, etc)
  2: required list<string> statements
}


struct TSGetOperationStatusReq {
  // Session to run this request against
  1: required TSOperationHandle operationHandle
}

// CancelOperation()
//
// Cancels processing on the specified operation handle and frees any resources which were allocated.
struct TSCancelOperationReq {
  // Operation to cancel
  1: required TSOperationHandle operationHandle
}

// CloseOperation()
struct TSCloseOperationReq {
  1: required TSOperationHandle operationHandle
  2: required i64 queryId
  3: optional i64 stmtId
}

struct TSQueryDataSet{
   1: required binary values
   2: required i32 rowCount
}

struct TSFetchResultsReq{
	1: required string statement
	2: required i32 fetchSize
	3: required i64 queryId
}

struct TSFetchResultsResp{
	1: required TSStatus status
	2: required bool hasResultSet
	3: optional TSQueryDataSet queryDataSet
}

struct TSFetchMetadataResp{
		1: required TSStatus status
		2: optional string metadataInJson
		3: optional list<string> columnsList
		4: optional i32 timeseriesNum
		5: optional string dataType
		6: optional list<list<string>> timeseriesList
		7: optional set<string> storageGroups
		8: optional set<string> devices
		9: optional list<string> nodesList
		10: optional map<string, string> nodeTimeseriesNum
		11: optional set<string> childPaths
		12: optional string version
}

struct TSFetchMetadataReq{
		1: required string type
		2: optional string columnPath
		3: optional i32 nodeLevel
}

struct TSColumnSchema{
	1: optional string name;
	2: optional string dataType;
	3: optional string encoding;
	4: optional map<string, string> otherArgs;
}

struct TSGetTimeZoneResp {
    1: required TSStatus status
    2: required string timeZone
}

struct TSSetTimeZoneReq {
    1: required string timeZone
}

// for prepared statement
struct TSInsertionReq {
    1: optional string deviceId
    2: optional list<string> measurements
    3: optional list<string> values
    4: optional i64 timestamp
    5: required i64 stmtId
}

// for session
struct TSInsertReq {
    1: required string deviceId
    2: required list<string> measurements
    3: required list<string> values
    4: required i64 timestamp
}

struct TSBatchInsertionReq {
    1: required string deviceId
    2: required list<string> measurements
    3: required binary values
    4: required binary timestamps
    5: required list<i32> types
    6: required i32 size
}

struct TSInsertInBatchReq {
    1: required list<string> deviceIds
    2: required list<list<string>> measurementsList
    3: required list<list<string>> valuesList
    4: required list<i64> timestamps
}

struct TSDeleteDataReq {
    1: required list<string> paths
    2: required i64 timestamp
}

struct TSCreateTimeseriesReq {
  1: required string path
  2: required i32 dataType
  3: required i32 encoding
  4: required i32 compressor
}

struct ServerProperties {
	1: required string version;
	2: required list<string> supportedTimeAggregationOperations;
	3: required string timestampPrecision;
}

service TSIService {
	TSOpenSessionResp openSession(1:TSOpenSessionReq req);

	TSStatus closeSession(1:TSCloseSessionReq req);

	TSExecuteStatementResp executeStatement(1:TSExecuteStatementReq req);

	TSExecuteBatchStatementResp executeBatchStatement(1:TSExecuteBatchStatementReq req);

	TSExecuteStatementResp executeQueryStatement(1:TSExecuteStatementReq req);

	TSExecuteStatementResp executeUpdateStatement(1:TSExecuteStatementReq req);

	TSFetchResultsResp fetchResults(1:TSFetchResultsReq req)

	TSFetchMetadataResp fetchMetadata(1:TSFetchMetadataReq req)

	TSStatus cancelOperation(1:TSCancelOperationReq req);

	TSStatus closeOperation(1:TSCloseOperationReq req);

	TSGetTimeZoneResp getTimeZone();

	TSStatus setTimeZone(1:TSSetTimeZoneReq req);

	ServerProperties getProperties();

	TSExecuteStatementResp insert(1:TSInsertionReq req);

	TSExecuteBatchStatementResp insertBatch(1:TSBatchInsertionReq req);

	TSStatus setStorageGroup(1:string storageGroup);

	TSStatus createTimeseries(1:TSCreateTimeseriesReq req);

  TSStatus deleteTimeseries(1:list<string> path)

  TSStatus deleteStorageGroups(1:list<string> storageGroup);

	TSStatus insertRow(1:TSInsertReq req);

	TSExecuteInsertRowInBatchResp insertRowInBatch(1:TSInsertInBatchReq req);

	TSStatus deleteData(1:TSDeleteDataReq req);

	i64 requestStatementId();
}
