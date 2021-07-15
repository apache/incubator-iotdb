/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Apache.IoTDB.DataStructure;
using NLog;
using NLog.Config;
using NLog.Targets;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Apache.IoTDB
{
    public class SessionPool
    {
        private static int SuccessCode => 200;
        private static readonly TSProtocolVersion ProtocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
        
        private readonly string _username;
        private readonly string _password;
        private string _zoneId;
        private readonly string _host;
        private readonly int _port;
        private readonly int _fetchSize;
        private readonly int _poolSize = 4;
        private readonly Utils _utilFunctions = new Utils();

        
        private bool _debugMode;
        private bool _isClose = true;
        private ConcurrentClientQueue _clients;
        private Logger _logger;

        public SessionPool(string host, int port, int poolSize)
        {
            // init success code 
            _host = host;
            _port = port;
            _username = "root";
            _password = "root";
            _zoneId = "UTC+08:00";
            _fetchSize = 1024;
            _poolSize = poolSize;
        }

        public SessionPool(
            string host, 
            int port, 
            string username, 
            string password, 
            int poolSize = 8)
        {
            _host = host;
            _port = port;
            _password = password;
            _username = username;
            _zoneId = "UTC+08:00";
            _fetchSize = 1024;
            _debugMode = false;
            _poolSize = poolSize;
        }

        public SessionPool(
            string host, 
            int port, 
            string username, 
            string password, 
            int fetchSize, 
            int poolSize = 8)
        {
            _host = host;
            _port = port;
            _username = username;
            _password = password;
            _fetchSize = fetchSize;
            _zoneId = "UTC+08:00";
            _debugMode = false;
            _poolSize = poolSize;
        }

        public SessionPool(
            string host, 
            int port, 
            string username = "root", 
            string password = "root",
            int fetchSize = 1000, 
            string zoneId = "UTC+08:00", 
            int poolSize = 8)
        {
            _host = host;
            _port = port;
            _username = username;
            _password = password;
            _zoneId = zoneId;
            _fetchSize = fetchSize;
            _debugMode = false;
            _poolSize = poolSize;
        }

        public void OpenDebugMode(LoggingConfiguration config = null)
        {
            _debugMode = true;
            if (config == null)
            {
                config = new LoggingConfiguration();
                config.AddRule(LogLevel.Debug, LogLevel.Fatal, new ConsoleTarget("logconsole"));
            }
            
            LogManager.Configuration = config;
            _logger = LogManager.GetCurrentClassLogger();
        }

        public void CloseDebugMode()
        {
            _debugMode = false;
        }

        public async Task Open(bool enableRpcCompression)
        {
            _clients = new ConcurrentClientQueue();
            
            for (var index = 0; index < _poolSize; index++)
            {
                _clients.Add(await CreateAndOpen(enableRpcCompression));
            }
        }

        public bool IsOpen() => !_isClose;

        public async Task Close()
        {
            if (_isClose)
            {
                return;
            }

            foreach (var client in _clients.ClientQueue.AsEnumerable())
            {
                var closeSessionRequest = new TSCloseSessionReq(client.SessionId);
                try
                {
                    await client.ServiceClient.closeSessionAsync(closeSessionRequest);
                }
                catch (TException e)
                {
                    throw new TException("Error occurs when closing session at server. Maybe server is down", e);
                }
                finally
                {
                    _isClose = true;
                    
                    client.Transport?.Close();
                }
            }
        }

        public async Task SetTimeZone(string zoneId)
        {
            _zoneId = zoneId;
            
            foreach (var client in _clients.ClientQueue.AsEnumerable())
            {
                var req = new TSSetTimeZoneReq(client.SessionId, zoneId);
                try
                {
                    var resp = await client.ServiceClient.setTimeZoneAsync(req);
                    if (_debugMode)
                    {
                        _logger.Info("setting time zone_id as {0}, server message:{1}", zoneId, resp.Message);
                    }
                }
                catch (TException e)
                {
                    throw new TException("could not set time zone", e);
                }
            }
        }

        public async Task<string> GetTimeZone()
        {
            if (_zoneId != "")
            {
                return _zoneId;
            }

            var client = _clients.Take();
            
            try
            {
                var response = await client.ServiceClient.getTimeZoneAsync(client.SessionId);

                return response?.TimeZone;
            }
            catch (TException e)
            {
                throw new TException("could not get time zone", e);
            }
            finally
            {
                _clients.Add(client); 
            }
        }

        private async Task<Client> CreateAndOpen(bool enableRpcCompression)
        {
            var tcpClient = new TcpClient(_host, _port);

            var transport = new TFramedTransport(new TSocketTransport(tcpClient, null));
            
            if (!transport.IsOpen)
            {
                await transport.OpenAsync(new CancellationToken());
            }

            var client = enableRpcCompression ? 
                new TSIService.Client(new TCompactProtocol(transport)) : 
                new TSIService.Client(new TBinaryProtocol(transport));

            var openReq = new TSOpenSessionReq(ProtocolVersion, _zoneId)
            {
                Username = _username, 
                Password = _password
            };
            
            try
            {
                var openResp = await client.openSessionAsync(openReq);
                
                if (openResp.ServerProtocolVersion != ProtocolVersion)
                {
                    throw new TException($"Protocol Differ, Client version is {ProtocolVersion} but Server version is {openResp.ServerProtocolVersion}", null);
                }

                if (openResp.ServerProtocolVersion == 0)
                {
                    throw new TException("Protocol not supported", null);
                }

                var sessionId = openResp.SessionId;
                var statementId = await client.requestStatementIdAsync(sessionId);
                
                _isClose = false;
            
                var returnClient = new Client(
                    client, 
                    sessionId, 
                    statementId, 
                    transport);
            
                return returnClient;
            }
            catch (Exception)
            {
                transport.Close();
                
                throw;
            }
        }

        public async Task<int> SetStorageGroup(string groupName)
        {
            var client = _clients.Take();
            
            try
            {
                var status = await client.ServiceClient.setStorageGroupAsync(client.SessionId, groupName);
                
                if (_debugMode)
                {
                    _logger.Info("set storage group {0} successfully, server message is {1}", groupName, status.Message);
                }
                
                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException($"set storage group {groupName} failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> CreateTimeSeries(
            string tsPath, 
            TSDataType dataType, 
            TSEncoding encoding,
            Compressor compressor)
        {
            var client = _clients.Take();
            var req = new TSCreateTimeseriesReq(
                client.SessionId, 
                tsPath, 
                (int) dataType, 
                (int) encoding,
                (int) compressor);
            try
            {
                var status = await client.ServiceClient.createTimeseriesAsync(req);
                
                if (_debugMode)
                {
                    _logger.Info("creating time series {0} successfully, server message is {1}", tsPath, status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException($"create time series {tsPath} failed", e);
            }
            finally
            {
                _clients.Add(client);
            }

        }

        public async Task<int> DeleteStorageGroupAsync(string groupName)
        {
            var client = _clients.Take();
            try
            {
                var status = await client.ServiceClient.deleteStorageGroupsAsync(
                    client.SessionId,
                    new List<string> {groupName});
                
                if (_debugMode)
                {
                    _logger.Info($"delete storage group {groupName} successfully, server message is {status?.Message}");
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException($"delete storage group {groupName} failed", e);
            }
            finally
            {
                _clients.Add(client); 
            }
        }

        public async Task<int> DeleteStorageGroupsAsync(List<string> groupNames)
        {
            var client = _clients.Take();

            try
            {
                var status = await client.ServiceClient.deleteStorageGroupsAsync(client.SessionId, groupNames);
                
                if (_debugMode)
                {
                    _logger.Info(
                        "delete storage group(s) {0} successfully, server message is {1}", 
                        groupNames,
                        status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException($"delete storage group(s) {groupNames} failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> CreateMultiTimeSeriesAsync(
            List<string> tsPathLst, 
            List<TSDataType> dataTypeLst,
            List<TSEncoding> encodingLst, 
            List<Compressor> compressorLst)
        {
            var client = _clients.Take();
            var dataTypes = dataTypeLst.ConvertAll(x => (int) x);
            var encodings = encodingLst.ConvertAll(x => (int) x);
            var compressors = compressorLst.ConvertAll(x => (int) x);

            var req = new TSCreateMultiTimeseriesReq(client.SessionId, tsPathLst, dataTypes, encodings, compressors);
            
            try
            {
                var status = await client.ServiceClient.createMultiTimeseriesAsync(req);
                
                if (_debugMode)
                {
                    _logger.Info("creating multiple time series {0}, server message is {1}", tsPathLst, status.Message);
                }
                
                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException($"create multiple time series {tsPathLst} failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> DeleteTimeSeriesAsync(List<string> pathList)
        {
            var client = _clients.Take();
            
            try
            {
                var status = await client.ServiceClient.deleteTimeseriesAsync(client.SessionId, pathList);
                
                if (_debugMode)
                {
                    _logger.Info("deleting multiple time series {0}, server message is {1}", pathList, status.Message);
                }
                
                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException($"delete time series {pathList} failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> DeleteTimeSeriesAsync(string tsPath)
        {
            return await DeleteTimeSeriesAsync(new List<string> {tsPath});
        }

        public async Task<bool> CheckTimeSeriesExistsAsync(string tsPath)
        {
            // TBD by dalong
            try
            {
                var sql = "SHOW TIMESERIES " + tsPath;
                var sessionDataset = await ExecuteQueryStatementAsync(sql);
                return sessionDataset.has_next();
            }
            catch (TException e)
            {
                throw new TException("could not check if certain time series exists", e);
            }
        }

        public async Task<int> DeleteDataAsync(List<string> tsPathLst, long startTime, long endTime)
        {
            var client = _clients.Take();
            var req = new TSDeleteDataReq(client.SessionId, tsPathLst, startTime, endTime);
            
            try
            {
                var status = await client.ServiceClient.deleteDataAsync(req);
                
                if (_debugMode)
                {
                    _logger.Info(
                        "delete data from {0}, server message is {1}",
                        tsPathLst, 
                        status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("data deletion fails because", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> InsertRecordAsync(string deviceId, RowRecord record)
        {
            // TBD by Luzhan
            var client = _clients.Take();
            var req = new TSInsertRecordReq(client.SessionId, deviceId, record.Measurements, record.ToBytes(),
                record.Timestamps);
            try
            {
                var status = await client.ServiceClient.insertRecordAsync(req);

                if (_debugMode)
                {
                    _logger.Info("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("Record insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public TSInsertStringRecordReq GenInsertStrRecordReq(string deviceId, List<string> measurements,
            List<string> values, long timestamp, long sessionId)
        {
            if (values.Count() != measurements.Count())
            {
                throw new TException("length of data types does not equal to length of values!", null);
            }

            return new TSInsertStringRecordReq(sessionId, deviceId, measurements, values, timestamp);
        }

        public TSInsertRecordsReq GenInsertRecordsReq(List<string> deviceId, List<RowRecord> rowRecords,
            long sessionId)
        {
            //TODO
            var measurementLst = rowRecords.Select(x => x.Measurements).ToList();
            var timestampLst = rowRecords.Select(x => x.Timestamps).ToList();
            var valuesLstInBytes = rowRecords.Select(row => row.ToBytes()).ToList();

            return new TSInsertRecordsReq(sessionId, deviceId, measurementLst, valuesLstInBytes, timestampLst);
        }

        public async Task<int> InsertRecordsAsync(List<string> deviceId, List<RowRecord> rowRecords)
        {
            var client = _clients.Take();
            
            var request = GenInsertRecordsReq(deviceId, rowRecords, client.SessionId);

            try
            {
                var status = await client.ServiceClient.insertRecordsAsync(request);
                
                if (_debugMode)
                {
                    _logger.Info("insert multiple records to devices {0}, server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("Multiple records insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public TSInsertTabletReq GenInsertTabletReq(Tablet tablet, long sessionId)
        {
            return new TSInsertTabletReq(
                sessionId, 
                tablet.DeviceId, 
                tablet.Measurements,
                tablet.get_binary_values(), 
                tablet.get_binary_timestamps(), 
                tablet.get_data_types(), 
                tablet.RowNumber);
        }

        public async Task<int> InsertTabletAsync(Tablet tablet)
        {
            var client = _clients.Take();
            var req = GenInsertTabletReq(tablet, client.SessionId);
            
            try
            {
                var status = await client.ServiceClient.insertTabletAsync(req);
                
                if (_debugMode)
                {
                    _logger.Info("insert one tablet to device {0}, server message: {1}", tablet.DeviceId, status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("Tablet insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public TSInsertTabletsReq GenInsertTabletsReq(List<Tablet> tabletLst, long sessionId)
        {
            var deviceIdLst = new List<string>();
            var measurementsLst = new List<List<string>>();
            var valuesLst = new List<byte[]>();
            var timestampsLst = new List<byte[]>();
            var typeLst = new List<List<int>>();
            var sizeLst = new List<int>();
            
            foreach (var tablet in tabletLst)
            {
                var dataTypeValues = tablet.get_data_types();
                deviceIdLst.Add(tablet.DeviceId);
                measurementsLst.Add(tablet.Measurements);
                valuesLst.Add(tablet.get_binary_values());
                timestampsLst.Add(tablet.get_binary_timestamps());
                typeLst.Add(dataTypeValues);
                sizeLst.Add(tablet.RowNumber);
            }

            return new TSInsertTabletsReq(
                sessionId, 
                deviceIdLst, 
                measurementsLst, 
                valuesLst, 
                timestampsLst,
                typeLst, 
                sizeLst);
        }

        public async Task<int> InsertTabletsAsync(List<Tablet> tabletLst)
        {
            var client = _clients.Take();
            var req = GenInsertTabletsReq(tabletLst, client.SessionId);
            
            try
            {
                var status = await client.ServiceClient.insertTabletsAsync(req);
                
                if (_debugMode)
                {
                    _logger.Info("insert multiple tablets, message: {0}", status.Message);
                }
                
                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                _clients.Add(client);

                throw new TException("Multiple tablets insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> InsertRecordsOfOneDeviceAsync(string deviceId, List<RowRecord> rowRecords)
        {
            var sortedRowRecords = rowRecords.OrderBy(x => x.Timestamps).ToList();
            return await InsertRecordsOfOneDeviceSortedAsync(deviceId, sortedRowRecords);
        }

        private TSInsertRecordsOfOneDeviceReq GenInsertRecordsOfOneDeviceRequest(
            string deviceId,
            List<RowRecord> records, 
            long sessionId)
        {
            var values = records.Select(row => row.ToBytes());
            var measurementsLst = records.Select(x => x.Measurements).ToList();
            var timestampLst = records.Select(x => x.Timestamps).ToList();
            
            return new TSInsertRecordsOfOneDeviceReq(
                sessionId, 
                deviceId, 
                measurementsLst, 
                values.ToList(),
                timestampLst);
        }

        public async Task<int> InsertRecordsOfOneDeviceSortedAsync(string deviceId, List<RowRecord> rowRecords)
        {
            var client = _clients.Take();
            
            var timestampLst = rowRecords.Select(x => x.Timestamps).ToList();
            
            if (!_utilFunctions.IsSorted(timestampLst))
            {
                throw new TException("insert records of one device error: timestamp not sorted", null);
            }

            var req = GenInsertRecordsOfOneDeviceRequest(deviceId, rowRecords, client.SessionId);
            
            try
            {
                var status = await client.ServiceClient.insertRecordsOfOneDeviceAsync(req);

                if (_debugMode)
                {
                    _logger.Info("insert records of one device, message: {0}", status.Message);
                }
                
                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("Sorted records of one device insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> TestInsertRecordAsync(string deviceId, RowRecord record)
        {
            var client = _clients.Take();
            
            var req = new TSInsertRecordReq(
                client.SessionId, 
                deviceId, 
                record.Measurements, 
                record.ToBytes(),
                record.Timestamps);

            try
            {
                var status = await client.ServiceClient.testInsertRecordAsync(req);
                
                if (_debugMode)
                {
                    _logger.Info("insert one record to device {0}， server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("Record insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> TestInsertRecordsAsync(List<string> deviceId, List<RowRecord> rowRecords)
        {
            var client = _clients.Take();
            var req = GenInsertRecordsReq(deviceId, rowRecords, client.SessionId);

            try
            {
                var status = await client.ServiceClient.testInsertRecordsAsync(req);
                
                if (_debugMode)
                {
                    _logger.Info("insert multiple records to devices {0}, server message: {1}", deviceId, status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("Multiple records insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> TestInsertTabletAsync(Tablet tablet)
        {
            var client = _clients.Take();
            
            var req = GenInsertTabletReq(tablet, client.SessionId);

            try
            {
                var status = await client.ServiceClient.testInsertTabletAsync(req);

                if (_debugMode)
                {
                    _logger.Info("insert one tablet to device {0}, server message: {1}", tablet.DeviceId,
                        status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("Tablet insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<int> TestInsertTabletsAsync(List<Tablet> tabletLst)
        {
            var client = _clients.Take();
            
            var req = GenInsertTabletsReq(tabletLst, client.SessionId);
            
            try
            {
                var status = await client.ServiceClient.testInsertTabletsAsync(req);

                if (_debugMode)
                {
                    _logger.Info("insert multiple tablets, message: {0}", status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("Multiple tablets insertion failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }

        public async Task<SessionDataSet> ExecuteQueryStatementAsync(string sql)
        {
            TSExecuteStatementResp resp;
            TSStatus status;
            var client = _clients.Take();
            var req = new TSExecuteStatementReq(client.SessionId, sql, client.StatementId)
            {
                FetchSize = _fetchSize
            };
            try
            {
                resp = await client.ServiceClient.executeQueryStatementAsync(req);
                status = resp.Status;
            }
            catch (TException e)
            {
                _clients.Add(client);
                
                throw new TException("could not execute query statement", e);
            }

            if (_utilFunctions.verify_success(status, SuccessCode) == -1)
            {
                _clients.Add(client);
                
                throw new TException("execute query failed", null);
            }

            _clients.Add(client);

            var sessionDataset = new SessionDataSet(sql, resp, _clients)
            {
                FetchSize = _fetchSize
            };
            
            return sessionDataset;
        }

        public async Task<int> ExecuteNonQueryStatementAsync(string sql)
        {
            var client = _clients.Take();
            var req = new TSExecuteStatementReq(client.SessionId, sql, client.StatementId);
            
            try
            {
                var resp = await client.ServiceClient.executeUpdateStatementAsync(req);
                var status = resp.Status;

                if (_debugMode)
                {
                    _logger.Info("execute non-query statement {0} message: {1}", sql, status.Message);
                }

                return _utilFunctions.verify_success(status, SuccessCode);
            }
            catch (TException e)
            {
                throw new TException("execution of non-query statement failed", e);
            }
            finally
            {
                _clients.Add(client);
            }
        }
    }
}