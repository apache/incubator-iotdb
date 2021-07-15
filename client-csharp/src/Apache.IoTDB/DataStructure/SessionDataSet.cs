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
using System.Threading.Tasks;
using Thrift;

namespace Apache.IoTDB.DataStructure
{
    public class SessionDataSet
    {
        private readonly long _queryId;
        private readonly string _sql;
        private readonly List<string> _columnNames;
        private readonly Dictionary<string, int> _columnNameIndexMap;
        private readonly Dictionary<int, int> _duplicateLocation;
        private readonly List<string> _columnTypeLst;
        private TSQueryDataSet _queryDataset;
        private readonly byte[] _currentBitmap;
        private readonly int _columnSize;
        private List<ByteBuffer> _valueBufferLst, _bitmapBufferLst;
        private ByteBuffer _timeBuffer;
        private readonly ConcurrentClientQueue _clientQueue;
        private int _rowIndex;
        private bool _hasCatchedResult;
        private RowRecord _cachedRowRecord;
        private readonly bool _isClosed = false;


        private string TimestampStr => "Time";
        private int StartIndex => 2;
        private int Flag => 0x80;
        private int DefaultTimeout => 10000;

        public int FetchSize { get; set; }
        
        public SessionDataSet(string sql, TSExecuteStatementResp resp, ConcurrentClientQueue clientQueue)
        {
            _clientQueue = clientQueue;
            _sql = sql;
            _queryDataset = resp.QueryDataSet;
            _queryId = resp.QueryId;
            _columnSize = resp.Columns.Count;
            _currentBitmap = new byte[_columnSize];
            _columnNames = new List<string>();
            _timeBuffer = new ByteBuffer(_queryDataset.Time);
            _columnNameIndexMap = new Dictionary<string, int>();
            _columnTypeLst = new List<string>();
            _duplicateLocation = new Dictionary<int, int>();
            _valueBufferLst = new List<ByteBuffer>();
            _bitmapBufferLst = new List<ByteBuffer>();
            // some internal variable
            _hasCatchedResult = false;
            _rowIndex = 0;
            if (resp.ColumnNameIndexMap != null)
            {
                for (var index = 0; index < resp.Columns.Count; index++)
                {
                    _columnNames.Add("");
                    _columnTypeLst.Add("");
                }

                for (var index = 0; index < resp.Columns.Count; index++)
                {
                    var name = resp.Columns[index];
                    _columnNames[resp.ColumnNameIndexMap[name]] = name;
                    _columnTypeLst[resp.ColumnNameIndexMap[name]] = resp.DataTypeList[index];
                }
            }
            else
            {
                _columnNames = resp.Columns;
                _columnTypeLst = resp.DataTypeList;
            }

            for (int index = 0; index < _columnNames.Count; index++)
            {
                var columnName = _columnNames[index];
                if (_columnNameIndexMap.ContainsKey(columnName))
                {
                    _duplicateLocation[index] = _columnNameIndexMap[columnName];
                }
                else
                {
                    _columnNameIndexMap[columnName] = index;
                }

                _valueBufferLst.Add(new ByteBuffer(_queryDataset.ValueList[index]));
                _bitmapBufferLst.Add(new ByteBuffer(_queryDataset.BitmapList[index]));
            }
        }

        private List<string> get_column_names()
        {
            var nameLst = new List<string> {"timestamp"};
            nameLst.AddRange(_columnNames);
            return nameLst;
        }

        public void show_table_names()
        {
            var str = get_column_names()
                .Aggregate("", (current, name) => current + (name + "\t\t"));

            Console.WriteLine(str);
        }

        public bool has_next()
        {
            if (_hasCatchedResult)
            {
                return true;
            }

            // we have consumed all current data, fetch some more
            if (!_timeBuffer.has_remaining())
            {
                if (!fetch_results())
                {
                    return false;
                }
            }

            construct_one_row();
            _hasCatchedResult = true;
            return true;
        }

        public RowRecord Next()
        {
            if (!_hasCatchedResult)
            {
                if (!has_next())
                {
                    return null;
                }
            }

            _hasCatchedResult = false;
            return _cachedRowRecord;
        }

        private TSDataType get_data_type_from_str(string str)
        {
            return str switch
            {
                "BOOLEAN" => TSDataType.BOOLEAN,
                "INT32" => TSDataType.INT32,
                "INT64" => TSDataType.INT64,
                "FLOAT" => TSDataType.FLOAT,
                "DOUBLE" => TSDataType.DOUBLE,
                "TEXT" => TSDataType.TEXT,
                "NULLTYPE" => TSDataType.NONE,
                _ => TSDataType.TEXT
            };
        }

        private void construct_one_row()
        {
            List<object> fieldLst = new List<Object>();
            
            for (int i = 0; i < _columnSize; i++)
            {
                if (_duplicateLocation.ContainsKey(i))
                {
                    var field = fieldLst[_duplicateLocation[i]];
                    fieldLst.Add(field);
                }
                else
                {
                    var columnValueBuffer = _valueBufferLst[i];
                    var columnBitmapBuffer = _bitmapBufferLst[i];
                    
                    if (_rowIndex % 8 == 0)
                    {
                        _currentBitmap[i] = columnBitmapBuffer.get_byte();
                    }

                    object localField;
                    if (!is_null(i, _rowIndex))
                    {
                        var columnDataType = get_data_type_from_str(_columnTypeLst[i]);


                        switch (columnDataType)
                        {
                            case TSDataType.BOOLEAN:
                                localField = columnValueBuffer.get_bool();
                                break;
                            case TSDataType.INT32:
                                localField = columnValueBuffer.get_int();
                                break;
                            case TSDataType.INT64:
                                localField = columnValueBuffer.get_long();
                                break;
                            case TSDataType.FLOAT:
                                localField = columnValueBuffer.get_float();
                                break;
                            case TSDataType.DOUBLE:
                                localField = columnValueBuffer.get_double();
                                break;
                            case TSDataType.TEXT:
                                localField = columnValueBuffer.get_str();
                                break;
                            default:
                                string err_msg = "value format not supported";
                                throw new TException(err_msg, null);
                        }

                        fieldLst.Add(localField);
                    }
                    else
                    {
                        localField = null;
                        fieldLst.Add("NULL");
                    }
                }
            }

            long timestamp = _timeBuffer.get_long();
            _rowIndex += 1;
            _cachedRowRecord = new RowRecord(timestamp, fieldLst, _columnNames);
        }

        private bool is_null(int loc, int row_index)
        {
            byte bitmap = _currentBitmap[loc];
            int shift = row_index % 8;
            return ((Flag >> shift) & bitmap) == 0;
        }

        private bool fetch_results()
        {
            _rowIndex = 0;
            var myClient = _clientQueue.Take();
            var req = new TSFetchResultsReq(myClient.SessionId, _sql, FetchSize, _queryId, true)
            {
                Timeout = DefaultTimeout
            };
            try
            {
                var task = myClient.ServiceClient.fetchResultsAsync(req);
                task.Wait();
                var resp = task.Result;
                
                if (resp.HasResultSet)
                {
                    _queryDataset = resp.QueryDataSet;
                    // reset buffer
                    _timeBuffer = new ByteBuffer(resp.QueryDataSet.Time);
                    _valueBufferLst = new List<ByteBuffer>();
                    _bitmapBufferLst = new List<ByteBuffer>();
                    for (int index = 0; index < _queryDataset.ValueList.Count; index++)
                    {
                        _valueBufferLst.Add(new ByteBuffer(_queryDataset.ValueList[index]));
                        _bitmapBufferLst.Add(new ByteBuffer(_queryDataset.BitmapList[index]));
                    }

                    // reset row index
                    _rowIndex = 0;
                }

                return resp.HasResultSet;
            }
            catch (TException e)
            {
                throw new TException("Cannot fetch result from server, because of network connection", e);
            }
            finally
            {
                _clientQueue.Add(myClient);
            }
        }

        public async Task Close()
        {
            if (!_isClosed)
            {
                var myClient = _clientQueue.Take();
                var req = new TSCloseOperationReq(myClient.SessionId)
                {
                    QueryId = _queryId
                };

                try
                {
                    await myClient.ServiceClient.closeOperationAsync(req);
                }
                catch (TException e)
                {
                    throw new TException("Operation Handle Close Failed", e);
                }
                finally
                {
                    _clientQueue.Add(myClient);
                }
            }
        }
    }
}