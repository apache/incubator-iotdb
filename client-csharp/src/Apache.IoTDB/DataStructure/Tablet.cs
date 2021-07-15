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

using System.Collections.Generic;
using System.Linq;
using Thrift;

namespace Apache.IoTDB.DataStructure
{
    /*
    * A tablet data of one device, the tablet contains multiple measurements of this device that share
    * the same time column.
    *
    * for example:  device root.sg1.d1
    *
    * time, m1, m2, m3
    *    1,  1,  2,  3
    *    2,  1,  2,  3
    *    3,  1,  2,  3
    *
    * Notice: The tablet should not have empty cell
    *
    */
    public class Tablet
    {
        private readonly List<long> _timestamps;
        private readonly List<List<object>> _values;
        
        public string DeviceId { get; }
        public List<string> Measurements { get; }
        public int RowNumber { get; }
        public int ColNumber { get; }

        private readonly Utils _utilFunctions = new Utils();

        public Tablet(
            string deviceId, 
            List<string> measurements, 
            List<List<object>> values,
            List<long> timestamps)
        {
            if (timestamps.Count != values.Count)
            {
                throw new TException(
                    $"Input error. TimeStamp. Timestamps.Count({timestamps.Count}) does not equal to Values.Count({values.Count}).", 
                    null);
            }
            
            if (!_utilFunctions.IsSorted(timestamps))
            {
                var sorted = timestamps
                    .Select((x, index) => (timestamp: x, values: values[index]))
                    .OrderBy(x => x.timestamp).ToList();
                
                _timestamps = sorted.Select(x => x.timestamp).ToList();
                _values = sorted.Select(x => x.values).ToList();
            }
            else
            {
                _values = values;
                _timestamps = timestamps;
            }

            DeviceId = deviceId;
            Measurements = measurements;
            RowNumber = timestamps.Count;
            ColNumber = measurements.Count;
        }

        public byte[] get_binary_timestamps()
        {
            var buffer = new ByteBuffer(new byte[] { });
            
            foreach (var timestamp in _timestamps)
            {
                buffer.add_long(timestamp);
            }

            return buffer.get_buffer();
        }

        public List<int> get_data_types()
        {
            var dataTypeValues = new List<int>();
            
            foreach (var valueType in _values[0].Select(value => value))
            {
                switch (valueType)
                {
                    case bool _:
                        dataTypeValues.Add((int) TSDataType.BOOLEAN);
                        break;
                    case int _:
                        dataTypeValues.Add((int) TSDataType.INT32);
                        break;
                    case long _:
                        dataTypeValues.Add((int) TSDataType.INT64);
                        break;
                    case float _:
                        dataTypeValues.Add((int) TSDataType.FLOAT);
                        break;
                    case double _:
                        dataTypeValues.Add((int) TSDataType.DOUBLE);
                        break;
                    case string _:
                        dataTypeValues.Add((int) TSDataType.TEXT);
                        break;
                }
            }

            return dataTypeValues;
        }

        private int estimate_buffer_size()
        {
            var estimateSize = 0;
            
            // estimate one row size
            foreach (var value in _values[0])
            {
                switch (value)
                {
                    case bool _:
                        estimateSize += 1;
                        break;
                    case int _:
                        estimateSize += 4;
                        break;
                    case long _:
                        estimateSize += 8;
                        break;
                    case float _:
                        estimateSize += 4;
                        break;
                    case double _:
                        estimateSize += 8;
                        break;
                    case string s:
                        estimateSize += s.Length;
                        break;
                }
            }

            estimateSize *= _timestamps.Count;
            return estimateSize;
        }

        public byte[] get_binary_values()
        {
            var estimateSize = estimate_buffer_size();
            var buffer = new ByteBuffer(estimateSize);
            
            for (var i = 0; i < ColNumber; i++)
            {
                var value = _values[0][i];
                
                switch (value)
                {
                    case bool _:
                    {
                        for (var j = 0; j < RowNumber; j++)
                        {
                            buffer.add_bool((bool) _values[j][i]);
                        }

                        break;
                    }
                    case int _:
                    {
                        for (var j = 0; j < RowNumber; j++)
                        {
                            buffer.add_int((int) _values[j][i]);
                        }

                        break;
                    }
                    case long _:
                    {
                        for (var j = 0; j < RowNumber; j++)
                        {
                            buffer.add_long((long) _values[j][i]);
                        }

                        break;
                    }
                    case float _:
                    {
                        for (int j = 0; j < RowNumber; j++)
                        {
                            buffer.add_float((float) _values[j][i]);
                        }

                        break;
                    }
                    case double _:
                    {
                        for (var j = 0; j < RowNumber; j++)
                        {
                            buffer.add_double((double) _values[j][i]);
                        }

                        break;
                    }
                    case string _:
                    {
                        for (var j = 0; j < RowNumber; j++)
                        {
                            buffer.add_str((string) _values[j][i]);
                        }

                        break;
                    }
                    default:
                        throw new TException($"Unsupported data type {value}", null);
                    
                }
            }

            return  buffer.get_buffer();
        }
    }
}