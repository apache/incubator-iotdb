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
using Thrift;

namespace Apache.IoTDB.DataStructure
{
    public class RowRecord
    {
        public long Timestamps { get; }
        public List<object> Values { get; }
        public List<string> Measurements { get; }

        public RowRecord(long timestamps, List<object> values, List<string> measurements)
        {
            Timestamps = timestamps;
            Values = values;
            Measurements = measurements;
        }

        public void Append(string measurement, object value)
        {
            Values.Add(value);
            Measurements.Add(measurement);
        }

        public DateTime get_date_time()
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(Timestamps).DateTime.ToLocalTime();
        }

        public override string ToString()
        {
            var str = "TimeStamp";
            foreach (var measurement in Measurements)
            {
                str += "\t\t";
                str += measurement;
            }

            str += "\n";

            str += Timestamps.ToString();
            foreach (var rowValue in Values)
            {
                str += "\t\t";
                str += rowValue.ToString();
            }

            return str;
        }

        public List<int> get_datatypes()
        {
            var dataTypeValues = new List<int>();
            
            foreach (var valueType in Values.Select(value => value))
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

        public byte[] ToBytes()
        {
            var buffer = new ByteBuffer(Values.Count * 8);
            
            foreach (var value in Values)
            {
                switch (value)
                {
                    case bool b:
                        buffer.add_byte((byte) TSDataType.BOOLEAN);
                        buffer.add_bool(b);
                        break;
                    case int i:
                        buffer.add_byte((byte) TSDataType.INT32);
                        buffer.add_int(i);
                        break;
                    case long l:
                        buffer.add_byte((byte) TSDataType.INT64);
                        buffer.add_long(l);
                        break;
                    case double d:
                        buffer.add_byte((byte) TSDataType.DOUBLE);
                        buffer.add_double(d);
                        break;
                    case float f:
                        buffer.add_byte((byte) TSDataType.FLOAT);
                        buffer.add_float(f);
                        break;
                    case string s:
                        buffer.add_byte((byte) TSDataType.TEXT);
                        buffer.add_str(s);
                        break;
                    default:
                        throw new TException($"Unsupported data type:{value.GetType()}", null);
                }
            }

            return buffer.get_buffer();;
        }
    }
}