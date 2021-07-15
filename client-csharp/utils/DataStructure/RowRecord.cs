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
using Thrift;
using System;
namespace iotdb_client_csharp.client.utils
{
    public class RowRecord
    {
        public long timestamp{get;set;}
        public List<Object> values {get;set;}
        public List<string> measurements{get;set;}
        public RowRecord(long timestamp, List<Object> values, List<string> measurements){
            this.timestamp = timestamp;
            this.values = values;
            this.measurements = measurements;
        }
        public void append(string measurement, Object value){
            values.Add(value);
            measurements.Add(measurement);
        }
        public DateTime get_date_time(){
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp).DateTime.ToLocalTime();;
        }

        public override string ToString()
        {
            var str = "TimeStamp";
             foreach(var measurement in measurements){
                str += "\t\t";
                str += measurement.ToString();
            }
            str += "\n";

            str += timestamp.ToString();
            foreach(var row_value in values){
                str += "\t\t";
                str += row_value.ToString();
            }
            return str;
        }
        public List<int> get_datatypes(){
            List<int> data_type_values = new List<int>(){};
            foreach(var value in values){
                var value_type = value.GetType();
                if(value_type.Equals(typeof(bool))){
                    data_type_values.Add((int)TSDataType.BOOLEAN);
                }
                else if(value_type.Equals(typeof(Int32))){
                    data_type_values.Add((int)TSDataType.INT32);
                }
                else if(value_type.Equals(typeof(Int64))){
                    data_type_values.Add((int)TSDataType.INT64);
                }
                else if(value_type.Equals(typeof(float))){
                    data_type_values.Add((int)TSDataType.FLOAT);
                }
                else if(value_type.Equals(typeof(double))){
                    data_type_values.Add((int)TSDataType.DOUBLE);
                }
                else if(value_type.Equals(typeof(string))){
                    data_type_values.Add((int)TSDataType.TEXT);
                }
            }
            return data_type_values;

        }
        public byte[] ToBytes(){
            ByteBuffer buffer = new ByteBuffer(values.Count * 8);
            foreach(var value in values){
                if(value.GetType().Equals(typeof(bool))){
                    buffer.add_char((char)TSDataType.BOOLEAN);
                    buffer.add_bool((bool)value);
                }
                else if((value.GetType().Equals(typeof(Int32)))){
                    buffer.add_char((char)TSDataType.INT32);
                    buffer.add_int((int)value);
                }
                else if((value.GetType().Equals(typeof(Int64)))){
                    buffer.add_char((char)TSDataType.INT64);
                    buffer.add_long((long)value);
                }
                else if((value.GetType().Equals(typeof(double)))){
                    buffer.add_char((char)TSDataType.DOUBLE);
                    buffer.add_double((double)value);
                }
                else if((value.GetType().Equals(typeof(float)))){
                    buffer.add_char((char)TSDataType.FLOAT);
                    buffer.add_float((float)value);
                }
                else if((value.GetType().Equals(typeof(string)))){
                    buffer.add_char((char)TSDataType.TEXT);
                    buffer.add_str((string)value);
                }
                else{
                    var message = String.Format("Unsupported data type:{0}",value.GetType().ToString());
                    throw new TException(message, null);
                }
            }
            var buf = buffer.get_buffer();
            return buf;
        }
        



    }
}