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
namespace iotdb_client_csharp.client.utils
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
       public string device_id{get;}
       public List<string> measurement_lst{get;}
       public List<long> timestamp_lst;
       public List<List<object>> value_lst;
       public int row_number{get;}
       public int col_number;

       public Utils util_functions = new Utils();

       public Tablet(string device_id, List<string> measurement_lst, List<List<object>> value_lst, List<long> timestamp_lst){
            if(timestamp_lst.Count != value_lst.Count){
                var err_msg = String.Format("Input error! len(timestamp_lst) does not equal to len(value_lst)!");
                throw new TException(err_msg, null);
            }
            if(!util_functions.check_sorted(timestamp_lst)){
                var sorted = timestamp_lst.Select((x, index) => (timestamp:x, values:value_lst[index])).OrderBy(x => x.timestamp).ToList();
                this.timestamp_lst = sorted.Select(x => x.timestamp).ToList();
                this.value_lst = sorted.Select(x => x.values).ToList();
            }else{
                this.value_lst = value_lst;
                this.timestamp_lst = timestamp_lst;
            }
           
           this.device_id = device_id;
           this.measurement_lst = measurement_lst;
           this.row_number = timestamp_lst.Count;
           this.col_number = measurement_lst.Count;
       }
       
       public byte[] get_binary_timestamps(){
           ByteBuffer buffer = new ByteBuffer(new byte[]{});
           foreach(var timestamp in timestamp_lst){
               buffer.add_long(timestamp);
           }
           return buffer.get_buffer();
       }
        public List<int> get_data_types(){
            List<int> data_type_values = new List<int>(){};
            foreach(var value in value_lst[0]){
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
       public int estimate_buffer_size(){
           var estimate_size = 0;
           // estimate one row size
           foreach(var value in value_lst[0]){
               var value_type = value.GetType();
               if(value_type.Equals(typeof(bool))){
                   estimate_size += 1;
               }
               else if(value_type.Equals(typeof(Int32))){
                    estimate_size += 4;
               }
               else if(value_type.Equals(typeof(Int64))){
                    estimate_size += 8;
               }
               else if(value_type.Equals(typeof(float))){
                    estimate_size += 4;
               }
               else if(value_type.Equals(typeof(double))){
                    estimate_size += 8;
               }
               else if(value_type.Equals(typeof(string))){
                    estimate_size += ((string)value).Length;
               }
           }
           estimate_size *= timestamp_lst.Count;
           return estimate_size;
       }
       
       public byte[] get_binary_values(){
           var estimate_size = estimate_buffer_size();
           ByteBuffer buffer = new ByteBuffer(estimate_size);
           for(int i = 0; i < col_number; i++){
               var value_type = value_lst[0][i].GetType();
               if(value_type.Equals(typeof(bool))){
                    for(int j=0; j< row_number; j++){
                        buffer.add_bool((bool)value_lst[j][i]);
                    }
               }
               else if(value_type.Equals(typeof(Int32))){
                    for(int j=0; j< row_number; j++){
                        buffer.add_int((Int32)value_lst[j][i]);
                    }
               }
               else if(value_type.Equals(typeof(Int64))){
                    for(int j=0; j< row_number; j++){
                        buffer.add_long((Int64)value_lst[j][i]);
                    }
               }
               else if(value_type.Equals(typeof(float))){
                    for(int j=0; j< row_number; j++){
                        buffer.add_float((float)value_lst[j][i]);
                    }
               }
               else if(value_type.Equals(typeof(double))){
                    for(int j=0; j< row_number; j++){
                        buffer.add_double((double)value_lst[j][i]);
                    }
               }
               else if(value_type.Equals(typeof(string))){
                    for(int j=0; j< row_number; j++){
                        buffer.add_str((string)value_lst[j][i]);
                    }
               }
               else{
                    var message = String.Format("Unsupported data type {0}", value_type);
                    throw new TException(message, null);
               }
           }
           var buf = buffer.get_buffer();
           return buf;
       }
    }
}