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
using Thrift;
using System.Threading.Tasks;
namespace iotdb_client_csharp.client.utils
{
    public class SessionDataSet
    {
        private long query_id;

        private string sql;
        List<string> column_name_lst;
        Dictionary<string, int> column_name_index_map;
        Dictionary<int, int> duplicate_location;
        List<string> column_type_lst;
        TSQueryDataSet query_dataset;
        byte[] current_bitmap;
        int column_size;
        List<ByteBuffer> value_buffer_lst, bitmap_buffer_lst;
        ByteBuffer time_buffer;
        ConcurentClientQueue clientQueue;
        private string TIMESTAMP_STR{
            get{return "Time";}
        }
        private int START_INDEX{
            get{return 2;}
        }
        private int FLAG{
            get{return 0x80;}
        }
        private int default_timeout{
            get{return 10000;}
        }
        public int fetch_size{get;set;}
        private int row_index;

        private bool has_catched_result;
        private RowRecord cached_row_record;
        private bool is_closed = false;

        public SessionDataSet(string sql, TSExecuteStatementResp resp,ConcurentClientQueue clientQueue){
            this.clientQueue = clientQueue;
            this.sql = sql;
            this.query_dataset = resp.QueryDataSet;
            this.query_id = resp.QueryId;
            this.column_size = resp.Columns.Count;
            this.current_bitmap = new byte[this.column_size];
            this.column_name_lst = new List<string>{};
            this.time_buffer = new ByteBuffer(query_dataset.Time);
            this.column_name_index_map = new Dictionary<string, int>{};
            this.column_type_lst = new List<string>{};
            this.duplicate_location = new Dictionary<int, int>{};
            this.value_buffer_lst = new List<ByteBuffer>{};
            this.bitmap_buffer_lst = new List<ByteBuffer>{};
            // some internal variable
            this.has_catched_result = false;
            this.row_index = 0;
            if(resp.ColumnNameIndexMap != null){
                for(var index = 0; index < resp.Columns.Count; index++){
                    this.column_name_lst.Add("");
                    this.column_type_lst.Add("");
                }
                for(var index = 0; index < resp.Columns.Count; index++){
                    var name = resp.Columns[index];
                    this.column_name_lst[resp.ColumnNameIndexMap[name]] = name;
                    this.column_type_lst[resp.ColumnNameIndexMap[name]] = resp.DataTypeList[index];
                    
                }
            }else{
                this.column_name_lst = resp.Columns;
                this.column_type_lst = resp.DataTypeList;
            }
        
            for(int index = 0; index < this.column_name_lst.Count; index++){
                var column_name = this.column_name_lst[index];
                if(this.column_name_index_map.ContainsKey(column_name)){
                    this.duplicate_location[index] = this.column_name_index_map[column_name];
                }else{
                    this.column_name_index_map[column_name] = index;
                }
                this.value_buffer_lst.Add(new ByteBuffer(this.query_dataset.ValueList[index]));
                this.bitmap_buffer_lst.Add(new ByteBuffer(this.query_dataset.BitmapList[index]));
            }

        }
        
        public List<string> get_column_names(){
            var name_lst = new List<string>{"timestamp"};
            name_lst.AddRange(this.column_name_lst);
            return name_lst;
        }
        public void show_table_names(){
            var str = "";
            var name_lst = get_column_names();
            foreach(var name in name_lst){
                str += name + "\t\t";
            }
            Console.WriteLine(str);
        }
        public bool has_next(){
            if(has_catched_result){
                return true;
            }
            // we have consumed all current data, fetch some more
            if(!this.time_buffer.has_remaining()){
                if(!fetch_results()){
                    return false;
                }
            }
            construct_one_row();
            has_catched_result = true;
            return true;
        }
        public RowRecord next(){
            if(!has_catched_result){
                if(!has_next()){
                    return null;
                }
            }
            has_catched_result = false;
            return cached_row_record;
        }
        private TSDataType get_data_type_from_str(string str){
            switch(str){
                case "BOOLEAN":
                    return TSDataType.BOOLEAN;
                case "INT32":
                    return TSDataType.INT32;
                case "INT64":
                    return TSDataType.INT64;
                case "FLOAT":
                    return TSDataType.FLOAT;
                case "DOUBLE":
                    return TSDataType.DOUBLE;
                case "TEXT":
                    return TSDataType.TEXT;
                case "NULLTYPE":
                    return TSDataType.NONE;
                default:
                    return TSDataType.TEXT;
            }
        }
        public void construct_one_row(){
            List<object> field_lst = new List<Object>{};
            for(int i = 0; i < this.column_size; i++){
                if(duplicate_location.ContainsKey(i)){
                    var field = field_lst[duplicate_location[i]];
                    field_lst.Add(field);
                }else{
                    ByteBuffer column_value_buffer = value_buffer_lst[i];
                    ByteBuffer column_bitmap_buffer = bitmap_buffer_lst[i];
                    if(row_index % 8 == 0){
                        current_bitmap[i] = column_bitmap_buffer.get_byte();
                    }
                    object local_field;
                    if(!is_null(i, row_index)){
                        TSDataType column_data_type = get_data_type_from_str(column_type_lst[i]);
                        


                        switch(column_data_type){
                            case TSDataType.BOOLEAN:
                                local_field = column_value_buffer.get_bool();
                                break;
                            case TSDataType.INT32:
                                local_field = column_value_buffer.get_int();
                                break;
                            case TSDataType.INT64:
                                local_field = column_value_buffer.get_long();
                                break;
                            case TSDataType.FLOAT:
                                local_field = column_value_buffer.get_float();
                                break;
                            case TSDataType.DOUBLE:
                                local_field = column_value_buffer.get_double();
                                break;
                            case TSDataType.TEXT:
                                local_field = column_value_buffer.get_str();
                                break;
                            default:
                                string err_msg = string.Format("value format not supported");
                                throw new TException(err_msg, null);
                        }
                        field_lst.Add(local_field);

                    }
                    else{
                        local_field = null;
                        field_lst.Add("NULL");
                    }
                }
            }
            long timestamp = time_buffer.get_long();
            row_index += 1;
            this.cached_row_record = new RowRecord(timestamp, field_lst,column_name_lst);
        }
        private bool is_null(int loc, int row_index){
            byte bitmap = current_bitmap[loc];
            int shift = row_index % 8;
            return ((FLAG >> shift) & bitmap) == 0;

        }
        private bool fetch_results(){
            row_index = 0;
            var my_client = clientQueue.Take();            
            var req = new TSFetchResultsReq(my_client.sessionId, sql, fetch_size, query_id, true);
            req.Timeout = default_timeout;
            try{
                var task = my_client.client.fetchResultsAsync(req);
                task.Wait();
                var resp = task.Result;
                if(resp.HasResultSet){
                    this.query_dataset = resp.QueryDataSet;
                    // reset buffer
                    this.time_buffer = new ByteBuffer(resp.QueryDataSet.Time);
                    this.value_buffer_lst = new List<ByteBuffer>{};
                    this.bitmap_buffer_lst = new List<ByteBuffer>{};
                    for(int index = 0; index < query_dataset.ValueList.Count; index++){
                        this.value_buffer_lst.Add(new ByteBuffer(query_dataset.ValueList[index]));
                        this.bitmap_buffer_lst.Add(new ByteBuffer(query_dataset.BitmapList[index]));
                    }
                    // reset row index
                    row_index = 0;
                }
                if(clientQueue!=null){
                    clientQueue.Add(my_client);
                }
                return resp.HasResultSet;
            }
            catch(TException e){
                if(clientQueue!=null){
                    clientQueue.Add(my_client);
                }
                var message = string.Format("Cannot fetch result from server, because of network connection");
                throw new TException(message, e);
            }
        }
        public async Task close(){
            if(!is_closed){
                var my_client = clientQueue.Take();
                var req = new TSCloseOperationReq(sessionId:my_client.sessionId){QueryId=query_id};
                try{
                    await my_client.client.closeOperationAsync(req);
                }catch(TException e){
                    clientQueue.Add(my_client);
                    throw new TException("Operation Handle Close Failed", e);
                }
                clientQueue.Add(my_client);

            }
        }
    }
}

        