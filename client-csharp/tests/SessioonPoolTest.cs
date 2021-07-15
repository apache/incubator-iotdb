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
using System;
using iotdb_client_csharp.client.utils;
using System.Threading.Tasks;

namespace iotdb_client_csharp.client.test
{
    public class SessionPoolTest
    {
        public string host = "localhost";
        public int port = 6667;
        public string user = "root";
        public string passwd = "root";
        public int fetch_size = 10000;
        public int processed_size = 8;
        public bool debug = false;
        int pool_size = 3;

        public void Test(){
            Task task;
            
            task = TestInsertRecord();
            task.Wait();
            
            task = TestCreateMultiTimeSeries();
            task.Wait();
            task = TestGetTimeZone();
            task.Wait();
            task = TestInsertStrRecord();
            task.Wait();
            task = TestInsertRecords();
            task.Wait();
            task = TestInsertRecordsOfOneDevice();
            task.Wait();
            task = TestInsertTablet();
            task.Wait();
            task = TestInsertTablets();
            task.Wait();          
            task = TestSetAndDeleteStorageGroup();
            task.Wait();
            task = TestCreateTimeSeries();
            task.Wait();
            task = TestDeleteStorageGroups();
            task.Wait();
            task = TestCheckTimeSeriesExists();
            task.Wait();
            task = TestSetTimeZone();
            task.Wait();
            task = TestDeleteData();
            task.Wait();
            task = TestNonSql();
            task.Wait();
            task = TestSqlQuery();
            task.Wait();
            
        }

        public async Task TestInsertRecord(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status;
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");

            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
          
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status== 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<object>{"test_text", true, (Int32)123};
            List<Task<int>> tasks = new List<Task<int>>();
            long start_ms= (DateTime.Now.Ticks / 10000);
            for(int timestamp = 1; timestamp <= fetch_size * processed_size; timestamp++){
                RowRecord rowRecord = new RowRecord(timestamp, values, measures);
                var task = session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", rowRecord);
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());
            long end_ms = (DateTime.Now.Ticks / 10000);
            Console.WriteLine(string.Format("total insert record time is {0}", end_ms - start_ms));
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.close();
            Console.WriteLine("TestInsertRecordAsync Passed");
        }
        public async Task TestCreateMultiTimeSeries(){
            // by Luzhan
            var session_pool = new SessionPool(host, port, user, passwd, pool_size);
            await session_pool.open(false);
            int status = 0;
            if(debug){
                session_pool.open_debug_mode();
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"};
            List<TSDataType> data_type_lst = new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT};
            List<TSEncoding> encoding_lst = new List<TSEncoding>(){TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN};
            List<Compressor> compressor_lst = new List<Compressor>(){Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY};
            status = await session_pool.create_multi_time_series_async(ts_path_lst, data_type_lst, encoding_lst, compressor_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();      
            Console.WriteLine("TestCreateMultiTimeSeries Passed!");
        }
        public async Task TestDeleteTimeSeries(){
            var session_pool = new SessionPool(host, port, user, passwd, pool_size);
            await session_pool.open(false);
            int status = 0;
            if(debug){
                session_pool.open_debug_mode();
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"};
            List<TSDataType> data_type_lst = new List<TSDataType>(){TSDataType.BOOLEAN, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.TEXT};
            List<TSEncoding> encoding_lst = new List<TSEncoding>(){TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN,TSEncoding.PLAIN};
            List<Compressor> compressor_lst = new List<Compressor>(){Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY,Compressor.SNAPPY};
            status = await session_pool.create_multi_time_series_async(ts_path_lst, data_type_lst, encoding_lst, compressor_lst);
            System.Diagnostics.Debug.Assert(status==0);
            status = await session_pool.delete_storage_groups_async(ts_path_lst);
            System.Diagnostics.Debug.Assert(status == 0);
            Console.WriteLine("TestDeleteTimeSeries Passed!");
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.close();            
        }
        public async Task TestGetTimeZone(){
           var session_pool = new SessionPool(host, port, pool_size);
           await session_pool.open(false);
           if(debug){
                session_pool.open_debug_mode();
            }
           await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
           System.Diagnostics.Debug.Assert(session_pool.is_open());
           var time_zone = await session_pool.get_time_zone();
           System.Diagnostics.Debug.Assert(time_zone == "UTC+08:00");
           await session_pool.close();
           Console.WriteLine("TestGetTimeZone Passed!");
        }
         public async Task TestInsertStrRecord(){
           var session_pool = new SessionPool(host, port, pool_size);
           int status = 0;
           await session_pool.open(false);
           if(debug){
                session_pool.open_debug_mode();
            }
           System.Diagnostics.Debug.Assert(session_pool.is_open());
           await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");

           status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           System.Diagnostics.Debug.Assert(status == 0);
           status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
           System.Diagnostics.Debug.Assert(status == 0);

           var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"};
           var values = new List<object>{(Int32)1, (Int32)2};
           RowRecord rowRecord = new RowRecord(1, values, measures);
           status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", rowRecord);
           System.Diagnostics.Debug.Assert(status == 0);
           var res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<2");
           res.show_table_names();
           while(res.has_next()){
               Console.WriteLine(res.next());
           }
           await res.close();

            var tasks = new List<Task<int>>();
           // large data test
           List<RowRecord> rowRecords = new List<RowRecord>(){};
           for(int timestamp = 2; timestamp <=fetch_size * processed_size; timestamp++){
               rowRecords.Add(new RowRecord(timestamp, values, measures));
           }
           for(int timestamp = 2; timestamp <=fetch_size * processed_size; timestamp++){
               var task = session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", rowRecords[timestamp-2]);
               tasks.Add(task);
           }
           Task.WaitAll(tasks.ToArray());
           res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
           int res_count = 0;
           while(res.has_next()){
               res.next();
               res_count += 1;
           }
           await res.close();
           System.Diagnostics.Debug.Assert(res_count == fetch_size * processed_size);
           await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
           await session_pool.close();
           Console.WriteLine("TestInsertStrRecord Passed!");
        }
        public async Task TestInsertRecords(){
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            int status = 0;
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            
            List<string> device_id = new List<string>(){};
            for(int i = 0; i < 3; i++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            }
            List<List<string>> measurements_lst = new List<List<string>>(){};
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4", "TEST_CSHARP_CLIENT_TS5", "TEST_CSHARP_CLIENT_TS6"});
            List<List<object>> values_lst = new List<List<object>>(){};
            values_lst.Add(new List<object>(){true, (Int32)123});
            values_lst.Add(new List<object>(){true, (Int32)123, (Int64)456, (Double)1.1});
            values_lst.Add(new List<object>(){true, (Int32)123, (Int64)456, (Double)1.1, (float)10001.1, "test_record"});
            List<long> timestamp_lst = new List<long>(){1, 2, 3};
            List<RowRecord> rowRecords = new List<RowRecord>(){};
            for(int i = 0; i < 3;i++){
                RowRecord rowRecord = new RowRecord(timestamp_lst[i], values_lst[i], measurements_lst[i]);
                rowRecords.Add(rowRecord);
            }
            status = await session_pool.insert_records_async(device_id, rowRecords);
            System.Diagnostics.Debug.Assert(status == 0);
            var res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            Console.WriteLine(status);

            // large data test
            device_id = new List<string>(){};
            rowRecords = new List<RowRecord>(){};
            var tasks = new List<Task<int>>();
            for(int timestamp = 4;timestamp <= fetch_size * processed_size;timestamp++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
                rowRecords.Add(new RowRecord(timestamp, new List<object>(){true, (Int32)123}, new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"}));
                if(timestamp % fetch_size == 0){
                    tasks.Add(session_pool.insert_records_async(device_id, rowRecords));
                    device_id = new List<string>(){};
                    rowRecords = new List<RowRecord>(){};
                }
            }
            Task.WaitAll(tasks.ToArray());
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int record_count = fetch_size * processed_size;
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            await res.close();
            Console.WriteLine(res_count + " "+ fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_count == record_count);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestInsertRecords Passed!");
        }
        public async Task TestInsertRecordsOfOneDevice(){
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            int status = 0;
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            var device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE";
            List<List<string>> measurements_lst = new List<List<string>>(){};
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4", "TEST_CSHARP_CLIENT_TS5", "TEST_CSHARP_CLIENT_TS6"});
            List<List<object>> values_lst = new List<List<object>>(){};
            values_lst.Add(new List<object>(){true, (Int32)123});
            values_lst.Add(new List<object>(){true, (Int32)123, (Int64)456, (Double)1.1});
            values_lst.Add(new List<object>(){true, (Int32)123, (Int64)456, (Double)1.1, (float)10001.1, "test_record"});
            List<long> timestamp_lst = new List<long>(){1, 2, 3};
            List<RowRecord> rowRecords = new List<RowRecord>(){};
            for(int i = 0; i < 3;i++){
                RowRecord rowRecord = new RowRecord(timestamp_lst[i], values_lst[i], measurements_lst[i]);
                rowRecords.Add(rowRecord);
            }
            status = await session_pool.insert_records_of_one_device_async(device_id, rowRecords);
            System.Diagnostics.Debug.Assert(status == 0);
            var res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();      
            // large data test
            rowRecords = new List<RowRecord>(){};
            var tasks = new List<Task<int>>();
            for(int timestamp = 4;timestamp <= fetch_size * processed_size;timestamp++){
                rowRecords.Add(new RowRecord(timestamp, new List<object>(){true, (Int32)123}, new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"}));
                if(timestamp % fetch_size == 0){
                    tasks.Add(session_pool.insert_records_of_one_device_async(device_id, rowRecords));
                    rowRecords = new List<RowRecord>(){};
                }
            }
            Task.WaitAll(tasks.ToArray());
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            await res.close();
            Console.WriteLine(res_count + " "+ fetch_size * processed_size);
            System.Diagnostics.Debug.Assert(res_count == fetch_size * processed_size);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestInsertRecordsOfOneDevice Passed!");
        }
        public async Task TestInsertTablet(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                    session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            string device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE";
            List<string> measurement_lst = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            List<List<object>> value_lst = new List<List<object>>{new List<object>{"iotdb", true, (Int32)12}, new List<object>{"c#", false, (Int32)13}, new List<object>{"client", true, (Int32)14}};
            List<long> timestamp_lst = new List<long>{1, 2, 3};
            var tablet = new Tablet(device_id, measurement_lst, value_lst, timestamp_lst);
            status = await session_pool.insert_tablet_async(tablet);
            System.Diagnostics.Debug.Assert(status == 0);
            var res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            // large data test
            value_lst = new List<List<object>>(){};
            timestamp_lst = new List<long>(){};
            var tasks = new List<Task<int>>();
            long start_ms= (DateTime.Now.Ticks / 10000);
            for (int timestamp = 4; timestamp <= fetch_size * processed_size; timestamp++){
                timestamp_lst.Add(timestamp);
                value_lst.Add(new List<object>(){"iotdb", true, (Int32)timestamp});
                if(timestamp % fetch_size == 0){
                    tablet = new Tablet(device_id, measurement_lst, value_lst, timestamp_lst);
                    tasks.Add(session_pool.insert_tablet_async(tablet));
                    value_lst = new List<List<object>>(){};
                    timestamp_lst = new List<long>(){};

                }
            }
            Task.WaitAll(tasks.ToArray());
            long end_ms = (DateTime.Now.Ticks / 10000);
            Console.WriteLine(string.Format("total tablet insert time is {0}", end_ms - start_ms));
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            await res.close();
            Console.WriteLine(res_count + " "+ fetch_size*processed_size);
            System.Diagnostics.Debug.Assert(res_count == fetch_size * processed_size);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestInsertTablet Passed!");
        }
        public async Task TestInsertTablets(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                    session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> device_id = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2"};
            List<List<string>> measurements_lst = new List<List<string>>(){new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"}, new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"}};
            List<List<List<object>>> values_lst = new List<List<List<object>>>(){new List<List<object>>(){new List<object>{"iotdb", true, (Int32)12}, new List<object>{"c#", false, (Int32)13}, new List<object>{"client", true, (Int32)14}}, new List<List<object>>(){new List<object>{"iotdb_2", true, (Int32)1}, new List<object>{"c#_2", false, (Int32)2}, new List<object>{"client_2", true, (Int32)3}}};
            List<List<long>> timestamp_lst = new List<List<long>>(){new List<long>(){2, 1, 3}, new List<long>(){3, 1, 2}};
            List<Tablet> tablets = new List<Tablet>(){};
            for(int i = 0;i < device_id.Count; i++){
                var tablet = new Tablet(device_id[i], measurements_lst[i], values_lst[i], timestamp_lst[i]);
                tablets.Add(tablet);
            }
            status = await session_pool.insert_tablets_async(tablets);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();

            // large data test

            var tasks = new List<Task<int>>();
            for(int timestamp = 4;timestamp <= processed_size * fetch_size; timestamp++){
                var local_device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1";
                var local_measurements = new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
                var local_value = new List<List<object>>(){new List<object>(){"iotdb", true, (Int32)timestamp}};
                var local_timestamp = new List<long>{timestamp};
                Tablet tablet = new Tablet(local_device_id, local_measurements, local_value, local_timestamp);
                tablets.Add(tablet);
                if(timestamp % fetch_size == 0){
                    tasks.Add(session_pool.insert_tablets_async(tablets));
                    tablets = new List<Tablet>(){};
                }
            }
            Task.WaitAll(tasks.ToArray());
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            await res.close();
            System.Diagnostics.Debug.Assert(res_count == fetch_size * processed_size);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestInsertTablets Passed!");
        }
        public async Task TestSetAndDeleteStorageGroup(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP")==0);
            System.Diagnostics.Debug.Assert(await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP")==0);
            await session_pool.close();
            Console.WriteLine("TestSetAndDeleteStorageGroup Passed!");
        }
        public async Task TestCreateTimeSeries(){
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1"), TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2"), TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3"), TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4"), TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5"), TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            System.Diagnostics.Debug.Assert(await session_pool.create_time_series_async(("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6"), TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY)==0);
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.close();
            Console.WriteLine("TestCreateTimeSeries Passed!");
        }
        public async Task TestDeleteStorageGroups(){
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
           
            await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP_01");
            await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP_02");
            await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP_03");
            await session_pool.set_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP_04");
            List<string> group_names = new List<string>(){};
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_01");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_02");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_03");
            group_names.Add("root.97209_TEST_CSHARP_CLIENT_GROUP_04");
            System.Diagnostics.Debug.Assert(await session_pool.delete_storage_groups_async(group_names)==0);
            await session_pool.close();
            Console.WriteLine("TestDeleteStorageGroups Passed!");
        }
        public async Task TestCheckTimeSeriesExists(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            var ifExist_1 = await session_pool.check_time_series_exists_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1");
            var ifExist_2 = await session_pool.check_time_series_exists_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2");
            System.Diagnostics.Debug.Assert(ifExist_1 == true && ifExist_2 == false);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestCheckTimeSeriesExists Passed!");
        }
        public async Task TestSetTimeZone(){
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.set_time_zone("GMT+8:00");
            System.Diagnostics.Debug.Assert(await session_pool.get_time_zone() == "GMT+8:00");
            await session_pool.close();
            Console.WriteLine("TestSetTimeZone Passed!");
        }
        public async Task TestDeleteData(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);

            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<object>{"test_text", true, (Int32)123};
            status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", new RowRecord(1, values, measures));
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", new RowRecord(2, values, measures));
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", new RowRecord(3, values, measures));
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", new RowRecord(4, values, measures));
            System.Diagnostics.Debug.Assert(status == 0);
            var res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            List<string> ts_path_lst = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2"};
            await session_pool.delete_data_async(ts_path_lst, 2 ,3);
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestDeleteData Passed!");
        }
        public async Task TestTestInsertRecord(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status;
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");

            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
          
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status== 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED);
            System.Diagnostics.Debug.Assert(status == 0);
            var measures = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            var values = new List<object>{"test_text", true, (Int32)123};
            List<Task<int>> tasks = new List<Task<int>>();
            long start_ms= (DateTime.Now.Ticks / 10000);
            for(int timestamp = 1; timestamp <= fetch_size * processed_size; timestamp++){
                RowRecord rowRecord = new RowRecord(timestamp, values, measures);
                var task = session_pool.test_insert_record_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", rowRecord);
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());
            long end_ms = (DateTime.Now.Ticks / 10000);
            Console.WriteLine(string.Format("total insert record time is {0}", end_ms - start_ms));
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.close();
            Console.WriteLine("TestTestInsertRecordAsync Passed");
        }
        public async Task TestTestInsertRecords(){
            var session_pool = new SessionPool(host, port, pool_size);
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            int status = 0;
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS1", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS2", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS3", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS4", TSDataType.DOUBLE, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS5", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.create_time_series_async("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.TEST_CSHARP_CLIENT_TS6", TSDataType.TEXT, TSEncoding.PLAIN, Compressor.SNAPPY);
            System.Diagnostics.Debug.Assert(status == 0);
            
            List<string> device_id = new List<string>(){};
            for(int i = 0; i < 3; i++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            }
            List<List<string>> measurements_lst = new List<List<string>>(){};
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4"});
            measurements_lst.Add(new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3", "TEST_CSHARP_CLIENT_TS4", "TEST_CSHARP_CLIENT_TS5", "TEST_CSHARP_CLIENT_TS6"});
            List<List<object>> values_lst = new List<List<object>>(){};
            values_lst.Add(new List<object>(){true, (Int32)123});
            values_lst.Add(new List<object>(){true, (Int32)123, (Int64)456, (Double)1.1});
            values_lst.Add(new List<object>(){true, (Int32)123, (Int64)456, (Double)1.1, (float)10001.1, "test_record"});
            List<long> timestamp_lst = new List<long>(){1, 2, 3};
            List<RowRecord> rowRecords = new List<RowRecord>(){};
            for(int i = 0; i < 3;i++){
                RowRecord rowRecord = new RowRecord(timestamp_lst[i], values_lst[i], measurements_lst[i]);
                rowRecords.Add(rowRecord);
            }
            status = await session_pool.test_insert_records_async(device_id, rowRecords);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();

            // large data test
            device_id = new List<string>(){};
            rowRecords = new List<RowRecord>(){};
            var tasks = new List<Task<int>>();
            for(int timestamp = 4;timestamp <= fetch_size * processed_size;timestamp++){
                device_id.Add("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
                rowRecords.Add(new RowRecord(timestamp, new List<object>(){true, (Int32)123}, new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2"}));
                if(timestamp % fetch_size == 0){
                    tasks.Add(session_pool.test_insert_records_async(device_id, rowRecords));
                    device_id = new List<string>(){};
                    rowRecords = new List<RowRecord>(){};
                }
            }
            Task.WaitAll(tasks.ToArray());
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int record_count = fetch_size * processed_size;
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            await res.close();
            System.Diagnostics.Debug.Assert(res_count == 0);
            System.Diagnostics.Debug.Assert(status == 0);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestTestInsertRecords Passed!");
        }
        public async Task TestTestInsertTablet(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                    session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            string device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE";
            List<string> measurement_lst = new List<string>{"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
            List<List<object>> value_lst = new List<List<object>>{new List<object>{"iotdb", true, (Int32)12}, new List<object>{"c#", false, (Int32)13}, new List<object>{"client", true, (Int32)14}};
            List<long> timestamp_lst = new List<long>{2, 1, 3};
            var tablet = new Tablet(device_id, measurement_lst, value_lst, timestamp_lst);
            status = await session_pool.test_insert_tablet_async(tablet);
            System.Diagnostics.Debug.Assert(status == 0);
            var res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            // large data test
            value_lst = new List<List<object>>(){};
            timestamp_lst = new List<long>(){};
            var tasks = new List<Task<int>>();
            long start_ms= (DateTime.Now.Ticks / 10000);
            for (int timestamp = 4; timestamp <= fetch_size * processed_size; timestamp++){
                timestamp_lst.Add(timestamp);
                value_lst.Add(new List<object>(){"iotdb", true, (Int32)timestamp});
                if(timestamp % (fetch_size / 32) == 0){
                    tablet = new Tablet(device_id, measurement_lst, value_lst, timestamp_lst);
                    tasks.Add(session_pool.test_insert_tablet_async(tablet));
                    value_lst = new List<List<object>>(){};
                    timestamp_lst = new List<long>(){};

                }
            }
            Task.WaitAll(tasks.ToArray());
            long end_ms = (DateTime.Now.Ticks / 10000);
            Console.WriteLine(string.Format("total tablet insert time is {0}", end_ms - start_ms));
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            await res.close();
            System.Diagnostics.Debug.Assert(res_count == 0);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestTestInsertTablet Passed!");
        }
        public async Task TestTestInsertTablets(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                    session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            List<string> device_id = new List<string>(){"root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1", "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2"};
            List<List<string>> measurements_lst = new List<List<string>>(){new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"}, new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"}};
            List<List<List<object>>> values_lst = new List<List<List<object>>>(){new List<List<object>>(){new List<object>{"iotdb", true, (Int32)12}, new List<object>{"c#", false, (Int32)13}, new List<object>{"client", true, (Int32)14}}, new List<List<object>>(){new List<object>{"iotdb_2", true, (Int32)1}, new List<object>{"c#_2", false, (Int32)2}, new List<object>{"client_2", true, (Int32)3}}};
            List<List<long>> timestamp_lst = new List<List<long>>(){new List<long>(){2, 1, 3}, new List<long>(){3, 1, 2}};
            List<Tablet> tablets = new List<Tablet>(){};
            for(int i = 0;i < device_id.Count; i++){
                var tablet = new Tablet(device_id[i], measurements_lst[i], values_lst[i], timestamp_lst[i]);
                tablets.Add(tablet);
            }
            status = await session_pool.test_insert_tablets_async(tablets);
            // System.Diagnostics.Debug.Assert(status == 0);
            var res=await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE2 where time<15");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();

            // large data test

            var tasks = new List<Task<int>>();
            for(int timestamp = 4;timestamp <= processed_size* fetch_size; timestamp++){
                var local_device_id = "root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1";
                var local_measurements = new List<string>(){"TEST_CSHARP_CLIENT_TS1", "TEST_CSHARP_CLIENT_TS2", "TEST_CSHARP_CLIENT_TS3"};
                var local_value = new List<List<object>>(){new List<object>(){"iotdb", true, (Int32)timestamp}};
                var local_timestamp = new List<long>{timestamp};
                Tablet tablet = new Tablet(local_device_id, local_measurements, local_value, local_timestamp);
                tablets.Add(tablet);
                if(timestamp % fetch_size == 0){
                    tasks.Add(session_pool.test_insert_tablets_async(tablets));
                    tablets = new List<Tablet>(){};
                }
            }
            Task.WaitAll(tasks.ToArray());
            res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE1");
            res.show_table_names();
            int res_count = 0;
            while(res.has_next()){
                res.next();
                res_count += 1;
            }
            await res.close();
            System.Diagnostics.Debug.Assert(res_count == 0);
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestTestInsertTablets Passed!");
        }

        public async Task TestNonSql(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.status with datatype=BOOLEAN,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.temperature with datatype=FLOAT,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.hardware with datatype=TEXT,encoding=PLAIN");
            status = await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (4, false, 20, 'yxl')");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (5, true, 12, 'myy')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (6, true, 21, 'lz')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            var res = await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("TestNonSql Passed");
        }
        public async Task TestSqlQuery(){
            var session_pool = new SessionPool(host, port, pool_size);
            int status = 0;
            await session_pool.open(false);
            if(debug){
                session_pool.open_debug_mode();
            }
            System.Diagnostics.Debug.Assert(session_pool.is_open());
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.status with datatype=BOOLEAN,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.temperature with datatype=FLOAT,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.hardware with datatype=TEXT,encoding=PLAIN");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (4, false, 20, 'yxl')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (5, true, 12, 'myy')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, temperature, hardware) VALUES (6, true, 21, 'lz')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");
            await session_pool.execute_non_query_statement_async("insert into root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE(timestamp, status, hardware) VALUES (7, true,'lz')");

            var res =await session_pool.execute_query_statement_async("show timeseries root");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            Console.WriteLine("SHOW TIMESERIES ROOT sql passed!");
            res =await session_pool.execute_query_statement_async("show devices");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            Console.WriteLine("SHOW DEVICES sql passed!");
            res = await session_pool.execute_query_statement_async("COUNT TIMESERIES root");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            Console.WriteLine("COUNT TIMESERIES root sql Passed");
            res= await session_pool.execute_query_statement_async("select * from root.ln.wf01 where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            Console.WriteLine("SELECT sql Passed");
            res= await session_pool.execute_query_statement_async("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<10");
            res.show_table_names();
            while(res.has_next()){
                Console.WriteLine(res.next());
            }
            await res.close();
            status = await session_pool.delete_storage_group_async("root.97209_TEST_CSHARP_CLIENT_GROUP");
            System.Diagnostics.Debug.Assert(status == 0);
            await session_pool.close();
            Console.WriteLine("SELECT sql Passed");
        }

    }
}