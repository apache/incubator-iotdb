<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# **核心概念**

### **Row Record**

- 对**IoTDB**中的`record`数据进行封装和抽象。
- 示例：

| timestamp | status | temperature |
| --------- | ------ | ----------- |
| 1         | 0      | 20          |

- 构造方法：

```c#
var rowRecord = 
  new RowRecord(long timestamps, List<object> values, List<string> measurements);
```

### **Tablet**

- 一种类似于表格的数据结构，包含一个设备的若干行非空数据块。
- 示例：

| time | status | temperature |
| ---- | ------ | ----------- |
| 1    | 0      | 20          |
| 2    | 0      | 20          |
| 3    | 3      | 21          |

- 构造方法：

```c#
var tablet = 
  Tablet(string deviceId,  List<string> measurements, List<List<object>> values, List<long> timestamps);
```



# **API**

### **基础接口**

| api name       | parameters                | notes                    | use example                   |
| -------------- | ------------------------- | ------------------------ | ----------------------------- |
| Open           | bool                      | open session             | session_pool.Open(false)      |
| Close          | null                      | close session            | session_pool.Close()          |
| IsOpen         | null                      | check if session is open | session_pool.IsOpen()         |
| OpenDebugMode  | LoggingConfiguration=null | open debug mode          | session_pool.OpenDebugMode()  |
| CloseDebugMode | null                      | close debug mode         | session_pool.CloseDebugMode() |
| SetTimeZone    | string                    | set time zone            | session_pool.GetTimeZone()    |
| GetTimeZone    | null                      | get time zone            | session_pool.GetTimeZone()    |

### **Record相关接口**

| api name                            | parameters                    | notes                               | use example                                                  |
| ----------------------------------- | ----------------------------- | ----------------------------------- | ------------------------------------------------------------ |
| InsertRecordAsync                   | string, RowRecord             | insert single record                | session_pool.InsertRecordAsync("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", new RowRecord(1, values, measures)); |
| InsertRecordsAsync                  | List<string>, List<RowRecord> | insert records                      | session_pool.InsertRecordsAsync(device_id, rowRecords)       |
| InsertRecordsOfOneDeviceAsync       | string, List<RowRecord>       | insert records of one device        | session_pool.InsertRecordsOfOneDeviceAsync(device_id, rowRecords) |
| InsertRecordsOfOneDeviceSortedAsync | string, List<RowRecord>       | insert sorted records of one device | InsertRecordsOfOneDeviceSortedAsync(deviceId, sortedRowRecords); |
| TestInsertRecordAsync               | string, RowRecord             | test insert record                  | session_pool.TestInsertRecordAsync("root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE", rowRecord) |
| TestInsertRecordsAsync              | List<string>, List<RowRecord> | test insert record                  | session_pool.TestInsertRecordsAsync(device_id, rowRecords)   |

### **Tablet相关接口**

| api name               | parameters   | notes                | use example                                  |
| ---------------------- | ------------ | -------------------- | -------------------------------------------- |
| InsertTabletAsync      | Tablet       | insert single tablet | session_pool.InsertTabletAsync(tablet)       |
| InsertTabletsAsync     | List<Tablet> | insert tablets       | session_pool.InsertTabletsAsync(tablets)     |
| TestInsertTabletAsync  | Tablet       | test insert tablet   | session_pool.TestInsertTabletAsync(tablet)   |
| TestInsertTabletsAsync | List<Tablet> | test insert tablets  | session_pool.TestInsertTabletsAsync(tablets) |

- ### **SQL语句接口**

| api name                      | parameters | notes                          | use example                                                  |
| ----------------------------- | ---------- | ------------------------------ | ------------------------------------------------------------ |
| ExecuteQueryStatementAsync    | string     | execute sql query statement    | session_pool.ExecuteQueryStatementAsync("select * from root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE where time<15"); |
| ExecuteNonQueryStatementAsync | string     | execute sql nonquery statement | session_pool.ExecuteNonQueryStatementAsync( "create timeseries root.97209_TEST_CSHARP_CLIENT_GROUP.TEST_CSHARP_CLIENT_DEVICE.status with datatype=BOOLEAN,encoding=PLAIN") |

- ### 数据表接口

| api name                   | parameters                                                   | notes                       | use example                                                  |
| -------------------------- | ------------------------------------------------------------ | --------------------------- | ------------------------------------------------------------ |
| SetStorageGroup            | string                                                       | set storage group           | session_pool.SetStorageGroup("root.97209_TEST_CSHARP_CLIENT_GROUP_01") |
| CreateTimeSeries           | string, TSDataType, TSEncoding, Compressor                   | create time series          | session_pool.InsertTabletsAsync(tablets)                     |
| DeleteStorageGroupAsync    | string                                                       | delete single storage group | session_pool.DeleteStorageGroupAsync("root.97209_TEST_CSHARP_CLIENT_GROUP_01") |
| DeleteStorageGroupsAsync   | List<string>                                                 | delete storage group        | session_pool.DeleteStorageGroupAsync("root.97209_TEST_CSHARP_CLIENT_GROUP") |
| CreateMultiTimeSeriesAsync | List<string>, List<TSDataType> , List<TSEncoding> , List<Compressor> | create multi time series    | session_pool.CreateMultiTimeSeriesAsync(ts_path_lst, data_type_lst, encoding_lst, compressor_lst); |
| DeleteTimeSeriesAsync      | List<string>                                                 | delete time series          |                                                              |
| DeleteTimeSeriesAsync      | string                                                       | delete time series          |                                                              |
| DeleteDataAsync            | List<string>, long, long                                     | delete data                 | session_pool.DeleteDataAsync(ts_path_lst, 2, 3)              |

- ### **辅助接口**

| api name                   | parameters | notes                       | use example                                          |
| -------------------------- | ---------- | --------------------------- | ---------------------------------------------------- |
| CheckTimeSeriesExistsAsync | string     | check if time series exists | session_pool.CheckTimeSeriesExistsAsync(time series) |

