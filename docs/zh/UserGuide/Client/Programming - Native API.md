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

# 编程 - 原生接口

## 依赖

* JDK >= 1.8
* Maven >= 3.1

## 安装到本地 maven 库

在根目录下运行:
> mvn clean install -pl session -am -Dmaven.test.skip=true

## 在 maven 中使用原生接口

```
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-session</artifactId>
      <version>0.10.0</version>
    </dependency>
</dependencies>
```

## 原生接口使用示例

下面将给出Session对应的接口的简要介绍和对应参数：

* 初始化Session

  ```
  ​Session(String host, int rpcPort)
  ​Session(String host, String rpcPort, String username, String password)
  ​Session(String host, int rpcPort, String username, String password)
  ```
  
* 开启Session

  ```
  ​Session.open()
  ```
  
* 关闭Session
  ​
  ```
  Session.close()
  ```
  
* 设置存储组

  ```
  void setStorageGroup(String storageGroupId)
  ```

* 删除单个或多个存储组

  ```
  void deleteStorageGroup(String storageGroup)
  void deleteStorageGroups(List<String> storageGroups)
  ```

* 创建单个或多个时间序列

  ```
  void createTimeseries(String path, TSDataType dataType,
          TSEncoding encoding, CompressionType compressor, Map<String, String> props,
          Map<String, String> tags, Map<String, String> attributes, String measurementAlias)
          
  void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
          List<TSEncoding> encodings, List<CompressionType> compressors,
          List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
          List<Map<String, String>> attributesList, List<String> measurementAliasList)
  ```

* 删除一个或多个时间序列

  ```
  void deleteTimeseries(String path)
  void deleteTimeseries(List<String> paths)
  ```

* 删除一个或多个时间序列在某个时间点前的数据

  ```
  void deleteData(String path, long time)
  void deleteData(List<String> paths, long time)
  ```

* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。服务器需要做类型推断，可能会有额外耗时

  ```
  void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
  ```

* 异步方式插入一个 Record，额外提供异步超时时间和异常处理回调函数。一个 Record 是一个设备一个时间戳下多个测点的数据。服务器需要做类型推断，可能会有额外耗时
  
  ```
  void asyncInsertRecord(String deviceId, long time, List<String> measurements, List<String> values
                          long timeout, Consumer<Exception> callback)
  ```

* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。提供数据类型后，服务器不需要做类型推断，可以提高性能

  ```
  void insertRecord(String deviceId, long time, List<String> measurements,
       List<TSDataType> types, List<Object> values)
  ```
  
* 异步插入一个 Record，额外提供异步超时时间和异常处理回调函数。一个 Record 是一个设备一个时间戳下多个测点的数据。提供数据类型后，服务器不需要做类型推断，可以提高性能

  ```
  void asyncInsertRecord(String deviceId, long time, List<String> measurements, 
        List<TSDataType> types, List<String> values, long timeout, Consumer<Exception> callback)
  ```
  
* 插入多个 Record。服务器需要做类型推断，可能会有额外耗时

  ```
  void insertRecords(List<String> deviceIds, List<Long> times, 
                       List<List<String>> measurementsList, List<List<String>> valuesList)
  ```

* 异步方式插入多个 Record，额外提供异步超时时间和异常处理回调函数。服务器需要做类型推断，可能会有额外耗时

  ```
  void asyncInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                          List<List<String>> valuesList,long timeout, Consumer<Exception> callback)
  ```

* 插入多个 Record。提供数据类型后，服务器不需要做类型推断，可以提高性能

  ```
  void insertRecords(List<String> deviceIds, List<Long> times,
        List<List<String>> measurementsList, List<List<TSDataType>> typesList,
        List<List<Object>> valuesList)
  ```
  
* 异步插入多个 Record，额外提供异步超时时间和异常处理回调函数。提供数据类型后，服务器不需要做类型推断，可以提高性能

  ```
  void asyncInsertRecords(List<String> deviceIds, List<Long> times, 
        List<List<String>> measurementsList, List<List<TSDataType>> typesList, 
        List<List<String>> valuesList, long timeout, Consumer<Exception> callback)
  ```

* 插入一个 Tablet，Tablet 是一个设备若干行非空数据块，每一行的列都相同

  ```
  void insertTablet(Tablet tablet)
  ```
  
* 异步方式插入一个 Tablet，额外提供异步超时时间和异常处理回调函数

  ```
  void asyncInsertTablet(Tablet tablet, boolean sorted, long timeout, Consumer<Exception> callback)
  ```

* 插入多个 Tablet

  ```
  void insertTablets(Map<String, Tablet> tablet)
  ```
  
* 异步方式插入多个 Tablet，额外提供异步超时时间和异常处理回调函数

  ```
  void asyncInsertTablets(Map<String, Tablet> tablet, boolean sorted, long timeout, Consumer<Exception> callback)
  ```
  
* 插入同属于一个device的多个 Record。

  ```
  void insertRecordsOfOneDevice(String deviceId, List<Long> times,
        List<List<String>> measurementsList, List<List<TSDataType>> typesList,
        List<List<Object>> valuesList)
  ```

* 原始数据查询。时间间隔包含开始时间，不包含结束时间

  ```
  SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
  ```

* 执行查询语句

  ```
  SessionDataSet executeQueryStatement(String sql)
  ```
  
* 执行非查询语句

  ```
  void executeNonQueryStatement(String sql)
  ```

## 测试客户端逻辑+网络传输代价的接口

* 测试 testInsertRecords，不实际写入数据，只将数据传输到 server 即返回。

   ```
   void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<String>> valuesList)
   ```
  或
  or
    ```
    void testInsertRecords(List<String> deviceIds, List<Long> times,
          List<List<String>> measurementsList, List<List<TSDataType>> typesList,
          List<List<Object>> valuesList)
    ```

* 测试 testInsertRecordsOfOneDevice，不实际写入数据，只将数据传输到 server 即返回。

    ```
    void testInsertRecordsOfOneDevice(List<String> deviceIds, List<Long> times,
          List<List<String>> measurementsList, List<List<TSDataType>> typesList,
          List<List<Object>> valuesList)
    ```

* 测试 insertRecord，不实际写入数据，只将数据传输到 server 即返回。

  ```
  void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
  ```
  或
  ```
    void testInsertRecord(String deviceId, long time, List<String> measurements,
          List<TSDataType> types, List<Object> values)
  ```


* 测试 insertTablet，不实际写入数据，只将数据传输到 server 即返回。

  ```
  void testInsertTablet(Tablet tablet)
  ```
  
  
## 示例代码

浏览上述接口的详细信息，请参阅代码 ```session/src/main/java/org/apache/iotdb/session/Session.java```

使用上述接口的示例代码在 ```example/session/src/main/java/org/apache/iotdb/SessionExample.java```

## 针对原生接口的连接池

我们提供了一个针对原生接口的连接池(`SessionPool`)，使用该接口时，你只需要指定连接池的大小，就可以在使用时从池中获取连接。
如果超过60s都没得到一个连接的话，那么会打印一条警告日志，但是程序仍将继续等待。

当一个连接被用完后，他会自动返回池中等待下次被使用；
当一个连接损坏后，他会从池中被删除，并重建一个连接重新执行用户的操作。

对于查询操作：

1. 使用SessionPool进行查询时，得到的结果集是`SessionDataSet`的封装类`SessionDataSetWrapper`;
2. 若对于一个查询的结果集，用户并没有遍历完且不再想继续遍历时，需要手动调用释放连接的操作`closeResultSet`;
3. 若对一个查询的结果集遍历时出现异常，也需要手动调用释放连接的操作`closeResultSet`.
4. 可以调用 `SessionDataSetWrapper` 的 `getColumnNames()` 方法得到结果集列名 

使用示例可以参见 ```session/src/test/java/org/apache/iotdb/session/pool/SessionPoolTest.java```

或 `example/session/src/main/java/org/apache/iotdb/SessionPoolExample.java`

## 0.9-0.10 版本IoTDB Session 接口更新

从0.9到0.10版本的IoTDB session接口有了较大改变。一部分接口名称和参数类型发生了变化，另外新增了大量可用接口。所有session接口抛出的异常类型 *IoTDBSessionExeception* 更改为 *IoTDBConnectionException* 和 *StatementExecutionExeception* 。下面详细介绍具体接口的变化。

### 接口名称更改

#### insert()

用于插入一行数据，需提供数据点的deviceId, time, 所有measurement和相应的value值。

```
void insert(String deviceId, long time, List<String> measurements, List<String> values)
```


该方法在0.10版本中方法名发生变化

```
void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```

#### insertRowInBatch()

用于插入多行数据，需提供各行数据的deviceId, time, 所有measurement名称和相应的value值。

```
void insertRowInBatch(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList,   
                      List<List<String>> valuesList)
```


该方法在0.10版本中方法名发生变化

```
void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                   List<List<String>> valuesList)
```

#### insertBatch()

在0.9版本中用于以"RowBatch"结构为单位插入数据

```
void insertBatch(RowBatch rowBatch)
```

在0.10版本中"RowBatch"类型更改为"Tablet"类型，因此方法名也随之改变。

```
void insertTablet(Tablet tablet)
```

#### testInsertRow()

用于测试插入一行接口的响应

```
void testInsertRow(String deviceId, long time, List<String> measurements, List<String> values)
```

在0.10版本中方法名改为testInsertRecord。

```
void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```

#### testInsertRowInBatch()

用于测试插入多行数据接口的响应

```
void testInsertRowInBatch(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                          List<List<String>> valuesList)
```

在0.10版本中方法名改为testInsertRecords

```
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                       List<List<String>> valuesList)
```

#### testInsertBatch

用于测试以RowBatch结构为单位插入数据的响应

```
void testInsertBatch(RowBatch rowBatch)
```

在0.10版本中RowBatch类型更改为Tabet类型，因此方法名也随之改变为testInsertTablet

```
void testInsertTablet(Tablet tablet)
```



### 新增接口

```
void open(boolean enableRPCCompression)
```

开启一个session，并指定是否启用RPC压缩。注意客户端开启PRC压缩的状态需与服务端保持一致。

```
void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
```

插入一行数据，该方法和已有的insertRecord()方法不同在于需额外提供每个measurement的类型信息types，且参数values以原始类型的方式提供。写入速度相对于参数为String格式的insertRecord接口要快一些。

```
void insertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                   List<List<TSDataType>> typesList, List<List<Object>> valuesList)
```

插入多行数据，该方法和已有的insertRecords()方法不同在于需额外提供每个measurement的类型信息typesList，且参数valuesList以原始类型的方式提供。写入速度相对于参数为String格式的insertRecords接口要快一些。

```
void insertTablet(Tablet tablet, boolean sorted)
```

提供额外的sorted参数，表示tablet是否内部已排好序，如sorted为真则会省去排序的过程从而提升处理速度。

```
void insertTablets(Map<String, Tablet> tablets)
```

新增insertTablets接口用于写入多个tablet结构，tablets参数为Map<device名, tablet数据>

```
void insertTablets(Map<String, Tablet> tablets, boolean sorted)
```

带额外sorted参数的insertTablets接口

```
void testInsertRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, 
                      List<Object> values)
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, 
                      List<List<TSDataType>> typesList, List<List<Object>> valuesList)
void testInsertTablet(Tablet tablet, boolean sorted)
void testInsertTablets(Map<String, Tablet> tablets)
void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
```

以上接口均为新增的测试rpc响应的方法，用于测试新增的写入接口

```
void createTimeseries(String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor, 	
                      Map<String, String> props, Map<String, String> tags, Map<String, String> attributes, 
                      String measurementAlias)
```

在原来createTimeseries接口的基础上，创建时间序列可以额外指定时间序列的props, tags, attributes和measurementAlias。如果不需要指定以上额外参数可以将参数设为null。

```
void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes, List<TSEncoding> encodings, 
                           List<CompressionType> compressors, List<Map<String, String>> propsList, 
                           List<Map<String, String>> tagsList, List<Map<String, String>> attributesList, 
                           List<String> measurementAliasList)
```

一次性创建多个时间序列，同时也可以指定多个时间序列的props, tags, attributes和measurementAlias。

```
boolean checkTimeseriesExists(String path)
```

用于检测时间序列是否存在

```
public Session(String host, int rpcPort, String username, String password,
      boolean isEnableCacheLeader)
```

开启一个session，并指定是否启用leader缓存。注意此接口对于分布式写入能够提高性能，但对于单机版写入反而会给客户端增加较少的消耗。