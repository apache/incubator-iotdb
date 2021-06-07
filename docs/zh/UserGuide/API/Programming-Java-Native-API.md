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

## Java 原生接口

### 依赖

* JDK >= 1.8
* Maven >= 3.6



### 安装方法

在根目录下运行:

```shell
mvn clean install -pl session -am -Dmaven.test.skip=true
```



### 在MAVEN中使用原生接口

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-session</artifactId>
      <version>0.13.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```



### 原生接口说明

下面将给出Session对应的接口的简要介绍和对应参数：

* 初始化Session

```java
Session(String host, int rpcPort)
Session(String host, String rpcPort, String username, String password)
Session(String host, int rpcPort, String username, String password)
```

* 开启Session

```java
Session.open()
```

* 关闭Session

```java
Session.close()
```

* 设置存储组

```java
void setStorageGroup(String storageGroupId)
```

* 删除单个或多个存储组

```java
void deleteStorageGroup(String storageGroup)
void deleteStorageGroups(List<String> storageGroups)
```

* 创建单个或多个时间序列

```java
void createTimeseries(String path, TSDataType dataType,
      TSEncoding encoding, CompressionType compressor, Map<String, String> props,
      Map<String, String> tags, Map<String, String> attributes, String measurementAlias)
      
void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
      List<TSEncoding> encodings, List<CompressionType> compressors,
      List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList, List<String> measurementAliasList)
```

* 创建对齐时间序列

```
void createAlignedTimeseries(String devicePath, List<String> measurements,
      List<TSDataType> dataTypes, List<TSEncoding> encodings,
      CompressionType compressor, List<String> measurementAliasList);
```

注意：目前**暂不支持**使用传感器别名。

* 删除一个或多个时间序列

```java
void deleteTimeseries(String path)
void deleteTimeseries(List<String> paths)
```

* 删除一个或多个时间序列在某个时间点前或这个时间点的数据

```java
void deleteData(String path, long time)
void deleteData(List<String> paths, long time)
```

* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。服务器需要做类型推断，可能会有额外耗时

```java
void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```

* 插入一个 Tablet，Tablet 是一个设备若干行非空数据块，每一行的列都相同

```java
void insertTablet(Tablet tablet)
```

* 插入多个 Tablet

```java
void insertTablets(Map<String, Tablet> tablet)
```

* 插入多个 Record。服务器需要做类型推断，可能会有额外耗时

```java
void insertRecords(List<String> deviceIds, List<Long> times, 
                   List<List<String>> measurementsList, List<List<String>> valuesList)
```

* 插入一个 Record，一个 Record 是一个设备一个时间戳下多个测点的数据。提供数据类型后，服务器不需要做类型推断，可以提高性能

```java
void insertRecord(String deviceId, long time, List<String> measurements,
   List<TSDataType> types, List<Object> values)
```

* 插入单个点，一个 Record 是一个设备一个时间戳下单个测点的数据。提供数据类型后，服务器不需要做类型推断，可以提高性能

```java
void insertSinglePoint(String deviceId, long time, String measurement, TSDataType type, String value)
```

* 插入单个点，一个 Record 是一个设备一个时间戳下单个测点的数据。服务器需要做类型推断，可能会有额外耗时

```java
void insertStringSinglePoint(String deviceId, long time, String measurement, String value)
```

* 插入多个 Record。提供数据类型后，服务器不需要做类型推断，可以提高性能

```java
void insertRecords(List<String> deviceIds, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```

* 插入同属于一个device的多个 Record。

```java
void insertRecordsOfOneDevice(String deviceId, List<Long> times,
    List<List<String>> measurementsList, List<List<TSDataType>> typesList,
    List<List<Object>> valuesList)
```

* 原始数据查询。时间间隔包含开始时间，不包含结束时间

```java
SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
```

* 执行查询语句

```java
SessionDataSet executeQueryStatement(String sql)
```

* 执行非查询语句

```java
void executeNonQueryStatement(String sql)
```

* 创建一个设备模板

```
* name: 设备模板名称
* measurements: 工况名称列表，如果该工况是非对齐的，直接将其名称放入一个list中再放入measurements中，
*               如果该工况是对齐的，将所有对齐工况名称放入一个list再放入measurements中
* dataTypes: 数据类型名称列表，如果该工况是非对齐的，直接将其数据类型放入一个list中再放入dataTypes中，
             如果该工况是对齐的，将所有对齐工况的数据类型放入一个list再放入dataTypes中
* encodings: 编码类型名称列表，如果该工况是非对齐的，直接将其数据类型放入一个list中再放入encodings中，
             如果该工况是对齐的，将所有对齐工况的编码类型放入一个list再放入encodings中
* compressors: 压缩方式列表                          
void createDeviceTemplate(
      String name,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
```


* 将名为'templateName'的设备模板挂载到'prefixPath'路径下，在执行这一步之前，你需要创建名为'templateName'的设备模板

``` 
void setDeviceTemplate(String templateName, String prefixPath)
```




### 测试接口说明

* 测试 testInsertRecords，不实际写入数据，只将数据传输到 server 即返回。

```java
void testInsertRecords(List<String> deviceIds, List<Long> times, List<List<String>> measurementsList, List<List<String>> valuesList)
```
  或

```java
void testInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
```

* 测试 insertRecord，不实际写入数据，只将数据传输到 server 即返回。

```java
void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
```
  或

```java
void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values)
```


* 测试 insertTablet，不实际写入数据，只将数据传输到 server 即返回。

```java
void testInsertTablet(Tablet tablet)
```



### 针对原生接口的连接池

我们提供了一个针对原生接口的连接池(`SessionPool`)，使用该接口时，你只需要指定连接池的大小，就可以在使用时从池中获取连接。
如果超过60s都没得到一个连接的话，那么会打印一条警告日志，但是程序仍将继续等待。

当一个连接被用完后，他会自动返回池中等待下次被使用；
当一个连接损坏后，他会从池中被删除，并重建一个连接重新执行用户的操作。

对于查询操作：

1. 使用SessionPool进行查询时，得到的结果集是`SessionDataSet`的封装类`SessionDataSetWrapper`;
2. 若对于一个查询的结果集，用户并没有遍历完且不再想继续遍历时，需要手动调用释放连接的操作`closeResultSet`;
3. 若对一个查询的结果集遍历时出现异常，也需要手动调用释放连接的操作`closeResultSet`.
4. 可以调用 `SessionDataSetWrapper` 的 `getColumnNames()` 方法得到结果集列名 

使用示例可以参见 `session/src/test/java/org/apache/iotdb/session/pool/SessionPoolTest.java`

或 `example/session/src/main/java/org/apache/iotdb/SessionPoolExample.java`

使用对齐时间序列和设备模板的示例可以参见 `example/session/src/main/java/org/apache/iotdb/VectorSessionExample.java`。

  

### 示例代码

浏览上述接口的详细信息，请参阅代码 ```session/src/main/java/org/apache/iotdb/session/Session.java```

使用上述接口的示例代码在 ```example/session/src/main/java/org/apache/iotdb/SessionExample.java```



### 集群信息相关的接口 (仅在集群模式下可用)

集群信息相关的接口允许用户获取如数据分区情况、节点是否当机等信息。
要使用该API，需要增加依赖：

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.iotdb</groupId>
      <artifactId>iotdb-thrift-cluster</artifactId>
      <version>0.13.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

建立连接与关闭连接的示例:

```java
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.iotdb.rpc.RpcTransportFactory;

    public class CluserInfoClient {
      TTransport transport;
      ClusterInfoService.Client client;
      public void connect() {
          transport =
              RpcTransportFactory.INSTANCE.getTransport(
                  new TSocket(
                      // the RPC address
                      IoTDBDescriptor.getInstance().getConfig().getRpcAddress(),
                      // the RPC port
                      ClusterDescriptor.getInstance().getConfig().getClusterRpcPort()));
          try {
            transport.open();
          } catch (TTransportException e) {
            Assert.fail(e.getMessage());
          }
          //get the client
          client = new ClusterInfoService.Client(new TBinaryProtocol(transport));
       }
      public void close() {
        transport.close();
      }  
    }
```

API列表：

* 获取集群中的各个节点的信息（构成哈希环）

```java
list<Node> getRing();
```

* 给定一个路径（应包括一个SG作为前缀）和起止时间，获取其覆盖的数据分区情况:

```java 
    /**
     * @param path input path (should contains a Storage group name as its prefix)
     * @return the data partition info. If the time range only covers one data partition, the the size
     * of the list is one.
     */
    list<DataPartitionEntry> getDataPartition(1:string path, 2:long startTime, 3:long endTime);
```

* 给定一个路径（应包括一个SG作为前缀），获取其被分到了哪个节点上:
```java  
    /**
     * @param path input path (should contains a Storage group name as its prefix)
     * @return metadata partition information
     */
    list<Node> getMetaPartition(1:string path);
```

* 获取所有节点的死活状态:
```java
    /**
     * @return key: node, value: live or not
     */
    map<Node, bool> getAllNodeStatus();
```

* 获取当前连接节点的Raft组信息（投票编号等）（一般用户无需使用该接口）:
```java  
    /**
     * @return A multi-line string with each line representing the total time consumption, invocation
     *     number, and average time consumption.
     */
    string getInstrumentingInfo();
```
