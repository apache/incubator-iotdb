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

# 元数据管理

IoTDB 的元数据统一由 MManger 管理，包括以下几个部分：

* 元数据树
* 元数据日志管理
* 标签/属性管理


## MManager

* 维护 tag 倒排索引：`Map<String, Map<String, Set<LeafMNode>>> tagIndex`

	> tag key -> tag value -> timeseries LeafMNode

该类初始化时，会replay mlog中的内容，将元数据信息还原出来，元数据操作日志主要有六种类型，每个操作前都会先获得整个元数据的写锁（存储在MManager中），操作完后释放：

* 创建时间序列
	* 检查存储组是否存在，如果不存在且开启自动创建，则创建存储组
	* 在 MTree 中创建时间序列，可带扩展名
	* 如果开启动态参数，检查内存是否满足
	* 如果非重启（需要记录日志）
		* 拿到标签文件当前 offset
		* 将时间序列信息记录到 mlog 中
		* 将 tags/attributes 持久化到tlog中
	* 如果是重启（需要恢复内存结构）
		* 根据 mlog 中的 offset 读取标签文件，重建 tagIndex 索引

* 删除时间序列
	* 获得所有满足前缀的时间序列的fullPath
	* 遍历上一步获得的所有时间序列，并逐个在 MTree 中删除
		* 删除的时候，需要首先获得该时间序列所在device的InternalMNode的写锁
		* 若获取成功
			* 删除MTree中对应的LeafMNode
			* 根据LeafMNode中的offset，读取标签文件，更新 tagIndex 索引
			* 若发现某一个存储组为空，则记录下该空的存储组名
		* 若获取失败
			* 返回此未删除成功的时间序列
	* 如果非重启（需要记录日志）
	      *  删除记录下的空的存储组
		* 将所删除的时间序列信息记录到 mlog 中
		* 目前并不会删除 tlog 中关于此时间序列的标签/属性信息。
	

* 设置存储组
	* 在 MTree 中创建存储组
	* 如果开启动态参数，检查内存是否满足
	* 如果非重启，将log持久化至mlog中

* 删除存储组
	* 在 MTree 中删除对应的存储组，并返回该存储组下的所有时间序列的LeafMNode
		* 删除的时候，需要首先获得该存储组的InternalMNode的写锁
		* 若获取成功
			* 删除MTree中对应的InternalMNode
		* 若获取失败
			* 返回此未删除成功的存储组名
	* 遍历上一步返回的LeafMNode，根据LeafMNode中的offset，读取标签文件，更新tagIndex 索引
	* 如果非重启，将log持久化至mlog中

* 设置TTL
	* 获得对应存储组的StorageGroupMNode，修改其TTL属性
	* 如果非重启，将log持久化至mlog中

* 改变时间序列标签信息offset
	* 修改时间序列对应的LeafMNode中的offset
	
* 改变时间序列的别名
    * 更新LeafMNode中的alias属性，并更新其父节点中的aliasMap属性


除了这七种需要记录日志的操作外，还有六种对时间序列标签/属性信息进行更新的操作，同样的，每个操作前都会先获得整个元数据的写锁（存储在MManager中），操作完后释放：

* 重命名标签或属性
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签和属性信息
	* 若新键已经存在，则抛异常
	* 若新键不存在
		* 若旧键不存在，则抛异常
		* 若旧键存在，用新键替换旧键，并持久化至tlog中
		* 如果旧键是标签，则还需更新 tagIndex

* 重新设置标签或属性的值
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签和属性信息
	* 若需要重新设置的标签或属性的键值不存在，则抛异常
	* 若需要重新设置的是标签，则还需更新 tagIndex
	* 将更新后的标签和属性信息持久化至tlog中

* 删除已经存在的标签或属性
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签和属性信息
	* 遍历需要删除的标签或属性，若不存在，则跳过
	* 若需要删除的是标签，则还需更新 tagIndex
	* 将更新后的标签和属性信息持久化至tlog中

* 添加新的标签
	* 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取标签信息
	* 遍历需要添加的标签，若已存在，则抛异常
	* 将更新后的标签信息持久化至tlog中
	* 根据添加的标签信息，更新 tagIndex

* 添加新的属性
   * 获得该时间序列的LeafMNode
	* 通过 LeafMNode 中的 offset 读取属性信息
	* 遍历需要添加的属性，若已存在，则抛异常
	* 将更新后的属性信息持久化至tlog中

* 更新插入标签和属性
	* 获得该时间序列的LeafMNode
	* 更新LeafMNode中的alias属性，并更新其父节点中的aliasMap属性
	* 讲更新后的别名持久化至mlog中
	* 通过 LeafMNode 中的 offset 读取标签和属性信息
	* 遍历需要更新插入的标签和属性，若已存在，则用新值更新；若不存在，则添加
	* 将更新后的属性信息持久化至tlog中
	* 如果包含更新插入中包含标签信息，还需更新 tagIndex



## 元数据树

* org.apache.iotdb.db.metadata.MTree

树中包括三种节点：StorageGroupMNode、InternalMNode（非叶子节点）、LeafMNode（叶子节点），他们都是MNode的子类。

每个InternalMNode中都有一个读写锁，查询元数据信息时，需要获得路径上每一个InternalMNode的读锁，修改元数据信息时，如果修改的是LeafMNode，需要获得其父节点的写锁，若修改的是InternalMNode，则只需获得本身的写锁。若该InternalMNode位于Device层，则还包含了一个`Map<String, MNode> aliasChildren`，用于存储别名信息。

StorageGroupMNode 继承 InternalMNode，包含存储组的元数据信息，如 TTL。

LeafMNode 中包含了对应时间序列的Schema信息，其别名(若没有别名，则为null)以及该时间序列的标签/属性信息在tlog文件中的offset(若没有标签/属性，则为-1)

示例：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625246-fc3e8200-467e-11ea-8815-67b9c4ab716e.png">

IoTDB 的元数据管理采用目录树的形式，倒数第二层为设备层，最后一层为传感器层。

默认存在根节点 root，创建存储组、删除存储组、创建时间序列、删除时间序列均为对树的节点的操作。

* 创建存储组（root.a.b.sg）
	* 为当前存储组创建中间节点（a.b）
	* 确保此路径前缀不存在其他存储组（不允许存储组嵌套）
	* 检查确保存储组不存在
	* 创建存储组节点

* 创建时间序列（root.a.b.sg.d.s1）
	* 遍历路径，确保存储组已经创建
	* 找到倒数第二层节点，检查叶子节点是否存在
	* 创建叶子节点，叶子节点内存储别名
	* 如果有别名，则在设备节点多创建一个链接指向叶子节点

* 删除存储组和删除时间序列的操作相似，即将存储组或时间序列节点在其父节点中删除，时间序列节点还需要将其别名在父节点中删除；若在删除过程中，发现某一节点没有任何子节点了，还需要递归删除此节点。

## MTree 检查点

### 创建条件

为了加快 IoTDB 重启速度，我们为 MTree 设置了检查点，这样避免了在重启时按行读取并复现 `mlog.bin` 中的信息。创建 MTree 的快照有两种方式：
1. 后台线程检查自动创建：每隔10分钟，后台线程检查 MTree 的最后修改时间，需要同时满足
  * 用户超过1小时（可配置）没修改 MTree，即`mlog.bin` 文件超过1小时没有修改
  * `mlog.bin` 中积累了100000行日志（可配置）
  
2. 手动创建：使用`create snapshot for schema`命令手动触发创建 MTree 快照

### 创建过程

方法见`MManager.createMTreeSnapshot()`：

1. 首先给 MTree 加读锁，防止创建快照过程中对其进行修改
2. 将 MTree 序列化进临时 snapshot 文件（`mtree.snapshot.tmp`）。MTree 的序列化采用“先子节点、后父节点”的深度优先序列化方式，将节点的信息按照类型转化成对应格式的字符串，便于反序列化时读取和组装MTree。
  * 普通节点：0,名字,子节点个数
  * 存储组节点：1,名字,TTL,子节点个数
  * 传感器节点：2,名字,别名,数据类型,编码,压缩方式,属性,偏移量,子节点个数
  
3. 序列化结束后，将临时文件重命名为正式文件（`mtree.snapshot`），防止在序列化过程中出现服务器人为或意外关闭，导致序列化失败的情况。
4. 调用`MLogWriter.clear()`方法，清空 `mlog.bin`：
  * 关闭 BufferedWriter，删除`mlog.bin`文件；
  * 新建一个 BufferedWriter；
  * 将 `logNumber` 置为0，`logNumber` 记录`mlog.bin`的日志行数，用于在后台检查时判断其是否超过用户配置的阈值而触发自动创建快照。

5. 释放 MTree 读锁

### 恢复过程

方法见`MManager.initFromLog()`：

1. 检查临时文件`mtree.snapshot.tmp`是否存在，如果存在证明在创建快照的序列化过程中出现服务器人为或意外关闭，导致序列化失败，删除临时文件；
2. 检查快照文件`mtree.snapshot`是否存在。如果不存在，则使用新的 MTree；否则启动反序列化过程，得到 MTree
3. 对于`mlog.bin`中的内容，逐行读取并操作，完成 MTree 的恢复。

## 元数据日志管理

所有元数据的操作均会记录到元数据日志文件中，此文件默认为 data/system/schema/mlog.bin。

系统重启时会重做 mlog.bin 中的日志，重做之前需要标记不需要记录日志。当重启结束后，标记需要记录日志。

mlog.bin 存储二进制编码。我们可以使用[MlogParser Tool](https://iotdb.apache.org/UserGuide/Master/System-Tools/MLogParser-Tool.html) 将mlog解析为可读的mlog.txt版本。

示例元数据操作及对应的 mlog 解析后记录：

* 创建一个名为root.turbine的存储组

	> mlog: 2,root.turbine
	
	> 格式: 2,path

* 删除一个名为root.turbine的存储组
	
	> mlog: 11,root.turbine
	
	> 格式: 11,path

* 创建时间序列 create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)

	> mlog: 0,root.turbine.d1.s1,3,2,1,,温度,offset
	
	> 格式: 0,path,TSDataType,TSEncoding,CompressionType,[properties],[alias],[tag-attribute offset]

* 删除时间序列 root.turbine.d1.s1

	> mlog: 1,root.turbine.d1.s1
	
	> 格式: 1,path	

* 给 root.turbine 设置时间为 10 秒的 ttl
	
	> mlog: 10,root.turbine,10
		
	> 格式: 10,path,ttl

* alter timeseries root.turbine.d1.s1 add tags(tag1=v1)
   > 只有当root.turbine.d1.s1原来不存在任何标签/属性信息时，该sql才会产生日志
   
   > mlog: 12,root.turbine.d1.s1,10
   
	> 格式: 10,path,[change offset]

* alter timeseries root.turbine.d1.s1 UPSERT ALIAS=newAlias
   
   > mlog: 13,root.turbine.d1.s1,newAlias
   
   > 格式: 13,path,[new alias]
   
* 创建元数据模版 create schema template temp1(
  s1 INT32 with encoding=Gorilla and compression SNAPPY,
  s2 FLOAT with encoding=RLE and compression=SNAPPY
)
   
   > mlog:5,temp1,0,s1,1,8,1
   
   > mlog:5,temp1,0,s2,3,2,1
   
   > 格式： 5,template name,is Aligned Timeseries,measurementId,TSDataType,TSEncoding,CompressionType

* 在某前缀路径上设置元数据模版 set schema template temp1 to root.turbine
 
    > mlog: 6,temp1,root.turbine
   
    > 格式: 6,template name,path

* 自动创建设备 （应用场景为在某个前缀路径上设置模版之后，写入时会自动创建设备）
 
    > mlog: 4,root.turbine.d1
   
    > 格式: 4,path

* 设置某设备正在使用模版 （应用场景为在某个设备路径上设置模版之后，表示该设备正在应用模版）
 
    > mlog: 61,root.turbine.d1
   
    > 格式: 61,path

## 标签文件
* org.apache.iotdb.db.metadata.TagLogFile


所有时间序列的标签/属性信息都会保存在标签文件中，此文件默认为 data/system/schema/mlog.bin。

* 每条时间序列的 tags 和 attributes 持久化总字节数为 L，在 iotdb-engine.properties 中配置。

* 持久化内容：Map<String,String> tags, Map<String,String> attributes，如果内容不足 L，则需补空。

> create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes (attr1=v1, attr2=v2)

> 在tlog.txt中的内容：

> tagsSize (tag1=v1, tag2=v2) attributesSize (attr1=v1, attr2=v2)

## 元数据查询

### 不带过滤条件的元数据查询

主要查询逻辑封装在`MManager`的`showTimeseries(ShowTimeSeriesPlan plan)`方法中

首先判断需不需要根据热度排序，如果需要，则调用`MTree`的`getAllMeasurementSchemaByHeatOrder`方法，否则调用`getAllMeasurementSchema`方法

#### getAllMeasurementSchemaByHeatOrder

这里的热度是用每个时间序列的`lastTimeStamp`来表征的，所以需要先取出所有满足条件的序列，然后根据`lastTimeStamp`进行排序，然后再做`offset`和`limit`的截断

#### getAllMeasurementSchema

这里需要在findPath的时候就将limit（如果没有limit，则将请求的fetchSize当成limit）和offset参数传递下去，减少内存占用。

#### findPath

这个方法封装了在MTree中遍历得到满足条件的时间序列的逻辑，是个递归方法，由根节点往下递归寻找，直到当前时间序列数量达到limit或者已经遍历完整个MTree。

### 带过滤条件的元数据查询

这里的过滤条件只能是tag属性，否则抛异常。

通过在MManager中维护的tag的倒排索引，获得所有满足索引条件的`MeasurementMNode`。

若需要根据热度排序，则根据`lastTimeStamp`进行排序，否则根据序列名的字母序排序，然后再做`offset`和`limit`的截断。

### ShowTimeseries结果集

如果元数据量过多，一次show timeseries的结果可能导致OOM，所以增加fetch size参数，客户端跟服务器端交互时，服务器端一次最多只会取fetch size个时间序列。

多次交互的状态信息就存在`ShowTimeseriesDataSet`中。`ShowTimeseriesDataSet`中保存了此次的`ShowTimeSeriesPlan`，当前的游标`index`以及缓存的结果行列表`List<RowRecord> result`。

* 判断游标`index`是否等于缓存的结果行`List<RowRecord> result`的size
    * 若相等，则调用MManager中的`showTimeseries`方法取结果，放入缓存
        * 需要相应的修改plan中的offset，将offset向前推fetch size大小
        * 若`hasLimit`为`false`，则将index重新置为0
    * 若不相等
        * `index < result.size()`，返回true
        * `index > result.size()`，返回false        
