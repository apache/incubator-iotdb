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

# 相似性索引框架文档

[TOC]

## 第1章 任务目标

### 背景

在最近的二十年间，谷歌一系列重要工作的发表 （GFS ，BigTable ，Map-Reduce 等），以及相应的开源框架的兴起 （Hadoop，HBase 等） 推动了大数据技术的兴起和蓬勃发展。以BigTable，HBase，Cassandra 为代表的NoSQL数据库系统对新一代数据库技术产生了巨大的影响。在这一背景下，为了管理气象、医疗、工业物联网等领域产生的大量时间序列数据，时序数据库系统也得到了快速发展 。

然而，现有数据库系统通常仅支持经典查询（点查询，范围查询和带值过滤条件的查询），常见的聚合查询（最大值，最小值，均值，求和等）和少量领域相关操作（如按时间间隔聚合，按时间戳对齐等）等等。
在传统的关系型数据库系统中，如Oracle和MySQL，往往只集成了几种被广泛使用的索引技术，如用于加速范围查询的B树和加速等值查询的哈希表。很多当前主流的时序数据库系统（如InfluxDB，OpenTSDB、Apache IoTDB）仍不支持时间序列的相似性索引。InfluxDB提出了时间序列索引（Time Series Index，TSI）。但主要用于对海量时间序列的路径、标签等元信息的快速查找。在机器学习领域，开发者提出了许多索引代码库，但主要面向近似检索，同时并未与时序数据库系统进行集成。基于时序数据库系统的索引机制仍然是有待解决的问题。

针对时序数据的相似性索引一直是非常重要的研究课题。研究者们提出了一系列索引结构，致力于解决全序列检索和子序列检索问题。然而，研究者提出的索引技术通常以独立项目的形式存在。在应用于生产场景时，需要全面考虑数据采集/存储、索引构建/查询接口、数据更新、数据容灾等问题。时序数据库系统具有规范化的查询语言、丰富而健壮的读写接口、高效的数据读写性能以及较为完善的容灾策略。将相似性索引技术集成到时序数据库系统中，不仅丰富了数据库系统的功能，同时也会简化索引在面对复杂应用场景时的适配工作，提升索引的易用性，有助于索引技术在生产实践中落地。然而，数据库系统是复杂的，与数据库系统的对接必须综合考虑SQL定义、执行计划、读写流程以及返回格式定义等技术细节。这些复杂的问题制约了索引与数据库系统的深度集成。



#### 目标

使IoTDB支持时间序列相似性检索功能，并集成包括复合模式索引、R树索引在内的多种索引技术。

## 详细设计

### 基本概念

#### 什么是“相似性检索”

相似性搜索是时间序列领域中最重要的方向之一。与IoTDB中现有的查询条件不同，相似性搜索输入一条“序列”作为查询，目的是高效地从数据库中找到一系列与“查询序列”相似的其他。

所谓“相似”：两条序列之间的“相似性”取决于它们的“欧氏距离”或其他距离函数。

如下图形象地展示了如何用“欧氏距离（Euclidean）”和“动态时间规整距离（DTW）”度量两条时间序列的相似性：

![image-20210225015048345](https://user-images.githubusercontent.com/5548915/109539640-849a8b00-7afc-11eb-8c95-b666d6dac2ca.png)



#### 什么是“特征索引”

在关系型数据库中，一些经典的索引结构，如B树和哈希表，都是对关系表中的属性创建索引。而相似性索引的检索对象是高维序列。由于高维序列存在维度灾难问题，相似性索引需要首先将高维序列转换成低维特征，然后对低维特征创建索引。如果将关系型数据库系统中的B树和哈希表称为“基于属性的索引”，那么时序数据库系统中的相似性索引可以被视为“基于特征的索引”。

数据库系统中的特征索引从逻辑上可以被划分为三层结构，自下而上依次是数据层、特征层和索引层：

 * 数据层：对应着数据空间中的原始序列。 在IoTDB中，系统接收大量传感器数据并将其组织成TsFile文件格式写入磁盘；
 * 特征层：对应着特征空间中的低维特征。 每条特征的大小通常远小于原始时间序列。 一条特征记录包含两部分信息：特征向量和指向数据文件的指针；
 * 索引层：对应着索引结构。通过额外的内存或磁盘结构，索引结构可以对特征进行有效的组织。索引结构的大小通常远小于由它组织的特征和原始序列。在索引结构底层的索引项中包含了一个或多个指向特征的指针；

在构造索引时，索引机制首先将数据层中的原始序列转换为特征，然后写入索引结构中。在执行查询时，索引机制将查询序列转换为特征，然后输入索引结构。通过索引结构和特征信息对被检索集进行剪枝，得到候选集，候选集中包含了一系列指向原始数据的指针；然后访问候选集所对应的原始数据，得到最终的查询结果。

###  功能定义

索引框架将满足两类使用者的需求：

* 对数据库管理员和用户来说，索引机制设计了**索引创建、查询和删除的SQL语句**，并设计了与IoTDB其他查询相一致的返回结果格式。
* 对索引实现者来说，索引机制暴露出一套与索引技术密切相关的接口，包括特征选型、索引插入和索引过滤等等。索引实现者可以在不必关心数据库系统技术细节的情况下，便捷地集成各种索引技术。

#### 用户功能：面向用户和DBA

> 注：面向用户和DBA的代码主要涉及SQL和查询计划的改动，这部分代码已经合并到主分支中，参见[PR-2024(IoTDB-804)](https://github.com/apache/iotdb/pull/2024)。

本节将从两个工业场景中的示例出发，阐述两种检索场景（全序列检索和子序列检索）的使用方式。

##### 全序列检索

![image](https://user-images.githubusercontent.com/5548915/109540349-4782c880-7afd-11eb-9c67-ae83697f77d3.png)

上图展示了制药工厂在IoTDB中的数据库模式。为了简洁清晰，图中省略了部分与本节无关的信息。工厂包含多个发酵罐（Ferm01、Ferm02等等），每个发酵罐逐批次地进行红霉素发酵工作，每个批次持续时间为7天。对于每个批次（如Ferm02发酵罐的20191017批次），工厂会对发酵过程中的物理量进行记录，包括葡萄糖补糖率（Glu）、二氧化碳释放率（CER）、酸碱度（pH）等等。

分析人员希望为所有发酵罐、所有批次的补糖率序列（Glu）创建基于“PAA特征的R树索引”（记为RTREE_PAA）。创建索引格式如下：

```sql
CREATE INDEX ON seriesPath
WITH INDEX=indexName (, key=value)* 
```

除了必须指定索引名之外，还需要传入一系列创建索引所需的参数。在本案例中，创建索引语句为：

```sql
CREATE INDEX ON root.Ery.*.Glu 
WITH INDEX=RTREE_PAA, PAA_DIM=8
```

索引类别为RTREE_PAA，即基于PAA特征的R树索引：将时间序列转换为8维PAA特征，然后交由R树组织。

创建索引语句不限制索引参数的数量和格式，以此保证良好的可拓展性。用户提交的参数将由索引注册表记录。当索引实例被加载到内存中时，索引机制会将参数信息传给索引实例。

分析人员希望在所有发酵罐的所有批次中，寻找与某种“补糖策略”最接近的2条补糖率曲线，查询语句如下：

```sql
SELECT TOP 2 Glu 
FROM root.Ery.* 
WHERE Glu LIKE (0, 120, 20, 80, ..., 120, 100, 80, 0)
```

通过关键字LIKE，IoTDB的查询处理器将这条查询语句交给索引机制执行。
为了与IoTDB的经典查询结果格式保持一致，全序列检索的每一条结果单独成列，并按照时间戳顺序对齐。如下图所示，与给定查询序列最接近的两条序列分别是发酵罐Ferm01的20191018批次和发酵罐Ferm02的20191024批次。两条序列按照时间戳对齐。

![image](https://user-images.githubusercontent.com/5548915/109540059-f70b6b00-7afc-11eb-938e-d921a9a5d667.png)

索引机制允许用户删除一个被创建的索引，删除索引格式如下：

```sql
DROP INDEX indexName ON seriesPath
```

删除索引的格式非常简单。在本例中，分析人员通过以下语句删除索引：

```sql
DROP INDEX RTREE_PAA ON root.Ery.*.Glu 
```

##### 子序列检索

![image](https://user-images.githubusercontent.com/5548915/109540439-65502d80-7afd-11eb-87d6-cf9b7e46ecfd.png)

上图展示了风力发电厂在IoTDB中的数据库模式。风电厂包含多个风机（AZQ01、AZQ02等等），每个风机对风机状态和周围环境进行持续监测。监测传感器包括风速（Speed）、风向（Direction）、风机功率（Power）等等。

分析人员希望为风机AZQ02的风速传感器序列（Speed）创建“等长数据块索引”。创建索引语句为：

```sql
CREATE INDEX ON root.Wind.AZQ02.Speed
WITH INDEX=ELB_INDEX, Block_Size=5
```

创建索引后，分析人员希望在AZQ02风机的风速序列中寻找与"极端运行阵风"相似的子序列。其中极端运行阵风为复合模式，在风速高峰期阈值要求小于4，在其他区间段阈值要求小于2。查询语句如下：

```sql
SELECT Speed.* FROM root.Wind.AZQ02
WHERE Speed 
CONTAIN (15, 14, 12, ..., 12, 12, 11) WITH TOLERANCE 2
CONCAT  (10, 20, 25, ..., 24, 14, 8) WITH TOLERANCE 4
CONCAT  (8, 9, 10, ..., 14, 15, 15) WITH TOLERANCE 2
```

通过 CONTAIN 和 WITH TOLERANCE等关键字，IoTDB的查询处理器将这条查询语句交给索引机制执行，索引机制进一步调用相应的ELB索引。

子序列检索的结果是长序列上的子片段，原本并不存在于数据库模式中。为了与IoTDB的其他查询结果格式保持一致，每一个检索结果单独成列，列名由“传感器序列名”和“子片段起始时间”连缀而成。如下图所示，复合模式的检索结果共两条，分别从时刻2019-10-18 12:30:00.000和2019-10-18 12:30:10.000开始。

![image](https://user-images.githubusercontent.com/5548915/109539955-db07c980-7afc-11eb-8848-41983d393f45.png)

删除索引操作与全序列检索场景类似，不再赘述。

#### 用户功能：面向索引实现者

索引机制致力于屏蔽数据库系统复杂的运行逻辑和消息处理，为开发者提供简洁而友好的索引集成平台。

如果想为IoTDB添加一种新的索引技术，索引实现者需要继承`IoTDBIndex` 类，并在`IndexType` 这一枚举类中添加新的索引类型（“以硬编码的方式向枚举类中添加索引”仍然不够优雅，我们会在未来的版本中用其他方式取代）。

##### IoTDBIndex

`IoTDBIndex`是一个抽象类，一些关键的属性和抽象方法如下：

```java
// 本索引的特征处理器。所有特征处理器都继承自IndexFeatureExtractor类
protected IndexFeatureExtractor indexFeatureExtractor;

/**
  为本索引使用的特征提取器进行初始化操作
  previous：如果特征处理器在上一次系统关闭时存储了一些状态信息，这些状态信息会以previous参数传入
  inQueryMode：用于索引写入还是索引查询。索引可以在两种场景中使用不同的特征提取器。
  */
public abstract void initFeatureExtractor(ByteBuffer previous, boolean inQueryMode);

// 从特征提取器中获取最新产生的序列和特征信息，写入索引。
public abstract boolean buildNext() throws IndexManagerException;

// 系统关闭时，将内存中的索引数据序列化到磁盘
protected abstract void flushIndex();

// 执行索引查询。
public abstract QueryDataSet query(
      Map<String, Object> queryProps,
      IIndexUsable iIndexUsable,
      QueryContext context,
      IIndexRefinePhaseOptimize refinePhaseOptimizer,
      boolean alignedByTime)
      throws QueryIndexException;

```

##### IndexType

每种索引都有自己的唯一类型`IndexType`。当索引实现者希望添加一种索引时，需要在`IndexType`中增加一种类型，并为以下函数添加一个新分支：

```java
public static IndexType deserialize(short i);

public short serialize();

private static IoTDBIndex newIndexByType(PartialPath path, TSDataType tsDataType, String indexDir, IndexType indexType, IndexInfo indexInfo);
```

##### IndexFeatureExtractor

> 在索引框架的第二次PR中暂不涉及IndexFeatureExtractor的具体实现。

在全序列特征提取器中，特征提取器接一条原始数据作为输入，从中提取出三种信息，以应对索引生成和查询过程中存在的各种需求。三种信息从简单到复杂分别为：

1. L1-序列指针：包含三元组信息：序列路径、起始时间戳和结束时间戳。
2. L2-规整序列：将不定频、有缺失数据的序列转换为维度固定或时间间隔整齐的序列，供索引处理。
3. L3-序列特征：基于规整序列进一步计算得到的特征。格式不固定。

以上三种信息分别在索引读写过程中起到不同的作用。序列指针是指向原始数据的指针。
由于IoTDB会在形成TsFile文件时对原始数据进行编码和压缩，因此无法简单地通过"文件路径+偏移量"来获取原始数据。取而代之的是，根据序列名和起止时间这三元组信息来唯一确定一段原始数据。序列指针会在特征文件和候选集中被使用；
规整序列是多数索引可以接受的输入形式。在实验条件下，索引结构可以认为时序数据维度固定、时间戳等间隔且没有缺失值。但在实际情况中，序列可能存在微小的时间戳偏移或少量的数据错漏，规整序列对原始序列进行了预处理，去除了上述数据缺陷。
序列特征是索引对原始序列进行特征提取后的结果。特征提取方法往往是索引技术的关键创新所在。

如果索引实现者希望自定义实现特征提取器 `IndexFeatureExtractor` ，需要实现以下接口：

```java
// 加入一段原始数据以备处理
public abstract void appendNewSrcData(TVList newData);

// 加入一段原始数据以备处理，由于代码历史原因，IoTDB在数据写入时生成TVList，而查询时却会得到BatchData
public abstract void appendNewSrcData(BatchData newData);

// 索引框架会首先询问是否有下一条数据
public abstract boolean hasNext();

// 如果有，调用本方法产生一个待插入数据。注意，在appendNewSrcData中插入一段原始数据后，可能产生多条待插入数据
public abstract void processNext();

// 处理了一批数据后，调用本方法来释放已经处理过的数据的资源
public abstract void clearProcessedSrcData();

// 关闭特征处理器，返回状态数据，用于下一次打开时恢复状态
public abstract ByteBuffer closeAndRelease() throws IOException;

// 返回 L2-规整数据
public Object getCurrent_L2_AlignedSequence();

// 返回 L3-特征
public Object getCurrent_L3_Feature();
```



#### 系统参数配置

系统参数如下：

| 参数名                                | 类型   | 默认值      | 说明                                                 |
| ------------------------------------- | ------ | ----------- | ---------------------------------------------------- |
| enable_index                          | bool   | false       | 是否开启索引                                         |
| index_root_dir                        | string | data/index  | 所有索引文件的总目录                                 |
| concurrent_index_build_thread         | int    | 系统CPU核数 | 执行索引写入时的最大并行线程数量                     |
| default_index_window_range            | int    | 10          | 目前未用到，拟删除                                   |
| index_buffer_size                     | long   | 134217728   | 目前未用到，拟删除                                   |
| max_index_query_result_size           | int    | 5           | 索引检索结果的最大返回数量                           |
| default_max_size_of_unusable_segments | int    | 20          | 可用区间管理器的最大段数，见`SubMatchIndexUsability` |

### 模块设计

下图展示了IoTDB索引机制的模块设计图。索引机制会与IoTDB中的多个模块（绿色框图）产生消息和数据交互（绿色箭头）。例如，在IoTDB系统启动和关闭时会启动或关闭索引机制；当某条序列有新的数据点写入时，IoTDB存储管理器会调用索引机制采取相应操作；当用户发起索引查询时，IoTDB查询处理器会将查询条件转交给索引机制，获取索引查询结果并返回给用户。

![image-20210227025206792](https://user-images.githubusercontent.com/5548915/109539696-92501080-7afc-11eb-8621-a7d4cabcd94c.png)

索引机制内部的各个模块（蓝色框图）也会产生消息和数据交互。索引实现者可以继承指定接口为索引机制添加新的索引实例。通过这些接口，索引机制会调用索引实例完成索引写入和查询等功能（蓝色箭头）。

下面分别介绍各个模块的功能和接口。

#### 特征提取器

对应代码类：`IndexFeatureExtractor` 及其子类。

特征提取器负责对IoTDB的原始序列进行预处理和特征提取。在索引写入阶段，对于全序列检索，特征提取器对整条序列进行特征提取；对于子序列检索，特征提取器首先利用滑动窗口模型将长序列截取为短片段，然后提取特征。特征方法和相关参数在创建索引的时候指定；在索引查询阶段，特征提取器为查询序列提取特征。由于索引技术细节的不同，查询阶段的特征方法可能与写入阶段的特征方法相同或不同。
系统已经内置了几种较为常见的特征提取器。索引实现者可以直接选用内置的特征提取器，或者实现自定义的新特征提取器。特征提取器的接口详见2.2.2.3节：IndexFeatureExtractor。

#### 索引调度器

首先介绍“索引序列”概念："索引序列"是一个索引实例覆盖的"序列"或"序列集合"。将索引序列或索引序列集合的所有数据会写入同一个索引实例中。例如，在全序列索引的例子中，`root.Ery.*.Glu ` 就是这个索引的“索引序列”，而在子序列索引的例子中，`root.Wind.AZQ02.Speed` 就是ELB索引的“索引序列”。

每个索引在创建的时候会指定它的“索引序列”，索引序列可能包含一条或多条时间序列。两个“索引序列”之间可能有交集。一个索引序列上可能会创建多种索引。

索引、索引序列和时间序列的ER图是：

![image](https://user-images.githubusercontent.com/5548915/109540557-9a5c8000-7afd-11eb-9be3-ede027e81bb2.png)

每个索引序列上可能会创建多种索引实例，索引框架中的每个索引序列对应一个“实力调度器”（IndexProcessor），负责调度该索引序列下的所有索引实例。

总调度器（IndexManager）、实例调度器（IndexProcessor）和索引实例（Index）的ER图是：

![image](https://user-images.githubusercontent.com/5548915/109540566-9c264380-7afd-11eb-92ef-c9bcc019f4f0.png)



**总调度器**：对应代码类：`IndexManager` 

IndexManager是索引机制的全局管理者，IoTDB接到的索引创建、删除、查询以及写入均会调用IndexManager。IndexManager会将索引操作分发给相应的IndexProcessor执行。

最初的想法是，"剪枝阶段"由索引实例得到候选集，而"精化阶段"完全由索引框架完成（即对候选集所对应的原始数据进行遍历）。这样的方案是足够通用的。然而，索引技术有各种优化，强制执行上述策略会影响索引的集成。例如，"剪枝阶段"也可能要访问原始数据（RTree），精化阶段也有特定的优化（ELB-Index）。由于已经实现的这两种索引技术均有各自的优化，因此，当前的查询接口将整个查询过程全部交给索引实例完成。

接口如下：

```java
// 创建索引，目前indexSeriesList中仅包含单条路径。
public void createIndex(List<PartialPath> indexSeriesList, IndexInfo indexInfo);

// 删除索引
public void dropIndex(List<PartialPath> indexSeriesList, IndexType indexType);

// 索引查询。
public QueryDataSet queryIndex(List<PartialPath> paths, IndexType indexType,
      Map<String, Object> queryProps, QueryContext context, boolean alignedByTime)

// 关闭操作
private synchronized void close();

```



**实例调度器**：对应代码类：` IndexProcessor`

每个IndexProcessor对应一个索引序列，负责管理一个索引序列下的所有索引实例操作。

属性如下：

```java
/**
每个索引实例在其关闭时会生成一段状态信息（previousData），用于下一次加载时恢复到当前状态。每个 IndexProcessor 的所有 previousData 会被存储于同一个文件。索引可以管理和存储自身的信息，因此 previousData 不是必须的。但由于特征处理器也会产生 previousData，如果索引不想管理这部分信息，则可以将其抛给 IndexProcessor 管理
*/
private final Map<IndexType, ByteBuffer> previousDataBufferMap;

// 每个索引实例对应一个可用区间管理器（IIndexUsable），每个IndexProcessor的所有IIndexUsable会被存储于同一个文件
private Map<IndexType, IIndexUsable> usableMap;

// 精化阶段（精化阶段）的优化器。目前并未用到。未来设计详见{@link IIndexCandidateOrderOptimize}.
private final IIndexCandidateOrderOptimize refinePhaseOptimizer;
```

接口如下：

```java
// 将已排序的序列写入所有索引中
public void buildIndexForOneSeries(PartialPath path, TVList tvList);

// 需要等待索引全部刷写完才会返回
public void endFlushMemTable();

// 将除了NoIndex之外的索引实例刷出到磁盘
public synchronized void close(boolean deleteFiles) throws IOException;

/**
根据传入的 indexInfoMap 刷新当前IndexProcessor中的索引实例。当用户进行创建或删除索引，则IndexProcessor中维护的索引实例就过期了，因此需要刷新。
*/
public void refreshSeriesIndexMapFromMManager(Map<IndexType, IndexInfo> indexInfoMap);

// 对于乱序数据，目前的设计中直接将数据标记为"索引不可用"
void updateUnsequenceData(PartialPath path, TVList tvList);
```



**索引刷写任务器**，对应`IndexMemTableFlushTask`。

在当前的设计中，索引不会每次有新数据点到来就实时更新，而是在IoTDB的flush时批量写入。当一个存储组执行flush任务时会创建一个flush线程`MemTableFlushTask`。如果系统索引功能开启（enableIndex==true）， `MemTableFlushTask` 会将MemTable中的时间序列交给IndexManager写入。考虑到系统代码解耦，在flush过程中的所有索引相关操作封装在一个``IndexMemTableFlushTask`` 中。

不直接写入IndexManager，而是要向IndexManager申请一个``IndexMemTableFlushTask``对象的原因是，IoTDB的存储组的flush任务是独立而并行的，但IndexManager是全局单例的。同时，IoTDB的不同存储组所创建的索引也可能互相不覆盖（即对应不同的IndexProcessor，互相可能并没有并行冲突），因此，每个存储组在flush的时候，向IndexManager申请独立的`IndexMemTableFlushTask` 可以提升并行效率。

接口如下：

```java
// 插入排序后的序列
public void buildIndexForOneSeries(PartialPath path, TVList tvList);

// 等待所有IndexProcessor写入完成后再返回
public void endFlush();
```



### 特征文件管理器、索引文件管理器

特征文件管理器指定特征文件的存放位置，索引文件管理器指定索引内存结构刷写到磁盘的文件位置。这两个单元的功能都蕴含在索引调度器中。

### 注册表

对应`IIndexRouter` 及其子类。

`IIndexRouter`的第一个功能是管理索引实例的元数据。当新的索引被创建或删除后，注册表会同步更新。

`IIndexRouter`的第二个功能是将索引操作命令高效地传递到相应的`IndexProcessor`。考虑到`IoTDBIndex`、`IndexProcessor`的映射关系较为复杂且可能在未来的设计中有所改动，`IIndexRouter` 可以将这些映射工作与`IndexManager` 解锁。

接口如下：

```java
// 添加索引
boolean addIndexIntoRouter(PartialPath prefixPath, IndexInfo indexInfo, CreateIndexProcessorFunc func, boolean doSerialize) throws MetadataException;

// 删除索引
boolean removeIndexFromRouter(PartialPath prefixPath, IndexType indexType);

// 返回给定 indexSeries 下面的索引信息
Map<IndexType, IndexInfo> getIndexInfosByIndexSeries(PartialPath indexSeries);

// 获取所有 IndexProcessor 及其信息
Iterable<IndexProcessorStruct> getAllIndexProcessorsAndInfo();

// 返回给定 timeSeries 所属的所有 IndexProcessors
Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath timeSeries);

// 序列化
void serialize(boolean doClose);

// 反序列化
void deserializeAndReload(CreateIndexProcessorFunc func);

// 返回一个存储组相关的 IIndexRouter 子集。这是为了提高 IIndexRouter 访问的并行性
IIndexRouter getRouterByStorageGroup(String storageGroupPath);

// 开始查询
IndexProcessorStruct startQueryAndCheck(
      PartialPath partialPath, IndexType indexType, QueryContext context)
      throws QueryIndexException;

// 结束查询
void endQuery(PartialPath indexSeries, IndexType indexType, QueryContext context);
```

**ProtoIndexRouter** ：是 `IIndexRouter` 的一种简单实现。

子序列索引针对单一序列创建，而全序列索引针对有通配符的一组序列创建，因此 `ProtoIndexRouter` 对两种场景用不同的Map结构管理。

```java
// 子序列索引，索引序列为全路径
private Map<String, IndexProcessorStruct> fullPathProcessorMap;

// 全序列索引，索引序列包含通配符
private Map<PartialPath, IndexProcessorStruct>` wildCardProcessorMap
```

`IIndexRouter` 的关键功能是为一个序列快速找到其所属的`IndexProcessor`。如果该序列是全路径的，则可以在`fullPathProcessorMap` 中以 $O(1)$ 找到；否则，必须遍历 `wildCardProcessorMap` 中的每一个包含通配符的路径。



### 可用区间管理器

可用区间管理器用于处理乱序数据。
大多数时间序列数据点会按照数据时间戳顺序到来，但当乱序数据到来，或者用户手动更新了某段数据后，这一段数据上的索引正确性无法保证。
对于这些数据，可用区间管理器将其标记为索引不可用。不可用区间的序列将被加入候选集中，接受"精化阶段"的检验。

接口如下：

```java
// 增加可用区间
void addUsableRange(PartialPath fullPath, long start, long end);

// 增加不可用区间
void minusUsableRange(PartialPath fullPath, long start, long end);

// 获取不可用区间，注意，全序列索引和子序列索引的返回格式不同
Object getUnusableRange();

// 序列化
void serialize(OutputStream outputStream) throws IOException;

// 反序列化
void deserialize(InputStream inputStream) throws IllegalPathException, IOException;
```

当前版本针对全序列索引和子序列索引，分别实现了一种可用区间管理器。

**SubMatchIndexUsability**：针对子序列索引的可用区间管理器。

子序列索引的“索引序列”为单一序列，不包含通配符。如果这条长序列上的某一子片段被更改，则将这一片段标记为“索引不可用”，剩余片段则仍为“索引可用”区间。

当前的实现采用一个链表来标记索引不可用的片段。每个链表的节点为 `RangeNode` ，代表一段不可用的时间段。

```java
class RangeNode {
    long start;
    long end;
    RangeNode next;
}
```

`SubMatchIndexUsability` 的构造函数有两个：

```java
/**
 * 构造函数
 * @param maxSizeOfUsableSegments 不可用区间的最大段数
 * @param initAllUsable 如果为true，最初时间段均为“索引可用”，如果为false，最初均为“索引不可用”
 */
SubMatchIndexUsability(int maxSizeOfUsableSegments, boolean initAllUsable) {
  ...
}
```

当前实现中对于 `RangeNode` 链表的访问和更新是线性遍历的，然而用户可以指定不可用区间的最大段数 $M$ ，因此链表的访问和更新复杂度可以控制在 `O(M)` ，即常数量级。当不可用区间的段数增加到上限时，会将新的不可用区间与较近的区间合并，从而控制区间段数。这样会使得“不可用区间”范围扩大，但将“可用区间”标记为“不可用区间”仅会让一些“被剪枝”的序列进入精化阶段，而不会造成漏解（即，将本来正确的结果错误地排除掉），因此 $M$ 参数并不会影响索引查询的正确性。

`getUnusableRange()` 函数返回一系列不可用区间的时间过滤器：`List<Filter>` 。

例如，在序列 `root.Wind.AZQ02.Speed` 上创建子序列索引。

1.  `initAllUsable = false，最初 整个区间段均为“索引不可用”；`maxSizeOfUsableSegments=2` 最多允许两个索引不可用片段存在；
2. 增加可用片段 $[5,7]$ ，不可用区间变为  $[MIN,4] \cup [8,MAX]$ ；
3. 增加可用片段 $[2,3]$ ，不可用区间应当分裂为  $[1,2] \cup [4,4]\cup [8,10]$ ，然而不可用区间超过了上限，因此停止分裂，仍然保持  $[MIN,4] \cup [8,MAX]$ 

**WholeMatchIndexUsability**：针对全序列索引的可用区间管理器。

全序列索引的“索引序列”包含通配符，每一条符合规则的时间序列作为一个整体插入索引。因此，对于一条序列的任何更改都会使整条序列变为“索引不可用”。因此，本类采用一个集合 `unusableSeriesSet` 来标记索引不可用的序列。

例如，在索引序列 `root.Ery.*.Glu ` 之上创建全序列索引。当某条序列 `root.Ery.01.Glu` 被更改，则将其加入集合 `unusableSeriesSet` ，该条序列被标记为“索引不可用”。

`getUnusableRange()` 函数返回被标记为“索引不可用”的序列的集合 `Set<PartialPath>` 。

### 索引查询处理器

查询处理模块负责执行索引相关的查询。包括索引查询执行器、查询优化器，在查询时也会用到可用区间管理器。

**查询执行器**：对应类`QueryIndexExecutor` 。

尽管我们可以从相似性索引的查询过程中提取共性，将其分为“剪枝阶段”和“精化阶段”，并且精化阶段可以由索引框架结果。然而，许多索引技术（包括当前实现的两种索引：RTree和ELB索引）会对“剪枝阶段”和“精化阶段”结合起来优化，强制接管“精化阶段”可能会影响索引查询效率。因此，现阶段的查询处理器直接调用`IndexManager#queryIndex` 函数。

**查询结果类**：对应类 `IndexQueryDataSet` 继承自 `ListDataSet`。



### 索引接口

对应`IoTDBIndex` 。

索引接口是索引实现者唯一需要实现的部分，接口包括指定特征提取器、写入索引、序列化索引、索引查询等等，详细阐述参见2.2.2.1节。

## 流程设计

### 索引写入流程

![image](https://user-images.githubusercontent.com/5548915/109540629-b6602180-7afd-11eb-8dbe-e38c91921a7e.png)



上图展现了索引机制的写入流程：

* 当用户向数据库系统写入新的数据后，数据库系统通知索引总调度器 `IndexManger`。
* 总调度器首先向注册表进行校验，判断数据所在的序列是否创建过索引。
* 当校验成功后，总调度器将数据传递到相应的索引实例调度器，图中省略这一步骤。
* 调度器首先使用特征提取器将新数据转换为特征，然后由索引实例消费掉。
* 当数据写入索引实例后，更新可用区间管理器。至此，完成整个写入流程。

在当前的实现中，仅当触发IoTDB的刷写操作时（`MemTableFlushTask`），索引机制才触发索引的批量写入操作。这样带来两个好处：首先，在批量写入的情况下，
传感器将多个数据点积攒成序列，方便了特征提取器的处理；其次，考虑到工业物联网数据的乱序情况，IoTDB会将一段时间内的数据进行排序再传给索引机制，这样减少了乱序数据给索引带来的负面影响。

延迟写入索引并不会影响索引查询的正确性。对于已经写入IoTDB但尚未触发刷写操作的数据，索引机制中的可用区间管理器会将其标记为"索引不可用"，因此，这部分数据会直接进入精化阶段进行计算，不会被遗漏。

### 索引查询流程

![image](https://user-images.githubusercontent.com/5548915/109540634-b95b1200-7afd-11eb-800a-71b3b11ce16d.png)

当用户发起索引查询时，索引机制为其创建单独的线程并执行查询。相似性索引的一般查询流程如下：

1. 用户发起索引查询时，IoTDB会为其创建一个单独的线程并执行整个查询流程。这一执行线程调用索引机制并传入查询条件。索引机制首先调用注册表，判断该序列上是否创建过索引、索引是否支持这一查询条件。
2. 校验成功后，`IndexManager` 根据查询条件中指定的查询目标，将执行线程和查询条件传递到相应的索引实例调度器`IndexProcessor` 。
3. 执行线程调用特征提取器 `FeatureExtractor`，对查询序列提取特征。然后将查询特征及其他查询条件输入索引实例 `IoTDBIndex` 。
   1. 执行过滤阶段，并返回候选集，候选集中包含了一系列指向原始数据的指针。至此，查询处理模块的**过滤单元**完成。
   2. 在进入精化阶段前，执行线程首先调用可用区间管理器，将索引不可用区间中的序列加入候选集中，避免乱序数据造成索引错误剪枝。
   3. 然后调用查询优化器，对候选集的访问顺序进行重排序。
   4. 接下来，执行线程访问IoTDB，读取候选集所指向的原始数据并完成精确计算。
   5. 至此，查询处理模块的精化单元完成，返回结果。

上述流程的第三步中，索引框架接管了精化阶段。但如果索引实例有更多的优化，也可以完全接管查询过程并直接返回结果。反映到模块调用上，即 `IndexProcessor` 直接调用 `IoTDBIndex#query` 函数，返回 `QueryDataSet` 。

### 索引更新与删除

索引更新主要涉及两个方面。首先，当IoTDB接收乱序数据后，索引可用区间管理器会相应更新，从而保证后续查询的正确性。其次，当IoTDB完成乱序数据整理和合并后，索引框架将对索引发起重建，这一步在未来实现。

当索引被删除后，索引目录下的文件将被删除。



## 文件结构

![image](https://user-images.githubusercontent.com/5548915/109540601-a9433280-7afd-11eb-8bdb-3f77181c1637.png)

上图左图是索引框架的文件组织结构。

* 总文件夹index下包括数据文件夹和元数据文件夹；
  * 元数据文件夹中目前仅包括注册表文件夹；
  * 数据文件夹下，每个索引序列（对应一个 `IndexProcessor` ）都对应一个文件夹；
    * 索引序列文件夹会创建一个状态文件和可用区间管理器文件；
    * 每个索引序列文件夹下可能创建了多个索引实例；

上图右图是一个例子。



## 并行性分析（TODO）

## 复杂度分析（TODO）

量化，时间复杂度，内存/磁盘空间复杂度。



## 问题讨论

### 索引读写并发能力-锁粒度

为每一个索引实例分配一个读写锁，并由IndexProcessor控制索引的读写。

由于原始数据是分批刷写磁盘的，因此索引也需要支持批量更新。这意味着索引不能在创建索引之前就看到全部原始数据。一些需要基于全局数据确定参数的索引的性能可能会受到影响（如VaFile和基于Kmeans的倒排索引）。

目前为每个索引维护了读写锁（`org.apache.iotdb.db.index.IndexProcessor#indexLockMap`）。允许对索引的并发查询，但索引的写入和查询是互斥的。这一设计有两点值得讨论：

1. 查询并发：并发查询提升了查询效率，但要求索引是查询并发安全的；
2. 读写互斥：这意味着IoTDB刷写操作和索引查询之间会互相阻塞，无疑降低了效率。然而要取消这一限制，则要求索引是读写并发安全的。

根据开发者对一些开源索引代码的调研，索引通常可以满足查询并发，但较难满足读写互斥，因此设计了读写锁机制。但是，值得讨论的是，是否对集成到IoTDB的索引代码提出更高的要求，从而提升IoTDB的效率呢？

### 索引更新删除能力-可用区间

TODO



# 未来规划

预计在第三次PR中提交的模块：

* 特征提取器的实现 `IndexFeatureExtractor` ：包括全序列检索和子序列检索中一些常见的特征提取方法，例如全序列检索用到的PAA特征、子序列检索中用到的两种滑动窗口模型等等；
* 索引算法：基于以上特征提取器，实现了两种基本的检索算法：基于PAA特征的R树索引和等长特征块索引（ELB，回答子序列检索）

留待未来实现的模块如下：

* 统计模块IndexStats：几乎所有相似性索引（甚至可以推广到所有索引）都会关注一些共通的统计量，例如：CPU时间、磁盘访问时间、内存占用、磁盘占用、查询剪枝率、近似查询中的查准率和查全率等等。统计模块为其提供全面而标准的工具函数，用于索引性能评估和不同索引之间的性能比较；
* 候选集顺序优化器 `IIndexCandidateOrderOptimize`：相似性索引剪枝后得到候选集列表。由于索引并未考虑IoTDB的数据组织方式，因此候选集的顺序可能不是最优的。候选集顺序优化器对此进行优化；
* 其他索引技术



# 测试验证（TODO）

## 测试目标

正确性测试或性能（对比）测试

##  测试方案

\- 测试系统

\- 被测系统

\- 测试环境

\- 负载描述

## 测试结果

## 测试结论