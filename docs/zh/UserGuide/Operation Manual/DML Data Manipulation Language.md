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

# DML (数据操作语言)

## 数据接入

IoTDB为用户提供多种插入实时数据的方式，例如在[Cli/Shell工具](../Client/Command%20Line%20Interface.md)中直接输入插入数据的INSERT语句，或使用Java API（标准[Java JDBC](../Client/Programming%20-%20JDBC.md)接口）单条或批量执行插入数据的INSERT语句。

本节主要为您介绍实时数据接入的INSERT语句在场景中的实际使用示例，有关INSERT SQL语句的详细语法请参见本文[INSERT语句](../Operation%20Manual/SQL%20Reference.md)节。

### 使用INSERT语句

使用INSERT语句可以向指定的已经创建的一条或多条时间序列中插入数据。对于每一条数据，均由一个时间戳类型的时间戳和一个数值或布尔值、字符串类型的传感器采集值组成。

在本节的场景实例下，以其中的两个时间序列`root.ln.wf02.wt02.status`和`root.ln.wf02.wt02.hardware`为例 ，它们的数据类型分别为BOOLEAN和TEXT。

单列数据插入示例代码如下：

```
IoTDB > insert into root.ln.wf02.wt02(timestamp,status) values(1,true)
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1, "v1")
```

以上示例代码将长整型的timestamp以及值为true的数据插入到时间序列`root.ln.wf02.wt02.status`中和将长整型的timestamp以及值为”v1”的数据插入到时间序列`root.ln.wf02.wt02.hardware`中。执行成功后会返回执行时间，代表数据插入已完成。 

> 注意：在IoTDB中，TEXT类型的数据单双引号都可以来表示,上面的插入语句是用的是双引号表示TEXT类型数据，下面的示例将使用单引号表示TEXT类型数据。

INSERT语句还可以支持在同一个时间点下多列数据的插入，同时向2时间点插入上述两个时间序列的值，多列数据插入示例代码如下：

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) VALUES (2, false, 'v2')
```

插入数据后我们可以使用SELECT语句简单查询已插入的数据。

```
IoTDB > select * from root.ln.wf02 where time < 3
```

结果如图所示。由查询结果可以看出，单列、多列数据的插入操作正确执行。
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51605021-c2ee1500-1f48-11e9-8f6b-ba9b48875a41.png"></center>

### INSERT语句的错误处理

若用户向一个不存在的时间序列中插入数据，例如执行以下命令：

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, temperature) values(1,"v1")
```

由于`root.ln.wf02.wt02. temperature`时间序列不存在，系统将会返回以下ERROR告知该Timeseries路径不存在：

```
Msg: The resultDataType or encoding or compression of the last node temperature is conflicting in the storage group root.ln
```

若用户插入的数据类型与该Timeseries对应的数据类型不一致，例如执行以下命令：

```
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1,100)
```

系统将会返回以下ERROR告知数据类型有误：

```
error: The TEXT data type should be covered by " or '
```

## 数据查询

### 时间切片查询

本节主要介绍时间切片查询的相关示例，主要使用的是[IoTDB SELECT语句](../Operation%20Manual/SQL%20Reference.md)。同时，您也可以使用[Java JDBC](../Client/Programming%20-%20JDBC.md)标准接口来执行相关的查询语句。

#### 根据一个时间区间选择一列数据

SQL语句为：

```
select temperature from root.ln.wf01.wt01 where time < 2017-11-01T00:08:00.000
```

其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为温度传感器（temperature）；该语句要求选择出该设备在“2017-11-01T00:08:00.000”（此处可以使用多种时间格式，详情可参看[2.1节](../Concept/Data%20Model%20and%20Terminology.md)）时间点以前的所有温度传感器的值。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280074-da1c0a00-a7e9-11e9-8eb8-3809428043a8.png"></center>

#### 根据一个时间区间选择多列数据

SQL语句为：

```
select status, temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000;
```

其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为供电状态（status）和温度传感器（temperature）；该语句要求选择出“2017-11-01T00:05:00.000”至“2017-11-01T00:12:00.000”之间的所选时间序列的值。

该SQL语句的执行结果如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280328-40a12800-a7ea-11e9-85b9-3b8db67673a3.png"></center>

#### 按照多个时间区间选择同一设备的多列数据

IoTDB支持在一次查询中指定多个时间区间条件，用户可以根据需求随意组合时间区间条件。例如，

SQL语句为：

```
select status,temperature from root.ln.wf01.wt01 where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```

其含义为：

被选择的设备为ln集团wf01子站wt01设备；被选择的时间序列为“供电状态（status）”和“温度传感器（temperature）”；该语句指定了两个不同的时间区间，分别为“2017-11-01T00:05:00.000至2017-11-01T00:12:00.000”和“2017-11-01T16:35:00.000至2017-11-01T16:37:00.000”；该语句要求选择出满足任一时间区间的被选时间序列的值。

该SQL语句的执行结果如下：
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280449-780fd480-a7ea-11e9-8ed0-70fa9dfda80f.png"></center>


#### 按照多个时间区间选择不同设备的多列数据

该系统支持在一次查询中选择任意列的数据，也就是说，被选择的列可以来源于不同的设备。例如，SQL语句为：

```
select wf01.wt01.status,wf02.wt02.hardware from root.ln where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```

其含义为：

被选择的时间序列为“ln集团wf01子站wt01设备的供电状态”以及“ln集团wf02子站wt02设备的硬件版本”；该语句指定了两个时间区间，分别为“2017-11-01T00:05:00.000至2017-11-01T00:12:00.000”和“2017-11-01T16:35:00.000至2017-11-01T16:37:00.000”；该语句要求选择出满足任意时间区间的被选时间序列的值。

该SQL语句的执行结果如下：
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577450-dcfe0800-1ef4-11e9-9399-4ba2b2b7fb73.jpg"></center>

#### 根据时间降序返回
IoTDB 在 0.11 版本开始支持 'order by time' 语句, 用于对结果按照时间进行降序展示。例如，SQL语句为：
```sql
select * from root.ln where time > 1 order by time desc limit 10;
```

更多语法请参照 [SQL REFERENCE](../Operation%20Manual/SQL%20Reference.md).

#### 其他结果返回形式

IoTDB支持另外两种结果返回形式: 按设备时间对齐 'align by device' 和 时序不对齐 'disable align'.

'align by device' 对齐方式下，设备ID会单独作为一列出现。在 select 子句中写了多少列，最终结果就会有该列数+2 （时间列和设备名字列）。SQL形如:

```
select s1,s2 from root.sg1.* GROUP BY DEVICE
```

更多语法请参照 SQL REFERENCE.

'disable align' 意味着每条时序就有3列存在。更多语法请参照 SQL REFERENCE.

### 聚合查询
本章节主要介绍聚合查询的相关示例，
主要使用的是IoTDB SELECT语句的聚合查询函数。

#### 统计总点数


```
select count(status) from root.ln.wf01.wt01;
```

| count(root.ln.wf01.wt01.status) |
| -------------- |
| 4              |


##### 按层级统计
通过定义LEVEL来统计指定层级下的数据点个数。

这可以用来查询不同层级下的数据点总个数

语法是：

这个可以用来查询某个路径下的总数据点数

```
select count(status) from root.ln.wf01.wt01 group by level=1;
```


| Time   | count(root.ln) |
| ------ | -------------- |
| 0      | 7              |


```
select count(status) from root.ln.wf01.wt01 group by level=2;
```

| Time   | count(root.ln.wf01) | count(root.ln.wf02) |
| ------ | ------------------- | ------------------- |
| 0      | 4                   | 3                   |

### 降频聚合查询

本章节主要介绍降频聚合查询的相关示例，
主要使用的是IoTDB SELECT语句的[GROUP BY子句](../Operation%20Manual/SQL%20Reference.md)，
该子句是IoTDB中用于根据用户给定划分条件对结果集进行划分，并对已划分的结果集进行聚合计算的语句。
IoTDB支持根据时间间隔和自定义的滑动步长（默认值与时间间隔相同，自定义的值必须大于等于时间间隔）对结果集进行划分，默认结果按照时间升序排列。
同时，您也可以使用Java JDBC标准接口来执行相关的查询语句。

GROUP BY语句为用户提供三类指定参数：

* 参数1：时间轴显示时间窗参数
* 参数2：划分时间轴的时间间隔参数（必须为正数）
* 参数3：滑动步长（可选参数，默认值与时间间隔相同，自定义的值必须大于等于时间间隔）

三类参数的实际含义已经在图5.2中指出，这三类参数里，第三个参数是可选的。
接下来，我们将给出三种典型的降频聚合查询的例子：
滑动步长未指定，
指定滑动步长，
带值过滤条件。

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69109512-f808bc80-0ab2-11ea-9e4d-b2b2f58fb474.png">
**图 5.2 三类参数的实际含义**</center>

#### 未指定滑动步长的降频聚合查询

对应的SQL语句是:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d);
```
这条查询的含义是:

由于用户没有指定滑动步长，滑动步长将会被默认设置为跟时间间隔参数相同，也就是`1d`。

上面这个例子的第一个参数是显示窗口参数，决定了最终的显示范围是[2017-11-01T00:00:00, 2017-11-07T23:00:00)。

上面这个例子的第二个参数是划分时间轴的时间间隔参数，将`1d`当作划分间隔，显示窗口参数的起始时间当作分割原点，时间轴即被划分为连续的时间间隔：[0,1d), [1d, 2d), [2d, 3d)等等。

然后系统将会用WHERE子句中的时间和值过滤条件以及GROUP BY语句中的第一个参数作为数据的联合过滤条件，获得满足所有过滤条件的数据（在这个例子里是在[2017-11-01T00:00:00, 2017-11-07 T23:00:00)这个时间范围的数据），并把这些数据映射到之前分割好的时间轴中（这个例子里是从2017-11-01T00:00:00到2017-11-07T23:00:00:00的每一天）

每个时间间隔窗口内都有数据，SQL执行后的结果集如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116068-eed51b00-0ac5-11ea-9731-b5a45c5cd224.png"></center>

#### 指定滑动步长的降频聚合查询

对应的SQL语句是:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d);
```

这条查询的含义是:

由于用户指定了滑动步长为`1d`，GROUP BY语句执行时将会每次把时间间隔往后移动一天的步长，而不是默认的3小时。

也就意味着，我们想要取从2017-11-01到2017-11-07每一天的凌晨0点到凌晨3点的数据。

上面这个例子的第一个参数是显示窗口参数，决定了最终的显示范围是[2017-11-01T00:00:00, 2017-11-07T23:00:00)。

上面这个例子的第二个参数是划分时间轴的时间间隔参数，将`3h`当作划分间隔，显示窗口参数的起始时间当作分割原点，时间轴即被划分为连续的时间间隔：[2017-11-01T00:00:00, 2017-11-01T03:00:00), [2017-11-02T00:00:00, 2017-11-02T03:00:00), [2017-11-03T00:00:00, 2017-11-03T03:00:00)等等。

上面这个例子的第三个参数是每次时间间隔的滑动步长。

然后系统将会用WHERE子句中的时间和值过滤条件以及GROUP BY语句中的第一个参数作为数据的联合过滤条件，获得满足所有过滤条件的数据（在这个例子里是在[2017-11-01T00:00:00, 2017-11-07 T23:00:00)这个时间范围的数据），并把这些数据映射到之前分割好的时间轴中（这个例子里是从2017-11-01T00:00:00到2017-11-07T23:00:00:00的每一天的凌晨0点到凌晨3点）

每个时间间隔窗口内都有数据，SQL执行后的结果集如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116083-f85e8300-0ac5-11ea-84f1-59d934eee96e.png"></center>

#### 带值过滤条件的降频聚合查询

对应的SQL语句是:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 where time > 2017-11-01T01:00:00 and temperature > 20 group by([2017-11-01T00:00:00, 2017-11-07T23:00:00), 3h, 1d);
```

这条查询的含义是:

由于用户指定了滑动步长为`1d`，GROUP BY语句执行时将会每次把时间间隔往后移动一天的步长，而不是默认的3小时。

也就意味着，我们想要取从2017-11-01到2017-11-07每一天的凌晨0点到凌晨3点的数据。

上面这个例子的第一个参数是显示窗口参数，决定了最终的显示范围是[2017-11-01T00:00:00, 2017-11-07T23:00:00)。

上面这个例子的第二个参数是划分时间轴的时间间隔参数，将`3h`当作划分间隔，显示窗口参数的起始时间当作分割原点，时间轴即被划分为连续的时间间隔：[2017-11-01T00:00:00, 2017-11-01T03:00:00), [2017-11-02T00:00:00, 2017-11-02T03:00:00), [2017-11-03T00:00:00, 2017-11-03T03:00:00)等等。

上面这个例子的第三个参数是每次时间间隔的滑动步长。

然后系统将会用WHERE子句中的时间和值过滤条件以及GROUP BY语句中的第一个参数作为数据的联合过滤条件，获得满足所有过滤条件的数据（在这个例子里是在[2017-11-01T00:00:00, 2017-11-07 T23:00:00)这个时间范围的并且满足root.ln.wf01.wt01.temperature > 20的数据），并把这些数据映射到之前分割好的时间轴中（这个例子里是从2017-11-01T00:00:00到2017-11-07T23:00:00:00的每一天的凌晨0点到凌晨3点）

每个时间间隔窗口内都有数据，SQL执行后的结果集如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116088-001e2780-0ac6-11ea-9a01-dc45271d1dad.png"></center>

GROUP BY的SELECT子句里的查询路径必须是聚合函数，否则系统将会抛出如下对应的错误。

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116099-0b715300-0ac6-11ea-8074-84e04797b8c7.png"></center>

#### 左开右闭区间

每个区间的结果时间戳为区间右端点，对应的SQL语句是:

```
select count(status) from root.ln.wf01.wt01 group by((5, 40], 5ms);
```

这条查询语句的时间区间是左开右闭的，结果中不会包含时间点5的数据，但是会包含时间点40的数据。

SQL执行后的结果集如下所示：

| Time   | count(root.ln.wf01.wt01.status) |
| ------ | ------------------------------- |
| 10     | 1                               |
| 15     | 2                               |
| 20     | 3                               |
| 25     | 4                               |
| 30     | 4                               |
| 35     | 3                               |
| 40     | 5                               |

#### 降采样后按Level聚合查询

除此之外，还可以通过定义LEVEL来统计指定层级下的数据点个数。

例如：

统计降采样后的数据点个数

```
select count(status) from root.ln.wf01.wt01 group by ([0,20),3ms), level=1;
```


| Time   | count(root.ln) |
| ------ | -------------- |
| 0      | 1              |
| 3      | 0              |
| 6      | 0              |
| 9      | 1              |
| 12     | 3              |
| 15     | 0              |
| 18     | 0              |

加上滑动Step的降采样后的结果也可以汇总

```
select count(status) from root.ln.wf01.wt01 group by ([0,20),2ms,3ms), level=1;
```


| Time   | count(root.ln) |
| ------ | -------------- |
| 0      | 1              |
| 3      | 0              |
| 6      | 0              |
| 9      | 0              |
| 12     | 2              |
| 15     | 0              |
| 18     | 0              |


#### 降频聚合查询补空值

降频聚合出的各个时间段的结果，支持使用前值补空。

不允许设置滑动步长，默认为聚合时间区间，实际为定长采样。现在只支持 last_value 聚合函数。

目前不支持线性插值补空值。


##### PREVIOUS 和 PREVIOUSUNTILLAST 的区别

* PREVIOUS：只要空值前边有值，就会用其填充空值。
* PREVIOUSUNTILLAST：不会填充此序列最新点后的空值

SQL 示例:

```
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([8, 39), 5m) FILL (int32[PREVIOUSUNTILLAST])
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([8, 39), 5m) FILL (int32[PREVIOUSUNTILLAST, 3m])
```

解释:

使用 PREVIOUSUNTILLAST 方式填充降频聚合的结果。

所有路径必须都伴随聚合函数，否则会报以下错误信息：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116099-0b715300-0ac6-11ea-8074-84e04797b8c7.png"></center>

### 最新数据查询

SQL语法：

```
select last <Path> [COMMA <Path>]* from < PrefixPath > [COMMA < PrefixPath >]* <WhereClause>
```

其含义是：查询时间序列prefixPath.path中最近时间戳的数据

\<WhereClause\>中当前只支持时间过滤条件，任何非时间过滤都将不会起任何作用。

结果集为三列的结构

```
| Time | Path | Value |
```

示例 1：查询 root.ln.wf01.wt01.speed 的最新数据点

```
> select last speed from root.ln.wf01.wt01

| Time | Path                    | Value |
| ---  | ----------------------- | ----- |
|  5   | root.ln.wf01.wt01.speed | 100   |
```

示例 2：查询 root.ln.wf01.wt01 下 speed，status，temperature 的在时间区间[5, 10)之内的最新数据点

```
> select last speed, status, temperature from root.ln.wf01  where time >= 5 and time < 10

| Time | Path                         | Value |
| ---  | ---------------------------- | ----- |
|  5   | root.ln.wf01.wt01.speed      | 100   |
|  7   | root.ln.wf01.wt01.status     | true  |
|  9   | root.ln.wf01.wt01.temperature| 35.7  |
```

### 自动填充

在IoTDB的实际使用中，当进行时间序列的查询操作时，可能会出现在某些时间点值为null的情况，这会妨碍用户进行进一步的分析。 为了更好地反映数据更改的程度，用户希望可以自动填充缺失值。 因此，IoTDB系统引入了自动填充功能。

自动填充功能是指对单列或多列执行时间序列查询时，根据用户指定的方法和有效时间范围填充空值。 如果查询点的值不为null，则填充功能将不起作用。

> 注意：在当前版本中，IoTDB为用户提供两种方法：Previous 和Linear。 Previous 方法用前一个值填充空白。 Linear方法通过线性拟合来填充空白。 并且填充功能只能在执行时间点查询时使用。

#### 填充功能

- Previous功能

当查询的时间戳值为空时，将使用前一个时间戳的值来填充空白。 形式化的先前方法如下（有关详细语法，请参见第7.1.3.6节）：

```
select <path> from <prefixPath> where time = <T> fill(<data_type>[previous, <before_range>], …)
```

表3-4给出了所有参数的详细说明。

<center>**表3-4previous填充参数列表**

| 参数名称（不区分大小写） | 解释                                                         |
| :----------------------- | :----------------------------------------------------------- |
| path, prefixPath         | 查询路径； 必填项                                            |
| T                        | 查询时间戳（只能指定一个）； 必填项                          |
| data\_type               | 填充方法使用的数据类型。 可选值是int32，int64，float，double，boolean，text; 可选字段 |
| before\_range            | 表示前一种方法的有效时间范围。 当[T-before \ _range，T]范围内的值存在时，前一种方法将起作用。 如果未指定before_range，则before_range会使用默认值default_fill_interval; -1表示无穷大； 可选字段 |

</center>

在这里，我们举一个使用先前方法填充空值的示例。 SQL语句如下：

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) 
```

意思是：

由于时间根目录root.sgcc.wf03.wt01.temperature在2017-11-01T16：37：50.000为空，因此系统使用以前的时间戳2017-11-01T16：37：00.000（且时间戳位于[2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000]范围）进行填充和显示。

在[样例数据中](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), 该语句的执行结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577616-67df0280-1ef5-11e9-9dff-2eb8342074eb.jpg"></center>

值得注意的是，如果在指定的有效时间范围内没有值，系统将不会填充空值，如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577679-9f4daf00-1ef5-11e9-8d8b-06a58de6efc1.jpg"></center>

- Linear方法

当查询的时间戳值为空时，将使用前一个和下一个时间戳的值来填充空白。 形式化线性方法如下：

```
select <path> from <prefixPath> where time = <T> fill(<data_type>[linear, <before_range>, <after_range>]…)
```

表3-5中给出了所有参数的详细说明。

<center>**表3-5线性填充参数列表**

| 参数名称（不区分大小写）    | 解释                                                         |
| :-------------------------- | :----------------------------------------------------------- |
| path, prefixPath            | 查询路径； 必填项                                            |
| T                           | 查询时间戳（只能指定一个）； 必填项                          |
| data_type                   | 填充方法使用的数据类型。 可选值是int32，int64，float，double，boolean，text; 可选字段 |
| before\_range, after\_range | 表示线性方法的有效时间范围。 当[T-before_range，T + after_range]范围内的值存在时，前一种方法将起作用。 如果未明确指定before_range和after_range，则使用default\_fill\_interval。 -1表示无穷大； 可选字段 |

</center>

需要注意的是一旦时间序列在查询时间戳T时刻存在有效值，线性填充就回使用这个值作为结果返回。
除此之外，如果在[T-before_range，T]或[T, T + after_range]两个范围中任意一个范围内不存在有效填充值，则线性填充返回null值。

在这里，我们举一个使用线性方法填充空值的示例。 SQL语句如下：

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float [linear, 1m, 1m])
```

意思是：

由于时间根目录root.sgcc.wf03.wt01.temperature在2017-11-01T16：37：50.000为空，因此系统使用以前的时间戳2017-11-01T16：37：00.000（且时间戳位于[2017- 11-01T16：36：50.000，2017-11-01T16：37：50.000]时间范围）及其值21.927326，下一个时间戳记2017-11-01T16：38：00.000（且时间戳记位于[2017-11-11] 01T16：37：50.000、2017-11-01T16：38：50.000]时间范围）及其值25.311783以执行线性拟合计算：

21.927326 +（25.311783-21.927326）/ 60s * 50s = 24.747707

在 [样例数据](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), 该语句的执行结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577727-d4f29800-1ef5-11e9-8ff3-3bb519da3993.jpg"></center>

#### 数据类型和填充方法之间的对应关系

数据类型和支持的填充方法如表3-6所示。

<center>**表3-6数据类型和支持的填充方法**

| 数据类型 | 支持的填充方法   |
| :------- | :--------------- |
| boolean  | previous         |
| int32    | previous, linear |
| int64    | previous, linear |
| float    | previous, linear |
| double   | previous, linear |
| text     | previous         |

</center>

值得注意的是，IoTDB将针对数据类型不支持的填充方法给出错误提示，如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577741-e340b400-1ef5-11e9-9238-a4eaf498ab84.jpg"></center>

如果未指定fill方法，则每种数据类型均具有其自己的默认fill方法和参数。 对应关系如表3-7所示。

<center>**表3-7各种数据类型的默认填充方法和参数**

| 数据类型 | 默认填充方法和参数     |
| :------- | :--------------------- |
| boolean  | previous, 600000       |
| int32    | linear, 600000, 600000 |
| int64    | linear, 600000, 600000 |
| float    | linear, 600000, 600000 |
| double   | linear, 600000, 600000 |
| text     | previous, 600000       |

</center>

> 注意：应在Fill语句中至少指定一种填充方法。

### 对查询结果的行和列控制

IoTDB提供 [LIMIT/SLIMIT](../Operation%20Manual/SQL%20Reference.md) 子句和 [OFFSET/SOFFSET](../Operation%20Manual/SQL%20Reference.md) 子句，以使用户可以更好地控制查询结果。使用LIMIT和SLIMIT子句可让用户控制查询结果的行数和列数，
并且使用OFFSET和SOFSET子句允许用户设置结果显示的起始位置。

请注意，按组查询不支持LIMIT和OFFSET。

本章主要介绍查询结果的行和列控制的相关示例。你还可以使用 [Java JDBC](../Client/Programming%20-%20JDBC.md) 标准接口执行查询。

#### 查询结果的行控制

通过使用LIMIT和OFFSET子句，用户可以以与行相关的方式控制查询结果。 我们将通过以下示例演示如何使用LIMIT和OFFSET子句。

- 示例1：基本的LIMIT子句

SQL语句是：

```
select status, temperature from root.ln.wf01.wt01 limit 10
```

意思是：

所选设备为ln组wf01工厂wt01设备； 选择的时间序列是“状态”和“温度”。 SQL语句要求返回查询结果的前10行。

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577752-efc50c80-1ef5-11e9-9071-da2bbd8b9bdd.jpg"></center>

- 示例2：带OFFSET的LIMIT子句

SQL语句是：

```
select status, temperature from root.ln.wf01.wt01 limit 5 offset 3
```

意思是：

所选设备为ln组wf01工厂wt01设备； 选择的时间序列是“状态”和“温度”。 SQL语句要求返回查询结果的第3至7行（第一行编号为0行）。

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577773-08352700-1ef6-11e9-883f-8d353bef2bdc.jpg"></center>

- 示例3：LIMIT子句与WHERE子句结合

SQL语句是：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time< 2017-11-01T00:12:00.000 limit 2 offset 3
```

意思是：

所选设备为ln组wf01工厂wt01设备； 选择的时间序列是“状态”和“温度”。 SQL语句要求返回时间“ 2017-11-01T00：05：00.000”和“ 2017-11-01T00：12：00.000”之间的状态和温度传感器值的第3至4行（第一行） 编号为第0行）。

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577789-15521600-1ef6-11e9-86ca-d7b2c947367f.jpg"></center>

- 示例4：LIMIT子句与GROUP BY子句组合

SQL语句是：

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d) limit 5 offset 3
```

意思是：

SQL语句子句要求返回查询结果的第3至7行（第一行编号为0行）。

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577796-1e42e780-1ef6-11e9-8987-be443000a77e.jpg"></center>

值得注意的是，由于当前的FILL子句只能在某个时间点填充时间序列的缺失值，也就是说，FILL子句的执行结果恰好是一行，因此LIMIT和OFFSET不会是 与FILL子句结合使用，否则将提示错误。 例如，执行以下SQL语句：

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) limit 10
```

SQL语句将不会执行，并且相应的错误提示如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61517266-6e2fe080-aa39-11e9-8015-154a8e8ace30.png"></center>

#### 查询结果的列控制

通过使用LIMIT和OFFSET子句，用户可以以与列相关的方式控制查询结果。 我们将通过以下示例演示如何使用SLIMIT和OFFSET子句。

- 示例1：基本的SLIMIT子句

SQL语句是：

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```

意思是：

所选设备为ln组wf01工厂wt01设备； 所选时间序列是该设备下的第一列，即电源状态。 SQL语句要求在“ 2017-11-01T00：05：00.000”和“ 2017-11-01T00：12：00.000”的时间点之间选择状态传感器值。

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577813-30bd2100-1ef6-11e9-94ef-dbeb450cf319.jpg"></center>

- 示例2：带OFFSET的LIMIT子句

SQL语句是：

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 1
```

意思是：

所选设备为ln组wf01工厂wt01设备； 所选时间序列是该设备下的第二列，即温度。 SQL语句要求在“ 2017-11-01T00：05：00.000”和“ 2017-11-01T00：12：00.000”的时间点之间选择温度传感器值。

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577827-39adf280-1ef6-11e9-81b5-876769607cd2.jpg"></center>

- 示例3：SLIMIT子句与GROUP BY子句结合

SQL语句是：

```
select max_value(*) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d) slimit 1 soffset 1

```

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577840-44688780-1ef6-11e9-8abc-04ae78efa85b.jpg"></center>

- 示例4：SLIMIT子句与FILL子句结合

SQL语句是：

```
select * from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) slimit 1 soffset 1

```

意思是：

所选设备为ln组wf01工厂wt01设备； 所选时间序列是该设备下的第二列，即温度。

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577855-4d595900-1ef6-11e9-8541-a4accd714b75.jpg"></center>

值得注意的是，预期SLIMIT子句将与星形路径或前缀路径一起使用，并且当SLIMIT子句与完整路径查询一起使用时，系统将提示错误。 例如，执行以下SQL语句：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1

```

SQL语句将不会执行，并且相应的错误提示如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577867-577b5780-1ef6-11e9-978c-e02c1294bcc5.jpg"></center>

#### 控制查询结果的行和列

除了对查询结果进行行或列控制之外，IoTDB还允许用户控制查询结果的行和列。 这是同时包含LIMIT子句和SLIMIT子句的完整示例。

SQL语句是：

```
select * from root.ln.wf01.wt01 limit 10 offset 100 slimit 2 soffset 0

```

意思是：

所选设备为ln组wf01工厂wt01设备； 所选时间序列是此设备下的第0列至第1列（第一列编号为第0列）。 SQL语句子句要求返回查询结果的第100至109行（第一行编号为0行）。

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577879-64984680-1ef6-11e9-9d7b-57dd60fab60e.jpg"></center>

### 使用别名

由于 IoTDB 独特的数据模型，在每个传感器前都附带有设备等诸多额外信息。有时，我们只针对某个具体设备查询，而这些前缀信息频繁显示造成了冗余，影响了结果集的显示与分析。这时我们可以使用 IoTDB 提供的 AS 函数，将查询中出现的时间序列给定一个别名。

例如：

```
select s1 as temperature, s2 as speed from root.ln.wf01.wt01;
```

则结果集将显示为：

| Time | temperature | speed |
| ---- | ----------- | ----- |
| ...  | ...         | ...   |

#### 其他结果集格式

此外，IoTDB支持两种其他结果集格式：“按设备对齐”和“禁用对齐”。

“按设备对齐”指示将deviceId视为一列。 因此，数据集中的列完全有限。

SQL语句是：

```
select s1,s2 from root.sg1.* ALIGN BY DEVICE

```

有关更多语法描述，请阅读SQL REFERENCE。

“禁用对齐”指示结果集中每个时间序列都有3列。 有关更多语法描述，请阅读SQL REFERENCE。

#### 错误处理

当LIMIT / SLIMIT的参数N / SN超过结果集的大小时，IoTDB将按预期返回所有结果。 例如，原始SQL语句的查询结果由六行组成，我们通过LIMIT子句选择前100行：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 100

```

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578187-ad9cca80-1ef7-11e9-897a-83e66a0f3d94.jpg"></center>

当LIMIT / SLIMIT子句的参数N / SN超过允许的最大值（N / SN的类型为int32）时，系统将提示错误。 例如，执行以下SQL语句：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 1234567890123456789

```

SQL语句将不会执行，并且相应的错误提示如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61517469-e696a180-aa39-11e9-8ca5-42ea991d520e.png"></center>

当LIMIT / LIMIT子句的参数N / SN不是正整数时，系统将提示错误。 例如，执行以下SQL语句：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 13.1

```

SQL语句将不会执行，并且相应的错误提示如下：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61518094-68d39580-aa3b-11e9-993c-fc73c27540f7.png"></center>

当LIMIT子句的参数OFFSET超过结果集的大小时，IoTDB将返回空结果集。 例如，执行以下SQL语句：

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 2 offset 6

```

结果如下所示：

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578227-c60ce500-1ef7-11e9-98eb-175beb8d4086.jpg"></center>

当SLIMIT子句的参数SOFFSET不小于可用时间序列数时，系统将提示错误。 例如，执行以下SQL语句：

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 2

```

SQL语句将不会执行，并且相应的错误提示如下：

### 数据删除

用户使用[DELETE语句](../Operation%20Manual/SQL%20Reference.md)可以删除指定的时间序列中符合时间删除条件的数据。在删除数据时，用户可以选择需要删除的一个或多个时间序列、时间序列的前缀、时间序列带\*路径对某一个时间区间内的数据进行删除。

在JAVA编程环境中，您可以使用JDBC API单条或批量执行DELETE语句。

#### 单传感器时间序列值删除

以测控ln集团为例，存在这样的使用场景：

wf02子站的wt02设备在2017-11-01 16:26:00之前的供电状态出现多段错误，且无法分析其正确数据，错误数据影响了与其他设备的关联分析。此时，需要将此时间段前的数据删除。进行此操作的SQL语句为：

```
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

如果我们仅仅想要删除2017年内的在2017-11-01 16:26:00之前的数据，可以使用以下SQL:
```
delete from root.ln.wf02.wt02.status where time>=2017-01-01T00:00:00 and time<=2017-11-01T16:26:00;
```

IoTDB 支持删除一个时间序列任何一个时间范围内的所有时序点，用户可以使用以下SQL语句指定需要删除的时间范围：
```
delete from root.ln.wf02.wt02.status where time < 10
delete from root.ln.wf02.wt02.status where time <= 10
delete from root.ln.wf02.wt02.status where time < 20 and time > 10
delete from root.ln.wf02.wt02.status where time <= 20 and time >= 10
delete from root.ln.wf02.wt02.status where time > 20
delete from root.ln.wf02.wt02.status where time >= 20
delete from root.ln.wf02.wt02.status where time = 20
```

需要注意，当前的删除语句不支持where子句后的时间范围为多个由OR连接成的时间区间。如下删除语句将会解析出错：
```
delete from root.ln.wf02.wt02.status where time > 4 or time < 0
Msg: 303: Check metadata error: For delete statement, where clause can only contain atomic
expressions like : time > XXX, time <= XXX, or two atomic expressions connected by 'AND'
```

#### 多传感器时间序列值删除    

当ln集团wf02子站的wt02设备在2017-11-01 16:26:00之前的供电状态和设备硬件版本都需要删除，此时可以使用含义更广的[前缀路径或带`*`路径](../Concept/Data%20Model%20and%20Terminology.md)进行删除操作，进行此操作的SQL语句为：

```
delete from root.ln.wf02.wt02 where time <= 2017-11-01T16:26:00;
```

或

```
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```

需要注意的是，当删除的路径不存在时，IoTDB会提示路径不存在，无法删除数据，如下所示。

```
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: TimeSeries does not exist and its data cannot be deleted
```

## 删除时间分区 (实验性功能)
您可以通过如下语句来删除某一个存储组下的指定时间分区:

```
DELETE PARTITION root.ln 0,1,2
```

上例中的0,1,2为待删除时间分区的id，您可以通过查看IoTDB的数据文件夹找到它，或者可以通过计算`timestamp / partitionInterval`(向下取整),
手动地将一个时间戳转换为对应的id，其中的`partitionInterval`可以在IoTDB的配置文件中找到（如果您使用的版本支持时间分区）。

请注意该功能目前只是实验性的，如果您不是开发者，使用时请务必谨慎。
