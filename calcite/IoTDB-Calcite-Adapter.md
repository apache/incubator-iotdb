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
# IoTDB - Calcite Adapter 功能文档

## 关系表结构

IoTDB - Calcite Adapter 中使用的关系表结构为：

| time | device | sensor1 | sensor2 | sensor3 | ... |
| ---- | ------ | ------- | ------- | ------- | --- |
|      |        |         |         |         |     |

其中，IoTDB 中每个存储组作为一张表，表中的列包括 time , device 列以及该存储组中所有设备中传感器的最大并集，其中不同设备的同名传感器应该具有相同的数据类型。

例如对于 IoTDB 中存储组 `root.sg`，其中设备及其对应的传感器为：
- d1 -> s1, s2
- d2 -> s2, s3
- d3 -> s1, s4

则在 IoTDB - Calcite Adapter 中的表名为 `root.sg`，其表结构为

| time | device | s1  | s2  | s3  | s4  |
| ---- | ------ | --- | --- | --- | --- |
|      |        |     |     |     |     |

## 工作原理

接下来简单介绍 IoTDB - Calcite Adapter 的工作原理。

输入的 SQL 语句在经过 Calicte 的解析验证后，对 `IoTDBRules` 中定义的优化（下推）规则进行匹配，对于能够下推的节点做相应转化后，得到能够在 IoTDB 端执行的 SQL 语句，然后在 IoTDB 端执行查询语句获取源数据；对于不能下推的节点则调用 Calcite 默认的物理计划进行执行，最后通过 `IoTDBEnumerator` 遍历结果集获取结果。


## 查询介绍

当前在 `IoTDBRules` 中定义的下推规则有：`IoTDBProjectRule`, `IoTDBFilterRule`, `IoTDBLimitRule`。

### IoTDBProjectRule

IoTDBProjectRule 实现了将查询语句中出现的投影列下推到 IoTDB 端进行执行。

例如：（以下 sql 均为测试中的语句）

1. 对于通配符

```sql
select * from "root.vehicle"
```

对于通配符 `*`，将在转化中保持原样，而不转化为列名，得到 IoTDB 中的查询语句为：

```sql
select * from root.vehicle.* align by device
```

2. 对于非通配符的传感器列

```sql
select s0 from "root.vehicle"
```

将转化为：

```sql
select s0 from root.vehicle.* align by device
```

3. 对于非通配符的非传感器列

```sql
select "time", device, s2 from "root.vehicle"
```

该语句中的 time 及 device 列是 IoTDB 的查询语句中不需要包括的，因此转化将去掉这两列，得到 IoTDB 中的查询语句为：

```sql
select s2 from root.vehicle.* align by device
```

特别地，如果查询语句中仅包含 time 及 device 列，则投影部分将转化为通配符 `*`。

4. 重命名 Alias

当前 IoTDB - Calcite Adapter 仅支持在 SELECT 语句中对投影列进行重命名，不支持在后续语句中使用重命名后的名称。

```sql
select "time" AS t, device AS d, s2 from "root.vehicle"
```

将得到结果中 time 列的名字为 t，device 列的名字为 d。

### IoTDBFilterRule

IoTDBFilterRule 实现了将查询语句中的 WHERE 子句下推到 IoTDB 端进行执行。

1. WHERE 子句中不限制 device 列

```sql
select * from "root.vehicle" where "time" < 10 AND s0 >= 150
```

对于 time 列将不作改变，由于未限制具体的设备，因此传感器列不会与具体的设备名进行拼接，得到 IoTDB 中的查询语句为：

```sql
select * from root.vehicle.* where time < 10 AND s0 >= 150
```

2. WHERE 子句中限制 device 列

- 仅限制单个设备

```sql
select * from "root.vehicle" where device = 'root.vehicle.d0' AND "time" > 10 AND s0 <= 100
```

如果 WHERE 中只限制了单个设备且其它限制条件均是对该设备的限制，则在 IoTDB 中将转化为对该设备的查询，上述查询将转化为：

```sql
select * from root.vehicle.d0 where time > 10 AND s0 <= 100
```

- 限制多个设备

```sql
select * from "root.vehicle" where (device = 'root.vehicle.d0' AND "time" <= 1) OR (device = 'root.vehicle.d1' AND s0 < 100)
```

如果 WHERE 中限制了多个设备，将转化为多条查询语句，根据对每个设备的限制条件分别进行查询。

如上述查询语句将转化为两条 SQL 在 IoTDB 中执行：

```sql
select * from root.vehicle.d0 where time <= 1
select * from root.vehicle.d1 where s0 < 100
```

- 既有限制设备的条件，又有全局条件

```sql
select * from "root.vehicle" where (device = 'root.vehicle.d0' AND "time" <= 1) OR s0 = 999
```

在上述 SQL 语句中，除了有对设备 `root.vehicle.d0` 的单独限制外，还有一个限制条件 `s0 = 999`，该限制条件被认为是一个全局条件，任何设备只要满足该条件都被认为是正确结果。

因此上述查询将转化为对存储组中所有设备的查询，对于有单独限制条件的设备将单独处理，其它剩余设备将使用全局条件统一查询。

```sql
select * from root.vehicle.d0 where time <= 1 OR s0 = 999
select * from root.vehicle.d1 where s0 = 999
```

注：由于测试中恰好只有两个设备，如果再有一个设备 d2，则将在 FROM 子句加上 `root.vehicle.d2` 而非为设备 d2 单独再次查询。

### IoTDBLimitRule

IoTDBLimitRule 实现了将查询语句中的 LIMIT 及 OFFSET 子句下推到 IoTDB 端进行执行。

```sql
select "time", s2 from "root.vehicle" limit 3 offset 2
```

上述查询语句将转化为：

```sql
select * from root.vehicle.* limit 3 offset 2 align by device
```


### 视图 Views

与普通的表不同，普通表通过 schema 自动生成，而如果需要创建额外的表即视图，则需要使用 schema 的 tables 属性。

下面是一个示例：

```json
{
  "version": "1.0",
  "defaultSchema": "IoTDBSchema",
  "schemas": [
    {
      "name": "IoTDBSchema",
      "type": "custom",
      "factory": "org.apache.iotdb.calcite.IoTDBSchemaFactory",
      "operand": {
        "host": "localhost",
        "port": 6667,
        "userName": "root",
        "password": "root"
      },
     "tables": [
        {
          "name": "device1",
          "type": "view",
          "sql": "select * from \"root.vehicle\" where \"device\" = 'root.vehicle.d1'"
        }
      ]
    }
  ]
}
```

上述 json 文件在 tables 属性中定义了一个视图，`"name": "device1"` 规定了视图名为 `device1`，`"type": "view"` 规定了表的类型为视图。

JSON 不容易处理长字符串，因此当 SQL 语句较长时，可以使用 Calcite 支持的另一个语法，提供一个字符串列表，而不是单行字符串。

```json
{
  "name": "device1",
  "type": "view",
  "sql": [
    "select * from \"root.vehicle\"",
    "where \"device\" = 'root.vehicle.d1'"
  ]
}
```
