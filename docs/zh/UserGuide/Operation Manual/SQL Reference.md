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

# SQL 参考文档

## 显示版本号

```sql
show version
```

```
+---------------+
|        version|
+---------------+
|0.11.0-SNAPSHOT|
+---------------+
Total line number = 1
It costs 0.417s
```

## Schema语句

* 设置存储组

``` SQL
SET STORAGE GROUP TO <FullPath>
Eg: IoTDB > SET STORAGE GROUP TO root.ln.wf01.wt01
Note: FullPath can not include `*`
```
* 删除存储组

```
DELETE STORAGE GROUP <FullPath> [COMMA <FullPath>]*
Eg: IoTDB > DELETE STORAGE GROUP root.ln.wf01.wt01
Eg: IoTDB > DELETE STORAGE GROUP root.ln.wf01.wt01, root.ln.wf01.wt02
Note: FullPath can not include `*`
```

* 创建时间序列语句

```
CREATE TIMESERIES <FullPath> WITH <AttributeClauses>
alias
    : LR_BRACKET ID RR_BRACKET
    ;
attributeClauses
    : DATATYPE OPERATOR_EQ dataType COMMA ENCODING OPERATOR_EQ encoding
    (COMMA (COMPRESSOR | COMPRESSION) OPERATOR_EQ compressor=propertyValue)?
    (COMMA property)*
    tagClause
    attributeClause
    ;
attributeClause
    : (ATTRIBUTES LR_BRACKET property (COMMA property)* RR_BRACKET)?
    ;
tagClause
    : (TAGS LR_BRACKET property (COMMA property)* RR_BRACKET)?
    ;
DataTypeValue: BOOLEAN | DOUBLE | FLOAT | INT32 | INT64 | TEXT
EncodingValue: GORILLA | PLAIN | RLE | TS_2DIFF | REGULAR
CompressorValue: UNCOMPRESSED | SNAPPY
Eg: CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN
Eg: CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE
Eg: CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSOR=SNAPPY, MAX_POINT_NUMBER=3
Eg: create timeseries root.turbine.d0.s0(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(unit=f, description='turbine this is a test1') attributes(H_Alarm=100, M_Alarm=50)
Note: Datatype and encoding type must be corresponding. Please check Chapter 3 Encoding Section for details.
```

* 删除时间序列语句

```
DELETE TIMESERIES <PrefixPath> [COMMA <PrefixPath>]*
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.status
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature
Eg: IoTDB > DELETE TIMESERIES root.ln.wf01.wt01.*
```

* 修改时间序列标签属性语句
```
ALTER TIMESERIES fullPath alterClause
alterClause
    : RENAME beforeName=ID TO currentName=ID
    | SET property (COMMA property)*
    | DROP ID (COMMA ID)*
    | ADD TAGS property (COMMA property)*
    | ADD ATTRIBUTES property (COMMA property)*
    | UPSERT tagClause attributeClause
    ;
attributeClause
    : (ATTRIBUTES LR_BRACKET property (COMMA property)* RR_BRACKET)?
    ;
tagClause
    : (TAGS LR_BRACKET property (COMMA property)* RR_BRACKET)?
    ;
Eg: ALTER timeseries root.turbine.d1.s1 RENAME tag1 TO newTag1
Eg: ALTER timeseries root.turbine.d1.s1 SET tag1=newV1, attr1=newV1
Eg: ALTER timeseries root.turbine.d1.s1 DROP tag1, tag2
Eg: ALTER timeseries root.turbine.d1.s1 ADD TAGS tag3=v3, tag4=v4
Eg: ALTER timeseries root.turbine.d1.s1 ADD ATTRIBUTES attr3=v3, attr4=v4
EG: ALTER timeseries root.turbine.d1.s1 UPSERT TAGS(tag2=newV2, tag3=v3) ATTRIBUTES(attr3=v3, attr4=v4)
```

* 显示所有时间序列语句

```
SHOW TIMESERIES
Eg: IoTDB > SHOW TIMESERIES
Note: This statement can only be used in IoTDB Client. If you need to show all timeseries in JDBC, please use `DataBaseMetadata` interface.
```

* 显示特定时间序列语句

```
SHOW TIMESERIES <Path>
Eg: IoTDB > SHOW TIMESERIES root
Eg: IoTDB > SHOW TIMESERIES root.ln
Eg: IoTDB > SHOW TIMESERIES root.ln.*.*.status
Eg: IoTDB > SHOW TIMESERIES root.ln.wf01.wt01.status
Note: The path can be prefix path, star path or timeseries path
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示满足条件的时间序列语句
```
SHOW TIMESERIES prefixPath? showWhereClause?
showWhereClause
    : WHERE (property | containsExpression)
    ;
containsExpression
    : name=ID OPERATOR_CONTAINS value=propertyValue
    ;

Eg: show timeseries root.ln where unit='c'
Eg: show timeseries root.ln where description contains 'test1'
```

* 分页显示满足条件的时间序列语句

```
SHOW TIMESERIES prefixPath? showWhereClause? limitClause?

showWhereClause
    : WHERE (property | containsExpression)
    ;
containsExpression
    : name=ID OPERATOR_CONTAINS value=propertyValue
    ;
limitClause
    : LIMIT INT offsetClause?
    | offsetClause? LIMIT INT
    ;
    
Eg: show timeseries root.ln where unit='c'
Eg: show timeseries root.ln where description contains 'test1'
Eg: show timeseries root.ln where unit='c' limit 10 offset 10
```

* 显示存储组语句

```
SHOW STORAGE GROUP
Eg: IoTDB > SHOW STORAGE GROUP
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示特定存储组语句

```
SHOW STORAGE GROUP <PrefixPath>
Eg: IoTDB > SHOW STORAGE GROUP root.*
Eg: IoTDB > SHOW STORAGE GROUP root.ln
Note: The path can be prefix path or star path.
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示Merge状态语句

```
SHOW MERGE
Eg: IoTDB > SHOW MERGE
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示指定路径下时间序列数语句

```
COUNT TIMESERIES <Path>
Eg: IoTDB > COUNT TIMESERIES root
Eg: IoTDB > COUNT TIMESERIES root.ln
Eg: IoTDB > COUNT TIMESERIES root.ln.*.*.status
Eg: IoTDB > COUNT TIMESERIES root.ln.wf01.wt01.status
Note: The path can be prefix path, star path or timeseries path.
Note: This statement can be used in IoTDB Client and JDBC.
```

```
COUNT TIMESERIES <Path> GROUP BY LEVEL=<INTEGER>
Eg: IoTDB > COUNT TIMESERIES root GROUP BY LEVEL=1
Eg: IoTDB > COUNT TIMESERIES root.ln GROUP BY LEVEL=2
Eg: IoTDB > COUNT TIMESERIES root.ln.wf01 GROUP BY LEVEL=3
Note: The path can be prefix path or timeseries path.
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示指定路径下特定层级的节点数语句

```
COUNT NODES <Path> LEVEL=<INTEGER>
Eg: IoTDB > COUNT NODES root LEVEL=2
Eg: IoTDB > COUNT NODES root.ln LEVEL=2
Eg: IoTDB > COUNT NODES root.ln.* LEVEL=3
Eg: IoTDB > COUNT NODES root.ln.wf01 LEVEL=3
Note: The path can be prefix path or timeseries path.
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示所有设备语句

```
SHOW DEVICES
Eg: IoTDB > SHOW DEVICES
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示特定设备语句

```
SHOW DEVICES <PrefixPath>
Eg: IoTDB > SHOW DEVICES root
Eg: IoTDB > SHOW DEVICES root.ln
Eg: IoTDB > SHOW DEVICES root.*.wf01
Note: The path can be prefix path or star path.
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示ROOT节点的子节点名称语句

```
SHOW CHILD PATHS
Eg: IoTDB > SHOW CHILD PATHS
Note: This statement can be used in IoTDB Client and JDBC.
```

* 显示子节点名称语句

```
SHOW CHILD PATHS <Path>
Eg: IoTDB > SHOW CHILD PATHS root
Eg: IoTDB > SHOW CHILD PATHS root.ln
Eg: IoTDB > SHOW CHILD PATHS root.*.wf01
Eg: IoTDB > SHOW CHILD PATHS root.ln.wf*
Note: The path can be prefix path or star path, the nodes can be in a "prefix + star" format. 
Note: This statement can be used in IoTDB Client and JDBC.
```

* 为 schema 创建快照
```
CREATE SNAPSHOT FOR SCHEMA
```

## 数据管理语句

* 插入记录语句

```
INSERT INTO <PrefixPath> LPAREN TIMESTAMP COMMA <Sensor> [COMMA <Sensor>]* RPAREN VALUES LPAREN <TimeValue>, <PointValue> [COMMA <PointValue>]* RPAREN
Sensor : Identifier
Eg: IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,status) values(1509465600000,true)
Eg: IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,status) VALUES(NOW(), false)
Eg: IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp,temperature) VALUES(2017-11-01T00:17:00.000+08:00,24.22028)
Eg: IoTDB > INSERT INTO root.ln.wf01.wt01(timestamp, status, temperature) VALUES (1509466680000, false, 20.060787);
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
Note: The order of Sensor and PointValue need one-to-one correspondence
```

* 删除记录语句

```
DELETE FROM <PrefixPath> [COMMA <PrefixPath>]* [WHERE <WhereClause>]?
WhereClause : <Condition> [(AND) <Condition>]*
Condition  : <TimeExpr> [(AND) <TimeExpr>]*
TimeExpr : TIME PrecedenceEqualOperator (<TimeValue> | <RelativeTime>)
Eg: DELETE FROM root.ln.wf01.wt01.temperature WHERE time > 2016-01-05T00:15:00+08:00 and time < 2017-11-1T00:05:00+08:00
Eg: DELETE FROM root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature WHERE time < NOW()
Eg: DELETE FROM root.ln.wf01.wt01.* WHERE time >= 1509466140000
```

* 选择记录语句

```
SELECT <SelectClause> FROM <FromClause> [WHERE <WhereClause>]?
SelectClause : <SelectPath> (COMMA <SelectPath>)*
SelectPath : <FUNCTION> LPAREN <Path> RPAREN | <Path>
FUNCTION : ‘COUNT’ , ‘MIN_TIME’, ‘MAX_TIME’, ‘MIN_VALUE’, ‘MAX_VALUE’
FromClause : <PrefixPath> (COMMA <PrefixPath>)?
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition  : <Expression> [(AND | OR) <Expression>]*
Expression : [NOT | !]? <TimeExpr> | [NOT | !]? <SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator (<TimeValue> | <RelativeTime>)
RelativeTimeDurationUnit = Integer ('Y'|'MO'|'W'|'D'|'H'|'M'|'S'|'MS'|'US'|'NS')
RelativeTime : (now() | <TimeValue>) [(+|-) RelativeTimeDurationUnit]+
SensorExpr : (<Timeseries> | <Path>) PrecedenceEqualOperator <PointValue>
Eg: IoTDB > SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00
Eg. IoTDB > SELECT * FROM root
Eg. IoTDB > SELECT * FROM root where time > now() - 5m
Eg. IoTDB > SELECT * FROM root.ln.*.wf*
Eg. IoTDB > SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Eg. IoTDB > SELECT MIN_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Eg. IoTDB > SELECT MAX_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature > 24
Eg. IoTDB > SELECT MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature > 23
Eg. IoTDB > SELECT MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25
Eg. IoTDB > SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 25 GROUP BY LEVEL=1
Note: the statement needs to satisfy this constraint: <Path>(SelectClause) + <PrefixPath>(FromClause) = <Timeseries>
Note: If the <SensorExpr>(WhereClause) is started with <Path> and not with ROOT, the statement needs to satisfy this constraint: <PrefixPath>(FromClause) + <Path>(SensorExpr) = <Timeseries>
Note: In Version 0.7.0, if <WhereClause> includes `OR`, time filter can not be used.
Note: There must be a space on both sides of the plus and minus operator appearing in the time expression 
```

* Group By 语句

```
SELECT <SelectClause> FROM <FromClause> WHERE  <WhereClause> GROUP BY <GroupByTimeClause>
SelectClause : <Function> [COMMA < Function >]*
Function : <AggregationFunction> LPAREN <Path> RPAREN
FromClause : <PrefixPath>
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition  : <Expression> [(AND | OR) <Expression>]*
Expression : [NOT | !]? <TimeExpr> | [NOT | !]? <SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator (<TimeValue> | <RelativeTime>)
RelativeTimeDurationUnit = Integer ('Y'|'MO'|'W'|'D'|'H'|'M'|'S'|'MS'|'US'|'NS')
RelativeTime : (now() | <TimeValue>) [(+|-) RelativeTimeDurationUnit]+
SensorExpr : (<Timeseries> | <Path>) PrecedenceEqualOperator <PointValue>
GroupByTimeClause : LPAREN <TimeInterval> COMMA <TimeUnit> (COMMA <TimeUnit>)? RPAREN
TimeInterval: LSBRACKET <TimeValue> COMMA <TimeValue> RRBRACKET | LRBRACKET <TimeValue> COMMA <TimeValue> RSBRACKET
TimeUnit : Integer <DurationUnit>
DurationUnit : "ms" | "s" | "m" | "h" | "d" | "w"
Eg: SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 where temperature < 24 GROUP BY([1509465720000, 1509466380000), 5m)
Eg: SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 where temperature < 24 GROUP BY((1509465720000, 1509466380000], 5m)
Eg. SELECT COUNT (status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE time < 1509466500000 GROUP BY([1509465720000, 1509466380000), 5m, 10m)
Eg. SELECT MIN_TIME(status), MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE temperature < 25 GROUP BY ([1509466140000, 1509466380000), 3m, 5ms)
Eg. SELECT MIN_TIME(status), MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE temperature < 25 GROUP BY ((1509466140000, 1509466380000], 3m, 5ms)
Note: the statement needs to satisfy this constraint: <Path>(SelectClause) + <PrefixPath>(FromClause) = <Timeseries>
Note: If the <SensorExpr>(WhereClause) is started with <Path> and not with ROOT, the statement needs to satisfy this constraint: <PrefixPath>(FromClause) + <Path>(SensorExpr) = <Timeseries>
Note: <TimeValue>(TimeInterval) needs to be greater than 0
Note: First <TimeValue>(TimeInterval) in needs to be smaller than second <TimeValue>(TimeInterval)
Note: <TimeUnit> needs to be greater than 0
Note: Third <TimeUnit> if set shouldn't be smaller than second <TimeUnit>
```

* Fill 语句

```
SELECT <SelectClause> FROM <FromClause> WHERE <WhereClause> FILL <FillClause>
SelectClause : <Path> [COMMA <Path>]*
FromClause : < PrefixPath > [COMMA < PrefixPath >]*
WhereClause : <WhereExpression>
WhereExpression : TIME EQUAL <TimeValue>
FillClause : LPAREN <TypeClause> [COMMA <TypeClause>]* RPAREN
TypeClause : <Int32Clause> | <Int64Clause> | <FloatClause> | <DoubleClause> | <BoolClause> | <TextClause>
Int32Clause: INT32 LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
Int64Clause: INT64 LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
FloatClause: FLOAT LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
DoubleClause: DOUBLE LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
BoolClause: BOOLEAN LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
TextClause: TEXT LBRACKET (<LinearClause> | <PreviousClause>)  RBRACKET
PreviousClause : PREVIOUS [COMMA <ValidPreviousTime>]?
LinearClause : LINEAR [COMMA <ValidPreviousTime> COMMA <ValidBehindTime>]?
ValidPreviousTime, ValidBehindTime: <TimeUnit>
TimeUnit : Integer <DurationUnit>
DurationUnit : "ms" | "s" | "m" | "h" | "d" | "w"
Eg: SELECT temperature FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL(float[previous, 1m])
Eg: SELECT temperature,status FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear, 1m, 1m], boolean[previous, 1m])
Eg: SELECT temperature,status,hardware FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear, 1m, 1m], boolean[previous, 1m], text[previous])
Eg: SELECT temperature,status,hardware FROM root.ln.wf01.wt01 WHERE time = 2017-11-01T16:37:50.000 FILL (float[linear], boolean[previous, 1m], text[previous])
Note: the statement needs to satisfy this constraint: <PrefixPath>(FromClause) + <Path>(SelectClause) = <Timeseries>
Note: Integer in <TimeUnit> needs to be greater than 0
```

* Group By Fill语句

```
SELECT <SelectClause> FROM <FromClause> WHERE  <WhereClause> GROUP BY <GroupByClause> (FILL <GROUPBYFillClause>)?
GroupByClause : LPAREN <TimeInterval> COMMA <TimeUnit> RPAREN
GROUPBYFillClause : LPAREN <TypeClause> RPAREN
TypeClause : <AllClause> | <Int32Clause> | <Int64Clause> | <FloatClause> | <DoubleClause> | <BoolClause> | <TextClause> 
AllClause: ALL LBRACKET (<PreviousUntilLastClause> | <PreviousClause>)  RBRACKET
Int32Clause: INT32 LBRACKET (<PreviousUntilLastClause> | <PreviousClause>)  RBRACKET
Int64Clause: INT64 LBRACKET (<PreviousUntilLastClause> | <PreviousClause>)  RBRACKET
FloatClause: FLOAT LBRACKET (<PreviousUntilLastClause> | <PreviousClause>)  RBRACKET
DoubleClause: DOUBLE LBRACKET (<PreviousUntilLastClause> | <PreviousClause>)  RBRACKET
BoolClause: BOOLEAN LBRACKET (<PreviousUntilLastClause> | <PreviousClause>)  RBRACKET
TextClause: TEXT LBRACKET (<PreviousUntilLastClause> | <PreviousClause>)  RBRACKET
PreviousClause : PREVIOUS
PreviousUntilLastClause : PREVIOUSUNTILLAST
Eg: SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([20, 100), 5m) FILL (float[PREVIOUS])
Eg: SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY((15, 100], 5m) FILL (float[PREVIOUS])
Eg: SELECT last_value(power) FROM root.ln.wf01.wt01 GROUP BY([20, 100), 5m) FILL (int32[PREVIOUSUNTILLAST])
Eg: SELECT last_value(power) FROM root.ln.wf01.wt01 GROUP BY([20, 100), 5m) FILL (int32[PREVIOUSUNTILLAST, 5m])
Eg: SELECT last_value(temperature), last_value(power) FROM root.ln.wf01.wt01 GROUP BY([20, 100), 5m) FILL (ALL[PREVIOUS])
Eg: SELECT last_value(temperature), last_value(power) FROM root.ln.wf01.wt01 GROUP BY([20, 100), 5m) FILL (ALL[PREVIOUS, 5m])
Note: In group by fill, sliding step is not supported in group by clause
Note: Now, only last_value aggregation function is supported in group by fill.
Note: Linear fill is not supported in group by fill.
```

* Order by time 语句

```
SELECT <SelectClause> FROM <FromClause> WHERE  <WhereClause> GROUP BY <GroupByClause> (FILL <GROUPBYFillClause>)? orderByTimeClause?
orderByTimeClause: order by time (asc | desc)?

Eg: SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([20, 100), 5m) FILL (float[PREVIOUS]) order by time desc
Eg: SELECT * from root order by time desc
Eg: SELECT * from root order by time desc align by device 
Eg: SELECT * from root order by time desc disable align
Eg: SELECT last * from root order by time desc
```

* Limit & SLimit 语句

```
SELECT <SelectClause> FROM <FromClause> [WHERE <WhereClause>] [<LIMITClause>] [<SLIMITClause>]
SelectClause : [<Path> | Function]+
Function : <AggregationFunction> LPAREN <Path> RPAREN
FromClause : <Path>
WhereClause : <Condition> [(AND | OR) <Condition>]*
Condition : <Expression> [(AND | OR) <Expression>]*
Expression: [NOT|!]?<TimeExpr> | [NOT|!]?<SensorExpr>
TimeExpr : TIME PrecedenceEqualOperator (<TimeValue> | <RelativeTime>)
RelativeTimeDurationUnit = Integer ('Y'|'MO'|'W'|'D'|'H'|'M'|'S'|'MS'|'US'|'NS')
RelativeTime : (now() | <TimeValue>) [(+|-) RelativeTimeDurationUnit]+
SensorExpr : (<Timeseries>|<Path>) PrecedenceEqualOperator <PointValue>
LIMITClause : LIMIT <N> [OFFSETClause]?
N : Integer
OFFSETClause : OFFSET <OFFSETValue>
OFFSETValue : Integer
SLIMITClause : SLIMIT <SN> [SOFFSETClause]?
SN : Integer
SOFFSETClause : SOFFSET <SOFFSETValue>
SOFFSETValue : Integer
Eg: IoTDB > SELECT status, temperature FROM root.ln.wf01.wt01 WHERE temperature < 24 and time > 2017-11-1 0:13:00 LIMIT 3 OFFSET 2
Eg. IoTDB > SELECT COUNT (status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE time < 1509466500000 GROUP BY([1509465720000, 1509466380000], 5m) LIMIT 3
Note: N, OFFSETValue, SN and SOFFSETValue must be greater than 0.
Note: The order of <LIMITClause> and <SLIMITClause> does not affect the grammatical correctness.
Note: <FillClause> can not use <LIMITClause> but not <SLIMITClause>.
```

* Align by device语句

```
AlignbyDeviceClause : ALIGN BY DEVICE

规则:  
1. 大小写不敏感.  
正例: select * from root.sg1 align by device
正例: select * from root.sg1 ALIGN BY DEVICE

2. AlignbyDeviceClause 只能放在末尾.  
正例: select * from root.sg1 where time > 10 align by device  
错例: select * from root.sg1 align by device where time > 10  

3. Select子句中的path只能是单层，或者通配符，不允许有path分隔符"."。
正例: select s0,s1 from root.sg1.* align by device  
正例: select s0,s1 from root.sg1.d0, root.sg1.d1 align by device  
正例: select * from root.sg1.* align by device  
正例: select * from root align by device  
正例: select s0,s1,* from root.*.* align by device  
错例: select d0.s1, d0.s2, d1.s0 from root.sg1 align by device  
错例: select *.s0, *.s1 from root.* align by device  
错例: select *.*.* from root align by device

4.相同measurement的各设备的数据类型必须都相同，

正例: select s0 from root.sg1.d0,root.sg1.d1 align by device   
root.sg1.d0.s0 and root.sg1.d1.s0 are both INT32.  

正例: select count(s0) from root.sg1.d0,root.sg1.d1 align by device   
count(root.sg1.d0.s0) and count(root.sg1.d1.s0) are both INT64.  

错例: select s0 from root.sg1.d0, root.sg2.d3 align by device  
root.sg1.d0.s0 is INT32 while root.sg2.d3.s0 is FLOAT. 

5. 结果集的展示规则：对于select中给出的列，不论是否有数据（是否被注册），均会被显示。此外，select子句中还支持常数列（例如，'a', '123'等等）。
例如, "select s0,s1,s2,'abc',s1,s2 from root.sg.d0, root.sg.d1, root.sg.d2 align by device". 假设只有下述三列有数据：
- root.sg.d0.s0
- root.sg.d0.s1
- root.sg.d1.s0

结果集形如:

| Time | Device   | s0 | s1 |  s2  | 'abc' | s1 |  s2  |
| ---  | ---      | ---| ---| null | 'abc' | ---| null |
|  1   |root.sg.d0| 20 | 2.5| null | 'abc' | 2.5| null |
|  2   |root.sg.d0| 23 | 3.1| null | 'abc' | 3.1| null |
| ...  | ...      | ...| ...| null | 'abc' | ...| null |
|  1   |root.sg.d1| 12 |null| null | 'abc' |null| null |
|  2   |root.sg.d1| 19 |null| null | 'abc' |null| null |
| ...  | ...      | ...| ...| null | 'abc' | ...| null |

注意注意 设备'root.sg.d1'的's0'的值全为null

6. 在From中重复写设备名字或者设备前缀是没有任何作用的。
例如, "select s0,s1 from root.sg.d0,root.sg.d0,root.sg.d1 align by device" 等于 "select s0,s1 from root.sg.d0,root.sg.d1 align by device".  
例如. "select s0,s1 from root.sg.*,root.sg.d0 align by device" 等于 "select s0,s1 from root.sg.* align by device".  

7. 在Select子句中重复写列名是生效的。例如, "select s0,s0,s1 from root.sg.* align by device" 不等于 "select s0,s1 from root.sg.* align by device".

8. 在Where子句中时间过滤条件和值过滤条件均可以使用，值过滤条件可以使用叶子节点 path，或以 root 开头的整个 path，不允许存在通配符。例如，
- select * from root.sg.* where time = 1 align by device
- select * from root.sg.* where s0 < 100 align by device
- select * from root.sg.* where time < 20 AND s0 > 50 align by device
- select * from root.sg.d0 where root.sg.d0.s0 = 15 align by device

9. 更多正例:
   - select * from root.vehicle align by device
   - select s0,s0,s1 from root.vehicle.* align by device
   - select s0,s1 from root.vehicle.* limit 10 offset 1 align by device
   - select * from root.vehicle slimit 10 soffset 2 align by device
   - select * from root.vehicle where time > 10 align by device
   - select * from root.vehicle.* where time < 10 AND s0 > 25 align by device
   - select * from root.vehicle where root.vehicle.d0.s0>0 align by device
   - select count(*) from root.vehicle align by device
   - select sum(*) from root.vehicle GROUP BY (20ms,0,[2,50]) align by device
   - select * from root.vehicle where time = 3 Fill(int32[previous, 5ms]) align by device
```

* Disable align 语句

```
规则:  
1. 大小写均可.  
正例: select * from root.sg1 disable align  
正例: select * from root.sg1 DISABLE ALIGN  

2. Disable Align只能用于查询语句句尾.  
正例: select * from root.sg1 where time > 10 disable align 
错例: select * from root.sg1 disable align where time > 10 

3. Disable Align 不能用于聚合查询、Fill语句、Group by或Group by device语句，但可用于Limit语句。
正例: select * from root.sg1 limit 3 offset 2 disable align
正例: select * from root.sg1 slimit 3 soffset 2 disable align
错例: select count(s0),count(s1) from root.sg1.d1 disable align
错例: select * from root.vehicle where root.vehicle.d0.s0>0 disable align
错例: select * from root.vehicle align by device disable align

4. 结果显示若无数据显示为空白.

查询结果样式如下表:
| Time | root.sg.d0.s1 | Time | root.sg.d0.s2 | Time | root.sg.d1.s1 |
| ---  | ---           | ---  | ---           | ---  | ---           |
|  1   | 100           | 20   | 300           | 400  | 600           |
|  2   | 300           | 40   | 800           | 700  | 900           |
|  4   | 500           |      |               | 800  | 1000          |
|      |               |      |               | 900  | 8000          |

5. 一些正确使用样例: 
   - select * from root.vehicle disable align
   - select s0,s0,s1 from root.vehicle.* disable align
   - select s0,s1 from root.vehicle.* limit 10 offset 1 disable align
   - select * from root.vehicle slimit 10 soffset 2 disable align
   - select * from root.vehicle where time > 10 disable align

```

* Last语句

Last 语句返回所要查询时间序列的最近时间戳的一条数据

```
SELECT LAST <SelectClause> FROM <FromClause> WHERE <WhereClause>
Select Clause : <Path> [COMMA <Path>]*
FromClause : < PrefixPath > [COMMA < PrefixPath >]*
WhereClause : <TimeExpr> [(AND | OR) <TimeExpr>]*
TimeExpr : TIME PrecedenceEqualOperator (<TimeValue> | <RelativeTime>)

Eg. SELECT LAST s1 FROM root.sg.d1
Eg. SELECT LAST s1, s2 FROM root.sg.d1
Eg. SELECT LAST s1 FROM root.sg.d1, root.sg.d2
Eg. SELECT LAST s1 FROM root.sg.d1 where time < 100
Eg. SELECT LAST s1, s2 FROM root.sg.d1 where time > 100 and time <= 500
Eg. SELECT LAST s1, s2 FROM root.sg.d1 where time < 300 or time > 1000

规则:
1. 需要满足PrefixPath.Path 为一条完整的时间序列，即 <PrefixPath> + <Path> = <Timeseries>

2. SELECT LAST 语句不支持值过滤条件.

3. 结果集以三列的表格的固定形式返回。
例如 "select last s1, s2 from root.sg.d1, root.sg.d2", 结果集返回如下：

| Time | Path         | Value |
| ---  | ------------ | ----- |
|  5   | root.sg.d1.s1| 100   |
|  2   | root.sg.d1.s2| 400   |
|  4   | root.sg.d2.s1| 250   |
|  9   | root.sg.d2.s2| 600   |

4. 注意LAST语句不支持与"disable align"关键词一起使用。

```

* As 语句

As 语句为 SELECT 语句中出现的时间序列规定一个别名

```
在每个查询中都可以使用 As 语句来规定时间序列的别名，但是对于通配符的使用有一定限制。

1. 原始数据查询：
select s1 as speed, s2 as temperature from root.sg.d1

结果集将显示为：
| Time | speed | temperature |
|  ... |  ...  |     ....    |

2. 聚合查询
select count(s1) as s1_num, max_value(s2) as s2_max from root.sg.d1

3. 降频聚合查询
select count(s1) as s1_num from root.sg.d1 group by ([100,500), 80ms)

4. 按设备对齐查询
select s1 as speed, s2 as temperature from root.sg.d1 align by device

select count(s1) as s1_num, count(s2), count(s3) as s3_num from root.sg.d2 align by device

5. 最新数据查询
select last s1 as speed, s2 from root.sg.d1

规则：
1. 除按设备对齐查询外，每一个 AS 语句必须唯一对应一个时间序列。

E.g. select s1 as temperature from root.sg.*

此时如果存储组 root.sg.* 中含有多个设备，则会抛出异常。

2. 按设备对齐查询中，每个 AS 语句对应的前缀路径可以含多个设备，而后缀路径不能含多个传感器。

E.g. select s1 as temperature from root.sg.*

这种情况即使有多个设备，也可以正常显示。

E.g. select * as temperature from root.sg.d1

这种情况如果 * 匹配多个传感器，则无法正常显示。

```

## 数据库管理语句

* 创建用户

```
CREATE USER <userName> <password>;  
userName:=identifier  
password:=string
Eg: IoTDB > CREATE USER thulab 'pwd';
```

* 删除用户

```
DROP USER <userName>;  
userName:=identifier
Eg: IoTDB > DROP USER xiaoming;
```

* 创建角色

```
CREATE ROLE <roleName>;  
roleName:=identifie
Eg: IoTDB > CREATE ROLE admin;
```

* 删除角色

```
DROP ROLE <roleName>;  
roleName:=identifier
Eg: IoTDB > DROP ROLE admin;
```

* 赋予用户权限

```
GRANT USER <userName> PRIVILEGES <privileges> ON <nodeName>;  
userName:=identifier  
nodeName:=identifier (DOT identifier)*  
privileges:= string (COMMA string)*
Eg: IoTDB > GRANT USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.ln;
```

* 赋予角色权限

```
GRANT ROLE <roleName> PRIVILEGES <privileges> ON <nodeName>;  
privileges:= string (COMMA string)*  
roleName:=identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > GRANT ROLE temprole PRIVILEGES 'DELETE_TIMESERIES' ON root.ln;
```

* 赋予用户角色

```
GRANT <roleName> TO <userName>;  
roleName:=identifier  
userName:=identifier
Eg: IoTDB > GRANT temprole TO tempuser;
```

* 撤销用户权限

```
REVOKE USER <userName> PRIVILEGES <privileges> ON <nodeName>;   
privileges:= string (COMMA string)*  
userName:=identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > REVOKE USER tempuser PRIVILEGES 'DELETE_TIMESERIES' on root.ln;
```

* 撤销角色权限

```
REVOKE ROLE <roleName> PRIVILEGES <privileges> ON <nodeName>;  
privileges:= string (COMMA string)*  
roleName:= identifier  
nodeName:=identifier (DOT identifier)*
Eg: IoTDB > REVOKE ROLE temprole PRIVILEGES 'DELETE_TIMESERIES' ON root.ln;
```

* 撤销用户角色

```
REVOKE <roleName> FROM <userName>;
roleName:=identifier
userName:=identifier
Eg: IoTDB > REVOKE temprole FROM tempuser;
```

* 列出用户

```
LIST USER
Eg: IoTDB > LIST USER
```

* 列出角色

```
LIST ROLE
Eg: IoTDB > LIST ROLE
```

* 列出权限

```
LIST PRIVILEGES USER  <username> ON <path>;    
username:=identifier    
path=‘root’ (DOT identifier)*
Eg: IoTDB > LIST PRIVILEGES USER sgcc_wirte_user ON root.sgcc;
```

* 列出角色权限

```
LIST PRIVILEGES ROLE <roleName> ON <path>;    
roleName:=identifier  
path=‘root’ (DOT identifier)*
Eg: IoTDB > LIST PRIVILEGES ROLE wirte_role ON root.sgcc;
```

* 列出用户权限

```
LIST USER PRIVILEGES <username> ;   
username:=identifier  
Eg: IoTDB > LIST USER PRIVILEGES tempuser;
```

* 列出角色权限

```
LIST ROLE PRIVILEGES <roleName>
roleName:=identifier
Eg: IoTDB > LIST ROLE PRIVILEGES actor;
```

* 列出用户角色 

```
LIST ALL ROLE OF USER <username> ;  
username:=identifier
Eg: IoTDB > LIST ALL ROLE OF USER tempuser;
```

* 列出角色用户

```
LIST ALL USER OF ROLE <roleName>;
roleName:=identifier
Eg: IoTDB > LIST ALL USER OF ROLE roleuser;
```

* 更新密码 

```
ALTER USER <username> SET PASSWORD <password>;
roleName:=identifier
password:=string
Eg: IoTDB > ALTER USER tempuser SET PASSWORD 'newpwd';
```

## 功能

* COUNT

```
SELECT COUNT(Path) (COMMA COUNT(Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT COUNT(status), COUNT(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* FIRST_VALUE
原有的 `FIRST` 方法在 `v0.10.0` 版本更名为 `FIRST_VALUE`。
```
SELECT FIRST_VALUE (Path) (COMMA FIRST_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT FIRST_VALUE (status), FIRST_VALUE (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* LAST_VALUE

```
SELECT LAST_VALUE (Path) (COMMA LAST_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT LAST_VALUE (status), LAST_VALUE (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MAX_TIME

```
SELECT MAX_TIME (Path) (COMMA MAX_TIME (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MAX_TIME(status), MAX_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MAX_VALUE

```
SELECT MAX_VALUE (Path) (COMMA MAX_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MAX_VALUE(status), MAX_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* AVG
原有的 `MEAN` 方法在 `v0.9.0` 版本更名为 `AVG`。
```
SELECT AVG (Path) (COMMA AVG (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT AVG (temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MIN_TIME

```
SELECT MIN_TIME (Path) (COMMA MIN_TIME (Path))*FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MIN_TIME(status), MIN_TIME(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* MIN_VALUE

```
SELECT MIN_VALUE (Path) (COMMA MIN_VALUE (Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT MIN_VALUE(status),MIN_VALUE(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

* NOW

```
NOW()
Eg. INSERT INTO root.ln.wf01.wt01(timestamp,status) VALUES(NOW(), false) 
Eg. DELETE FROM root.ln.wf01.wt01.status, root.ln.wf01.wt01.temperature WHERE time < NOW()
Eg. SELECT * FROM root WHERE time < NOW()
Eg. SELECT COUNT(temperature) FROM root.ln.wf01.wt01 WHERE time < NOW()
```

* SUM

```
SELECT SUM(Path) (COMMA SUM(Path))* FROM <FromClause> [WHERE <WhereClause>]?
Eg. SELECT SUM(temperature) FROM root.ln.wf01.wt01 WHERE root.ln.wf01.wt01.temperature < 24
Note: the statement needs to satisfy this constraint: <PrefixPath> + <Path> = <Timeseries>
```

## TTL

IoTDB支持对存储组级别设置数据存活时间（TTL），这使得IoTDB可以定期、自动地删除一定时间之前的数据。合理使用TTL
可以帮助您控制IoTDB占用的总磁盘空间以避免出现磁盘写满等异常。并且，随着文件数量的增多，查询性能往往随之下降,
内存占用也会有所提高。及时地删除一些较老的文件有助于使查询性能维持在一个较高的水平和减少内存资源的占用。
IoTDB中的TTL操作通可以由以下的语句进行实现：

* 设置 TTL
```
SET TTL TO StorageGroupName TTLTime
Eg. SET TTL TO root.group1 3600000
这个例子展示了如何使得root.group1这个存储组只保留近一个小时的数据，一个小时前的数据会被删除或者进入不可见状态。
注意: TTLTime 应是毫秒时间戳。一旦TTL被设置，超过TTL时间范围的写入将被拒绝。
```

* 取消 TTL
```
UNSET TTL TO StorageGroupName
Eg. UNSET TTL TO root.group1
这个例子展示了如何取消存储组root.group1的TTL，这将使得该存储组接受任意时刻的数据。
```

* 显示 TTL
```
SHOW ALL TTL
SHOW TTL ON StorageGroupNames
Eg.1 SHOW ALL TTL
这个例子会给出所有存储组的TTL。
Eg.2 SHOW TTL ON root.group1,root.group2,root.group3
这个例子会显示指定的三个存储组的TTL。
注意: 没有设置TTL的存储组的TTL将显示为null。
```

注意：当您对某个存储组设置TTL的时候，超过TTL范围的数据将会立即不可见。但由于数据文件可能混合包含处在TTL范围内
与范围外的数据，同时数据文件可能正在接受查询，数据文件的物理删除不会立即进行。如果你在此时取消或者调大TTL，
一部分之前不可见的数据可能重新可见，而那些已经被物理删除的数据则将永久丢失。也就是说，TTL操作不会原子性地删除
对应的数据。因此我们不推荐您频繁修改TTL，除非您能接受该操作带来的一定程度的不可预知性。

* 删除时间分区 (实验性功能)
```
DELETE PARTITION StorageGroupName INT(COMMA INT)*
Eg DELETE PARTITION root.sg1 0,1,2
该例子将删除存储组root.sg1的前三个时间分区
```
partitionId 可以通过查看数据文件夹获取，或者是计算 `timestamp / partitionInterval`得到。 

## 性能追踪

IoTDB 支持使用 `TRACING` 语句来追踪查询语句的执行，通过日志文件输出该查询访问的 Tsfile 文件数，chunk 数等信息，默认输出位置位于 `./data/tracing`. 性能追踪功能默认处于关闭状态，用户可以使用 TRACING ON/OFF 命令来打开/关闭该功能。

```
TRACING ON    //打开性能追踪
TRACING OFF   //关闭性能追踪
```

# 参考

## 关键字

```
Keywords for IoTDB (case insensitive):
ADD, BY, COMPRESSOR, CREATE, DATATYPE, DELETE, DESCRIBE, DROP, ENCODING, EXIT, FOR, FROM, GRANT, GROUP, LABLE, LINK, INDEX, INSERT, INTO, LOAD, MAX_POINT_NUMBER, MERGE, METADATA, ON, ORDER, PASSWORD, PRIVILEGES, PROPERTY, QUIT, REVOKE, ROLE, ROOT, SCHEMA, SELECT, SET, SHOW, SNAPSHOT, STORAGE, TIME, TIMESERIES, TIMESTAMP, TO, UNLINK, USER, USING, VALUE, VALUES, WHERE, WITH

Keywords with special meanings (case insensitive):
* Data Types: BOOLEAN, DOUBLE, FLOAT, INT32, INT64, TEXT 
* Encoding Methods: BITMAP, DFT, GORILLA, PLAIN, RLE, TS_2DIFF
* Compression Methods: UNCOMPRESSED, SNAPPY
* Logical symbol: AND, &, &&, OR, | , ||, NOT, !, TRUE, FALSE
```

## 标识符

```
QUOTE := '\'';
DOT := '.';
COLON : ':' ;
COMMA := ',' ;
SEMICOLON := ';' ;
LPAREN := '(' ;
RPAREN := ')' ;
LBRACKET := '[';
RBRACKET := ']';
EQUAL := '=' | '==';
NOTEQUAL := '<>' | '!=';
LESSTHANOREQUALTO := '<=';
LESSTHAN := '<';
GREATERTHANOREQUALTO := '>=';
GREATERTHAN := '>';
DIVIDE := '/';
PLUS := '+';
MINUS := '-';
STAR := '*';
Letter := 'a'..'z' | 'A'..'Z';
HexDigit := 'a'..'f' | 'A'..'F';
Digit := '0'..'9';
Boolean := TRUE | FALSE | 0 | 1 (case insensitive)

```

```
StringLiteral := ( '\'' ( ~('\'') )* '\'' | '\"' ( ~('\"') )* '\"');
eg. 'abc'
eg. "abc"
```

```
Integer := ('-' | '+')? Digit+;
eg. 123
eg. -222
```

```
Float := ('-' | '+')? Digit+ DOT Digit+ (('e' | 'E') ('-' | '+')? Digit+)?;
eg. 3.1415
eg. 1.2E10
eg. -1.33
```

```
Identifier := (Letter | '_') (Letter | Digit | '_' | MINUS)*;
eg. a123
eg. _abc123

```

## 常量


```
PointValue : Integer | Float | StringLiteral | Boolean
```
```
TimeValue : Integer | DateTime | ISO8601 | NOW()
Note: Integer means timestamp type.

DateTime : 
eg. 2016-11-16T16:22:33+08:00
eg. 2016-11-16 16:22:33+08:00
eg. 2016-11-16T16:22:33.000+08:00
eg. 2016-11-16 16:22:33.000+08:00
Note: DateTime Type can support several types, see Chapter 3 Datetime section for details.
```
```
PrecedenceEqualOperator : EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
```
```
Timeseries : ROOT [DOT <LayerName>]* DOT <SensorName>
LayerName : Identifier
SensorName : Identifier
eg. root.ln.wf01.wt01.status
eg. root.sgcc.wf03.wt01.temperature
Note: Timeseries must be start with `root`(case insensitive) and end with sensor name.
```

```
PrefixPath : ROOT (DOT <LayerName>)*
LayerName : Identifier | STAR
eg. root.sgcc
eg. root.*
```
```
Path: (ROOT | <LayerName>) (DOT <LayerName>)* 
LayerName: Identifier | STAR
eg. root.ln.wf01.wt01.status
eg. root.*.wf01.wt01.status
eg. root.ln.wf01.wt01.*
eg. *.wt01.*
eg. *
```
