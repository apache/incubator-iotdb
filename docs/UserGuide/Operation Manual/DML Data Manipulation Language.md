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

# DML (Data Manipulation Language)

## INSERT
### Insert Real-time Data

IoTDB provides users with a variety of ways to insert real-time data, such as directly inputting [INSERT SQL statement](../Operation%20Manual/SQL%20Reference.md) in [Client/Shell tools](../Client/Command%20Line%20Interface.md), or using [Java JDBC](../Client/Programming%20-%20JDBC.md) to perform single or batch execution of [INSERT SQL statement](../Operation%20Manual/SQL%20Reference.md).

This section mainly introduces the use of [INSERT SQL statement](../Operation%20Manual/SQL%20Reference.md) for real-time data import in the scenario.

#### Use of INSERT Statements
The [INSERT SQL statement](../Operation%20Manual/SQL%20Reference.md) statement can be used to insert data into one or more specified timeseries that have been created. For each point of data inserted, it consists of a [timestamp](../Concept/Data%20Model%20and%20Terminology.md) and a sensor acquisition value (see [Data Type](../Concept/Data%20Type.md)).

In the scenario of this section, take two timeseries `root.ln.wf02.wt02.status` and `root.ln.wf02.wt02.hardware` as an example, and their data types are BOOLEAN and TEXT, respectively.

The sample code for single column data insertion is as follows:
```
IoTDB > insert into root.ln.wf02.wt02(timestamp,status) values(1,true)
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1, "v1")
```

The above example code inserts the long integer timestamp and the value "true" into the timeseries `root.ln.wf02.wt02.status` and inserts the long integer timestamp and the value "v1" into the timeseries `root.ln.wf02.wt02.hardware`. When the execution is successful, cost time is shown to indicate that the data insertion has been completed.

> Note: In IoTDB, TEXT type data can be represented by single and double quotation marks. The insertion statement above uses double quotation marks for TEXT type data. The following example will use single quotation marks for TEXT type data.

The INSERT statement can also support the insertion of multi-column data at the same time point.  The sample code of  inserting the values of the two timeseries at the same time point '2' is as follows:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, status, hardware) VALUES (2, false, 'v2')
```

After inserting the data, we can simply query the inserted data using the SELECT statement:

```
IoTDB > select * from root.ln.wf02 where time < 3
```

The result is shown below. From the query results, it can be seen that the insertion statements of single column and multi column data are performed correctly.

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51605021-c2ee1500-1f48-11e9-8f6b-ba9b48875a41.png"></center>

### Error Handling of INSERT Statements
If the user inserts data into a non-existent timeseries, for example, execute the following commands:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp, temperature) values(1,"v1")
```

Because `root.ln.wf02.wt02. temperature` does not exist, the system will return the following ERROR information:

```
Msg: The resultDataType or encoding or compression of the last node temperature is conflicting in the storage group root.ln
```
If the data type inserted by the user is inconsistent with the corresponding data type of the timeseries, for example, execute the following command:

```
IoTDB > insert into root.ln.wf02.wt02(timestamp,hardware) values(1,100)
```
The system will return the following ERROR information:

```
error: The TEXT data type should be covered by " or '
```

## SELECT

### Time Slice Query

This chapter mainly introduces the relevant examples of time slice query using IoTDB SELECT statements. Detailed SQL syntax and usage specifications can be found in [SQL Documentation](../Operation%20Manual/SQL%20Reference.md). You can also use the [Java JDBC](../Client/Programming%20-%20JDBC.md) standard interface to execute related queries.

#### Select a Column of Data Based on a Time Interval

The SQL statement is:

```
select temperature from root.ln.wf01.wt01 where time < 2017-11-01T00:08:00.000
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the temperature sensor (temperature). The SQL statement requires that all temperature sensor values before the time point of "2017-11-01T00:08:00.000" be selected.

The execution result of this SQL statement is as follows:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280074-da1c0a00-a7e9-11e9-8eb8-3809428043a8.png"></center>

#### Select Multiple Columns of Data Based on a Time Interval

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000;
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires that the status and temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The execution result of this SQL statement is as follows:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280328-40a12800-a7ea-11e9-85b9-3b8db67673a3.png"></center>

#### Select Multiple Columns of Data for the Same Device According to Multiple Time Intervals

IoTDB supports specifying multiple time interval conditions in a query. Users can combine time interval conditions at will according to their needs. For example, the SQL statement is:

```
select status,temperature from root.ln.wf01.wt01 where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature"; the statement specifies two different time intervals, namely "2017-11-01T00:05:00.000 to 2017-11-01T00:12:00.000" and "2017-11-01T16:35:00.000 to 2017-11-01T16:37:00.000". The SQL statement requires that the values of selected timeseries satisfying any time interval be selected.

The execution result of this SQL statement is as follows:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/23614968/61280449-780fd480-a7ea-11e9-8ed0-70fa9dfda80f.png"></center>


#### Choose Multiple Columns of Data for Different Devices According to Multiple Time Intervals

The system supports the selection of data in any column in a query, i.e., the selected columns can come from different devices. For example, the SQL statement is:

```
select wf01.wt01.status,wf02.wt02.hardware from root.ln where (time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000) or (time >= 2017-11-01T16:35:00.000 and time <= 2017-11-01T16:37:00.000);
```
which means:

The selected timeseries are "the power supply status of ln group wf01 plant wt01 device" and "the hardware version of ln group wf02 plant wt02 device"; the statement specifies two different time intervals, namely "2017-11-01T00:05:00.000 to 2017-11-01T00:12:00.000" and "2017-11-01T16:35:00.000 to 2017-11-01T16:37:00.000". The SQL statement requires that the values of selected timeseries satisfying any time interval be selected.

The execution result of this SQL statement is as follows:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577450-dcfe0800-1ef4-11e9-9399-4ba2b2b7fb73.jpg"></center>


#### Order By Time Query
IoTDB supports the 'order by time' statement since 0.11, it's used to display results in descending order by time.
For example, the SQL statement is:

```sql
select * from root.ln where time > 1 order by time desc limit 10;
```

### Aggregate Query
This section mainly introduces the related examples of aggregate query.

#### Count Points

```
select count(status) from root.ln.wf01.wt01;
```

| count(root.ln.wf01.wt01.status) |
| -------------- |
| 4              |


##### Count Points By Level

Level could be defined to show count the number of points of each node at the given level in current Metadata Tree.

This could be used to query the number of points under each device.

The SQL statement is:

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

### Down-Frequency Aggregate Query

This section mainly introduces the related examples of down-frequency aggregation query, 
using the [GROUP BY clause](../Operation%20Manual/SQL%20Reference.md), 
which is used to partition the result set according to the user's given partitioning conditions and aggregate the partitioned result set. 
IoTDB supports partitioning result sets according to time interval and customized sliding step which should not be smaller than the time interval and defaults to equal the time interval if not set. And by default results are sorted by time in ascending order. 
You can also use the [Java JDBC](../Client/Programming%20-%20Native%20API.md) standard interface to execute related queries.

The GROUP BY statement provides users with three types of specified parameters:

* Parameter 1: The display window on the time axis
* Parameter 2: Time interval for dividing the time axis(should be positive)
* Parameter 3: Time sliding step (optional and should not be smaller than the time interval and defaults to equal the time interval if not set)

The actual meanings of the three types of parameters are shown in Figure 5.2 below. 
Among them, the parameter 3 is optional. 
Next we will give three typical examples of frequency reduction aggregation: 
parameter 3 not specified, 
parameter 3 specified, 
and value filtering conditions specified.

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69109512-f808bc80-0ab2-11ea-9e4d-b2b2f58fb474.png">
    </center>

**Figure 5.2 The actual meanings of the three types of parameters**

#### Down-Frequency Aggregate Query without Specifying the Sliding Step Length

The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d);
```
which means:

Since the user does not specify the sliding step length, the GROUP BY statement will by default set the sliding step same as the time interval which is `1d`.

The fist parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00).

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (1d) as time interval and startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [0,1d), [1d, 2d), [2d, 3d), etc.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-11-01T00:00:00, 2017-11-07 T23:00:00]), and map these data to the previously segmented time axis (in this case there are mapped data in every 1-day period from 2017-11-01T00:00:00 to 2017-11-07T23:00:00:00).

Since there is data for each time period in the result range to be displayed, the execution result of the SQL statement is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116068-eed51b00-0ac5-11ea-9731-b5a45c5cd224.png"></center>

#### Down-Frequency Aggregate Query Specifying the Sliding Step Length

The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d);
```

which means:

Since the user specifies the sliding step parameter as 1d, the GROUP BY statement will move the time interval `1 day` long instead of `3 hours` as default.

That means we want to fetch all the data of 00:00:00 to 02:59:59 every day from 2017-11-01 to 2017-11-07.

The first parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00).

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (3h) as time interval and the startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-11-01T00:00:00, 2017-11-01T03:00:00), [2017-11-02T00:00:00, 2017-11-02T03:00:00), [2017-11-03T00:00:00, 2017-11-03T03:00:00), etc.

The third parameter of the GROUP BY statement above is the sliding step for each time interval moving.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of [2017-11-01T00:00:00, 2017-11-07T23:00:00]), and map these data to the previously segmented time axis (in this case there are mapped data in every 3-hour period for each day from 2017-11-01T00:00:00 to 2017-11-07T23:00:00:00).

Since there is data for each time period in the result range to be displayed, the execution result of the SQL statement is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116083-f85e8300-0ac5-11ea-84f1-59d934eee96e.png"></center>

#### Down-Frequency Aggregate Query Specifying the value Filtering Conditions

The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 where time > 2017-11-01T01:00:00 and temperature > 20 group by([2017-11-01T00:00:00, 2017-11-07T23:00:00), 3h, 1d);
```
which means:

Since the user specifies the sliding step parameter as 1d, the GROUP BY statement will move the time interval `1 day` long instead of `3 hours` as default.

The first parameter of the GROUP BY statement above is the display window parameter, which determines the final display range is [2017-11-01T00:00:00, 2017-11-07T23:00:00).

The second parameter of the GROUP BY statement above is the time interval for dividing the time axis. Taking this parameter (3h) as time interval and the startTime of the display window as the dividing origin, the time axis is divided into several continuous intervals, which are [2017-11-01T00:00:00, 2017-11-01T03:00:00), [2017-11-02T00:00:00, 2017-11-02T03:00:00), [2017-11-03T00:00:00, 2017-11-03T03:00:00), etc.

The third parameter of the GROUP BY statement above is the sliding step for each time interval moving.

Then the system will use the time and value filtering condition in the WHERE clause and the first parameter of the GROUP BY statement as the data filtering condition to obtain the data satisfying the filtering condition (which in this case is the data in the range of (2017-11-01T01:00:00, 2017-11-07T23:00:00] and satisfying root.ln.wf01.wt01.temperature > 20), and map these data to the previously segmented time axis (in this case there are mapped data in every 3-hour period for each day from 2017-11-01T00:00:00 to 2017-11-07T23:00:00).

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116088-001e2780-0ac6-11ea-9a01-dc45271d1dad.png"></center>

#### Left Open And Right Close Range

The SQL statement is:

```
select count(status) from root.ln.wf01.wt01 group by((5, 40], 5ms);
```

In this sql, the time interval is left open and right close, so we won't include the value of timestamp 5 and instead we will include the value of timestamp 40.

We will get the result like following:

| Time   | count(root.ln.wf01.wt01.status) |
| ------ | ------------------------------- |
| 10     | 1                               |
| 15     | 2                               |
| 20     | 3                               |
| 25     | 4                               |
| 30     | 4                               |
| 35     | 3                               |
| 40     | 5                               |


#### Down-Frequency Aggregate Query with Level Clause

Level could be defined to show count the number of points of each node at the given level in current Metadata Tree.

This could be used to query the number of points under each device.

The SQL statement is:

Get down-frequency aggregate query by level.

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

Down-frequency aggregate query with sliding step and by level.

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

#### Down-Frequency Aggregate Query with Fill Clause

In group by fill, sliding step is not supported in group by clause

Now, only last_value aggregation function is supported in group by fill.

Linear fill is not supported in group by fill.


##### Difference Between PREVIOUSUNTILLAST And PREVIOUS

* PREVIOUS will fill any null value as long as there exist value is not null before it.
* PREVIOUSUNTILLAST won't fill the result whose time is after the last time of that time series.

The SQL statement is:

```
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([8, 39), 5m) FILL (int32[PREVIOUSUNTILLAST])
SELECT last_value(temperature) FROM root.ln.wf01.wt01 GROUP BY([8, 39), 5m) FILL (int32[PREVIOUSUNTILLAST, 3m])
```
which means:

using PREVIOUSUNTILLAST Fill way to fill the origin down-frequency aggregate query result.



The path after SELECT in GROUP BY statement must be aggregate function, otherwise the system will give the corresponding error prompt, as shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/16079446/69116099-0b715300-0ac6-11ea-8074-84e04797b8c7.png"></center>

### Last point Query

In scenarios when IoT devices updates data in a fast manner, users are more interested in the most recent point of IoT devices.
 
The Last point query is to return the most recent data point of the given timeseries in a three column format.

The SQL statement is defined as:

```
select last <Path> [COMMA <Path>]* from < PrefixPath > [COMMA < PrefixPath >]* <WhereClause>
```

which means: Query and return the last data points of timeseries prefixPath.path.

Only time filter is supported in \<WhereClause\>. Any value filters given in the \<WhereClause\> take no effects.

The result will be returned in a three column table format.

```
| Time | Path | Value |
```

Example 1: get the last point of root.ln.wf01.wt01.speed:

```
> select last speed from root.ln.wf01.wt01

| Time | Path                    | Value |
| ---  | ----------------------- | ----- |
|  5   | root.ln.wf01.wt01.speed | 100   |
```

Example 2: get the last speed, status and temperature points of root.ln.wf01.wt01,
whose timestamp in range [5, 10).

```
> select last speed, status, temperature from root.ln.wf01.wt01 where time >= 5 and time < 10

| Time | Path                         | Value |
| ---  | ---------------------------- | ----- |
|  5   | root.ln.wf01.wt01.speed      | 100   |
|  7   | root.ln.wf01.wt01.status     | true  |
|  9   | root.ln.wf01.wt01.temperature| 35.7  |
```


### Automated Fill

In the actual use of IoTDB, when doing the query operation of timeseries, situations where the value is null at some time points may appear, which will obstruct the further analysis by users. In order to better reflect the degree of data change, users expect missing values to be automatically filled. Therefore, the IoTDB system introduces the function of Automated Fill.

Automated fill function refers to filling empty values according to the user's specified method and effective time range when performing timeseries queries for single or multiple columns. If the queried point's value is not null, the fill function will not work.

> Note: In the current version, IoTDB provides users with two methods: Previous and Linear. The previous method fills blanks with previous value. The linear method fills blanks through linear fitting. And the fill function can only be used when performing point-in-time queries.

#### Fill Function

* Previous Function

When the value of the queried timestamp is null, the value of the previous timestamp is used to fill the blank. The formalized previous method is as follows (see Section 7.1.3.6 for detailed syntax):

```
select <path> from <prefixPath> where time = <T> fill(<data_type>[previous, <before_range>], …)
```

Detailed descriptions of all parameters are given in Table 3-4.

<center>**Table 3-4 Previous fill paramter list**

|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|data\_type|the type of data used by the fill method. Optional values are int32, int64, float, double, boolean, text; optional field|
|before\_range|represents the valid time range of the previous method. The previous method works when there are values in the [T-before\_range, T] range. When before\_range is not specified, before\_range takes the default value default\_fill\_interval; -1 represents infinit; optional field|
</center>

Here we give an example of filling null values using the previous method. The SQL statement is as follows:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) 
```
which means:

Because the timeseries root.sgcc.wf03.wt01.temperature is null at 2017-11-01T16:37:50.000, the system uses the previous timestamp 2017-11-01T16:37:00.000 (and the timestamp is in the [2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000] time range) for fill and display.

On the [sample data](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), the execution result of this statement is shown below:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577616-67df0280-1ef5-11e9-9dff-2eb8342074eb.jpg"></center>

It is worth noting that if there is no value in the specified valid time range, the system will not fill the null value, as shown below:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577679-9f4daf00-1ef5-11e9-8d8b-06a58de6efc1.jpg"></center>

* Linear Method

When the value of the queried timestamp is null, the value of the previous and the next timestamp is used to fill the blank. The formalized linear method is as follows:

```
select <path> from <prefixPath> where time = <T> fill(<data_type>[linear, <before_range>, <after_range>]…)
```
Detailed descriptions of all parameters are given in Table 3-5.

<center>**Table 3-5 Linear fill paramter list**

|Parameter name (case insensitive)|Interpretation|
|:---|:---|
|path, prefixPath|query path; mandatory field|
|T|query timestamp (only one can be specified); mandatory field|
|data_type|the type of data used by the fill method. Optional values are int32, int64, float, double, boolean, text; optional field|
|before\_range, after\_range|represents the valid time range of the linear method. The previous method works when there are values in the [T-before\_range, T+after\_range] range. When before\_range and after\_range are not explicitly specified, default\_fill\_interval is used. -1 represents infinity; optional field|
</center>

**Note** if the timeseries has a valid value at query timestamp T, this value will be used as the linear fill value.
Otherwise, if there is no valid fill value in either range [T-before_range，T] or [T, T + after_range], linear fill method will return null.

Here we give an example of filling null values using the linear method. The SQL statement is as follows:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float [linear, 1m, 1m])
```
which means:

Because the timeseries root.sgcc.wf03.wt01.temperature is null at 2017-11-01T16:37:50.000, the system uses the previous timestamp 2017-11-01T16:37:00.000 (and the timestamp is in the [2017-11-01T16:36:50.000, 2017-11-01T16:37:50.000] time range) and its value 21.927326, the next timestamp 2017-11-01T16:38:00.000 (and the timestamp is in the [2017-11-01T16:37:50.000, 2017-11-01T16:38:50.000] time range) and its value 25.311783 to perform linear fitting calculation: 21.927326 + (25.311783-21.927326)/60s * 50s = 24.747707

On the [sample data](https://github.com/thulab/iotdb/files/4438687/OtherMaterial-Sample.Data.txt), the execution result of this statement is shown below:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577727-d4f29800-1ef5-11e9-8ff3-3bb519da3993.jpg"></center>

#### Correspondence between Data Type and Fill Method

Data types and the supported fill methods are shown in Table 3-6.

<center>**Table 3-6 Data types and the supported fill methods**

|Data Type|Supported Fill Methods|
|:---|:---|
|boolean|previous|
|int32|previous, linear|
|int64|previous, linear|
|float|previous, linear|
|double|previous, linear|
|text|previous|
</center>

It is worth noting that IoTDB will give error prompts for fill methods that are not supported by data types, as shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577741-e340b400-1ef5-11e9-9238-a4eaf498ab84.jpg"></center>

When the fill method is not specified, each data type bears its own default fill methods and parameters. The corresponding relationship is shown in Table 3-7.

<center>**Table 3-7 Default fill methods and parameters for various data types**

|Data Type|Default Fill Methods and Parameters|
|:---|:---|
|boolean|previous, 600000|
|int32|previous, 600000|
|int64|previous, 600000|
|float|previous, 600000|
|double|previous, 600000|
|text|previous, 600000|
</center>

> Note: In version 0.7.0, at least one fill method should be specified in the Fill statement.

### Row and Column Control over Query Results

IoTDB provides [LIMIT/SLIMIT](../Operation%20Manual/SQL%20Reference.md) clause and [OFFSET/SOFFSET](../Operation%20Manual/SQL%20Reference.md) 
clause in order to make users have more control over query results. 
The use of LIMIT and SLIMIT clauses allows users to control the number of rows and columns of query results, 
and the use of OFFSET and SOFSET clauses allows users to set the starting position of the results for display.

Note that the LIMIT and OFFSET are not supported in group by query.

This chapter mainly introduces related examples of row and column control of query results. You can also use the [Java JDBC](../Client/Programming%20-%20JDBC.md) standard interface to execute queries.

#### Row Control over Query Results

By using LIMIT and OFFSET clauses, users can control the query results in a row-related manner. We will demonstrate how to use LIMIT and OFFSET clauses through the following examples.

* Example 1: basic LIMIT clause

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 limit 10
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires the first 10 rows of the query result be returned.

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577752-efc50c80-1ef5-11e9-9071-da2bbd8b9bdd.jpg"></center>

* Example 2: LIMIT clause with OFFSET

The SQL statement is:

```
select status, temperature from root.ln.wf01.wt01 limit 5 offset 3
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires rows 3 to 7 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577773-08352700-1ef6-11e9-883f-8d353bef2bdc.jpg"></center>

* Example 3: LIMIT clause combined with WHERE clause

The SQL statement is:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time< 2017-11-01T00:12:00.000 limit 2 offset 3
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is "status" and "temperature". The SQL statement requires rows 3 to 4 of  the status and temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be returned (with the first row numbered as row 0).

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577789-15521600-1ef6-11e9-86ca-d7b2c947367f.jpg"></center>

* Example 4: LIMIT clause combined with GROUP BY clause

The SQL statement is:

```
select count(status), max_value(temperature) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d) limit 5 offset 3
```
which means:

The SQL statement clause requires rows 3 to 7 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577796-1e42e780-1ef6-11e9-8987-be443000a77e.jpg"></center>

It is worth noting that because the current FILL clause can only fill in the missing value of timeseries at a certain time point, that is to say, the execution result of FILL clause is exactly one line, so LIMIT and OFFSET are not expected to be used in combination with FILL clause, otherwise errors will be prompted. For example, executing the following SQL statement:

```
select temperature from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) limit 10
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61517266-6e2fe080-aa39-11e9-8015-154a8e8ace30.png"></center>

#### Column Control over Query Results

By using SLIMIT and SOFFSET clauses, users can control the query results in a column-related manner. We will demonstrate how to use SLIMIT and SOFFSET clauses through the following examples.

* Example 1: basic SLIMIT clause

The SQL statement is:

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the first column under this device, i.e., the power supply status. The SQL statement requires the status sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577813-30bd2100-1ef6-11e9-94ef-dbeb450cf319.jpg"></center>

* Example 2: SLIMIT clause with SOFFSET

The SQL statement is:

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature. The SQL statement requires the temperature sensor values between the time point of "2017-11-01T00:05:00.000" and "2017-11-01T00:12:00.000" be selected.

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577827-39adf280-1ef6-11e9-81b5-876769607cd2.jpg"></center>

* Example 3: SLIMIT clause combined with GROUP BY clause

The SQL statement is:

```
select max_value(*) from root.ln.wf01.wt01 group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00),1d) slimit 1 soffset 1
```

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577840-44688780-1ef6-11e9-8abc-04ae78efa85b.jpg"></center>

* Example 4: SLIMIT clause combined with FILL clause

The SQL statement is:

```
select * from root.sgcc.wf03.wt01 where time = 2017-11-01T16:37:50.000 fill(float[previous, 1m]) slimit 1 soffset 1
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is the second column under this device, i.e., the temperature.

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577855-4d595900-1ef6-11e9-8541-a4accd714b75.jpg"></center>

It is worth noting that SLIMIT clause is expected to be used in conjunction with star path or prefix path, and the system will prompt errors when SLIMIT clause is used in conjunction with complete path query. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577867-577b5780-1ef6-11e9-978c-e02c1294bcc5.jpg"></center>

#### Row and Column Control over Query Results

In addition to row or column control over query results, IoTDB allows users to control both rows and columns of query results. Here is a complete example with both LIMIT clauses and SLIMIT clauses.

The SQL statement is:

```
select * from root.ln.wf01.wt01 limit 10 offset 100 slimit 2 soffset 0
```
which means:

The selected device is ln group wf01 plant wt01 device; the selected timeseries is columns 0 to 1 under this device (with the first column numbered as column 0). The SQL statement clause requires rows 100 to 109 of the query result be returned (with the first row numbered as row 0).

The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51577879-64984680-1ef6-11e9-9d7b-57dd60fab60e.jpg"></center>

### Use Alias

Since the unique data model of IoTDB, lots of additional information like device will be carried before each sensor. Sometimes, we want to query just one specific device, then these prefix information show frequently will be redundant in this situation, influencing the analysis of result set. At this time, we can use `AS` function provided by IoTDB, assign an alias to time series selected in query.  

For example：

```
select s1 as temperature, s2 as speed from root.ln.wf01.wt01;
```

The result set will be like：

| Time | temperature | speed |
| ---- | ----------- | ----- |
| ...  | ...         | ...   |

#### Other ResultSet Format

In addition, IoTDB supports two another resultset format: 'align by device' and 'disable align'.

The 'align by device' indicates that the deviceId is considered as a column. Therefore, there are totally limited columns in the dataset. 

The SQL statement is:

```
select s1,s2 from root.sg1.* ALIGN BY DEVICE
```

For more syntax description, please read SQL REFERENCE.

The 'disable align' indicaes that there are 3 columns for each time series in the resultset. For more syntax description, please read SQL REFERENCE.

####  Error Handling

When the parameter N/SN of LIMIT/SLIMIT exceeds the size of the result set, IoTDB will return all the results as expected. For example, the query result of the original SQL statement consists of six rows, and we select the first 100 rows through the LIMIT clause:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 100
```
The result is shown below:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578187-ad9cca80-1ef7-11e9-897a-83e66a0f3d94.jpg"></center>

When the parameter N/SN of LIMIT/SLIMIT clause exceeds the allowable maximum value (N/SN is of type int32), the system will prompt errors. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 1234567890123456789
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61517469-e696a180-aa39-11e9-8ca5-42ea991d520e.png"></center>

When the parameter N/SN of LIMIT/SLIMIT clause is not a positive intege, the system will prompt errors. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 13.1
```

The SQL statement will not be executed and the corresponding error prompt is given as follows:

<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/61518094-68d39580-aa3b-11e9-993c-fc73c27540f7.png"></center>

When the parameter OFFSET of LIMIT clause exceeds the size of the result set, IoTDB will return an empty result set. For example, executing the following SQL statement:

```
select status,temperature from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 limit 2 offset 6
```
The result is shown below:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578227-c60ce500-1ef7-11e9-98eb-175beb8d4086.jpg"></center>

When the parameter SOFFSET of SLIMIT clause is not smaller than the number of available timeseries, the system will prompt errors. For example, executing the following SQL statement:

```
select * from root.ln.wf01.wt01 where time > 2017-11-01T00:05:00.000 and time < 2017-11-01T00:12:00.000 slimit 1 soffset 2
```
The SQL statement will not be executed and the corresponding error prompt is given as follows:
<center><img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51578237-cd33f300-1ef7-11e9-9aef-2a717c56ab54.jpg"></center>



## DELETE

Users can delete data that meet the deletion condition in the specified timeseries by using the [DELETE statement](../Operation%20Manual/SQL%20Reference.md). When deleting data, users can select one or more timeseries paths, prefix paths, or paths with star  to delete data within a certain time interval.

In a JAVA programming environment, you can use the [Java JDBC](../Client/Programming%20-%20JDBC.md) to execute single or batch UPDATE statements.

### Delete Single Timeseries
Taking ln Group as an example, there exists such a usage scenario:

The wf02 plant's wt02 device has many segments of errors in its power supply status before 2017-11-01 16:26:00, and the data cannot be analyzed correctly. The erroneous data affected the correlation analysis with other devices. At this point, the data before this time point needs to be deleted. The SQL statement for this operation is

```
delete from root.ln.wf02.wt02.status where time<=2017-11-01T16:26:00;
```

In case we hope to merely delete the data before 2017-11-01 16:26:00 in the year of 2017, The SQL statement is:
```
delete from root.ln.wf02.wt02.status where time>=2017-01-01T00:00:00 and time<=2017-11-01T16:26:00;
```

IoTDB supports to delete a range of timeseries points. Users can write SQL expressions as follows to specify the delete interval:

```
delete from root.ln.wf02.wt02.status where time < 10
delete from root.ln.wf02.wt02.status where time <= 10
delete from root.ln.wf02.wt02.status where time < 20 and time > 10
delete from root.ln.wf02.wt02.status where time <= 20 and time >= 10
delete from root.ln.wf02.wt02.status where time > 20
delete from root.ln.wf02.wt02.status where time >= 20
delete from root.ln.wf02.wt02.status where time = 20
```

Please pay attention that multiple intervals connected by "OR" expression are not supported in delete statement:

```
delete from root.ln.wf02.wt02.status where time > 4 or time < 0
Msg: 303: Check metadata error: For delete statement, where clause can only contain atomic
expressions like : time > XXX, time <= XXX, or two atomic expressions connected by 'AND'
```


### Delete Multiple Timeseries
When both the power supply status and hardware version of the ln group wf02 plant wt02 device before 2017-11-01 16:26:00 need to be deleted, [the prefix path with broader meaning or the path with star](../Concept/Data%20Model%20and%20Terminology.md) can be used to delete the data. The SQL statement for this operation is:

```
delete from root.ln.wf02.wt02 where time <= 2017-11-01T16:26:00;
```
or

```
delete from root.ln.wf02.wt02.* where time <= 2017-11-01T16:26:00;
```
It should be noted that when the deleted path does not exist, IoTDB will give the corresponding error prompt as shown below:

```
IoTDB> delete from root.ln.wf03.wt02.status where time < now()
Msg: TimeSeries does not exist and its data cannot be deleted
```

## Delete Time Partition (experimental)
You may delete all data in a time partition of a storage group using the following grammar:

```
DELETE PARTITION root.ln 0,1,2
```

The `0,1,2` above is the id of the partition that is to be deleted, you can find it from the IoTDB
data folders or convert a timestamp manually to an id using `timestamp / partitionInterval
` (flooring), and the `partitionInterval` should be in your config (if time-partitioning is
supported in your version).

Please notice that this function is experimental and mainly for development, please use it with
extreme care.
