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
# Chunk 分析工具

当您希望分析一个 tsfile 文件特定时间序列的数据时，可以使用此工具。

## 使用

```
# Unix/OS X
> tools/print-tsfile-specific-measurement.sh file_path timeseries_path

# Windows
> tools\print-tsfile-specific-measurement.bat file_path timeseries_path
```

file_path：tsfile 文件的绝对路径

timeseries_path：时间序列完整路径

## 示例

假设在 Linux 环境下，有一个 tsfile 文件(`/Users/Desktop/test.tsfile`)包含两条时间序列，分别为 `root.sg.d0.s0, root.sg.d0.s1` ，我们希望了解该数据文件特定时间序列（如 `root.sg.d0.s0`，该时间序列有两个 `Chunk` ，每个 `Chunk` 有三个数据点）的 Chunk 在文件中的数据分布情况，可以借助该工具的如下指令。

```
> tools/print-tsfile-specific-measurement.sh /Users/Desktop/test.tsfile root.sg.d0.s0
```

>输出结果示例
```
|--[Chunk]
			time, value: 1, 1
			time, value: 2, 2
			time, value: 3, 3
|--[Chunk]
			time, value: 4, 4
			time, value: 5, 5
			time, value: 6, 6
```