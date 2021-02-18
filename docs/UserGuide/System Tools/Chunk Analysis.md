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
# Chunk Analysis tool

You can use this tool when you want to analyze the chunk data of a specific time series in a tsfile file.

## Usage 

```
# Unix/OS X
> tools/print-tsfile-specific-measurement.sh file_path timeseries_path

# Windows
> tools\print-tsfile-specific-measurement.bat file_path timeseries_path
```

file_path：The absolute path of the tsfile file

timeseries_path：time series absolute path

## Example


Suppose that in a Linux environment, there is a tsfile file (`/Users/Desktop/test.tsfile`) containing two time series, namely `root.sg.d0.s0, root.sg.d0.s1`, we want to analyze chunk data distribution of the specific time series (such as `root.sg.d0.s0`, the time series has two `Chunk`, each `Chunk` has three data points)  in the file, then you can use the following instruction.
```
> tools/print-tsfile-specific-measurement.sh /Users/Desktop/test.tsfile root.sg.d0.s0
```

>Output Example
```
|--[Chunk]
			time, value: 1, 1
			time, value: 2, 2
			time, value: 3, 3
|--[Chunk]
			time, value: 4, 4
			time, value: 5, 5
			time, value: 6, 6
