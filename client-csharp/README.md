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
[English](./README.md) | [中文](./README_ZH.md)

# Apache IoTDB Client for C#

## Overview

This is the C# client of Apache IoTDB.

[Apache IoTDB](https://iotdb.apache.org) (Internet of Things Database) is a data management system for time series data, which can provide users specific services, such as, data collection, storage and analysis. Due to its light weight structure, high performance and usable features together with its seamless integration with the Hadoop and Spark ecology, IoTDB meets the requirements of massive dataset storage, high throughput data input, and complex data analysis in the industrial IoT field.

Apache IoTDB website: https://iotdb.apache.org
Apache IoTDB Github: https://github.com/apache/iotdb

## Installation

### Install from NuGet Package

We have prepared Nuget Package for C# users. Users can directly install the client through .NET CLI. [The link of our NuGet Package is here](https://www.nuget.org/packages/iotdb_client_csharp/0.12.0). Run the following command in the command line to complete installation

```sh
dotnet add package iotdb_client_csharp --version 0.12.0
```

## Prerequisites

    .NET SDK Version == 5.0 

## How to Use the Client (Quick Start)

Users can refer to the test code in [tests](https://github.com/eedalong/iotdb-client-csharp/tree/main/client/tests) to understand the usage mode of each interface.

## Developer environment requirements for iotdb-client-csharp

```
.NET SDK Version == 5.0
ApacheThrift >= 0.14.1
NLog >= 4.7.9
```

### OS

* Linux, Macos or other unix-like OS
* Windows+bash(WSL, cygwin, Git Bash)

### Command Line Tools
