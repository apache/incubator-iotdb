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

# 文件合并机制

## 设计原理

写入的文件既有乱序的也有顺序的，既有小文件也有大文件，并且在不同系统中有不同的最佳合并算法，

故 MergeManager 提供多种合并策略接口，并提供灵活的新策略接入方式

## 调用过程

- 每一次 merge 会在用户 client 调用"merge"命令或根据配置中的 mergeIntervalSec 定时进行
- merge 分为三个过程，包括选择文件(selector)，进行 merge ，以及 merge 过程中断后的恢复(recover)
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/75313978-6c64b000-5899-11ea-8565-40b012f9c8a2.png">

## 合并的例子

在 squeeze 合并策略下，当一系列的 seq 以及 unseq 文件进行合并，且并未超过时间及内存限制时，所有文件都将合并为一个命名为{时间戳}-{版本}-{merge次数+1}.tsfile.merge.squeeze的文件
​    
当超出了时间或内存限制，文件选择过程将被中断，当前已被选中的 seq 以及 unseq 文件则会进行如上合并形成一个文件

时间限制是指在选择文件过程中花费的时间不能超过一个给定值，而非对合并过程耗时的估计，目的是防止文件过多时选择文件花费太多时间

内存限制是指对已经被选择的文件在进行合并时消耗的最大内存进行估计，并使该估计值不会超过一个给定值，从而方式合并过程产生内存溢出。

恢复的时候有两种选择，一个是接着之前的进度继续进行，另一个是放弃之前的进度

## 相关代码

* org.apache.iotdb.db.engine.merge.BaseFileSelector

    文件选择过程的基类，规定了选择文件的基本框架，及在不同情况下计算文件内存消耗的方法，所有自定义的文件选择策略均需继承此类
    
* org.apache.iotdb.db.engine.merge.IRecoverMergeTask
  
    recover 过程的接口类，规定了 recoverMerge 接口，所有自定义的merge恢复策略均需继承此类

此外，每一个自定义的MergeTask均需继承Callable\<void\>接口，以保证可以被回调

* org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeContext

    Merge 过程中的公用上下文类

* org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager

    Merge 过程中的线程池类，管理了多个 merge task 的运行

* org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeResource

    Merge 过程中的资源类，负责管理 merge 过程中的 files,readers,writers,measurementSchemas,modifications 等资源

## inplace策略

### selector

在受限的内存和时间下，先依次选择 unseq 文件，每次直接根据 unseq 文件的时间范围选择与之重叠的 seq 文件

### merge

首先根据 storageGroupName 选出所有需要 merge 的 series ，然后针对 selector 中选出的每一个 seq 文件建立 chunkMetaHeap ,并根据配置中的 mergeChunkSubThreadNum 分多个子线程进行合并

## squeeze策略

### selector

在受限的内存和时间下，先依次选择 unseq 文件，每次根据 unseq 文件的时间范围选择与之重叠的seq文件，然后按次序重试每一个 seq 文件，尽可能在内存和时间受限的情况下多取一些 seq 文件

### merge

基本与 inplace 策略类似，首先根据 storageGroupName 选出所有需要 merge 的 series ，然后针对 selector 中选出的每一个 seq 文件建立 chunkMetaHeap ,并根据配置中的 mergeChunkSubThreadNum 分多个子线程进行合并

## merge中断后的恢复

merge 在系统突然关闭或者出现故障时，可能会被强行中断，此时系统会记录下被中断的 merge 并在下一次 StorageGroupProcessor 被创建时扫描 merge.log 文件，根据配置进行重新 merge，也就是 recover 过程

merge 会有以下几个状态，其中恢复过程均为优先放弃合并策略

### NONE
基本什么都没干
恢复时直接删除相应的 merge log，以等待下一次的手动或自动 merge

### MERGE_START
将要合并的文件以及 timeseries 已经被选出
恢复时删除对应的 merge 文件，清空选出的文件，对其他 merge 相关公用资源均做清空操作

### ALL_TS_MERGED
所有的 timeseries 已经被合并
恢复时直接进行 cleanUp ，并执行 merge 完成的回调操作

### MERGE_END
表面所有的文件已经被合并，此次merge已经完成
原则上不会在 merge log 中出现此状态