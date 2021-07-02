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

# File merge mechanism

## Design principle

The written files are both out of order and in order, and there are both small files and large files, and there are different optimal merge algorithms in different systems. Therefore, MergeManager provides multiple merge policy interfaces and provides flexible new policy interfaces.  Entry method

## Calling procedure

- Each merge will be called when the user client calls the "merge" command or according to the mergeIntervalSec in the configuration
- The merge is divided into three processes, including selecting a file (selector), performing a merge, and recovering after the merge process is interrupted (recover)
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/24886743/75313978-6c64b000-5899-11ea-8565-40b012f9c8a2.png">

## Merged example

Under the squeeze merge strategy, when a series of seq and unseq files are merged and the time and memory limits are not exceeded, all files will be merged into one named {timestamp}-{version}-{merge times + 1  } .tsfile.merge.squeeze
​    
When the time or memory limit is exceeded, the file selection process will be interrupted, and the currently selected seq and unseq files will be merged as above to form a file

The time limit is that the time spent in the process of selecting files cannot exceed a given value, not an estimate of the time taken by the merge process, the purpose is to prevent the selection of files from taking too much time when there are too many files

Memory limit refers to the estimation of the maximum memory consumed by the selected files when merging, and making the estimated value not exceed a given value, so that the process of merging generates a memory overflow.

There are two options when recovering, one is to continue the previous progress, and the other is to give up the previous progress

## Related code

* org.apache.iotdb.db.engine.merge.BaseFileSelector

    The base class for the file selection process, which specifies the basic framework for selecting files and methods for calculating file memory consumption in different situations. All custom file selection strategies need to inherit this class
    
* org.apache.iotdb.db.engine.merge.IRecoverMergeTask
  
    The interface class of the recover process, which specifies the recoverMerge interface. All custom merge recovery strategies must inherit this class.

In addition, each custom MergeTask needs to inherit the Callable \<void\> interface to ensure that it can be called back

* org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeContext

    Common context classes in the Merge process

* org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeManager

    The thread pool class in the Merge process, which manages the operation of multiple merge tasks

* org.apache.iotdb.db.engine.compaction.cross.inplace.manage.MergeResource

    Resource class in the Merge process, responsible for managing files, readers, writers, measurement Schemas, modifications, and other resources during the merge process

## inplace strategy

### selector

Under limited memory and time, first select the unseq file in turn, Select the SEQ files that overlap each time directly based on the unseq file's time range.
### merge

First select all series that need to be merged according to storageGroupName, then create a chunkMetaHeap for each seq file selected in the selector, and merge them into multiple sub-threads according to the mergeChunkSubThreadNum in the configuration.

## squeeze strategy

### selector

Under the limited memory and time, first select the unseq file in turn, each time select the seq file that overlaps with the time range of the unseq file, and then retry each seq file in order.  Take more seq files under circumstances

### merge

Basically similar to the inplace strategy, first select all series that need to be merged according to storageGroupName, then create a chunkMetaHeap for each seq file selected in the selector, and merge into multiple child threads according to the mergeChunkSubThreadNum in the configuration to merge

## Recoverying after merge interruption

The merge may be forcibly interrupted when the system shuts down or fails suddenly. At this time, the system records the interrupted merge and scans the merge.log file when the next StorageGroupProcessor is created, and re-merges according to the configuration.  There are the following states, among which the recovery process is to give up the merge strategy first

### NONE
Basically did nothing, delete the corresponding merge log directly during recovery, and wait for the next manual or automatic merge

### MERGE_START
When the files to be merged and the TimeSeries have been selected, delete the corresponding merge file, empty the selected file, and empty the other merge related public resources.
### ALL_TS_MERGED
All timeseries have been merged
Perform cleanUp directly on recovery and execute the callback operation completed by merge

### MERGE_END
All the files on the surface have been merged, this time the merge has been completed
This state does not appear in the merge log in principle
