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

# Metadata Management

Metadata of IoTDB is managed by MManger, including:

* MTree
* Log management of metadata
* Tag/Attribute management


## MManager

* Maintain an inverted index for tag: `Map<String, Map<String, Set<LeafMNode>>> tagIndex`

	> tag key -> tag value -> timeseries LeafMNode

In the process of initializing, MManager will replay the mlog to load the metadata into memory. There are seven types of operation log:
> At the beginning of each operation, it will try to obatin the write lock of MManager, and release it after operation.

* Create Timeseries
    * check if the storage group exists, if not and the auto create is enable, create it.
    * create a leafMNode in the MTree with alias
	* If dynamic parameters are turned on, check the memory is satisfied or not
	* if not restart
	    * persist tags/attributes into tlog, and return the offset
		* set the offset of the leafMNode
		* persist the log into mlog
	* if restart
		* read tlog using offset in mlog, rebuilding the tag inverted index

* Delete Timeseries
    * obtain fullPath list of timeseries satisfying the prefix path
    * iterate the fullPath list, and delete them in MTree
		* before deleting, we need to obtain the parent node's write lock
		* if succeed
			* delete the LeafMNode
			* read tlog using offset in the LeafMNode, update tag inverted index
			* if the storage group becomes empty after deleting, record its name
		* if failed
			* return the full path of failed timeseries
	* if not restart
	   * delete the recorded empty storage group
		* persist log into mlog
		* currently, we won't delete the tag/attribute info of that timeseries in tlog
	

* Set Storage Group
    * add StorageGroupMNode in MTree
	* If dynamic parameters are turned on, check the memory is satisfied or not
	* if not restart, persist log into mlog

* Delete Storage Group
    * delete the StorageGroupMNode in MTree, and return all the LeafMNode in that storage group
		* While deleting StorageGroupMNode, we need to obtain the write lock of that StorageGroupMNode
		* if succeed
			* delete that StorageGroupMNode
		* if failed
			* return the failed storage group name
	* iterate the returned LeafMNode list, reading the tlog using the offset in LeafMNode, and then update tag inverted index
	* if not restart, persist log into mlog

* Set TTL
    * obtain the corresponding StorageGroupMNode, modify the TTL property in it.
	* if not restart, persist log into mlog

* Change the offset of Timeseries
	* modify the offset of the timeseries's LeafMNode

* Change the alias of Timeseries
	* modify the alias of the timeseries's LeafMNode and update the aliasMap in its parent node.


In addition to these seven operation that are needed to be logged, there are another six alter operation to tag/attribute info of timeseries.
 
Same as above, at the beginning of each operation, it will try to obatin the write lock of MManager, and release it after operation.

* Rename Tag/Attribute
	* obtain the LeafMNode of that timeseries
	* read tag and attribute information through the offset in LeafMNode
	* if the new name has existed, then throw exception
	* otherwise:
		* if the old name does not exist, then throw exception
		* otherwise, replace the old one with the new name, and persist it into tlog
		* if the old one is tag, we still need to update tag inverted index

* reset tag/attribute value
	* obtain the LeafMNode of that timeseries
	* read tag and attribute information through the offset in LeafMNode
	* if the key does not exist, then throw exception
	* if the reset one is tag, we still need to update tag inverted index
	* persist the updated tag and attribute information into tlog

* drop existing tag/attribute
	* obtain the LeafMNode of that timeseries
	* read tag and attribute information through the offset in LeafMNode
	* iterate the tags or attributes needed to be dropped, if it doesn't exist, then skip it, otherwise, drop it
	* if the drooped one is tag, we still need to update tag inverted index
	* persist the updated tag and attribute information into tlog

* add new tags
	* obtain the LeafMNode of that timeseries
	* read tag information through the offset in LeafMNode
	* iterate the tags needed to be added, if it has existed, then throw exception, otherwise, add it
	* persist the new tag information into tlog
	* update tag inverted index

* add new attributes
    * obtain the LeafMNode of that timeseries
	* read attribute information through the offset in LeafMNode
	* iterate the attributes needed to be added, if it has existed, then throw exception, otherwise, add it
	* persist the new attribute information into tlog

* upsert alias/tags/attributes
	* obtain the LeafMNode of that timeseries
	* change the alias of the timeseries's LeafMNode and update the aliasMap in its parent node if exists
	* persist the updated alias into mlog
	* read tag information through the offset in LeafMNode
	* iterate the tags and attributes needed to be upserted, if it has existed，use the new value to update it, otherwise, add it
	* persist the updated tags and attributes information into tlog
	* if the upserted ones include tag, we still need to update tag inverted index



## MTree

* org.apache.iotdb.db.metadata.MTree

There three types of nodes in MTree: StorageGroupMNode、InternalMNode(Non-leaf node)、LeafMNode(leaf node), they all extend to MNode.

Each InternalMNode has a read-write lock. When querying metadata information, you need to obtain a read lock for each InternalMNode on the path. When modifying metadata information, if you modify the LeafMNode, you need to obtain the write lock of its parent node. If you modify a non-leaf node, only need to obtain its own write lock. If the InternalMNode is located in the device layer, it also contains a `Map <String, MNode> aliasChildren`, which is used to store alias information.

StorageGroupMNode extends to InternalMNode, containing metadata information for storage groups, such as TTL.

LeafMNode contains the schema information of the corresponding time series, its alias(if it doesn't have, it is null) and the offset of the time series tag/attribute information in the tlog file(if there is no tag/attribute, it is -1)

example：

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/19167280/73625246-fc3e8200-467e-11ea-8815-67b9c4ab716e.png">

The metadata management of IoTDB takes the form of a directory tree, the penultimate layer is the device layer, and the last layer is the sensor layer.

The root node exists by default. Creating storage groups, deleting storage groups, creating time series and deleting time series are all operations on the nodes of the tree.

* create storage group（root.a.b.sg）
	* create InternalMNode(a.b) for current storage group
	* make sure this prefix path doesn't contain any other storage group(storage group nesting is not allowed)
	* check if the storage group has existed
	* create StorageGroupMNode(sg)

* create timeseries（root.a.b.sg.d.s1）
	* walk the path and make sure the storage group has been created
	* find the node in the penultimate layer(device layer), check if it already has the child leaf node with same name
	* create LeafMNode, and store the alias in LeafMNode if it has
	* If it has alias, create another links with alias to LeafMNode

* Deleting a storage group is similar to deleting a time series. That is, the storage group or time series node is deleted in its parent node. The time series node also needs to delete its alias in the parent node; If in the deletion process,it is found that a node does not have any child nodes, it also needs to delete this node recursively.
	
## MTree checkpoint

### Create condition

To speed up restarting of IoTDB, we set checkpoint for MTree to avoid reading `mlog.bin` and executing the commands line by line. There are two ways to create MTree snapshot:
1. Background checking and creating automatically: Every 10 minutes, background thread checks the last modified time of MTree. If:
  * If users haven’t modified MTree for more than 1 hour (could be configured), which means `mlog.bin` hasn’t been updated for more than 1 hour
  * `mlog.bin` has reached 100000 lines (could be configured)

2. Creating manually: Users can use `create snapshot for schema` to create MTree snapshot

### Create process

The method is `MManager.createMTreeSnapshot()`:
1. Add read lock for MTree to avoid modifying during creating snapshot
2. Serialize MTree into temporary snapshot file (`mtree.snapshot.tmp`). The serialization of MTree is depth-first from children to parent. Information of nodes are converted into String according to different node types, which is convenient for deserialization.
  * MNode: 0, name, children size
  * StorageGroupMNode: 1, name, TTL, children size
  * MeasurementMNode: 2, name, alias, TSDataType, TSEncoding, CompressionType, props, offset, children size

3. After serialization, rename the temp file to a formal file (`mtree.snapshot`), to avoid crush of server and failure of serialization.
4. Clear `mlog.bin` by `MLogWriter.clear()` method:
  * Close BufferedWriter and delete `mlog.bin` file
  * Create a new BufferedWriter
  * Set `logNumber` as 0. `logNumber` records the log number of `mlog.bin`, which is used for background thread to check whether it is larger than the threshold configured by user.

5. Release the read lock.

### Recover process

The method is `MManager.initFromLog()`:

1. Check whether the temp file `mtree.snapshot.tmp` exists. If so, there may exist crush of server and failure of serialization. Delete the temp file.
2. Check whether the snapshot file `mtree.snapshot` exists. If not, use a new MTree; otherwise, start deserializing from snapshot and get MTree
3. Read and operate all lines in `mlog.bin` and finish the recover process of MTree. Update `lineNumber` at the same time and return it for recording the line number of `mlog.bin` afterwards.

## Log management of metadata


All metadata operations are recorded in a metadata log file, which defaults to data/system/schema/mlog.bin.

When the system restarted, the logs in mlog will be replayed. Until the replaying finished, you need to mark writeToLog to false. When the restart is complete, the writeToLog needs to be set to true.

mlog stores the binary encoding. We can use [MlogParser Tool](https://iotdb.apache.org/UserGuide/Master/System-Tools/MLogParser-Tool.html) to parse the mlog.bin to a human-readable txt version.

Schema operation examples and the corresponding parsed mlog record:

* set storage group to root.turbine

	> mlog: 2,root.turbine
	
	> format: 2,path

* delete storage group root.turbine	
	
	> mlog: 1,root.turbine
	
	> format: 1,path

* create timeseries root.turbine.d1.s1(temperature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes(attr1=v1, attr2=v2)

	> mlog: 0,root.turbine.d1.s1,3,2,1,,temperature,offset
	
	> format: 0,path,TSDataType,TSEncoding,CompressionType,[properties],[alias],[tag-attribute offset]

* delete timeseries root.turbine.d1.s1

	> mlog: 1,root.turbine.d1.s1
	
	> format: 1,path	

* set ttl to root.turbine 10
	
	> mlog: 10,root.turbine,10
		
	> format: 10,path,ttl

* alter timeseries root.turbine.d1.s1 add tags(tag1=v1)
   > Only when root.turbine.d1.s1 does not have any tag/attribute information before, the sql will generate logs
   
   > mlog: 12,root.turbine.d1.s1,10
   
   > format: 10,path,[change offset]

* alter timeseries root.turbine.d1.s1 UPSERT ALIAS=newAlias
   
   > mlog: 13,root.turbine.d1.s1,newAlias
   
   > format: 13,path,[new alias]
                                                                                                               
* create schema template temp1(
  s1 INT32 with encoding=Gorilla and compression SNAPPY,
  s2 FLOAT with encoding=RLE and compression=SNAPPY
)
   
   > mlog:5,temp1,0,s1,1,8,1
   
   > mlog:5,temp1,0,s2,3,2,1
   
   > format: 5,template name,is Aligned Timeseries,measurementId,TSDataType,TSEncoding,CompressionType

* set schema template temp1 to root.turbine
 
    > mlog: 6,temp1,root.turbine
   
    > format: 6,template name,path

* Auto create device root.turbine.d1 (after set a template to a prefix path,  create a device path in mtree automatically when insert data to the device)
 
    > mlog: 4,root.turbine.d1
   
    > format: 4,path

* set root.turbine.d1 is using template (after set a template to a device path, this log shows the device is using template)
 
    > mlog: 61,root.turbine.d1
   
    > format: 61,path                                                                                                              

## TLog
* org.apache.iotdb.db.metadata.TagLogFile

All timeseries tag/attribute information will be saved in the tag file, which defaults to data/system/schema/mlog.bin.

* Total number of bytes of persistence for tags and attributes of each time series is L, which can be configured in the iotdb-engine.properties

* persist content: `Map<String,String> tags, Map<String,String> attributes`, if the content length doesn't reach L, we need to fill it with blank.

> create timeseries root.turbine.d1.s1(temprature) with datatype=FLOAT, encoding=RLE, compression=SNAPPY tags(tag1=v1, tag2=v2) attributes (attr1=v1, attr2=v2)

> content in tlog:

> tagsSize (tag1=v1, tag2=v2) attributesSize (attr1=v1, attr2=v2)

## Metadata Query

### show timeseries without index

The main logic of query is in the `showTimeseries(ShowTimeSeriesPlan plan)` function of `MManager`

First of all, we should judge whether we need to order by heat, if so, call the `getAllMeasurementSchemaByHeatOrder` function of `MTree`. Otherwise, call the `getAllMeasurementSchema` function.

#### getAllMeasurementSchemaByHeatOrder

The heat here is represented by the `lastTimeStamp` of each time series, so we need to fetch all the satisfied time series, and then order them by `lastTimeStamp`, cut them by `offset` and `limit`.

#### getAllMeasurementSchema

In this case, we need to pass the limit(if not exists, set fetch size as limit) and offset to the function `findPath` to reduce the memory footprint.

#### findPath

It's a recursive function to get all the statisfied MNode in MTree from root until the number of timeseries list has reached limit or all the MTree has been traversed.

### show timeseries with index

The filter condition here can only be tag attribute, or it will throw an exception.

We can fetch all the satisfied `MeasurementMNode` through the inverted tag index in MTree fast without traversing the whole tree.

If the result needs to be ordered by heat, we should sort them by the order of `lastTimeStamp` or by the natural order, and then we will trim the result by limit and offset.

### ShowTimeseries Dataset

If there is too much metadata , one whole `show timeseris` processing will cause OOM, so we need to add a `fetch size` parameter.

While the client interacting with the server, it will get at most `fetch_size` records once.

And the intermediate state will be saved in the `ShowTimeseriesDataSet`. The `queryId -> ShowTimeseriesDataSet` key-value pair will be saved in `TsServieImpl`.

In `ShowTimeseriesDataSet`, we saved the `ShowTimeSeriesPlan`, current cursor `index` and cached result list `List<RowRecord> result`.

* judge whether the cursor `index`is equal to the size of `List<RowRecord> result`
    * if so, call the method `showTimeseries` in MManager to fetch result and put them into cache.
        * we need to update the offset in plan each time we call the method in MManger to fetch result, we should add it with `fetch size`.
        * if `hasLimit` is `false`, then reset `index` to zero.
    * if not
        * if `index < result.size()`，return true
        * if `index > result.size()`，return false 
