/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.conf;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.engine.merge.selector.MergeFileStrategy;
import org.apache.iotdb.db.exception.LoadConfigurationException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.service.TSServiceImpl;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBConfig {

  /* Names of Watermark methods */
  public static final String WATERMARK_GROUPED_LSB = "GroupBasedLSBMethod";
  static final String CONFIG_NAME = "iotdb-engine.properties";
  private static final Logger logger = LoggerFactory.getLogger(IoTDBConfig.class);
  private static final String MULTI_DIR_STRATEGY_PREFIX =
      "org.apache.iotdb.db.conf.directories.strategy.";
  private static final String DEFAULT_MULTI_DIR_STRATEGY = "MaxDiskUsableSpaceFirstStrategy";

  // e.g., a31+/$%#&[]{}3e4
  private static final String ID_MATCHER = "([a-zA-Z0-9/@#$%&{}\\[\\]\\-+\\u2E80-\\u9FFF_]+)";

  private static final String STORAGE_GROUP_MATCHER = "([a-zA-Z0-9_.\\u2E80-\\u9FFF]+)";

  // e.g.,  .s1
  private static final String PARTIAL_NODE_MATCHER = "[" + PATH_SEPARATOR + "]" + ID_MATCHER;

  // for path like: root.sg1.d1."1.2.3", root.sg.d1."1.2.3"
  private static final String NODE_MATCHER =
      "[" + PATH_SEPARATOR + "]([\"])?" + ID_MATCHER + "(" + PARTIAL_NODE_MATCHER + ")*([\"])?";

  public static final Pattern STORAGE_GROUP_PATTERN = Pattern.compile(STORAGE_GROUP_MATCHER);

  /**
   * Port which the metrics service listens to.
   */
  private int metricsPort = 8181;

  private boolean enableMetricService = false;

  /**
   * whether to enable the mqtt service.
   */
  private boolean enableMQTTService = false;

  /**
   * the mqtt service binding host.
   */
  private String mqttHost = "0.0.0.0";

  /**
   * the mqtt service binding port.
   */
  private int mqttPort = 1883;

  /**
   * the handler pool size for handing the mqtt messages.
   */
  private int mqttHandlerPoolSize = 1;

  /**
   * the mqtt message payload formatter.
   */
  private String mqttPayloadFormatter = "json";

  /**
   * max mqtt message size
   */
  private int mqttMaxMessageSize = 1048576;

  /**
   * Rpc binding address.
   */
  private String rpcAddress = "0.0.0.0";

  /**
   * whether to use thrift compression.
   */
  private boolean rpcThriftCompressionEnable = false;

  /**
   * whether to use Snappy compression before sending data through the network
   */
  private boolean rpcAdvancedCompressionEnable = false;

  /**
   * Port which the JDBC server listens to.
   */
  private int rpcPort = 6667;

  /**
   * Max concurrent client number
   */
  private int rpcMaxConcurrentClientNum = 65535;

  /**
   * Memory allocated for the write process
   */
  private long allocateMemoryForWrite = Runtime.getRuntime().maxMemory() * 4 / 10;

  /**
   * Memory allocated for the read process
   */
  private long allocateMemoryForRead = Runtime.getRuntime().maxMemory() * 3 / 10;

  /**
   * Memory allocated for the mtree
   */
  private long allocateMemoryForSchema = Runtime.getRuntime().maxMemory() * 1 / 10;

  /**
   * Memory allocated for the read process besides cache
   */
  private long allocateMemoryForReadWithoutCache = Runtime.getRuntime().maxMemory() * 9 / 100;

  private volatile int maxQueryDeduplicatedPathNum = 1000;

  /**
   * Ratio of memory allocated for buffered arrays
   */
  private double bufferedArraysMemoryProportion = 0.6;

  /**
   * Flush proportion for system
   */
  private double flushProportion = 0.4;

  /**
   * Force flush proportion for system
   */
  private double forceFlushProportion = 0.6;

  /**
   * Reject proportion for system
   */
  private double rejectProportion = 0.8;

  /**
   * If storage group increased more than this threshold, report to system.
   */
  private long storageGroupSizeReportThreshold = 16 * 1024 * 1024L;

  /**
   * When inserting rejected, waiting period to check system again
   */
  private int checkPeriodWhenInsertBlocked = 50;

  /**
   * When inserting rejected exceeds this, throw an exception
   */
  private int maxWaitingTimeWhenInsertBlockedInMs = 10000; 
  /**
   * Is the write ahead log enable.
   */
  private boolean enableWal = true;

  private volatile boolean readOnly = false;

  private boolean enableDiscardOutOfOrderData = false;

  /**
   * When a certain amount of write ahead logs is reached, they will be flushed to the disk. It is
   * possible to lose at most flush_wal_threshold operations.
   */
  private int flushWalThreshold = 10000;

  /**
   * this variable set timestamp precision as millisecond, microsecond or nanosecond
   */
  private String timestampPrecision = "ms";

  /**
   * The cycle when write ahead log is periodically forced to be written to disk(in milliseconds) If
   * set this parameter to 0 it means call outputStream.force(true) after every each insert
   */
  private long forceWalPeriodInMs = 100;

  /**
   * The size of the log buffer in each log node (in bytes). Due to the double buffer mechanism, if
   * WAL is enabled and the size of the inserted plan is greater than one-half of this parameter,
   * then the insert plan will be rejected by WAL.
   */
  private int walBufferSize = 16 * 1024 * 1024;

  private int estimatedSeriesSize = 300;

  /**
   * Size of log buffer for every MetaData operation. If the size of a MetaData operation plan
   * is larger than this parameter, then the MetaData operation plan will be rejected by MManager.
   */
  private int mlogBufferSize = 1024 * 1024;

  /**
   * Whether to enable sliding memory table
   */
  private boolean enableSlidingMemTable = true;

  /**
   * Save the flushing memtable in the memory during the period, can help reduce the unseq ratio, Unit: millis.
   * If flush_wait_time = 0 it means system will keep the flushing memtable during 0.1 * working memtable living time
   * Manually setting this value may result in larger memory usage
   */
  private int flushWaitTime = 0;

  /**
   * default base dir, stores all IoTDB runtime files
   */
  private static final String DEFAULT_BASE_DIR = "data";

  /**
   * System directory, including version file for each storage group and metadata
   */
  private String systemDir = DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME;

  /**
   * Schema directory, including storage set of values.
   */
  private String schemaDir = DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME
      + File.separator + IoTDBConstant.SCHEMA_FOLDER_NAME;

  /**
   * Sync directory, including the lock file, uuid file, device owner map
   */
  private String syncDir = DEFAULT_BASE_DIR + File.separator + IoTDBConstant.SYSTEM_FOLDER_NAME
      + File.separator + IoTDBConstant.SYNC_FOLDER_NAME;

  /**
   * Performance tracing directory, stores performance tracing files
   */
  private String tracingDir = DEFAULT_BASE_DIR + File.separator + IoTDBConstant.TRACING_FOLDER_NAME;

  /**
   * Query directory, stores temporary files of query
   */
  private String queryDir = DEFAULT_BASE_DIR + File.separator + IoTDBConstant.QUERY_FOLDER_NAME;

  /**
   * External lib directory, stores user-uploaded JAR files
   */
  private String extDir = IoTDBConstant.EXT_FOLDER_NAME;

  /**
   * External lib directory for UDF, stores user-uploaded JAR files
   */
  private String udfDir = IoTDBConstant.EXT_FOLDER_NAME + File.separator
      + IoTDBConstant.UDF_FOLDER_NAME;

  /**
   * Data directory of data. It can be settled as dataDirs = {"data1", "data2", "data3"};
   */
  private String[] dataDirs = {"data" + File.separator + "data"};

  /**
   * Strategy of multiple directories.
   */
  private String multiDirStrategyClassName = null;

  /**
   * Wal directory.
   */
  private String walDir = DEFAULT_BASE_DIR + File.separator + "wal";

  /**
   * Maximum MemTable number. Invalid when enableMemControl is true.
   */
  private int maxMemtableNumber = 0;

  /**
   * The amount of data iterate each time in server
   */
  private int batchSize = 100000;

  /**
   * How many threads can concurrently flush. When <= 0, use CPU core number.
   */
  private int concurrentFlushThread = Runtime.getRuntime().availableProcessors();

  /**
   * How many threads can concurrently query. When <= 0, use CPU core number.
   */
  private int concurrentQueryThread = Runtime.getRuntime().availableProcessors();

  /**
   * Is the write mem control for writing enable.
   */
  private boolean enableMemControl = true;

  /**
   * Is the write ahead log enable.
   */
  private boolean enableIndex = false;

  /**
   * How many threads can concurrently build index. When <= 0, use CPU core number.
   */
  private int concurrentIndexBuildThread = Runtime.getRuntime().availableProcessors();

  /**
   * If we enable the memory-control mechanism during index building , {@code indexBufferSize}
   * refers to the byte-size of memory buffer threshold. For each index processor, all indexes in
   * one {@linkplain org.apache.iotdb.db.index.IndexFileProcessor IndexFileProcessor} share a total
   * common buffer size. With the memory-control mechanism, the occupied memory of all raw data and
   * index structures will be counted. If the memory buffer size reaches this threshold, the indexes
   * will be flushed to the disk file. As a result, data in one series may be divided into more than
   * one part and indexed separately.
   */
  private long indexBufferSize = 128 * 1024 * 1024L;

  /**
   * the index framework adopts sliding window model to preprocess the original tv list in the
   * subsequence matching task.
   */
  private int defaultIndexWindowRange = 10;

  /**
   * index directory.
   */
  private String indexRootFolder = "data" + File.separator + "index";

  /**
   * When a TsFile's file size (in byte) exceed this, the TsFile is forced closed.
   */
  private long tsFileSizeThreshold = 1L;

  /**
   * When a memTable's size (in byte) exceeds this, the memtable is flushed to disk.
   */
  private long memtableSizeThreshold = 1024 * 1024 * 1024L;

  /**
   * When average series point number reaches this, flush the memtable to disk
   */
  private int avgSeriesPointNumberThreshold = 100000;

  /**
   * Work when tsfile_manage_strategy is level_strategy. When merge point number reaches this, merge
   * the files to the last level.
   * During a merge, if a chunk with less number of chunks than this parameter, the chunk will be
   * merged with its succeeding chunks even if it is not overflowed, until the merged chunks reach
   * this threshold and the new chunk will be flushed.
   */
  private int mergeChunkPointNumberThreshold = 100000;

  /**
   * Works when the compaction_strategy is LEVEL_COMPACTION.
   * When point number of a page reaches this, use "append merge" instead of "deserialize merge".
   */
  private int mergePagePointNumberThreshold = 100;

  /**
   * LEVEL_COMPACTION, NO_COMPACTION
   */
  private CompactionStrategy compactionStrategy = CompactionStrategy.LEVEL_COMPACTION;

  /**
   * Works when the compaction_strategy is LEVEL_COMPACTION.
   * Whether to merge unseq files into seq files or not.
   */
  private boolean enableUnseqCompaction = true;

  /**
   * Works when the compaction_strategy is LEVEL_COMPACTION.
   * The max seq file num of each level.
   * When the num of files in one level exceeds this,
   * the files in this level will merge to one and put to upper level.
   */
  private int seqFileNumInEachLevel = 6;

  /**
   * Works when the compaction_strategy is LEVEL_COMPACTION.
   * The max num of seq level.
   */
  private int seqLevelNum = 3;

  /**
   * Works when compaction_strategy is LEVEL_COMPACTION.
   * The max ujseq file num of each level.
   * When the num of files in one level exceeds this,
   * the files in this level will merge to one and put to upper level.
   */
  private int unseqFileNumInEachLevel = 10;

  /**
   * Works when the compaction_strategy is LEVEL_COMPACTION.
   * The max num of unseq level.
   */
  private int unseqLevelNum = 1;

  /**
   * whether to cache meta data(ChunkMetaData and TsFileMetaData) or not.
   */
  private boolean metaDataCacheEnable = true;

  /**
   * Memory allocated for timeSeriesMetaData cache in read process
   */
  private long allocateMemoryForTimeSeriesMetaDataCache = allocateMemoryForRead / 10;

  /**
   * Memory allocated for chunkMetaData cache in read process
   */
  private long allocateMemoryForChunkMetaDataCache = allocateMemoryForRead / 10;

  /**
   * Memory allocated for chunk cache in read process
   */
  private long allocateMemoryForChunkCache = allocateMemoryForRead / 10;

  /**
   * Whether to enable Last cache
   */
  private boolean lastCacheEnable = true;

  /**
   * Set true to enable statistics monitor service, false to disable statistics service.
   */
  private boolean enableStatMonitor = false;

  /**
   * Set true to enable writing monitor time series.
   */
  private boolean enableMonitorSeriesWrite = false;

  /**
   * Cache size of {@code checkAndGetDataTypeCache} in {@link MManager}.
   */
  private int mManagerCacheSize = 300000;

  /**
   * Cache size of {@code checkAndGetDataTypeCache} in {@link MManager}.
   */
  private int mRemoteSchemaCacheSize = 100000;

  /**
   * Is external sort enable.
   */
  private boolean enableExternalSort = true;

  /**
   * The threshold of items in external sort. If the number of chunks participating in sorting
   * exceeds this threshold, external sorting is enabled, otherwise memory sorting is used.
   */
  private int externalSortThreshold = 1000;

  /**
   * Is this IoTDB instance a receiver of sync or not.
   */
  private boolean isSyncEnable = false;
  /**
   * If this IoTDB instance is a receiver of sync, set the server port.
   */
  private int syncServerPort = 5555;
  /**
   * Set the language version when loading file including error information, default value is "EN"
   */
  private String languageVersion = "EN";

  private String ipWhiteList = "0.0.0.0/0";
  /**
   * Examining period of cache file reader : 100 seconds.
   */
  private long cacheFileReaderClearPeriod = 100000;

  /**
   * Replace implementation class of JDBC service
   */
  private String rpcImplClassName = TSServiceImpl.class.getName();

  /**
   * Is stat performance of sub-module enable.
   */
  private boolean enablePerformanceStat = false;

  /**
   * Is performance tracing enable.
   */
  private boolean enablePerformanceTracing = false;

  /**
   * The display of stat performance interval in ms.
   */
  private long performanceStatDisplayInterval = 60000;

  /**
   * The memory used for stat performance.
   */
  private int performanceStatMemoryInKB = 20;
  /**
   * whether use chunkBufferPool.
   */
  private boolean chunkBufferPoolEnable = false;

  /**
   * Switch of watermark function
   */
  private boolean enableWatermark = false;

  /**
   * Secret key for watermark
   */
  private String watermarkSecretKey = "IoTDB*2019@Beijing";

  /**
   * Bit string of watermark
   */
  private String watermarkBitString = "100101110100";

  /**
   * Watermark method and parameters
   */
  private String watermarkMethod = "GroupBasedLSBMethod(embed_row_cycle=2,embed_lsb_num=5)";

  /**
   * Switch of creating schema automatically
   */
  private boolean enableAutoCreateSchema = true;

  /**
   * register time series as which type when receiving boolean string "true" or "false"
   */
  private TSDataType booleanStringInferType = TSDataType.BOOLEAN;

  /**
   * register time series as which type when receiving an integer string "67"
   */
  private TSDataType integerStringInferType = TSDataType.FLOAT;

  /**
   * register time series as which type when receiving an integer string and using float may lose precision
   * num > 2 ^ 24
   */
  private TSDataType longStringInferType = TSDataType.DOUBLE;

  /**
   * register time series as which type when receiving a floating number string "6.7"
   */
  private TSDataType floatingStringInferType = TSDataType.FLOAT;

  /**
   * register time series as which type when receiving the Literal NaN. Values can be DOUBLE, FLOAT
   * or TEXT
   */
  private TSDataType nanStringInferType = TSDataType.DOUBLE;

  /**
   * Storage group level when creating schema automatically is enabled
   */
  private int defaultStorageGroupLevel = 1;

  /**
   * BOOLEAN encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultBooleanEncoding = TSEncoding.RLE;

  /**
   * INT32 encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultInt32Encoding = TSEncoding.RLE;

  /**
   * INT64 encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultInt64Encoding = TSEncoding.RLE;

  /**
   * FLOAT encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultFloatEncoding = TSEncoding.GORILLA;

  /**
   * DOUBLE encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultDoubleEncoding = TSEncoding.GORILLA;

  /**
   * TEXT encoding when creating schema automatically is enabled
   */
  private TSEncoding defaultTextEncoding = TSEncoding.PLAIN;

  /**
   * How much memory (in byte) can be used by a single merge task.
   */
  private long mergeMemoryBudget = (long) (Runtime.getRuntime().maxMemory() * 0.1);

  /**
   * How many threads will be set up to perform upgrade tasks.
   */
  private int upgradeThreadNum = 1;

  /**
   * How many threads will be set up to perform main merge tasks.
   */
  private int mergeThreadNum = 1;

  /**
   * How many threads will be set up to perform unseq merge chunk sub-tasks.
   */
  private int mergeChunkSubThreadNum = 4;

  /**
   * If one merge file selection runs for more than this time, it will be ended and its current
   * selection will be used as final selection. Unit: millis. When < 0, it means time is unbounded.
   */
  private long mergeFileSelectionTimeBudget = 30 * 1000L;

  /**
   * When set to true, if some crashed merges are detected during system rebooting, such merges will
   * be continued, otherwise, the unfinished parts of such merges will not be continued while the
   * finished parts still remain as they are.
   */
  private boolean continueMergeAfterReboot = false;

  /**
   * A global merge will be performed each such interval, that is, each storage group will be merged
   * (if proper merge candidates can be found). Unit: second.
   */
  private long mergeIntervalSec = 0L;

  /**
   * When set to true, all unseq merges becomes full merge (the whole SeqFiles are re-written despite how
   * much they are overflowed). This may increase merge overhead depending on how much the SeqFiles
   * are overflowed.
   */
  private boolean forceFullMerge = false;

  /**
   * The limit of compaction merge can reach per second
   */
  private int mergeWriteThroughputMbPerSec = 8;

  /**
   * How many thread will be set up to perform compaction, 10 by default. Set to 1 when less
   * than or equal to 0.
   */
  private int compactionThreadNum = 10;

  private MergeFileStrategy mergeFileStrategy = MergeFileStrategy.MAX_SERIES_NUM;

  /**
   * Default system file storage is in local file system (unsupported)
   */
  private FSType systemFileStorageFs = FSType.LOCAL;

  /**
   * Default TSfile storage is in local file system
   */
  private FSType tsFileStorageFs = FSType.LOCAL;

  /**
   * Default core-site.xml file path is /etc/hadoop/conf/core-site.xml
   */
  private String coreSitePath = "/etc/hadoop/conf/core-site.xml";

  /**
   * Default hdfs-site.xml file path is /etc/hadoop/conf/hdfs-site.xml
   */
  private String hdfsSitePath = "/etc/hadoop/conf/hdfs-site.xml";

  /**
   * Default HDFS ip is localhost
   */
  private String hdfsIp = "localhost";

  /**
   * Default HDFS port is 9000
   */
  private String hdfsPort = "9000";

  /**
   * Default DFS NameServices is hdfsnamespace
   */
  private String dfsNameServices = "hdfsnamespace";

  /**
   * Default DFS HA name nodes are nn1 and nn2
   */
  private String dfsHaNamenodes = "nn1,nn2";

  /**
   * Default DFS HA automatic failover is enabled
   */
  private boolean dfsHaAutomaticFailoverEnabled = true;

  /**
   * Default DFS client failover proxy provider is "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   */
  private String dfsClientFailoverProxyProvider = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider";

  /**
   * whether use kerberos to authenticate hdfs
   */
  private boolean useKerberos = false;

  /**
   * full path of kerberos keytab file
   */
  private String kerberosKeytabFilePath = "/path";

  /**
   * kerberos principal
   */
  private String kerberosPrincipal = "principal";

  /**
   * the num of memtable in each storage group
   */
  private int concurrentWritingTimePartition = 1;

  /**
   * the default fill interval in LinearFill and PreviousFill, -1 means infinite past time
   */
  private int defaultFillInterval = -1;

  /**
   * default TTL for storage groups that are not set TTL by statements, in ms
   * <p>
   * Notice: if this property is changed, previous created storage group which are not set TTL will
   * also be affected.
   */
  private long defaultTTL = Long.MAX_VALUE;

  /**
   * The default value of primitive array size in array pool
   */
  private int primitiveArraySize = 128;

  /**
   * whether enable data partition. If disabled, all data belongs to partition 0
   */
  private boolean enablePartition = false;

  /**
   * whether enable MTree snapshot
   */
  private boolean enableMTreeSnapshot = false;

  /**
   * Interval line number of mlog.txt when creating a checkpoint and saving snapshot of mtree
   */
  private int mtreeSnapshotInterval = 100000;

  /**
   * Threshold interval time of MTree modification. If the last modification time is less than this
   * threshold, MTree snapshot will not be created. Unit: second. Default: 1 hour(3600 seconds)
   */
  private int mtreeSnapshotThresholdTime = 3600;

  /**
   * Time range for partitioning data inside each storage group, the unit is second
   */
  private long partitionInterval = 604800;

  //just for test
  //wait for 60 second by default.
  private int thriftServerAwaitTimeForStopService = 60;

  private int queryCacheSizeInMetric = 50;

  // max size for tag and attribute of one time series
  private int tagAttributeTotalSize = 700;

  // In one insert (one device, one timestamp, multiple measurements),
  // if enable partial insert, one measurement failure will not impact other measurements
  private boolean enablePartialInsert = true;

  // Open ID Secret
  private String openIdProviderUrl = null;

  // the authorizer provider class which extends BasicAuthorizer
  private String authorizerProvider = "org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer";

  /**
   * Used to estimate the memory usage of text fields in a UDF query. It is recommended to set this
   * value to be slightly larger than the average length of all text records.
   */
  private int udfInitialByteArrayLengthForMemoryControl = 48;

  /**
   * How much memory may be used in ONE UDF query (in MB).
   * <p>
   * The upper limit is 20% of allocated memory for read.
   * <p>
   * udfMemoryBudgetInMB = udfReaderMemoryBudgetInMB + udfTransformerMemoryBudgetInMB +
   * udfCollectorMemoryBudgetInMB
   */
  private float udfMemoryBudgetInMB = (float) Math.min(300f, 0.2 * allocateMemoryForRead);

  private float udfReaderMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  private float udfTransformerMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  private float udfCollectorMemoryBudgetInMB = (float) (1.0 / 3 * udfMemoryBudgetInMB);

  // time in nanosecond precision when starting up
  private long startUpNanosecond = System.nanoTime();

  /**
   * thrift max frame size, the default is 15MB, we change it to 64MB
   */
  private int thriftMaxFrameSize = 67108864;

  /**
   * thrift init buffer size, the default is 1KB.
   */
  private int thriftInitBufferSize = 1024;

  /**
   * time interval in minute for calculating query frequency
   */
  private int frequencyIntervalInMinute = 1;

  /**
   * time cost(ms) threshold for slow query
   */
  private long slowQueryThreshold = 5000;

  /**
   * if the debug_state is true, we will print more details about the process of query
   */
  private boolean debugState = false;

  public IoTDBConfig() {
    // empty constructor
  }

  public float getUdfMemoryBudgetInMB() {
    return udfMemoryBudgetInMB;
  }

  public void setUdfMemoryBudgetInMB(float udfMemoryBudgetInMB) {
    this.udfMemoryBudgetInMB = udfMemoryBudgetInMB;
  }

  public float getUdfReaderMemoryBudgetInMB() {
    return udfReaderMemoryBudgetInMB;
  }

  public void setUdfReaderMemoryBudgetInMB(float udfReaderMemoryBudgetInMB) {
    this.udfReaderMemoryBudgetInMB = udfReaderMemoryBudgetInMB;
  }

  public float getUdfTransformerMemoryBudgetInMB() {
    return udfTransformerMemoryBudgetInMB;
  }

  public void setUdfTransformerMemoryBudgetInMB(float udfTransformerMemoryBudgetInMB) {
    this.udfTransformerMemoryBudgetInMB = udfTransformerMemoryBudgetInMB;
  }

  public float getUdfCollectorMemoryBudgetInMB() {
    return udfCollectorMemoryBudgetInMB;
  }

  public void setUdfCollectorMemoryBudgetInMB(float udfCollectorMemoryBudgetInMB) {
    this.udfCollectorMemoryBudgetInMB = udfCollectorMemoryBudgetInMB;
  }

  public int getUdfInitialByteArrayLengthForMemoryControl() {
    return udfInitialByteArrayLengthForMemoryControl;
  }

  public void setUdfInitialByteArrayLengthForMemoryControl(
      int udfInitialByteArrayLengthForMemoryControl) {
    this.udfInitialByteArrayLengthForMemoryControl = udfInitialByteArrayLengthForMemoryControl;
  }

  public int getConcurrentWritingTimePartition() {
    return concurrentWritingTimePartition;
  }

  void setConcurrentWritingTimePartition(int concurrentWritingTimePartition) {
    this.concurrentWritingTimePartition = concurrentWritingTimePartition;
  }

  public int getDefaultFillInterval() {
    return defaultFillInterval;
  }

  public void setDefaultFillInterval(int defaultFillInterval) {
    this.defaultFillInterval = defaultFillInterval;
  }

  public boolean isEnablePartition() {
    return enablePartition;
  }

  public void setEnablePartition(boolean enablePartition) {
    this.enablePartition = enablePartition;
  }

  public boolean isEnableMTreeSnapshot() {
    return enableMTreeSnapshot;
  }

  public void setEnableMTreeSnapshot(boolean enableMTreeSnapshot) {
    this.enableMTreeSnapshot = enableMTreeSnapshot;
  }

  public int getMtreeSnapshotInterval() {
    return mtreeSnapshotInterval;
  }

  public void setMtreeSnapshotInterval(int mtreeSnapshotInterval) {
    this.mtreeSnapshotInterval = mtreeSnapshotInterval;
  }

  public int getMtreeSnapshotThresholdTime() {
    return mtreeSnapshotThresholdTime;
  }

  public void setMtreeSnapshotThresholdTime(int mtreeSnapshotThresholdTime) {
    this.mtreeSnapshotThresholdTime = mtreeSnapshotThresholdTime;
  }

  public long getPartitionInterval() {
    return partitionInterval;
  }

  public void setPartitionInterval(long partitionInterval) {
    this.partitionInterval = partitionInterval;
  }

  void updatePath() {
    formulateFolders();
    confirmMultiDirStrategy();
  }

  /**
   * if the folders are relative paths, add IOTDB_HOME as the path prefix
   */
  private void formulateFolders() {
    systemDir = addHomeDir(systemDir);
    schemaDir = addHomeDir(schemaDir);
    syncDir = addHomeDir(syncDir);
    tracingDir = addHomeDir(tracingDir);
    walDir = addHomeDir(walDir);
    indexRootFolder = addHomeDir(indexRootFolder);
    extDir = addHomeDir(extDir);
    udfDir = addHomeDir(udfDir);

    if (TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs().equals(FSType.HDFS)) {
      String hdfsDir = getHdfsDir();
      queryDir = hdfsDir + File.separatorChar + queryDir;
      for (int i = 0; i < dataDirs.length; i++) {
        dataDirs[i] = hdfsDir + File.separatorChar + dataDirs[i];
      }
    } else {
      queryDir = addHomeDir(queryDir);
      for (int i = 0; i < dataDirs.length; i++) {
        dataDirs[i] = addHomeDir(dataDirs[i]);
      }
    }
  }

  void reloadDataDirs(String[] dataDirs) throws LoadConfigurationException {
    if (TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs().equals(FSType.HDFS)) {
      String hdfsDir = getHdfsDir();
      for (int i = 0; i < dataDirs.length; i++) {
        dataDirs[i] = hdfsDir + File.separatorChar + dataDirs[i];
      }
    } else {
      for (int i = 0; i < dataDirs.length; i++) {
        dataDirs[i] = addHomeDir(dataDirs[i]);
      }
    }
    this.dataDirs = dataDirs;
    DirectoryManager.getInstance().updateFileFolders();
  }

  private String addHomeDir(String dir) {
    String homeDir = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
    if (!new File(dir).isAbsolute() && homeDir != null && homeDir.length() > 0) {
      if (!homeDir.endsWith(File.separator)) {
        dir = homeDir + File.separatorChar + dir;
      } else {
        dir = homeDir + dir;
      }
    }
    return dir;
  }

  private void confirmMultiDirStrategy() {
    if (getMultiDirStrategyClassName() == null) {
      multiDirStrategyClassName = DEFAULT_MULTI_DIR_STRATEGY;
    }
    if (!getMultiDirStrategyClassName().contains(TsFileConstant.PATH_SEPARATOR)) {
      multiDirStrategyClassName = MULTI_DIR_STRATEGY_PREFIX + multiDirStrategyClassName;
    }

    try {
      Class.forName(multiDirStrategyClassName);
    } catch (ClassNotFoundException e) {
      logger.warn("Cannot find given directory strategy {}, using the default value",
          getMultiDirStrategyClassName(), e);
      setMultiDirStrategyClassName(MULTI_DIR_STRATEGY_PREFIX + DEFAULT_MULTI_DIR_STRATEGY);
    }
  }

  private String getHdfsDir() {
    String[] hdfsIps = TSFileDescriptor.getInstance().getConfig().getHdfsIp();
    String hdfsDir = "hdfs://";
    if (hdfsIps.length > 1) {
      hdfsDir += TSFileDescriptor.getInstance().getConfig().getDfsNameServices();
    } else {
      hdfsDir += hdfsIps[0] + ":" + TSFileDescriptor.getInstance().getConfig().getHdfsPort();
    }
    return hdfsDir;
  }

  public String[] getDataDirs() {
    return dataDirs;
  }

  public int getMetricsPort() {
    return metricsPort;
  }

  void setMetricsPort(int metricsPort) {
    this.metricsPort = metricsPort;
  }

  public boolean isEnableMetricService() {
    return enableMetricService;
  }

  public void setEnableMetricService(boolean enableMetricService) {
    this.enableMetricService = enableMetricService;
  }

  void setDataDirs(String[] dataDirs) {
    this.dataDirs = dataDirs;
  }

  public String getRpcAddress() {
    return rpcAddress;
  }

  void setRpcAddress(String rpcAddress) {
    this.rpcAddress = rpcAddress;
  }

  public int getRpcPort() {
    return rpcPort;
  }

  void setRpcPort(int rpcPort) {
    this.rpcPort = rpcPort;
  }

  public String getTimestampPrecision() {
    return timestampPrecision;
  }

  public void setTimestampPrecision(String timestampPrecision) {
    if (!(timestampPrecision.equals("ms") || timestampPrecision.equals("us")
        || timestampPrecision.equals("ns"))) {
      logger.error("Wrong timestamp precision, please set as: ms, us or ns ! Current is: "
          + timestampPrecision);
      System.exit(-1);
    }
    this.timestampPrecision = timestampPrecision;
  }

  public boolean isEnableWal() {
    return enableWal;
  }

  public void setEnableWal(boolean enableWal) {
    this.enableWal = enableWal;
  }

  public boolean isEnableDiscardOutOfOrderData() {
    return enableDiscardOutOfOrderData;
  }

  public void setEnableDiscardOutOfOrderData(boolean enableDiscardOutOfOrderData) {
    this.enableDiscardOutOfOrderData = enableDiscardOutOfOrderData;
  }

  public int getFlushWalThreshold() {
    return flushWalThreshold;
  }

  public void setFlushWalThreshold(int flushWalThreshold) {
    this.flushWalThreshold = flushWalThreshold;
  }

  public long getForceWalPeriodInMs() {
    return forceWalPeriodInMs;
  }

  public void setForceWalPeriodInMs(long forceWalPeriodInMs) {
    this.forceWalPeriodInMs = forceWalPeriodInMs;
  }

  public boolean isEnableSlidingMemTable() {
    return enableSlidingMemTable;
  }

  public void setEnableSlidingMemTable(boolean enableSlidingMemTable) {
    this.enableSlidingMemTable = enableSlidingMemTable;
  }

  public int getFlushWaitTime() {
    return flushWaitTime;
  }

  public void setFlushWaitTime(int flushWaitTime) {
    this.flushWaitTime = flushWaitTime;
  }

  public String getSystemDir() {
    return systemDir;
  }

  void setSystemDir(String systemDir) {
    this.systemDir = systemDir;
  }

  public String getSchemaDir() {
    return schemaDir;
  }

  void setSchemaDir(String schemaDir) {
    this.schemaDir = schemaDir;
  }

  public String getSyncDir() {
    return syncDir;
  }

  void setSyncDir(String syncDir) {
    this.syncDir = syncDir;
  }

  public String getTracingDir() {
    return tracingDir;
  }

  void setTracingDir(String tracingDir) {
    this.tracingDir = tracingDir;
  }

  public String getQueryDir() {
    return queryDir;
  }

  void setQueryDir(String queryDir) {
    this.queryDir = queryDir;
  }

  public String getWalDir() {
    return walDir;
  }

  void setWalDir(String walDir) {
    this.walDir = walDir;
  }

  public String getExtDir() {
    return extDir;
  }

  public void setExtDir(String extDir) {
    this.extDir = extDir;
  }

  public String getUdfDir() {
    return udfDir;
  }

  public void setUdfDir(String udfDir) {
    this.udfDir = udfDir;
  }

  public String getMultiDirStrategyClassName() {
    return multiDirStrategyClassName;
  }

  void setMultiDirStrategyClassName(String multiDirStrategyClassName) {
    this.multiDirStrategyClassName = multiDirStrategyClassName;
  }

  public int getBatchSize() {
    return batchSize;
  }

  void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getMaxMemtableNumber() {
    return maxMemtableNumber;
  }

  public void setMaxMemtableNumber(int maxMemtableNumber) {
    this.maxMemtableNumber = maxMemtableNumber;
  }

  public int getConcurrentFlushThread() {
    return concurrentFlushThread;
  }

  void setConcurrentFlushThread(int concurrentFlushThread) {
    this.concurrentFlushThread = concurrentFlushThread;
  }

  public int getConcurrentQueryThread() {
    return concurrentQueryThread;
  }

  void setConcurrentQueryThread(int concurrentQueryThread) {
    this.concurrentQueryThread = concurrentQueryThread;
  }

  public long getTsFileSizeThreshold() {
    return tsFileSizeThreshold;
  }

  public void setTsFileSizeThreshold(long tsFileSizeThreshold) {
    this.tsFileSizeThreshold = tsFileSizeThreshold;
  }

  public boolean isEnableStatMonitor() {
    return enableStatMonitor;
  }

  public void setEnableStatMonitor(boolean enableStatMonitor) {
    this.enableStatMonitor = enableStatMonitor;
  }

  public boolean isEnableMonitorSeriesWrite() {
    return enableMonitorSeriesWrite;
  }

  public void setEnableMonitorSeriesWrite(boolean enableMonitorSeriesWrite) {
    this.enableMonitorSeriesWrite = enableMonitorSeriesWrite;
  }

  public int getRpcMaxConcurrentClientNum() {
    return rpcMaxConcurrentClientNum;
  }

  void setRpcMaxConcurrentClientNum(int rpcMaxConcurrentClientNum) {
    this.rpcMaxConcurrentClientNum = rpcMaxConcurrentClientNum;
  }

  public int getmManagerCacheSize() {
    return mManagerCacheSize;
  }

  void setmManagerCacheSize(int mManagerCacheSize) {
    this.mManagerCacheSize = mManagerCacheSize;
  }

  public int getmRemoteSchemaCacheSize() {
    return mRemoteSchemaCacheSize;
  }

  public void setmRemoteSchemaCacheSize(int mRemoteSchemaCacheSize) {
    this.mRemoteSchemaCacheSize = mRemoteSchemaCacheSize;
  }

  public boolean isSyncEnable() {
    return isSyncEnable;
  }

  public void setSyncEnable(boolean syncEnable) {
    isSyncEnable = syncEnable;
  }

  public int getSyncServerPort() {
    return syncServerPort;
  }

  void setSyncServerPort(int syncServerPort) {
    this.syncServerPort = syncServerPort;
  }

  String getLanguageVersion() {
    return languageVersion;
  }

  void setLanguageVersion(String languageVersion) {
    this.languageVersion = languageVersion;
  }

  public String getIpWhiteList() {
    return ipWhiteList;
  }

  public void setIpWhiteList(String ipWhiteList) {
    this.ipWhiteList = ipWhiteList;
  }

  public long getCacheFileReaderClearPeriod() {
    return cacheFileReaderClearPeriod;
  }

  public void setCacheFileReaderClearPeriod(long cacheFileReaderClearPeriod) {
    this.cacheFileReaderClearPeriod = cacheFileReaderClearPeriod;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public String getRpcImplClassName() {
    return rpcImplClassName;
  }

  public void setRpcImplClassName(String rpcImplClassName) {
    this.rpcImplClassName = rpcImplClassName;
  }

  public int getWalBufferSize() {
    return walBufferSize;
  }

  public void setWalBufferSize(int walBufferSize) {
    this.walBufferSize = walBufferSize;
  }

  public int getEstimatedSeriesSize() {
    return estimatedSeriesSize;
  }

  public void setEstimatedSeriesSize(int estimatedSeriesSize) {
    this.estimatedSeriesSize = estimatedSeriesSize;
  }

  public boolean isChunkBufferPoolEnable() {
    return chunkBufferPoolEnable;
  }

  void setChunkBufferPoolEnable(boolean chunkBufferPoolEnable) {
    this.chunkBufferPoolEnable = chunkBufferPoolEnable;
  }

  public long getMergeMemoryBudget() {
    return mergeMemoryBudget;
  }

  void setMergeMemoryBudget(long mergeMemoryBudget) {
    this.mergeMemoryBudget = mergeMemoryBudget;
  }

  public int getMergeThreadNum() {
    return mergeThreadNum;
  }

  void setMergeThreadNum(int mergeThreadNum) {
    this.mergeThreadNum = mergeThreadNum;
  }

  public boolean isContinueMergeAfterReboot() {
    return continueMergeAfterReboot;
  }

  void setContinueMergeAfterReboot(boolean continueMergeAfterReboot) {
    this.continueMergeAfterReboot = continueMergeAfterReboot;
  }

  public long getMergeIntervalSec() {
    return mergeIntervalSec;
  }

  void setMergeIntervalSec(long mergeIntervalSec) {
    this.mergeIntervalSec = mergeIntervalSec;
  }

  public double getBufferedArraysMemoryProportion() {
    return bufferedArraysMemoryProportion;
  }

  public void setBufferedArraysMemoryProportion(double bufferedArraysMemoryProportion) {
    this.bufferedArraysMemoryProportion = bufferedArraysMemoryProportion;
  }

  public double getFlushProportion() {
    return flushProportion;
  }

  public void setFlushProportion(double flushProportion) {
    this.flushProportion = flushProportion;
  }

  public double getForceFlushProportion() {
    return forceFlushProportion;
  }

  public void setForceFlushProportion(double forceFlushProportion) {
    this.forceFlushProportion = forceFlushProportion;
  }

  public double getRejectProportion() {
    return rejectProportion;
  }

  public void setRejectProportion(double rejectProportion) {
    this.rejectProportion = rejectProportion;
  }

  public long getStorageGroupSizeReportThreshold() {
    return storageGroupSizeReportThreshold;
  }

  public void setStorageGroupSizeReportThreshold(long storageGroupSizeReportThreshold) {
    this.storageGroupSizeReportThreshold = storageGroupSizeReportThreshold;
  }

  public long getAllocateMemoryForWrite() {
    return allocateMemoryForWrite;
  }

  public void setAllocateMemoryForWrite(long allocateMemoryForWrite) {
    this.allocateMemoryForWrite = allocateMemoryForWrite;
  }

  public long getAllocateMemoryForSchema() {
    return allocateMemoryForSchema;
  }

  void setAllocateMemoryForSchema(long allocateMemoryForSchema) {
    this.allocateMemoryForSchema = allocateMemoryForSchema;
  }

  long getAllocateMemoryForRead() {
    return allocateMemoryForRead;
  }

  void setAllocateMemoryForRead(long allocateMemoryForRead) {
    this.allocateMemoryForRead = allocateMemoryForRead;
  }

  public long getAllocateMemoryForReadWithoutCache() {
    return allocateMemoryForReadWithoutCache;
  }

  public void setAllocateMemoryForReadWithoutCache(long allocateMemoryForReadWithoutCache) {
    this.allocateMemoryForReadWithoutCache = allocateMemoryForReadWithoutCache;
  }

  public boolean isEnableExternalSort() {
    return enableExternalSort;
  }

  void setEnableExternalSort(boolean enableExternalSort) {
    this.enableExternalSort = enableExternalSort;
  }

  public int getExternalSortThreshold() {
    return externalSortThreshold;
  }

  void setExternalSortThreshold(int externalSortThreshold) {
    this.externalSortThreshold = externalSortThreshold;
  }

  public boolean isEnablePerformanceStat() {
    return enablePerformanceStat;
  }

  public void setEnablePerformanceStat(boolean enablePerformanceStat) {
    this.enablePerformanceStat = enablePerformanceStat;
  }

  public boolean isEnablePerformanceTracing() {
    return enablePerformanceTracing;
  }

  public void setEnablePerformanceTracing(boolean enablePerformanceTracing) {
    this.enablePerformanceTracing = enablePerformanceTracing;
  }

  public long getPerformanceStatDisplayInterval() {
    return performanceStatDisplayInterval;
  }

  void setPerformanceStatDisplayInterval(long performanceStatDisplayInterval) {
    this.performanceStatDisplayInterval = performanceStatDisplayInterval;
  }

  public int getPerformanceStatMemoryInKB() {
    return performanceStatMemoryInKB;
  }

  void setPerformanceStatMemoryInKB(int performanceStatMemoryInKB) {
    this.performanceStatMemoryInKB = performanceStatMemoryInKB;
  }

  public boolean isEnablePartialInsert() {
    return enablePartialInsert;
  }

  public void setEnablePartialInsert(boolean enablePartialInsert) {
    this.enablePartialInsert = enablePartialInsert;
  }

  public boolean isForceFullMerge() {
    return forceFullMerge;
  }

  void setForceFullMerge(boolean forceFullMerge) {
    this.forceFullMerge = forceFullMerge;
  }

  public int getCompactionThreadNum() {
    return compactionThreadNum;
  }

  public void setCompactionThreadNum(int compactionThreadNum) {
    this.compactionThreadNum = compactionThreadNum;
  }

  public int getMergeWriteThroughputMbPerSec() {
    return mergeWriteThroughputMbPerSec;
  }

  public void setMergeWriteThroughputMbPerSec(int mergeWriteThroughputMbPerSec) {
    this.mergeWriteThroughputMbPerSec = mergeWriteThroughputMbPerSec;
  }

  public boolean isEnableMemControl() {
    return enableMemControl;
  }

  public void setEnableMemControl(boolean enableMemControl) {
    this.enableMemControl = enableMemControl;
  }

  public long getMemtableSizeThreshold() {
    return memtableSizeThreshold;
  }

  public void setMemtableSizeThreshold(long memtableSizeThreshold) {
    this.memtableSizeThreshold = memtableSizeThreshold;
  }

  public int getAvgSeriesPointNumberThreshold() {
    return avgSeriesPointNumberThreshold;
  }

  public void setAvgSeriesPointNumberThreshold(int avgSeriesPointNumberThreshold) {
    this.avgSeriesPointNumberThreshold = avgSeriesPointNumberThreshold;
  }

  public int getMergeChunkPointNumberThreshold() {
    return mergeChunkPointNumberThreshold;
  }

  public void setMergeChunkPointNumberThreshold(int mergeChunkPointNumberThreshold) {
    this.mergeChunkPointNumberThreshold = mergeChunkPointNumberThreshold;
  }

  public int getMergePagePointNumberThreshold() {
    return mergePagePointNumberThreshold;
  }

  public void setMergePagePointNumberThreshold(int mergePagePointNumberThreshold) {
    this.mergePagePointNumberThreshold = mergePagePointNumberThreshold;
  }

  public MergeFileStrategy getMergeFileStrategy() {
    return mergeFileStrategy;
  }

  public void setMergeFileStrategy(
      MergeFileStrategy mergeFileStrategy) {
    this.mergeFileStrategy = mergeFileStrategy;
  }


  public CompactionStrategy getCompactionStrategy() {
    return compactionStrategy;
  }

  public void setCompactionStrategy(
      CompactionStrategy compactionStrategy) {
    this.compactionStrategy = compactionStrategy;
  }

  public boolean isEnableUnseqCompaction() {
    return enableUnseqCompaction;
  }

  public void setEnableUnseqCompaction(boolean enableUnseqCompaction) {
    this.enableUnseqCompaction = enableUnseqCompaction;
  }

  public int getSeqFileNumInEachLevel() {
    return seqFileNumInEachLevel;
  }

  public void setSeqFileNumInEachLevel(int seqFileNumInEachLevel) {
    this.seqFileNumInEachLevel = seqFileNumInEachLevel;
  }

  public int getSeqLevelNum() {
    return seqLevelNum;
  }

  public void setSeqLevelNum(int seqLevelNum) {
    this.seqLevelNum = seqLevelNum;
  }

  public int getUnseqFileNumInEachLevel() {
    return unseqFileNumInEachLevel;
  }

  public void setUnseqFileNumInEachLevel(int unseqFileNumInEachLevel) {
    this.unseqFileNumInEachLevel = unseqFileNumInEachLevel;
  }

  public int getUnseqLevelNum() {
    return unseqLevelNum;
  }

  public void setUnseqLevelNum(int unseqLevelNum) {
    this.unseqLevelNum = unseqLevelNum;
  }

  public int getMergeChunkSubThreadNum() {
    return mergeChunkSubThreadNum;
  }

  void setMergeChunkSubThreadNum(int mergeChunkSubThreadNum) {
    this.mergeChunkSubThreadNum = mergeChunkSubThreadNum;
  }

  public long getMergeFileSelectionTimeBudget() {
    return mergeFileSelectionTimeBudget;
  }

  void setMergeFileSelectionTimeBudget(long mergeFileSelectionTimeBudget) {
    this.mergeFileSelectionTimeBudget = mergeFileSelectionTimeBudget;
  }

  public boolean isRpcThriftCompressionEnable() {
    return rpcThriftCompressionEnable;
  }

  public void setRpcThriftCompressionEnable(boolean rpcThriftCompressionEnable) {
    this.rpcThriftCompressionEnable = rpcThriftCompressionEnable;
  }

  public boolean isMetaDataCacheEnable() {
    return metaDataCacheEnable;
  }

  public void setMetaDataCacheEnable(boolean metaDataCacheEnable) {
    this.metaDataCacheEnable = metaDataCacheEnable;
  }

  public long getAllocateMemoryForTimeSeriesMetaDataCache() {
    return allocateMemoryForTimeSeriesMetaDataCache;
  }

  public void setAllocateMemoryForTimeSeriesMetaDataCache(
      long allocateMemoryForTimeSeriesMetaDataCache) {
    this.allocateMemoryForTimeSeriesMetaDataCache = allocateMemoryForTimeSeriesMetaDataCache;
  }

  public long getAllocateMemoryForChunkMetaDataCache() {
    return allocateMemoryForChunkMetaDataCache;
  }

  public void setAllocateMemoryForChunkMetaDataCache(long allocateMemoryForChunkMetaDataCache) {
    this.allocateMemoryForChunkMetaDataCache = allocateMemoryForChunkMetaDataCache;
  }

  public long getAllocateMemoryForChunkCache() {
    return allocateMemoryForChunkCache;
  }

  public void setAllocateMemoryForChunkCache(long allocateMemoryForChunkCache) {
    this.allocateMemoryForChunkCache = allocateMemoryForChunkCache;
  }

  public boolean isLastCacheEnabled() {
    return lastCacheEnable;
  }

  public void setEnableLastCache(boolean lastCacheEnable) {
    this.lastCacheEnable = lastCacheEnable;
  }

  public boolean isEnableWatermark() {
    return enableWatermark;
  }

  public void setEnableWatermark(boolean enableWatermark) {
    this.enableWatermark = enableWatermark;
  }

  public String getWatermarkSecretKey() {
    return watermarkSecretKey;
  }

  public void setWatermarkSecretKey(String watermarkSecretKey) {
    this.watermarkSecretKey = watermarkSecretKey;
  }

  public String getWatermarkBitString() {
    return watermarkBitString;
  }

  public void setWatermarkBitString(String watermarkBitString) {
    this.watermarkBitString = watermarkBitString;
  }

  String getWatermarkMethod() {
    return this.watermarkMethod;
  }

  public void setWatermarkMethod(String watermarkMethod) {
    this.watermarkMethod = watermarkMethod;
  }

  public String getWatermarkMethodName() {
    return watermarkMethod.split("\\(")[0];
  }

  public int getWatermarkParamMarkRate() {
    return Integer.parseInt(getWatermarkParamValue("embed_row_cycle", "5"));
  }

  public int getWatermarkParamMaxRightBit() {
    return Integer.parseInt(getWatermarkParamValue("embed_lsb_num", "5"));
  }

  private String getWatermarkParamValue(String key, String defaultValue) {
    String res = getWatermarkParamValue(key);
    if (res != null) {
      return res;
    }
    return defaultValue;
  }

  private String getWatermarkParamValue(String key) {
    String pattern = key + "=(\\w*)";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(watermarkMethod);
    if (m.find() && m.groupCount() > 0) {
      return m.group(1);
    }
    return null;
  }

  public boolean isAutoCreateSchemaEnabled() {
    return enableAutoCreateSchema;
  }

  public void setAutoCreateSchemaEnabled(boolean enableAutoCreateSchema) {
    this.enableAutoCreateSchema = enableAutoCreateSchema;
  }

  public TSDataType getBooleanStringInferType() {
    return booleanStringInferType;
  }

  public void setBooleanStringInferType(
      TSDataType booleanStringInferType) {
    this.booleanStringInferType = booleanStringInferType;
  }

  public TSDataType getIntegerStringInferType() {
    return integerStringInferType;
  }

  public void setIntegerStringInferType(
      TSDataType integerStringInferType) {
    this.integerStringInferType = integerStringInferType;
  }

  public void setLongStringInferType(
      TSDataType longStringInferType) {
    this.longStringInferType = longStringInferType;
  }

  public TSDataType getLongStringInferType() {
    return longStringInferType;
  }

  public TSDataType getFloatingStringInferType() {
    return floatingStringInferType;
  }

  public void setFloatingStringInferType(
      TSDataType floatingNumberStringInferType) {
    this.floatingStringInferType = floatingNumberStringInferType;
  }

  public TSDataType getNanStringInferType() {
    return nanStringInferType;
  }

  public void setNanStringInferType(TSDataType nanStringInferType) {
    if (nanStringInferType != TSDataType.DOUBLE &&
        nanStringInferType != TSDataType.FLOAT &&
        nanStringInferType != TSDataType.TEXT) {
      throw new IllegalArgumentException(
          "Config Property nan_string_infer_type can only be FLOAT, DOUBLE or TEXT but is "
              + nanStringInferType);
    }
    this.nanStringInferType = nanStringInferType;
  }

  public int getDefaultStorageGroupLevel() {
    return defaultStorageGroupLevel;
  }

  void setDefaultStorageGroupLevel(int defaultStorageGroupLevel) {
    this.defaultStorageGroupLevel = defaultStorageGroupLevel;
  }

  public TSEncoding getDefaultBooleanEncoding() {
    return defaultBooleanEncoding;
  }

  public void setDefaultBooleanEncoding(TSEncoding defaultBooleanEncoding) {
    this.defaultBooleanEncoding = defaultBooleanEncoding;
  }

  void setDefaultBooleanEncoding(String defaultBooleanEncoding) {
    this.defaultBooleanEncoding = TSEncoding.valueOf(defaultBooleanEncoding);
  }

  public TSEncoding getDefaultInt32Encoding() {
    return defaultInt32Encoding;
  }

  public void setDefaultInt32Encoding(TSEncoding defaultInt32Encoding) {
    this.defaultInt32Encoding = defaultInt32Encoding;
  }

  void setDefaultInt32Encoding(String defaultInt32Encoding) {
    this.defaultInt32Encoding = TSEncoding.valueOf(defaultInt32Encoding);
  }

  public TSEncoding getDefaultInt64Encoding() {
    return defaultInt64Encoding;
  }

  public void setDefaultInt64Encoding(TSEncoding defaultInt64Encoding) {
    this.defaultInt64Encoding = defaultInt64Encoding;
  }

  void setDefaultInt64Encoding(String defaultInt64Encoding) {
    this.defaultInt64Encoding = TSEncoding.valueOf(defaultInt64Encoding);
  }

  public TSEncoding getDefaultFloatEncoding() {
    return defaultFloatEncoding;
  }

  public void setDefaultFloatEncoding(TSEncoding defaultFloatEncoding) {
    this.defaultFloatEncoding = defaultFloatEncoding;
  }

  void setDefaultFloatEncoding(String defaultFloatEncoding) {
    this.defaultFloatEncoding = TSEncoding.valueOf(defaultFloatEncoding);
  }

  public TSEncoding getDefaultDoubleEncoding() {
    return defaultDoubleEncoding;
  }

  public void setDefaultDoubleEncoding(TSEncoding defaultDoubleEncoding) {
    this.defaultDoubleEncoding = defaultDoubleEncoding;
  }

  void setDefaultDoubleEncoding(String defaultDoubleEncoding) {
    this.defaultDoubleEncoding = TSEncoding.valueOf(defaultDoubleEncoding);
  }

  public TSEncoding getDefaultTextEncoding() {
    return defaultTextEncoding;
  }

  public void setDefaultTextEncoding(TSEncoding defaultTextEncoding) {
    this.defaultTextEncoding = defaultTextEncoding;
  }

  void setDefaultTextEncoding(String defaultTextEncoding) {
    this.defaultTextEncoding = TSEncoding.valueOf(defaultTextEncoding);
  }

  public FSType getSystemFileStorageFs() {
    return systemFileStorageFs;
  }

  public void setSystemFileStorageFs(String systemFileStorageFs) {
    this.systemFileStorageFs = FSType.valueOf(systemFileStorageFs);
  }

  FSType getTsFileStorageFs() {
    return tsFileStorageFs;
  }

  void setTsFileStorageFs(String tsFileStorageFs) {
    this.tsFileStorageFs = FSType.valueOf(tsFileStorageFs);
  }

  String getCoreSitePath() {
    return coreSitePath;
  }

  void setCoreSitePath(String coreSitePath) {
    this.coreSitePath = coreSitePath;
  }

  String getHdfsSitePath() {
    return hdfsSitePath;
  }

  void setHdfsSitePath(String hdfsSitePath) {
    this.hdfsSitePath = hdfsSitePath;
  }

  public String[] getHdfsIp() {
    return hdfsIp.split(",");
  }

  String getRawHDFSIp() {
    return hdfsIp;
  }

  void setHdfsIp(String[] hdfsIp) {
    this.hdfsIp = String.join(",", hdfsIp);
  }

  String getHdfsPort() {
    return hdfsPort;
  }

  void setHdfsPort(String hdfsPort) {
    this.hdfsPort = hdfsPort;
  }

  public int getUpgradeThreadNum() {
    return upgradeThreadNum;
  }

  void setUpgradeThreadNum(int upgradeThreadNum) {
    this.upgradeThreadNum = upgradeThreadNum;
  }

  String getDfsNameServices() {
    return dfsNameServices;
  }

  void setDfsNameServices(String dfsNameServices) {
    this.dfsNameServices = dfsNameServices;
  }

  public String[] getDfsHaNamenodes() {
    return dfsHaNamenodes.split(",");
  }

  String getRawDfsHaNamenodes() {
    return dfsHaNamenodes;
  }

  void setDfsHaNamenodes(String[] dfsHaNamenodes) {
    this.dfsHaNamenodes = String.join(",", dfsHaNamenodes);
  }

  boolean isDfsHaAutomaticFailoverEnabled() {
    return dfsHaAutomaticFailoverEnabled;
  }

  void setDfsHaAutomaticFailoverEnabled(boolean dfsHaAutomaticFailoverEnabled) {
    this.dfsHaAutomaticFailoverEnabled = dfsHaAutomaticFailoverEnabled;
  }

  String getDfsClientFailoverProxyProvider() {
    return dfsClientFailoverProxyProvider;
  }

  void setDfsClientFailoverProxyProvider(String dfsClientFailoverProxyProvider) {
    this.dfsClientFailoverProxyProvider = dfsClientFailoverProxyProvider;
  }

  boolean isUseKerberos() {
    return useKerberos;
  }

  void setUseKerberos(boolean useKerberos) {
    this.useKerberos = useKerberos;
  }

  String getKerberosKeytabFilePath() {
    return kerberosKeytabFilePath;
  }

  void setKerberosKeytabFilePath(String kerberosKeytabFilePath) {
    this.kerberosKeytabFilePath = kerberosKeytabFilePath;
  }

  String getKerberosPrincipal() {
    return kerberosPrincipal;
  }

  void setKerberosPrincipal(String kerberosPrincipal) {
    this.kerberosPrincipal = kerberosPrincipal;
  }

  public long getDefaultTTL() {
    return defaultTTL;
  }

  public void setDefaultTTL(long defaultTTL) {
    this.defaultTTL = defaultTTL;
  }

  public int getThriftServerAwaitTimeForStopService() {
    return thriftServerAwaitTimeForStopService;
  }

  public void setThriftServerAwaitTimeForStopService(int thriftServerAwaitTimeForStopService) {
    this.thriftServerAwaitTimeForStopService = thriftServerAwaitTimeForStopService;
  }

  public int getQueryCacheSizeInMetric() {
    return queryCacheSizeInMetric;
  }

  public void setQueryCacheSizeInMetric(int queryCacheSizeInMetric) {
    this.queryCacheSizeInMetric = queryCacheSizeInMetric;
  }

  public boolean isEnableMQTTService() {
    return enableMQTTService;
  }

  public void setEnableMQTTService(boolean enableMQTTService) {
    this.enableMQTTService = enableMQTTService;
  }

  public String getMqttHost() {
    return mqttHost;
  }

  public void setMqttHost(String mqttHost) {
    this.mqttHost = mqttHost;
  }

  public int getMqttPort() {
    return mqttPort;
  }

  public void setMqttPort(int mqttPort) {
    this.mqttPort = mqttPort;
  }

  public int getMqttHandlerPoolSize() {
    return mqttHandlerPoolSize;
  }

  public void setMqttHandlerPoolSize(int mqttHandlerPoolSize) {
    this.mqttHandlerPoolSize = mqttHandlerPoolSize;
  }

  public String getMqttPayloadFormatter() {
    return mqttPayloadFormatter;
  }

  public void setMqttPayloadFormatter(String mqttPayloadFormatter) {
    this.mqttPayloadFormatter = mqttPayloadFormatter;
  }

  public int getMqttMaxMessageSize() {
    return mqttMaxMessageSize;
  }

  public void setMqttMaxMessageSize(int mqttMaxMessageSize) {
    this.mqttMaxMessageSize = mqttMaxMessageSize;
  }

  public int getTagAttributeTotalSize() {
    return tagAttributeTotalSize;
  }

  public void setTagAttributeTotalSize(int tagAttributeTotalSize) {
    this.tagAttributeTotalSize = tagAttributeTotalSize;
  }

  public int getPrimitiveArraySize() {
    return primitiveArraySize;
  }

  public void setPrimitiveArraySize(int primitiveArraySize) {
    this.primitiveArraySize = primitiveArraySize;
  }

  public String getOpenIdProviderUrl() {
    return openIdProviderUrl;
  }

  public void setOpenIdProviderUrl(String openIdProviderUrl) {
    this.openIdProviderUrl = openIdProviderUrl;
  }

  public String getAuthorizerProvider() {
    return authorizerProvider;
  }

  public void setAuthorizerProvider(String authorizerProvider) {
    this.authorizerProvider = authorizerProvider;
  }

  public long getStartUpNanosecond() {
    return startUpNanosecond;
  }

  public int getThriftMaxFrameSize() {
    return thriftMaxFrameSize;
  }

  public void setThriftMaxFrameSize(int thriftMaxFrameSize) {
    this.thriftMaxFrameSize = thriftMaxFrameSize;
  }

  public int getThriftInitBufferSize() {
    return thriftInitBufferSize;
  }

  public void setThriftInitBufferSize(int thriftInitBufferSize) {
    this.thriftInitBufferSize = thriftInitBufferSize;
  }

  public int getMaxQueryDeduplicatedPathNum() {
    return maxQueryDeduplicatedPathNum;
  }

  public void setMaxQueryDeduplicatedPathNum(int maxQueryDeduplicatedPathNum) {
    this.maxQueryDeduplicatedPathNum = maxQueryDeduplicatedPathNum;
  }

  public int getCheckPeriodWhenInsertBlocked() {
    return checkPeriodWhenInsertBlocked;
  }

  public void setCheckPeriodWhenInsertBlocked(int checkPeriodWhenInsertBlocked) {
    this.checkPeriodWhenInsertBlocked = checkPeriodWhenInsertBlocked;
  }

  public int getMaxWaitingTimeWhenInsertBlocked() {
    return maxWaitingTimeWhenInsertBlockedInMs;
  }

  public void setMaxWaitingTimeWhenInsertBlocked(int maxWaitingTimeWhenInsertBlocked) {
    this.maxWaitingTimeWhenInsertBlockedInMs = maxWaitingTimeWhenInsertBlocked;
  }

  public int getFrequencyIntervalInMinute() {
    return frequencyIntervalInMinute;
  }

  public void setFrequencyIntervalInMinute(int frequencyIntervalInMinute) {
    this.frequencyIntervalInMinute = frequencyIntervalInMinute;
  }

  public long getSlowQueryThreshold() {
    return slowQueryThreshold;
  }

  public void setSlowQueryThreshold(long slowQueryThreshold) {
    this.slowQueryThreshold = slowQueryThreshold;
  }

  public boolean isDebugOn() {
    return debugState;
  }

  public void setDebugState(boolean debugState) {
    this.debugState = debugState;
  }

  public boolean isEnableIndex() {
    return enableIndex;
  }

  public void setEnableIndex(boolean enableIndex) {
    this.enableIndex = enableIndex;
  }

  void setConcurrentIndexBuildThread(int concurrentIndexBuildThread) {
    this.concurrentIndexBuildThread = concurrentIndexBuildThread;
  }

  public int getConcurrentIndexBuildThread() {
    return concurrentIndexBuildThread;
  }

  public long getIndexBufferSize() {
    return indexBufferSize;
  }

  public void setIndexBufferSize(long indexBufferSize) {
    this.indexBufferSize = indexBufferSize;
  }

  public String getIndexRootFolder() {
    return indexRootFolder;
  }

  public void setIndexRootFolder(String indexRootFolder) {
    this.indexRootFolder = indexRootFolder;
  }

  public int getDefaultIndexWindowRange() {
    return defaultIndexWindowRange;
  }

  public void setDefaultIndexWindowRange(int defaultIndexWindowRange) {
    this.defaultIndexWindowRange = defaultIndexWindowRange;
  }

  public boolean isRpcAdvancedCompressionEnable() {
    return rpcAdvancedCompressionEnable;
  }

  public void setRpcAdvancedCompressionEnable(boolean rpcAdvancedCompressionEnable) {
    this.rpcAdvancedCompressionEnable = rpcAdvancedCompressionEnable;
    RpcTransportFactory.setUseSnappy(this.rpcAdvancedCompressionEnable);
  }

  public int getMlogBufferSize() {
    return mlogBufferSize;
  }

  public void setMlogBufferSize(int mlogBufferSize) {
    this.mlogBufferSize = mlogBufferSize;
  }
}
