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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.CompactionStrategy;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.utils.FilePathUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBDescriptor.class);
  private IoTDBConfig conf = new IoTDBConfig();
  private static CommandLine commandLine;

  protected IoTDBDescriptor() {
    loadProps();
  }

  public static IoTDBDescriptor getInstance() {
    return IoTDBDescriptorHolder.INSTANCE;
  }

  public IoTDBConfig getConfig() {
    return conf;
  }

  public void replaceProps(String[] params) {
    Options options = new Options();
    final String RPC_PORT = "rpc_port";
    Option rpcPort = new Option(RPC_PORT, RPC_PORT, true,
        "The jdbc service listens on the port");
    rpcPort.setRequired(false);
    options.addOption(rpcPort);

    boolean ok = parseCommandLine(options, params);
    if (!ok) {
      logger.error("replaces properties failed, use default conf params");
    } else {
      if (commandLine.hasOption(RPC_PORT)) {
        conf.setRpcPort(Integer.parseInt(commandLine.getOptionValue(RPC_PORT)));
        logger.debug("replace rpc port with={}", conf.getRpcPort());
      }
    }
  }

  private boolean parseCommandLine(Options options, String[] params) {
    try {
      CommandLineParser parser = new DefaultParser();
      commandLine = parser.parse(options, params);
    } catch (ParseException e) {
      logger.error("parse conf params failed, {}", e.toString());
      return false;
    }
    return true;
  }

  public URL getPropsUrl() {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(IoTDBConstant.IOTDB_CONF, null);
    // If it wasn't, check if a home directory was provided (This usually contains a config)
    if (urlString == null) {
      urlString = System.getProperty(IoTDBConstant.IOTDB_HOME, null);
      if (urlString != null) {
        urlString =
            urlString + File.separatorChar + "conf" + File.separatorChar + IoTDBConfig.CONFIG_NAME;
      } else {
        // If this too wasn't provided, try to find a default config in the root of the classpath.
        URL uri = IoTDBConfig.class.getResource("/" + IoTDBConfig.CONFIG_NAME);
        if (uri != null) {
          return uri;
        }
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            IoTDBConfig.CONFIG_NAME);
        // update all data seriesPath
        conf.updatePath();
        return null;
      }
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    else if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + IoTDBConfig.CONFIG_NAME);
    }

    // If the url doesn't start with "file:" or "classpath:", it's provided as a no path.
    // So we need to add it to make it a real URL.
    if (!urlString.startsWith("file:") && !urlString.startsWith("classpath:")) {
      urlString = "file:" + urlString;
    }
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      return null;
    }
  }

  /**
   * load an property file and set TsfileDBConfig variables.
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void loadProps() {
    URL url = getPropsUrl();
    if (url == null) {
      logger.warn("Couldn't load the configuration from any of the known sources.");
      return;
    }

    try (InputStream inputStream = url.openStream()) {

      logger.info("Start to read config file {}", url);
      Properties properties = new Properties();
      properties.load(inputStream);
      conf.setEnableStatMonitor(Boolean
          .parseBoolean(properties.getProperty("enable_stat_monitor",
              Boolean.toString(conf.isEnableStatMonitor()))));

      conf.setEnableMonitorSeriesWrite(Boolean
          .parseBoolean(properties.getProperty("enable_monitor_series_write",
              Boolean.toString(conf.isEnableStatMonitor()))));

      conf.setEnableMetricService(Boolean.parseBoolean(properties
          .getProperty("enable_metric_service", Boolean.toString(conf.isEnableMetricService()))));

      conf.setMetricsPort(Integer.parseInt(properties.getProperty("metrics_port",
          Integer.toString(conf.getMetricsPort()))));

      conf.setQueryCacheSizeInMetric(Integer
          .parseInt(properties.getProperty("query_cache_size_in_metric",
              Integer.toString(conf.getQueryCacheSizeInMetric()))
          ));

      conf.setRpcAddress(properties.getProperty("rpc_address", conf.getRpcAddress()));

      conf.setRpcThriftCompressionEnable(
          Boolean.parseBoolean(properties.getProperty("rpc_thrift_compression_enable",
              Boolean.toString(conf.isRpcThriftCompressionEnable()))));

      conf.setRpcAdvancedCompressionEnable(
          Boolean.parseBoolean(properties.getProperty("rpc_advanced_compression_enable",
              Boolean.toString(conf.isRpcAdvancedCompressionEnable()))));

      conf.setRpcPort(Integer.parseInt(properties.getProperty("rpc_port",
          Integer.toString(conf.getRpcPort()))));

      conf.setTimestampPrecision(properties.getProperty("timestamp_precision",
          conf.getTimestampPrecision()));

      conf.setBufferedArraysMemoryProportion(
          Double.parseDouble(properties.getProperty("buffered_arrays_memory_proportion",
              Double.toString(conf.getBufferedArraysMemoryProportion()))));

      conf.setFlushProportion(Double.parseDouble(properties.getProperty("flush_proportion",
          Double.toString(conf.getFlushProportion()))));

      conf.setForceFlushProportion(Double.parseDouble(properties.getProperty("force_flush_proportion",
          Double.toString(conf.getForceFlushProportion()))));

      conf.setRejectProportion(Double.parseDouble(properties.getProperty("reject_proportion",
          Double.toString(conf.getRejectProportion()))));

      conf.setStorageGroupSizeReportThreshold(
          Long.parseLong(properties.getProperty("storage_group_report_threshold",
              Long.toString(conf.getStorageGroupSizeReportThreshold()))));

      conf.setMetaDataCacheEnable(
          Boolean.parseBoolean(properties.getProperty("meta_data_cache_enable",
              Boolean.toString(conf.isMetaDataCacheEnable()))));

      conf.setEnableLastCache(Boolean.parseBoolean(properties.getProperty("enable_last_cache",
          Boolean.toString(conf.isLastCacheEnabled()))));

      initMemoryAllocate(properties);

      loadWALProps(properties);

      String systemDir = properties.getProperty("system_dir");
      if (systemDir == null) {
        systemDir = properties.getProperty("base_dir");
        if (systemDir != null) {
          systemDir = FilePathUtils.regularizePath(systemDir) + IoTDBConstant.SYSTEM_FOLDER_NAME;
        } else {
          systemDir = conf.getSystemDir();
        }
      }
      conf.setSystemDir(systemDir);

      conf.setSchemaDir(
          FilePathUtils.regularizePath(conf.getSystemDir()) + IoTDBConstant.SCHEMA_FOLDER_NAME);

      conf.setSyncDir(
          FilePathUtils.regularizePath(conf.getSystemDir()) + IoTDBConstant.SYNC_FOLDER_NAME);

      conf.setQueryDir(
          FilePathUtils.regularizePath(conf.getSystemDir() + IoTDBConstant.QUERY_FOLDER_NAME));

      conf.setTracingDir(properties.getProperty("tracing_dir", conf.getTracingDir()));

      conf.setDataDirs(properties.getProperty("data_dirs", conf.getDataDirs()[0])
          .split(","));

      conf.setWalDir(properties.getProperty("wal_dir", conf.getWalDir()));

      int walBufferSize = Integer.parseInt(properties.getProperty("wal_buffer_size",
          Integer.toString(conf.getWalBufferSize())));
      if (walBufferSize > 0) {
        conf.setWalBufferSize(walBufferSize);
      }

      int mlogBufferSize = Integer.parseInt(properties.getProperty("mlog_buffer_size",
          Integer.toString(conf.getMlogBufferSize())));
      if (mlogBufferSize > 0) {
        conf.setMlogBufferSize(mlogBufferSize);
      }

      conf.setMultiDirStrategyClassName(properties.getProperty("multi_dir_strategy",
          conf.getMultiDirStrategyClassName()));

      conf.setBatchSize(Integer.parseInt(properties.getProperty("batch_size",
          Integer.toString(conf.getBatchSize()))));

      conf.setEnableMemControl((Boolean
          .parseBoolean(properties.getProperty("enable_mem_control",
              Boolean.toString(conf.isEnableMemControl())))));
      logger.info("IoTDB enable memory control: {}", conf.isEnableMemControl());

      long tsfileSizeThreshold = Long.parseLong(properties
          .getProperty("tsfile_size_threshold",
              Long.toString(conf.getTsFileSizeThreshold())).trim());
      if (tsfileSizeThreshold >= 0) {
        conf.setTsFileSizeThreshold(tsfileSizeThreshold);
      }

      long memTableSizeThreshold = Long.parseLong(properties
          .getProperty("memtable_size_threshold",
              Long.toString(conf.getMemtableSizeThreshold())).trim());
      if (memTableSizeThreshold > 0) {
        conf.setMemtableSizeThreshold(memTableSizeThreshold);
      }

      conf.setAvgSeriesPointNumberThreshold(Integer.parseInt(properties
          .getProperty("avg_series_point_number_threshold",
              Integer.toString(conf.getAvgSeriesPointNumberThreshold()))));

      conf.setCheckPeriodWhenInsertBlocked(Integer.parseInt(properties
          .getProperty("check_period_when_insert_blocked",
              Integer.toString(conf.getCheckPeriodWhenInsertBlocked()))));

      conf.setMaxWaitingTimeWhenInsertBlocked(Integer.parseInt(properties
          .getProperty("max_waiting_time_when_insert_blocked",
              Integer.toString(conf.getMaxWaitingTimeWhenInsertBlocked()))));

      conf.setEstimatedSeriesSize(Integer.parseInt(properties
          .getProperty("estimated_series_size",
              Integer.toString(conf.getEstimatedSeriesSize()))));

      conf.setEnableSlidingMemTable(Boolean.parseBoolean(properties
          .getProperty("enable_sliding_mem_table",
              Boolean.toString(conf.isEnableSlidingMemTable()))));

      conf.setFlushWaitTime(Integer.parseInt(properties
          .getProperty("flush_wait_time",
              Integer.toString(conf.getFlushWaitTime()))));

      conf.setMergeChunkPointNumberThreshold(Integer.parseInt(properties
          .getProperty("merge_chunk_point_number",
              Integer.toString(conf.getMergeChunkPointNumberThreshold()))));

      conf.setMergePagePointNumberThreshold(Integer.parseInt(properties
          .getProperty("merge_page_point_number",
              Integer.toString(conf.getMergePagePointNumberThreshold()))));

      conf.setCompactionStrategy(CompactionStrategy.valueOf(properties
          .getProperty("compaction_strategy",
              conf.getCompactionStrategy().toString())));

      conf.setEnableUnseqCompaction(Boolean.parseBoolean(
          properties.getProperty("enable_unseq_compaction",
              Boolean.toString(conf.isEnableUnseqCompaction()))));

      conf.setSeqLevelNum(Integer.parseInt(properties
          .getProperty("seq_level_num",
              Integer.toString(conf.getSeqLevelNum()))));

      conf.setSeqFileNumInEachLevel(Integer.parseInt(properties
          .getProperty("seq_file_num_in_each_level",
              Integer.toString(conf.getSeqFileNumInEachLevel()))));

      conf.setUnseqLevelNum(Integer.parseInt(properties
          .getProperty("unseq_level_num",
              Integer.toString(conf.getUnseqLevelNum()))));

      conf.setUnseqFileNumInEachLevel(Integer.parseInt(properties
          .getProperty("unseq_file_num_in_each_level",
              Integer.toString(conf.getUnseqFileNumInEachLevel()))));

      conf.setSyncEnable(Boolean
          .parseBoolean(properties.getProperty("is_sync_enable",
              Boolean.toString(conf.isSyncEnable()))));

      conf.setSyncServerPort(Integer
          .parseInt(properties.getProperty("sync_server_port",
              Integer.toString(conf.getSyncServerPort())).trim()));

      conf.setIpWhiteList(properties.getProperty("ip_white_list", conf.getIpWhiteList()));

      conf.setConcurrentFlushThread(Integer
          .parseInt(properties.getProperty("concurrent_flush_thread",
              Integer.toString(conf.getConcurrentFlushThread()))));

      if (conf.getConcurrentFlushThread() <= 0) {
        conf.setConcurrentFlushThread(Runtime.getRuntime().availableProcessors());
      }

      // start: index parameter setting
      conf.setIndexRootFolder(properties.getProperty("index_root_dir", conf.getIndexRootFolder()));

      conf.setEnableIndex(Boolean.parseBoolean(properties.getProperty("enable_index",
          Boolean.toString(conf.isEnableIndex()))));

      conf.setConcurrentIndexBuildThread(Integer
          .parseInt(properties.getProperty("concurrent_index_build_thread",
              Integer.toString(conf.getConcurrentIndexBuildThread()))));
      if (conf.getConcurrentIndexBuildThread() <= 0) {
        conf.setConcurrentIndexBuildThread(Runtime.getRuntime().availableProcessors());
      }

      conf.setDefaultIndexWindowRange(Integer
          .parseInt(properties.getProperty("default_index_window_range",
              Integer.toString(conf.getDefaultIndexWindowRange()))));

      conf.setIndexBufferSize(Long
          .parseLong(properties.getProperty("index_buffer_size",
              Long.toString(conf.getIndexBufferSize()))));
      // end: index parameter setting

      conf.setConcurrentQueryThread(Integer
          .parseInt(properties.getProperty("concurrent_query_thread",
              Integer.toString(conf.getConcurrentQueryThread()))));

      if (conf.getConcurrentQueryThread() <= 0) {
        conf.setConcurrentQueryThread(Runtime.getRuntime().availableProcessors());
      }

      conf.setmManagerCacheSize(Integer
          .parseInt(properties.getProperty("metadata_node_cache_size",
              Integer.toString(conf.getmManagerCacheSize())).trim()));

      conf.setmRemoteSchemaCacheSize(Integer
          .parseInt(properties.getProperty("remote_schema_cache_size",
              Integer.toString(conf.getmRemoteSchemaCacheSize())).trim()));

      conf.setLanguageVersion(properties.getProperty("language_version",
          conf.getLanguageVersion()).trim());

      if (properties.containsKey("chunk_buffer_pool_enable")) {
        conf.setChunkBufferPoolEnable(Boolean
            .parseBoolean(properties.getProperty("chunk_buffer_pool_enable")));
      }

      conf.setEnableExternalSort(Boolean.parseBoolean(properties
          .getProperty("enable_external_sort", Boolean.toString(conf.isEnableExternalSort()))));
      conf.setExternalSortThreshold(Integer.parseInt(properties
          .getProperty("external_sort_threshold",
              Integer.toString(conf.getExternalSortThreshold()))));
      conf.setUpgradeThreadNum(Integer.parseInt(properties.getProperty("upgrade_thread_num",
          Integer.toString(conf.getUpgradeThreadNum()))));
      conf.setMergeMemoryBudget(Long.parseLong(properties.getProperty("merge_memory_budget",
          Long.toString(conf.getMergeMemoryBudget()))));
      conf.setMergeThreadNum(Integer.parseInt(properties.getProperty("merge_thread_num",
          Integer.toString(conf.getMergeThreadNum()))));
      conf.setMergeChunkSubThreadNum(Integer.parseInt(properties.getProperty
          ("merge_chunk_subthread_num",
              Integer.toString(conf.getMergeChunkSubThreadNum()))));
      conf.setContinueMergeAfterReboot(Boolean.parseBoolean(properties.getProperty(
          "continue_merge_after_reboot", Boolean.toString(conf.isContinueMergeAfterReboot()))));
      conf.setMergeFileSelectionTimeBudget(Long.parseLong(properties.getProperty
          ("merge_fileSelection_time_budget",
              Long.toString(conf.getMergeFileSelectionTimeBudget()))));
      conf.setMergeIntervalSec(Long.parseLong(properties.getProperty("merge_interval_sec",
          Long.toString(conf.getMergeIntervalSec()))));
      conf.setForceFullMerge(Boolean.parseBoolean(properties.getProperty("force_full_merge",
          Boolean.toString(conf.isForceFullMerge()))));
      conf.setCompactionThreadNum(Integer.parseInt(properties.getProperty(
          "compaction_thread_num", Integer.toString(conf.getCompactionThreadNum()))));
      conf.setMergeWriteThroughputMbPerSec(Integer.parseInt(properties.getProperty(
          "merge_write_throughput_mb_per_sec",
          Integer.toString(conf.getMergeWriteThroughputMbPerSec()))));

      conf.setEnablePartialInsert(
          Boolean.parseBoolean(properties.getProperty("enable_partial_insert",
              String.valueOf(conf.isEnablePartialInsert()))));

      conf.setEnableMTreeSnapshot(Boolean.parseBoolean(properties.getProperty(
          "enable_mtree_snapshot", Boolean.toString(conf.isEnableMTreeSnapshot()))));
      conf.setMtreeSnapshotInterval(Integer.parseInt(properties.getProperty(
          "mtree_snapshot_interval", Integer.toString(conf.getMtreeSnapshotInterval()))));
      conf.setMtreeSnapshotThresholdTime(Integer.parseInt(properties.getProperty(
          "mtree_snapshot_threshold_time",
          Integer.toString(conf.getMtreeSnapshotThresholdTime()))));

      conf.setEnablePerformanceStat(Boolean
          .parseBoolean(properties.getProperty("enable_performance_stat",
              Boolean.toString(conf.isEnablePerformanceStat())).trim()));

      conf.setEnablePerformanceTracing(Boolean
          .parseBoolean(properties.getProperty("enable_performance_tracing",
              Boolean.toString(conf.isEnablePerformanceTracing())).trim()));

      conf.setPerformanceStatDisplayInterval(Long
          .parseLong(properties.getProperty("performance_stat_display_interval",
              Long.toString(conf.getPerformanceStatDisplayInterval())).trim()));
      conf.setPerformanceStatMemoryInKB(Integer
          .parseInt(properties.getProperty("performance_stat_memory_in_kb",
              Integer.toString(conf.getPerformanceStatMemoryInKB())).trim()));

      int maxConcurrentClientNum = Integer.parseInt(properties.
          getProperty("rpc_max_concurrent_client_num",
              Integer.toString(conf.getRpcMaxConcurrentClientNum()).trim()));
      if (maxConcurrentClientNum <= 0) {
        maxConcurrentClientNum = 65535;
      }

      conf.setEnableWatermark(Boolean.parseBoolean(properties.getProperty("watermark_module_opened",
          Boolean.toString(conf.isEnableWatermark()).trim())));
      conf.setWatermarkSecretKey(
          properties.getProperty("watermark_secret_key", conf.getWatermarkSecretKey()));
      conf.setWatermarkBitString(
          properties.getProperty("watermark_bit_string", conf.getWatermarkBitString()));
      conf.setWatermarkMethod(
          properties.getProperty("watermark_method", conf.getWatermarkMethod()));

      loadAutoCreateSchemaProps(properties);

      conf.setRpcMaxConcurrentClientNum(maxConcurrentClientNum);

      conf.setTsFileStorageFs(properties.getProperty("tsfile_storage_fs",
          conf.getTsFileStorageFs().toString()));
      conf.setCoreSitePath(
          properties.getProperty("core_site_path", conf.getCoreSitePath()));
      conf.setHdfsSitePath(
          properties.getProperty("hdfs_site_path", conf.getHdfsSitePath()));
      conf.setHdfsIp(properties.getProperty("hdfs_ip", conf.getRawHDFSIp()).split(","));
      conf.setHdfsPort(properties.getProperty("hdfs_port", conf.getHdfsPort()));
      conf.setDfsNameServices(
          properties.getProperty("dfs_nameservices", conf.getDfsNameServices()));
      conf.setDfsHaNamenodes(
          properties.getProperty("dfs_ha_namenodes", conf.getRawDfsHaNamenodes()).split(","));
      conf.setDfsHaAutomaticFailoverEnabled(
          Boolean.parseBoolean(properties.getProperty("dfs_ha_automatic_failover_enabled",
              String.valueOf(conf.isDfsHaAutomaticFailoverEnabled()))));
      conf.setDfsClientFailoverProxyProvider(
          properties.getProperty("dfs_client_failover_proxy_provider",
              conf.getDfsClientFailoverProxyProvider()));
      conf.setUseKerberos(Boolean.parseBoolean(
          properties.getProperty("hdfs_use_kerberos", String.valueOf(conf.isUseKerberos()))));
      conf.setKerberosKeytabFilePath(
          properties.getProperty("kerberos_keytab_file_path", conf.getKerberosKeytabFilePath()));
      conf.setKerberosPrincipal(
          properties.getProperty("kerberos_principal", conf.getKerberosPrincipal()));

      conf.setDefaultTTL(Long.parseLong(properties.getProperty("default_ttl",
          String.valueOf(conf.getDefaultTTL()))));

//      conf.setEnablePartition(Boolean.parseBoolean(
//          properties.getProperty("enable_partition", String.valueOf(conf.isEnablePartition()))));

      // Time range for dividing storage group
//      conf.setPartitionInterval(Long.parseLong(properties
//              .getProperty("partition_interval", String.valueOf(conf.getPartitionInterval()))));

      // the num of memtables in each storage group
//      conf.setConcurrentWritingTimePartition(
//          Integer.parseInt(properties.getProperty("concurrent_writing_time_partition",
//              String.valueOf(conf.getConcurrentWritingTimePartition()))));

      // the default fill interval in LinearFill and PreviousFill
      conf.setDefaultFillInterval(
          Integer.parseInt(properties.getProperty("default_fill_interval",
              String.valueOf(conf.getDefaultFillInterval()))));

      conf.setTagAttributeTotalSize(
          Integer.parseInt(properties.getProperty("tag_attribute_total_size",
              String.valueOf(conf.getTagAttributeTotalSize())))
      );
      conf.setPrimitiveArraySize((Integer.parseInt(
          properties.getProperty(
              "primitive_array_size", String.valueOf(conf.getPrimitiveArraySize())))));

      conf.setThriftMaxFrameSize(Integer.parseInt(properties
          .getProperty("thrift_max_frame_size", String.valueOf(conf.getThriftMaxFrameSize()))));

      if (conf.getThriftMaxFrameSize() < IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2) {
        conf.setThriftMaxFrameSize(IoTDBConstant.LEFT_SIZE_IN_REQUEST * 2);
      }

      conf.setThriftInitBufferSize(Integer.parseInt(properties
          .getProperty("thrift_init_buffer_size", String.valueOf(conf.getThriftInitBufferSize()))));

      conf.setFrequencyIntervalInMinute(Integer.parseInt(properties
          .getProperty("frequency_interval_in_minute",
              String.valueOf(conf.getFrequencyIntervalInMinute()))));

      conf.setSlowQueryThreshold(Long.parseLong(properties
          .getProperty("slow_query_threshold", String.valueOf(conf.getSlowQueryThreshold()))));

      conf.setDebugState(Boolean.parseBoolean(properties
          .getProperty("debug_state", String.valueOf(conf.isDebugOn()))));

      // mqtt
      if (properties.getProperty(IoTDBConstant.MQTT_HOST_NAME) != null) {
        conf.setMqttHost(properties.getProperty(IoTDBConstant.MQTT_HOST_NAME));
      }
      if (properties.getProperty(IoTDBConstant.MQTT_PORT_NAME) != null) {
        conf.setMqttPort(Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_PORT_NAME)));
      }
      if (properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME) != null) {
        conf.setMqttHandlerPoolSize(
            Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_HANDLER_POOL_SIZE_NAME)));
      }
      if (properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME) != null) {
        conf.setMqttPayloadFormatter(
            properties.getProperty(IoTDBConstant.MQTT_PAYLOAD_FORMATTER_NAME));
      }
      if (properties.getProperty(IoTDBConstant.ENABLE_MQTT) != null) {
        conf.setEnableMQTTService(
            Boolean.parseBoolean(properties.getProperty(IoTDBConstant.ENABLE_MQTT)));
      }
      if (properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE) != null) {
        conf.setMqttMaxMessageSize(
            Integer.parseInt(properties.getProperty(IoTDBConstant.MQTT_MAX_MESSAGE_SIZE)));
      }

      conf.setAuthorizerProvider(properties.getProperty("authorizer_provider_class",
          "org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer"));
      //if using org.apache.iotdb.db.auth.authorizer.OpenIdAuthorizer, openID_url is needed.
      conf.setOpenIdProviderUrl(properties.getProperty("openID_url", ""));

      conf.setEnablePartition(Boolean.parseBoolean(properties.getProperty("enable_partition", conf.isEnablePartition() + "")));

      conf.setPartitionInterval(Long.parseLong(properties.getProperty("partition_interval", conf.getPartitionInterval() + "")));

      // At the same time, set TSFileConfig
      TSFileDescriptor.getInstance().getConfig()
          .setTSFileStorageFs(FSType.valueOf(
              properties.getProperty("tsfile_storage_fs", conf.getTsFileStorageFs().name())));
      TSFileDescriptor.getInstance().getConfig().setCoreSitePath(
          properties.getProperty("core_site_path", conf.getCoreSitePath()));
      TSFileDescriptor.getInstance().getConfig().setHdfsSitePath(
          properties.getProperty("hdfs_site_path", conf.getHdfsSitePath()));
      TSFileDescriptor.getInstance().getConfig()
          .setHdfsIp(properties.getProperty("hdfs_ip", conf.getRawHDFSIp()).split(","));
      TSFileDescriptor.getInstance().getConfig()
          .setHdfsPort(properties.getProperty("hdfs_port", conf.getHdfsPort()));
      TSFileDescriptor.getInstance().getConfig()
          .setDfsNameServices(
              properties.getProperty("dfs_nameservices", conf.getDfsNameServices()));
      TSFileDescriptor.getInstance().getConfig()
          .setDfsHaNamenodes(
              properties.getProperty("dfs_ha_namenodes", conf.getRawDfsHaNamenodes()).split(","));
      TSFileDescriptor.getInstance().getConfig().setDfsHaAutomaticFailoverEnabled(
          Boolean.parseBoolean(properties.getProperty("dfs_ha_automatic_failover_enabled",
              String.valueOf(conf.isDfsHaAutomaticFailoverEnabled()))));
      TSFileDescriptor.getInstance().getConfig().setDfsClientFailoverProxyProvider(
          properties.getProperty("dfs_client_failover_proxy_provider",
              conf.getDfsClientFailoverProxyProvider()));
      TSFileDescriptor.getInstance().getConfig().setUseKerberos(Boolean.parseBoolean(
          properties.getProperty("hdfs_use_kerberos", String.valueOf(conf.isUseKerberos()))));
      TSFileDescriptor.getInstance().getConfig().setKerberosKeytabFilePath(
          properties.getProperty("kerberos_keytab_file_path", conf.getKerberosKeytabFilePath()));
      TSFileDescriptor.getInstance().getConfig().setKerberosPrincipal(
          properties.getProperty("kerberos_principal", conf.getKerberosPrincipal()));
      TSFileDescriptor.getInstance().getConfig().setBatchSize(conf.getBatchSize());

      // set tsfile-format config
      loadTsFileProps(properties);

      // UDF
      loadUDFProps(properties);

    } catch (FileNotFoundException e) {
      logger.warn("Fail to find config file {}", url, e);
    } catch (IOException e) {
      logger.warn("Cannot load config file, use default configuration", e);
    } catch (Exception e) {
      logger.warn("Incorrect format in config file, use default configuration", e);
    } finally {
      // update all data seriesPath
      conf.updatePath();
    }
  }

  private void loadWALProps(Properties properties) {
    conf.setEnableWal(Boolean.parseBoolean(properties.getProperty("enable_wal",
        Boolean.toString(conf.isEnableWal()))));

    conf.setFlushWalThreshold(Integer
        .parseInt(properties.getProperty("flush_wal_threshold",
            Integer.toString(conf.getFlushWalThreshold()))));

    conf.setForceWalPeriodInMs(Long
        .parseLong(properties.getProperty("force_wal_period_in_ms",
            Long.toString(conf.getForceWalPeriodInMs()))));

    conf.setEnableDiscardOutOfOrderData(Boolean.parseBoolean(
        properties.getProperty("enable_discard_out_of_order_data",
            Boolean.toString(conf.isEnableDiscardOutOfOrderData()))));

  }

  private void loadAutoCreateSchemaProps(Properties properties) {
    conf.setAutoCreateSchemaEnabled(
        Boolean.parseBoolean(properties.getProperty("enable_auto_create_schema",
            Boolean.toString(conf.isAutoCreateSchemaEnabled()).trim())));
    conf.setBooleanStringInferType(
        TSDataType.valueOf(properties.getProperty("boolean_string_infer_type",
            conf.getBooleanStringInferType().toString())));
    conf.setIntegerStringInferType(
        TSDataType.valueOf(properties.getProperty("integer_string_infer_type",
            conf.getIntegerStringInferType().toString())));
    conf.setLongStringInferType(
        TSDataType.valueOf(properties.getProperty("long_string_infer_type",
            conf.getLongStringInferType().toString())));
    conf.setFloatingStringInferType(
        TSDataType.valueOf(properties.getProperty("floating_string_infer_type",
            conf.getFloatingStringInferType().toString())));
    conf.setNanStringInferType(TSDataType.valueOf(properties.getProperty("nan_string_infer_type",
        conf.getNanStringInferType().toString())));
    conf.setDefaultStorageGroupLevel(
        Integer.parseInt(properties.getProperty("default_storage_group_level",
            Integer.toString(conf.getDefaultStorageGroupLevel()))));
    conf.setDefaultBooleanEncoding(
        properties.getProperty("default_boolean_encoding",
            conf.getDefaultBooleanEncoding().toString()));
    conf.setDefaultInt32Encoding(
        properties.getProperty("default_int32_encoding",
            conf.getDefaultInt32Encoding().toString()));
    conf.setDefaultInt64Encoding(
        properties.getProperty("default_int64_encoding",
            conf.getDefaultInt64Encoding().toString()));
    conf.setDefaultFloatEncoding(
        properties.getProperty("default_float_encoding",
            conf.getDefaultFloatEncoding().toString()));
    conf.setDefaultDoubleEncoding(
        properties.getProperty("default_double_encoding",
            conf.getDefaultDoubleEncoding().toString()));
    conf.setDefaultTextEncoding(
        properties.getProperty("default_text_encoding",
            conf.getDefaultTextEncoding().toString()));
  }

  private void loadTsFileProps(Properties properties) {
    TSFileDescriptor.getInstance().getConfig().setGroupSizeInByte(Integer
        .parseInt(properties.getProperty("group_size_in_byte",
            Integer.toString(TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte()))));
    TSFileDescriptor.getInstance().getConfig().setPageSizeInByte(Integer
        .parseInt(properties.getProperty("page_size_in_byte",
            Integer.toString(TSFileDescriptor.getInstance().getConfig().getPageSizeInByte()))));
    if (TSFileDescriptor.getInstance().getConfig().getPageSizeInByte() > TSFileDescriptor
        .getInstance().getConfig().getGroupSizeInByte()) {
      logger
          .warn("page_size is greater than group size, will set it as the same with group size");
      TSFileDescriptor.getInstance().getConfig()
          .setPageSizeInByte(TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte());
    }
    TSFileDescriptor.getInstance().getConfig().setMaxNumberOfPointsInPage(Integer
        .parseInt(properties.getProperty("max_number_of_points_in_page",
            Integer.toString(
                TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage()))));
    TSFileDescriptor.getInstance().getConfig().setTimeSeriesDataType(properties
        .getProperty("time_series_data_type",
            TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType()));
    TSFileDescriptor.getInstance().getConfig().setMaxStringLength(Integer
        .parseInt(properties.getProperty("max_string_length",
            Integer.toString(TSFileDescriptor.getInstance().getConfig().getMaxStringLength()))));
    TSFileDescriptor.getInstance().getConfig().setBloomFilterErrorRate(Double
        .parseDouble(properties.getProperty("bloom_filter_error_rate",
            Double.toString(
                TSFileDescriptor.getInstance().getConfig().getBloomFilterErrorRate()))));
    TSFileDescriptor.getInstance().getConfig().setFloatPrecision(Integer
        .parseInt(properties
            .getProperty("float_precision", Integer
                .toString(TSFileDescriptor.getInstance().getConfig().getFloatPrecision()))));
    TSFileDescriptor.getInstance().getConfig().setTimeEncoder(properties
        .getProperty("time_encoder",
            TSFileDescriptor.getInstance().getConfig().getTimeEncoder()));
    TSFileDescriptor.getInstance().getConfig().setValueEncoder(properties
        .getProperty("value_encoder",
            TSFileDescriptor.getInstance().getConfig().getValueEncoder()));
    TSFileDescriptor.getInstance().getConfig().setCompressor(properties
        .getProperty("compressor",
            TSFileDescriptor.getInstance().getConfig().getCompressor().toString()));
    TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(Integer.parseInt(properties
        .getProperty("max_degree_of_index_node", Integer
            .toString(TSFileDescriptor.getInstance().getConfig().getMaxDegreeOfIndexNode()))));
  }

  public void loadHotModifiedProps(Properties properties)
      throws QueryProcessException {
    try {
      // update data dirs
      String dataDirs = properties.getProperty("data_dirs", null);
      if (dataDirs != null) {
        conf.reloadDataDirs(dataDirs.split(","));
      }

      // update dir strategy
      String multiDirStrategyClassName = properties.getProperty("multi_dir_strategy", null);
      if (multiDirStrategyClassName != null && !multiDirStrategyClassName
          .equals(conf.getMultiDirStrategyClassName())) {
        conf.setMultiDirStrategyClassName(multiDirStrategyClassName);
        DirectoryManager.getInstance().updateDirectoryStrategy();
      }

      // update WAL conf
      loadWALProps(properties);

      long tsfileSizeThreshold = Long.parseLong(properties
          .getProperty("tsfile_size_threshold",
              Long.toString(conf.getTsFileSizeThreshold())).trim());
      if (tsfileSizeThreshold >= 0) {
        conf.setTsFileSizeThreshold(tsfileSizeThreshold);
      }

      long memTableSizeThreshold = Long.parseLong(properties
          .getProperty("memtable_size_threshold",
              Long.toString(conf.getMemtableSizeThreshold())).trim());
      if (memTableSizeThreshold > 0) {
        conf.setMemtableSizeThreshold(memTableSizeThreshold);
      }

      // update params of creating schema automatically
      loadAutoCreateSchemaProps(properties);

      // update tsfile-format config
      loadTsFileProps(properties);

      // update max_deduplicated_path_num
      conf.setMaxQueryDeduplicatedPathNum(
          Integer.parseInt(properties.getProperty("max_deduplicated_path_num")));

      // update frequency_interval_in_minute
      conf.setFrequencyIntervalInMinute(
          Integer.parseInt(properties.getProperty("frequency_interval_in_minute")));

      // update slow_query_threshold
      conf.setSlowQueryThreshold(Long.parseLong(properties.getProperty("slow_query_threshold")));

      // update debug_state
      conf.setDebugState(Boolean.parseBoolean(properties.getProperty("debug_state")));

    } catch (Exception e) {
      throw new QueryProcessException(
          String.format("Fail to reload configuration because %s", e));
    }
  }

  public void loadHotModifiedProps() throws QueryProcessException {
    URL url = getPropsUrl();
    if (url == null) {
      logger.warn("Couldn't load the configuration from any of the known sources.");
      return;
    }

    try (InputStream inputStream = url.openStream()) {
      logger.info("Start to reload config file {}", url);
      Properties properties = new Properties();
      properties.load(inputStream);
      loadHotModifiedProps(properties);
    } catch (Exception e) {
      logger.warn("Fail to reload config file {}", url, e);
      throw new QueryProcessException(
          String.format("Fail to reload config file %s because %s", url, e.getMessage()));
    }
  }

  private void initMemoryAllocate(Properties properties) {
    String memoryAllocateProportion = properties
        .getProperty("write_read_schema_free_memory_proportion");
    if (memoryAllocateProportion != null) {
      String[] proportions = memoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = Runtime.getRuntime().maxMemory();
      if (proportionSum != 0) {
        conf.setAllocateMemoryForWrite(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        conf.setAllocateMemoryForRead(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        conf.setAllocateMemoryForSchema(
            maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
      }
    }

    logger.info("allocateMemoryForRead = {}", conf.getAllocateMemoryForRead());
    logger.info("allocateMemoryForWrite = {}", conf.getAllocateMemoryForWrite());
    logger.info("allocateMemoryForSchema = {}", conf.getAllocateMemoryForSchema());

    if (!conf.isMetaDataCacheEnable()) {
      return;
    }

    String queryMemoryAllocateProportion = properties
        .getProperty("chunkmeta_chunk_timeseriesmeta_free_memory_proportion");
    if (queryMemoryAllocateProportion != null) {
      String[] proportions = queryMemoryAllocateProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      long maxMemoryAvailable = conf.getAllocateMemoryForRead();
      if (proportionSum != 0) {
        try {
          conf.setAllocateMemoryForChunkMetaDataCache(
              maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
          conf.setAllocateMemoryForChunkCache(
              maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
          conf.setAllocateMemoryForTimeSeriesMetaDataCache(
              maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
          conf.setAllocateMemoryForReadWithoutCache(
              maxMemoryAvailable * Integer.parseInt(proportions[3].trim()) / proportionSum);
        } catch (Exception e) {
          throw new RuntimeException(
              "Each subsection of configuration item chunkmeta_chunk_timeseriesmeta_free_memory_proportion"
                  + " should be an integer, which is "
                  + queryMemoryAllocateProportion);
        }
      }

      conf.setMaxQueryDeduplicatedPathNum(
          Integer.parseInt(properties.getProperty("max_deduplicated_path_num")));
    }
  }

  @SuppressWarnings("squid:S3518") // "proportionSum" can't be zero
  private void loadUDFProps(Properties properties) {
    String initialByteArrayLengthForMemoryControl = properties
        .getProperty("udf_initial_byte_array_length_for_memory_control");
    if (initialByteArrayLengthForMemoryControl != null) {
      conf.setUdfInitialByteArrayLengthForMemoryControl(
          Integer.parseInt(initialByteArrayLengthForMemoryControl));
    }

    conf.setUdfDir(properties.getProperty("udf_root_dir", conf.getUdfDir()));

    String memoryBudgetInMb = properties.getProperty("udf_memory_budget_in_mb");
    if (memoryBudgetInMb != null) {
      conf.setUdfMemoryBudgetInMB((float) Math
          .min(Float.parseFloat(memoryBudgetInMb), 0.2 * conf.getAllocateMemoryForRead()));
    }

    String readerTransformerCollectorMemoryProportion = properties
        .getProperty("udf_reader_transformer_collector_memory_proportion");
    if (readerTransformerCollectorMemoryProportion != null) {
      String[] proportions = readerTransformerCollectorMemoryProportion.split(":");
      int proportionSum = 0;
      for (String proportion : proportions) {
        proportionSum += Integer.parseInt(proportion.trim());
      }
      float maxMemoryAvailable = conf.getUdfMemoryBudgetInMB();
      try {
        conf.setUdfReaderMemoryBudgetInMB(
            maxMemoryAvailable * Integer.parseInt(proportions[0].trim()) / proportionSum);
        conf.setUdfTransformerMemoryBudgetInMB(
            maxMemoryAvailable * Integer.parseInt(proportions[1].trim()) / proportionSum);
        conf.setUdfCollectorMemoryBudgetInMB(
            maxMemoryAvailable * Integer.parseInt(proportions[2].trim()) / proportionSum);
      } catch (Exception e) {
        throw new RuntimeException(
            "Each subsection of configuration item udf_reader_transformer_collector_memory_proportion"
                + " should be an integer, which is "
                + readerTransformerCollectorMemoryProportion);
      }
    }
  }

  /**
   * Get default encode algorithm by data type
   */
  public TSEncoding getDefualtEncodingByType(TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return conf.getDefaultBooleanEncoding();
      case INT32:
        return conf.getDefaultInt32Encoding();
      case INT64:
        return conf.getDefaultInt64Encoding();
      case FLOAT:
        return conf.getDefaultFloatEncoding();
      case DOUBLE:
        return conf.getDefaultDoubleEncoding();
      default:
        return conf.getDefaultTextEncoding();
    }
  }

  private static class IoTDBDescriptorHolder {

    private static final IoTDBDescriptor INSTANCE = new IoTDBDescriptor();
  }
}
