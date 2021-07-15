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
package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.engine.trigger.executor.TriggerEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.AlignedTimeseriesException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.DeleteFailedException;
import org.apache.iotdb.db.exception.metadata.DuplicatedTemplateException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.metadata.UndefinedTemplateException;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.logfile.TagLogFile;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.monitor.MonitorConstants;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.SetDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetUsingDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.dataset.ShowTimeSeriesResult;
import org.apache.iotdb.db.rescon.MemTableManager;
import org.apache.iotdb.db.utils.RandomDeleteCache;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.cache.CacheException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

/**
 * This class takes the responsibility of serialization of all the metadata info and persistent it
 * into files. This class contains all the interfaces to modify the metadata for delta system. All
 * the operations will be insert into the logs temporary in case the downtime of the delta system.
 */
@SuppressWarnings("java:S1135") // ignore todos
public class MManager {

  public static final String TIME_SERIES_TREE_HEADER = "===  Timeseries Tree  ===\n\n";
  private static final String TAG_FORMAT = "tag key is %s, tag value is %s, tlog offset is %d";
  private static final String DEBUG_MSG = "%s : TimeSeries %s is removed from tag inverted index, ";
  private static final String DEBUG_MSG_1 =
      "%s: TimeSeries %s's tag info has been removed from tag inverted index ";
  private static final String PREVIOUS_CONDITION =
      "before deleting it, tag key is %s, tag value is %s, tlog offset is %d, contains key %b";

  private static final Logger logger = LoggerFactory.getLogger(MManager.class);

  /** A thread will check whether the MTree is modified lately each such interval. Unit: second */
  private static final long MTREE_SNAPSHOT_THREAD_CHECK_TIME = 600L;

  private final int mtreeSnapshotInterval;
  private final long mtreeSnapshotThresholdTime;
  // the log file seriesPath
  private String logFilePath;
  private String mtreeSnapshotPath;
  private String mtreeSnapshotTmpPath;
  private MTree mtree;
  private MLogWriter logWriter;
  private TagLogFile tagLogFile;
  private boolean isRecovering;
  // device -> DeviceMNode
  private RandomDeleteCache<PartialPath, Pair<MNode, Template>> mNodeCache;
  // tag key -> tag value -> LeafMNode
  private Map<String, Map<String, Set<MeasurementMNode>>> tagIndex = new ConcurrentHashMap<>();

  private AtomicLong totalSeriesNumber = new AtomicLong();
  private boolean initialized;
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private File logFile;
  private ScheduledExecutorService timedCreateMTreeSnapshotThread;
  private ScheduledExecutorService timedForceMLogThread;

  /** threshold total size of MTree */
  private static final long MTREE_SIZE_THRESHOLD = config.getAllocateMemoryForSchema();

  private boolean allowToCreateNewSeries = true;

  private static final int ESTIMATED_SERIES_SIZE = config.getEstimatedSeriesSize();

  // template name -> template
  private Map<String, Template> templateMap = new ConcurrentHashMap<>();

  private static class MManagerHolder {

    private MManagerHolder() {
      // allowed to do nothing
    }

    private static final MManager INSTANCE = new MManager();
  }

  protected MManager() {
    mtreeSnapshotInterval = config.getMtreeSnapshotInterval();
    mtreeSnapshotThresholdTime = config.getMtreeSnapshotThresholdTime() * 1000L;
    String schemaDir = config.getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.info("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }
    logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;
    mtreeSnapshotPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT;
    mtreeSnapshotTmpPath = schemaDir + File.separator + MetadataConstant.MTREE_SNAPSHOT_TMP;

    // do not write log when recover
    isRecovering = true;

    int cacheSize = config.getmManagerCacheSize();
    mNodeCache =
        new RandomDeleteCache<PartialPath, Pair<MNode, Template>>(cacheSize) {

          @Override
          public Pair<MNode, Template> loadObjectByKey(PartialPath key) throws CacheException {
            try {
              return mtree.getNodeByPathWithStorageGroupCheck(key);
            } catch (MetadataException e) {
              throw new CacheException(e);
            }
          }
        };

    if (config.isEnableMTreeSnapshot()) {
      timedCreateMTreeSnapshotThread =
          Executors.newSingleThreadScheduledExecutor(
              r -> new Thread(r, "timedCreateMTreeSnapshotThread"));
      timedCreateMTreeSnapshotThread.scheduleAtFixedRate(
          this::checkMTreeModified,
          MTREE_SNAPSHOT_THREAD_CHECK_TIME,
          MTREE_SNAPSHOT_THREAD_CHECK_TIME,
          TimeUnit.SECONDS);
    }
  }

  /** we should not use this function in other place, but only in IoTDB class */
  public static MManager getInstance() {
    return MManagerHolder.INSTANCE;
  }

  // Because the writer will be used later and should not be closed here.
  @SuppressWarnings("squid:S2093")
  public synchronized void init() {
    if (initialized) {
      return;
    }
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);

    try {
      tagLogFile = new TagLogFile(config.getSchemaDir(), MetadataConstant.TAG_LOG);

      isRecovering = true;
      int lineNumber = initFromLog(logFile);

      logWriter = new MLogWriter(config.getSchemaDir(), MetadataConstant.METADATA_LOG);
      logWriter.setLogNum(lineNumber);
      isRecovering = false;
    } catch (IOException e) {
      logger.error(
          "Cannot recover all MTree from file, we try to recover as possible as we can", e);
    }
    initialized = true;
  }

  /** @return line number of the logFile */
  @SuppressWarnings("squid:S3776")
  private int initFromLog(File logFile) throws IOException {
    File tmpFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
    if (tmpFile.exists()) {
      logger.warn("Creating MTree snapshot not successful before crashing...");
      Files.delete(tmpFile.toPath());
    }

    File mtreeSnapshot = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
    long time = System.currentTimeMillis();
    if (!mtreeSnapshot.exists()) {
      mtree = new MTree();
    } else {
      mtree = MTree.deserializeFrom(mtreeSnapshot);
      logger.debug(
          "spend {} ms to deserialize mtree from snapshot", System.currentTimeMillis() - time);
    }

    time = System.currentTimeMillis();
    // init the metadata from the operation log
    if (logFile.exists()) {
      int idx = 0;
      try (MLogReader mLogReader =
          new MLogReader(config.getSchemaDir(), MetadataConstant.METADATA_LOG); ) {
        idx = applyMlog(mLogReader);
        logger.debug(
            "spend {} ms to deserialize mtree from mlog.bin", System.currentTimeMillis() - time);
        return idx;
      } catch (Exception e) {
        throw new IOException("Failed to parser mlog.bin for err:" + e);
      }
    } else {
      return 0;
    }
  }

  private int applyMlog(MLogReader mLogReader) {
    int idx = 0;

    while (mLogReader.hasNext()) {
      PhysicalPlan plan = null;
      try {
        plan = mLogReader.next();
        if (plan == null) {
          continue;
        }
        operation(plan);
        idx++;
      } catch (Exception e) {
        logger.error(
            "Can not operate cmd {} for err:", plan == null ? "" : plan.getOperatorType(), e);
      }
    }

    return idx;
  }

  /** function for clearing MTree */
  public void clear() {
    try {
      templateMap.clear();
      this.mtree = new MTree();
      this.mNodeCache.clear();
      this.tagIndex.clear();
      this.totalSeriesNumber.set(0);
      this.templateMap.clear();
      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
      if (tagLogFile != null) {
        tagLogFile.close();
        tagLogFile = null;
      }
      initialized = false;
      if (config.isEnableMTreeSnapshot() && timedCreateMTreeSnapshotThread != null) {
        timedCreateMTreeSnapshotThread.shutdownNow();
        timedCreateMTreeSnapshotThread = null;
      }
      if (timedForceMLogThread != null) {
        timedForceMLogThread.shutdownNow();
        timedForceMLogThread = null;
      }
    } catch (IOException e) {
      logger.error("Cannot close metadata log writer, because:", e);
    }
  }

  public void operation(PhysicalPlan plan) throws IOException, MetadataException {
    switch (plan.getOperatorType()) {
      case CREATE_TIMESERIES:
        CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) plan;
        createTimeseries(createTimeSeriesPlan, createTimeSeriesPlan.getTagOffset());
        break;
      case CREATE_ALIGNED_TIMESERIES:
        CreateAlignedTimeSeriesPlan createAlignedTimeSeriesPlan =
            (CreateAlignedTimeSeriesPlan) plan;
        createAlignedTimeSeries(createAlignedTimeSeriesPlan);
        break;
      case DELETE_TIMESERIES:
        DeleteTimeSeriesPlan deleteTimeSeriesPlan = (DeleteTimeSeriesPlan) plan;
        // cause we only has one path for one DeleteTimeSeriesPlan
        deleteTimeseries(deleteTimeSeriesPlan.getPaths().get(0));
        break;
      case SET_STORAGE_GROUP:
        SetStorageGroupPlan setStorageGroupPlan = (SetStorageGroupPlan) plan;
        setStorageGroup(setStorageGroupPlan.getPath());
        break;
      case DELETE_STORAGE_GROUP:
        DeleteStorageGroupPlan deleteStorageGroupPlan = (DeleteStorageGroupPlan) plan;
        deleteStorageGroups(deleteStorageGroupPlan.getPaths());
        break;
      case TTL:
        SetTTLPlan setTTLPlan = (SetTTLPlan) plan;
        setTTL(setTTLPlan.getStorageGroup(), setTTLPlan.getDataTTL());
        break;
      case CHANGE_ALIAS:
        ChangeAliasPlan changeAliasPlan = (ChangeAliasPlan) plan;
        changeAlias(changeAliasPlan.getPath(), changeAliasPlan.getAlias());
        break;
      case CHANGE_TAG_OFFSET:
        ChangeTagOffsetPlan changeTagOffsetPlan = (ChangeTagOffsetPlan) plan;
        changeOffset(changeTagOffsetPlan.getPath(), changeTagOffsetPlan.getOffset());
        break;
      case CREATE_TEMPLATE:
        CreateTemplatePlan createTemplatePlan = (CreateTemplatePlan) plan;
        createDeviceTemplate(createTemplatePlan);
        break;
      case SET_DEVICE_TEMPLATE:
        SetDeviceTemplatePlan setDeviceTemplatePlan = (SetDeviceTemplatePlan) plan;
        setDeviceTemplate(setDeviceTemplatePlan);
        break;
      case SET_USING_DEVICE_TEMPLATE:
        SetUsingDeviceTemplatePlan setUsingDeviceTemplatePlan = (SetUsingDeviceTemplatePlan) plan;
        setUsingDeviceTemplate(setUsingDeviceTemplatePlan);
        break;
      case AUTO_CREATE_DEVICE_MNODE:
        AutoCreateDeviceMNodePlan autoCreateDeviceMNodePlan = (AutoCreateDeviceMNodePlan) plan;
        autoCreateDeviceMNode(autoCreateDeviceMNodePlan);
        break;
      default:
        logger.error("Unrecognizable command {}", plan.getOperatorType());
    }
  }

  public void createContinuousQuery(CreateContinuousQueryPlan plan) throws IOException {
    logWriter.createContinuousQuery(plan);
  }

  public void dropContinuousQuery(DropContinuousQueryPlan plan) throws IOException {
    logWriter.dropContinuousQuery(plan);
  }

  public void createTimeseries(CreateTimeSeriesPlan plan) throws MetadataException {
    createTimeseries(plan, -1);
  }

  private void ensureStorageGroup(PartialPath path) throws MetadataException {
    try {
      mtree.getStorageGroupPath(path);
    } catch (StorageGroupNotSetException e) {
      if (!config.isAutoCreateSchemaEnabled()) {
        throw e;
      }
      PartialPath storageGroupPath =
          MetaUtils.getStorageGroupPathByLevel(path, config.getDefaultStorageGroupLevel());
      setStorageGroup(storageGroupPath);
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    if (!allowToCreateNewSeries) {
      throw new MetadataException(
          "IoTDB system load is too large to create timeseries, "
              + "please increase MAX_HEAP_SIZE in iotdb-env.sh/bat and restart");
    }
    try {
      PartialPath path = plan.getPath();
      SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

      ensureStorageGroup(path);

      TSDataType type = plan.getDataType();
      // create time series in MTree
      MeasurementMNode leafMNode =
          mtree.createTimeseries(
              path,
              type,
              plan.getEncoding(),
              plan.getCompressor(),
              plan.getProps(),
              plan.getAlias());

      // update tag index
      if (plan.getTags() != null) {
        // tag key, tag value
        for (Entry<String, String> entry : plan.getTags().entrySet()) {
          if (entry.getKey() == null || entry.getValue() == null) {
            continue;
          }
          tagIndex
              .computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
              .computeIfAbsent(entry.getValue(), v -> new CopyOnWriteArraySet<>())
              .add(leafMNode);
        }
      }

      // update statistics and schemaDataTypeNumMap
      totalSeriesNumber.addAndGet(1);
      if (totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE >= MTREE_SIZE_THRESHOLD) {
        logger.warn("Current series number {} is too large...", totalSeriesNumber);
        allowToCreateNewSeries = false;
      }

      // write log
      if (!isRecovering) {
        // either tags or attributes is not empty
        if ((plan.getTags() != null && !plan.getTags().isEmpty())
            || (plan.getAttributes() != null && !plan.getAttributes().isEmpty())) {
          offset = tagLogFile.write(plan.getTags(), plan.getAttributes());
        }
        plan.setTagOffset(offset);
        logWriter.createTimeseries(plan);
      }
      leafMNode.setOffset(offset);

    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Add one timeseries to metadata tree, if the timeseries already exists, throw exception
   *
   * @param path the timeseries path
   * @param dataType the dateType {@code DataType} of the timeseries
   * @param encoding the encoding function {@code Encoding} of the timeseries
   * @param compressor the compressor function {@code Compressor} of the time series
   */
  public void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props)
      throws MetadataException {
    try {
      createTimeseries(
          new CreateTimeSeriesPlan(path, dataType, encoding, compressor, props, null, null, null));
    } catch (PathAlreadyExistException | AliasAlreadyExistException e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Ignore PathAlreadyExistException and AliasAlreadyExistException when Concurrent inserting"
                + " a non-exist time series {}",
            path);
      }
    }
  }

  public void createAlignedTimeSeries(
      PartialPath prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      CompressionType compressor)
      throws MetadataException {
    createAlignedTimeSeries(
        new CreateAlignedTimeSeriesPlan(
            prefixPath, measurements, dataTypes, encodings, compressor, null));
  }

  /**
   * create aligned timeseries
   *
   * @param plan CreateAlignedTimeSeriesPlan
   */
  public void createAlignedTimeSeries(CreateAlignedTimeSeriesPlan plan) throws MetadataException {
    if (!allowToCreateNewSeries) {
      throw new MetadataException(
          "IoTDB system load is too large to create timeseries, "
              + "please increase MAX_HEAP_SIZE in iotdb-env.sh/bat and restart");
    }
    try {
      PartialPath prefixPath = plan.getPrefixPath();
      List<String> measurements = plan.getMeasurements();
      List<TSDataType> dataTypes = plan.getDataTypes();
      List<TSEncoding> encodings = plan.getEncodings();

      for (int i = 0; i < measurements.size(); i++) {
        SchemaUtils.checkDataTypeWithEncoding(dataTypes.get(i), encodings.get(i));
      }

      ensureStorageGroup(prefixPath);

      // create time series in MTree
      mtree.createAlignedTimeseries(
          prefixPath, measurements, plan.getDataTypes(), plan.getEncodings(), plan.getCompressor());

      // update statistics and schemaDataTypeNumMap
      totalSeriesNumber.addAndGet(measurements.size());
      if (totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE >= MTREE_SIZE_THRESHOLD) {
        logger.warn("Current series number {} is too large...", totalSeriesNumber);
        allowToCreateNewSeries = false;
      }
      // write log
      if (!isRecovering) {
        logWriter.createAlignedTimeseries(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Delete all timeseries under the given path, may cross different storage group
   *
   * @param prefixPath path to be deleted, could be root or a prefix path or a full path
   * @return deletion failed Timeseries
   */
  public String deleteTimeseries(PartialPath prefixPath) throws MetadataException {
    if (isStorageGroup(prefixPath)) {
      mNodeCache.clear();
    }
    try {
      List<PartialPath> allTimeseries = mtree.getAllTimeseriesPath(prefixPath);
      if (allTimeseries.isEmpty()) {
        throw new PathNotExistException(prefixPath.getFullPath());
      }

      // for not support deleting part of aligned timeseies
      // should be removed after partial deletion is supported
      MNode lastNode = getNodeByPath(allTimeseries.get(0));
      if (lastNode instanceof MeasurementMNode) {
        IMeasurementSchema schema = ((MeasurementMNode) lastNode).getSchema();
        if (schema instanceof VectorMeasurementSchema) {
          if (schema.getValueMeasurementIdList().size() != allTimeseries.size()) {
            throw new AlignedTimeseriesException(
                "Not support deleting part of aligned timeseies!", prefixPath.getFullPath());
          } else {
            allTimeseries.add(lastNode.getPartialPath());
          }
        }
      }

      // Monitor storage group seriesPath is not allowed to be deleted
      allTimeseries.removeIf(p -> p.startsWith(MonitorConstants.STAT_STORAGE_GROUP_ARRAY));

      Set<String> failedNames = new HashSet<>();
      for (PartialPath p : allTimeseries) {
        deleteSingleTimeseriesInternal(p, failedNames);
      }
      return failedNames.isEmpty() ? null : String.join(",", failedNames);
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  private void deleteSingleTimeseriesInternal(PartialPath p, Set<String> failedNames)
      throws MetadataException, IOException {
    DeleteTimeSeriesPlan deleteTimeSeriesPlan = new DeleteTimeSeriesPlan();
    try {
      PartialPath emptyStorageGroup = deleteOneTimeseriesUpdateStatisticsAndDropTrigger(p);
      if (!isRecovering) {
        if (emptyStorageGroup != null) {
          StorageEngine.getInstance().deleteAllDataFilesInOneStorageGroup(emptyStorageGroup);
          StorageEngine.getInstance()
              .releaseWalDirectByteBufferPoolInOneStorageGroup(emptyStorageGroup);
        }
        deleteTimeSeriesPlan.setDeletePathList(Collections.singletonList(p));
        logWriter.deleteTimeseries(deleteTimeSeriesPlan);
      }
    } catch (DeleteFailedException e) {
      failedNames.add(e.getName());
    }
  }

  /** remove the node from the tag inverted index */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void removeFromTagInvertedIndex(MeasurementMNode node) throws IOException {
    if (node.getOffset() < 0) {
      return;
    }
    Map<String, String> tagMap =
        tagLogFile.readTag(config.getTagAttributeTotalSize(), node.getOffset());
    if (tagMap != null) {
      for (Entry<String, String> entry : tagMap.entrySet()) {
        if (tagIndex.containsKey(entry.getKey())
            && tagIndex.get(entry.getKey()).containsKey(entry.getValue())) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                String.format(
                    String.format(DEBUG_MSG, "Delete" + TAG_FORMAT, node.getFullPath()),
                    entry.getKey(),
                    entry.getValue(),
                    node.getOffset()));
          }
          tagIndex.get(entry.getKey()).get(entry.getValue()).remove(node);
          if (tagIndex.get(entry.getKey()).get(entry.getValue()).isEmpty()) {
            tagIndex.get(entry.getKey()).remove(entry.getValue());
            if (tagIndex.get(entry.getKey()).isEmpty()) {
              tagIndex.remove(entry.getKey());
            }
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                String.format(
                    String.format(DEBUG_MSG_1, "Delete" + PREVIOUS_CONDITION, node.getFullPath()),
                    entry.getKey(),
                    entry.getValue(),
                    node.getOffset(),
                    tagIndex.containsKey(entry.getKey())));
          }
        }
      }
    }
  }

  /**
   * @param path full path from root to leaf node
   * @return After delete if the storage group is empty, return its path, otherwise return null
   */
  private PartialPath deleteOneTimeseriesUpdateStatisticsAndDropTrigger(PartialPath path)
      throws MetadataException, IOException {
    Pair<PartialPath, MeasurementMNode> pair =
        mtree.deleteTimeseriesAndReturnEmptyStorageGroup(path);
    // if one of the aligned timeseries is deleted, pair.right could be null
    IMeasurementSchema schema = pair.right.getSchema();
    int timeseriesNum = 0;
    if (schema instanceof MeasurementSchema) {
      removeFromTagInvertedIndex(pair.right);
      timeseriesNum = 1;
    } else if (schema instanceof VectorMeasurementSchema) {
      timeseriesNum += schema.getValueTSDataTypeList().size();
    }
    PartialPath storageGroupPath = pair.left;

    // drop trigger with no exceptions
    TriggerEngine.drop(pair.right);

    // TODO: delete the path node and all its ancestors
    mNodeCache.clear();
    totalSeriesNumber.addAndGet(-timeseriesNum);
    if (!allowToCreateNewSeries
        && totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE < MTREE_SIZE_THRESHOLD) {
      logger.info("Current series number {} come back to normal level", totalSeriesNumber);
      allowToCreateNewSeries = true;
    }
    return storageGroupPath;
  }

  /**
   * Set storage group of the given path to MTree.
   *
   * @param storageGroup root.node.(node)*
   */
  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    try {
      mtree.setStorageGroup(storageGroup);
      if (!config.isEnableMemControl()) {
        MemTableManager.getInstance().addOrDeleteStorageGroup(1);
      }
      if (!isRecovering) {
        logWriter.setStorageGroup(storageGroup);
      }
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * Delete storage groups of given paths from MTree. Log format: "delete_storage_group,sg1,sg2,sg3"
   *
   * @param storageGroups list of paths to be deleted. Format: root.node
   */
  public void deleteStorageGroups(List<PartialPath> storageGroups) throws MetadataException {
    try {
      for (PartialPath storageGroup : storageGroups) {
        totalSeriesNumber.addAndGet(-mtree.getAllTimeseriesCount(storageGroup));
        // clear cached MNode
        if (!allowToCreateNewSeries
            && totalSeriesNumber.get() * ESTIMATED_SERIES_SIZE < MTREE_SIZE_THRESHOLD) {
          logger.info("Current series number {} come back to normal level", totalSeriesNumber);
          allowToCreateNewSeries = true;
        }
        mNodeCache.clear();

        // try to delete storage group
        List<MeasurementMNode> leafMNodes = mtree.deleteStorageGroup(storageGroup);
        for (MeasurementMNode leafMNode : leafMNodes) {
          removeFromTagInvertedIndex(leafMNode);
        }

        // drop triggers with no exceptions
        TriggerEngine.drop(leafMNodes);

        if (!config.isEnableMemControl()) {
          MemTableManager.getInstance().addOrDeleteStorageGroup(-1);
        }

        // if success
        if (!isRecovering) {
          logWriter.deleteStorageGroup(storageGroup);
        }
      }
    } catch (IOException e) {
      throw new MetadataException(e.getMessage());
    }
  }

  /**
   * Check if the given path is storage group or not.
   *
   * @param path Format: root.node.(node)*
   * @apiNote :for cluster
   */
  public boolean isStorageGroup(PartialPath path) {
    return mtree.isStorageGroup(path);
  }

  /**
   * Get series type for given seriesPath.
   *
   * @param path full path
   */
  public TSDataType getSeriesType(PartialPath path) throws MetadataException {
    if (path.equals(SQLConstant.TIME_PATH)) {
      return TSDataType.INT64;
    }

    if (path instanceof VectorPartialPath) {
      if (((VectorPartialPath) path).getSubSensorsPathList().size() != 1) {
        return TSDataType.VECTOR;
      } else {
        path = ((VectorPartialPath) path).getSubSensorsPathList().get(0);
      }
    }
    IMeasurementSchema schema = mtree.getSchema(path);
    if (schema instanceof MeasurementSchema) {
      return schema.getType();
    } else {
      List<String> measurements = schema.getValueMeasurementIdList();
      return schema.getValueTSDataTypeList().get(measurements.indexOf(path.getMeasurement()));
    }
  }

  public MeasurementMNode[] getMNodes(PartialPath deviceId, String[] measurements)
      throws MetadataException {
    MeasurementMNode[] mNodes = new MeasurementMNode[measurements.length];
    for (int i = 0; i < mNodes.length; i++) {
      try {
        mNodes[i] = (MeasurementMNode) getNodeByPath(deviceId.concatNode(measurements[i]));
      } catch (PathNotExistException ignored) {
        logger.warn("{} does not exist in {}", measurements[i], deviceId);
      }
      if (mNodes[i] == null && !IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert()) {
        throw new MetadataException(measurements[i] + " does not exist in " + deviceId);
      }
    }
    return mNodes;
  }

  /**
   * Get all devices under given prefixPath.
   *
   * @param prefixPath a prefix of a full path. if the wildcard is not at the tail, then each
   *     wildcard can only match one level, otherwise it can match to the tail.
   * @return A HashSet instance which stores devices paths with given prefixPath.
   */
  public Set<PartialPath> getDevices(PartialPath prefixPath) throws MetadataException {
    return mtree.getDevices(prefixPath);
  }

  public List<ShowDevicesResult> getDevices(ShowDevicesPlan plan) throws MetadataException {
    return mtree.getDevices(plan);
  }

  /**
   * Get all nodes from the given level
   *
   * @param prefixPath can be a prefix of a full path. Can not be a full path. can not have
   *     wildcard. But, the level of the prefixPath can be smaller than the given level, e.g.,
   *     prefixPath = root.a while the given level is 5
   * @param nodeLevel the level can not be smaller than the level of the prefixPath
   * @return A List instance which stores all node at given level
   */
  public List<PartialPath> getNodesList(PartialPath prefixPath, int nodeLevel)
      throws MetadataException {
    return getNodesList(prefixPath, nodeLevel, null);
  }

  public List<PartialPath> getNodesList(
      PartialPath prefixPath, int nodeLevel, StorageGroupFilter filter) throws MetadataException {
    return mtree.getNodesList(prefixPath, nodeLevel, filter);
  }

  /**
   * Get storage group name by path
   *
   * <p>e.g., root.sg1 is a storage group and path = root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  public PartialPath getStorageGroupPath(PartialPath path) throws StorageGroupNotSetException {
    return mtree.getStorageGroupPath(path);
  }

  /** Get all storage group paths */
  public List<PartialPath> getAllStorageGroupPaths() {
    return mtree.getAllStorageGroupPaths();
  }

  public List<PartialPath> searchAllRelatedStorageGroups(PartialPath path)
      throws MetadataException {
    return mtree.searchAllRelatedStorageGroups(path);
  }

  /**
   * Get all storage group under given prefixPath.
   *
   * @param prefixPath a prefix of a full path. if the wildcard is not at the tail, then each
   *     wildcard can only match one level, otherwise it can match to the tail.
   * @return A ArrayList instance which stores storage group paths with given prefixPath.
   */
  public List<PartialPath> getStorageGroupPaths(PartialPath prefixPath) throws MetadataException {
    return mtree.getStorageGroupPaths(prefixPath);
  }

  /** Get all storage group MNodes */
  public List<StorageGroupMNode> getAllStorageGroupNodes() {
    return mtree.getAllStorageGroupNodes();
  }

  /**
   * Return all paths for given path if the path is abstract. Or return the path itself. Regular
   * expression in this method is formed by the amalgamation of seriesPath and the character '*'.
   *
   * @param prefixPath can be a prefix or a full path. if the wildcard is not at the tail, then each
   *     wildcard can only match one level, otherwise it can match to the tail.
   */
  public List<PartialPath> getAllTimeseriesPath(PartialPath prefixPath) throws MetadataException {
    return mtree.getAllTimeseriesPath(prefixPath);
  }

  /** Similar to method getAllTimeseriesPath(), but return Path with alias alias. */
  public Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(
      PartialPath prefixPath, int limit, int offset) throws MetadataException {
    return mtree.getAllTimeseriesPathWithAlias(prefixPath, limit, offset);
  }

  /** To calculate the count of timeseries for given prefix path. */
  public int getAllTimeseriesCount(PartialPath prefixPath) throws MetadataException {
    return mtree.getAllTimeseriesCount(prefixPath);
  }

  /** To calculate the count of devices for given prefix path. */
  public int getDevicesNum(PartialPath prefixPath) throws MetadataException {
    return mtree.getDevicesNum(prefixPath);
  }

  /** To calculate the count of storage group for given prefix path. */
  public int getStorageGroupNum(PartialPath prefixPath) throws MetadataException {
    return mtree.getStorageGroupNum(prefixPath);
  }

  /**
   * To calculate the count of nodes in the given level for given prefix path.
   *
   * @param prefixPath a prefix path or a full path, can not contain '*'
   * @param level the level can not be smaller than the level of the prefixPath
   */
  public int getNodesCountInGivenLevel(PartialPath prefixPath, int level) throws MetadataException {
    return mtree.getNodesCountInGivenLevel(prefixPath, level);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private List<ShowTimeSeriesResult> showTimeseriesWithIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    if (!tagIndex.containsKey(plan.getKey())) {
      throw new MetadataException("The key " + plan.getKey() + " is not a tag.", true);
    }
    Map<String, Set<MeasurementMNode>> value2Node = tagIndex.get(plan.getKey());
    if (value2Node.isEmpty()) {
      throw new MetadataException("The key " + plan.getKey() + " is not a tag.");
    }

    List<MeasurementMNode> allMatchedNodes = new ArrayList<>();
    if (plan.isContains()) {
      for (Entry<String, Set<MeasurementMNode>> entry : value2Node.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          continue;
        }
        String tagValue = entry.getKey();
        if (tagValue.contains(plan.getValue())) {
          allMatchedNodes.addAll(entry.getValue());
        }
      }
    } else {
      for (Entry<String, Set<MeasurementMNode>> entry : value2Node.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          continue;
        }
        String tagValue = entry.getKey();
        if (plan.getValue().equals(tagValue)) {
          allMatchedNodes.addAll(entry.getValue());
        }
      }
    }

    // if ordered by heat, we sort all the timeseries by the descending order of the last insert
    // timestamp
    if (plan.isOrderByHeat()) {
      List<StorageGroupProcessor> list;
      try {
        list =
            StorageEngine.getInstance()
                .mergeLock(allMatchedNodes.stream().map(MNode::getPartialPath).collect(toList()));
        try {
          allMatchedNodes =
              allMatchedNodes.stream()
                  .sorted(
                      Comparator.comparingLong(
                              (MeasurementMNode mNode) -> MTree.getLastTimeStamp(mNode, context))
                          .reversed()
                          .thenComparing(MNode::getFullPath))
                  .collect(toList());
        } finally {
          StorageEngine.getInstance().mergeUnLock(list);
        }
      } catch (StorageEngineException e) {
        throw new MetadataException(e);
      }
    } else {
      // otherwise, we just sort them by the alphabetical order
      allMatchedNodes =
          allMatchedNodes.stream()
              .sorted(Comparator.comparing(MNode::getFullPath))
              .collect(toList());
    }

    List<ShowTimeSeriesResult> res = new LinkedList<>();
    String[] prefixNodes = plan.getPath().getNodes();
    int curOffset = -1;
    int count = 0;
    int limit = plan.getLimit();
    int offset = plan.getOffset();
    for (MeasurementMNode leaf : allMatchedNodes) {
      if (match(leaf.getPartialPath(), prefixNodes)) {
        if (limit != 0 || offset != 0) {
          curOffset++;
          if (curOffset < offset || count == limit) {
            continue;
          }
        }
        try {
          Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
              tagLogFile.read(config.getTagAttributeTotalSize(), leaf.getOffset());
          IMeasurementSchema measurementSchema = leaf.getSchema();
          res.add(
              new ShowTimeSeriesResult(
                  leaf.getFullPath(),
                  leaf.getAlias(),
                  getStorageGroupPath(leaf.getPartialPath()).getFullPath(),
                  measurementSchema.getType(),
                  measurementSchema.getEncodingType(),
                  measurementSchema.getCompressor(),
                  tagAndAttributePair.left,
                  tagAndAttributePair.right));
          if (limit != 0) {
            count++;
          }
        } catch (IOException e) {
          throw new MetadataException(
              "Something went wrong while deserialize tag info of " + leaf.getFullPath(), e);
        }
      }
    }
    return res;
  }

  /** whether the full path has the prefixNodes */
  private boolean match(PartialPath fullPath, String[] prefixNodes) {
    String[] nodes = fullPath.getNodes();
    if (nodes.length < prefixNodes.length) {
      return false;
    }
    for (int i = 0; i < prefixNodes.length; i++) {
      if (!"*".equals(prefixNodes[i]) && !prefixNodes[i].equals(nodes[i])) {
        return false;
      }
    }
    return true;
  }

  public List<ShowTimeSeriesResult> showTimeseries(ShowTimeSeriesPlan plan, QueryContext context)
      throws MetadataException {
    // show timeseries with index
    if (plan.getKey() != null && plan.getValue() != null) {
      return showTimeseriesWithIndex(plan, context);
    } else {
      return showTimeseriesWithoutIndex(plan, context);
    }
  }

  /**
   * Get the result of ShowTimeseriesPlan
   *
   * @param plan show time series query plan
   */
  private List<ShowTimeSeriesResult> showTimeseriesWithoutIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    List<Pair<PartialPath, String[]>> ans;
    if (plan.isOrderByHeat()) {
      ans = mtree.getAllMeasurementSchemaByHeatOrder(plan, context);
    } else {
      ans = mtree.getAllMeasurementSchema(plan);
    }
    List<ShowTimeSeriesResult> res = new LinkedList<>();
    for (Pair<PartialPath, String[]> ansString : ans) {
      long tagFileOffset = Long.parseLong(ansString.right[5]);
      try {
        Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
            new Pair<>(Collections.emptyMap(), Collections.emptyMap());
        if (tagFileOffset >= 0) {
          tagAndAttributePair = tagLogFile.read(config.getTagAttributeTotalSize(), tagFileOffset);
        }
        res.add(
            new ShowTimeSeriesResult(
                ansString.left.getFullPath(),
                ansString.right[0],
                ansString.right[1],
                TSDataType.valueOf(ansString.right[2]),
                TSEncoding.valueOf(ansString.right[3]),
                CompressionType.valueOf(ansString.right[4]),
                tagAndAttributePair.left,
                tagAndAttributePair.right));
      } catch (IOException e) {
        throw new MetadataException(
            "Something went wrong while deserialize tag info of " + ansString.left.getFullPath(),
            e);
      }
    }
    return res;
  }

  /**
   * get MeasurementSchema or VectorMeasurementSchema which contains the measurement
   *
   * @param device device path
   * @param measurement measurement name, could be vector name
   * @return MeasurementSchema or VectorMeasurementSchema
   */
  public IMeasurementSchema getSeriesSchema(PartialPath device, String measurement)
      throws MetadataException {
    MNode deviceMNode = getDeviceNode(device);
    MeasurementMNode measurementMNode = (MeasurementMNode) deviceMNode.getChild(measurement);
    if (measurementMNode == null) {
      // Just for the initial adaptation of the template functionality and merge functionality
      // The getSeriesSchema interface needs to be cleaned up later
      return getSeriesSchema(device.concatNode(measurement));
    }
    return measurementMNode.getSchema();
  }

  /**
   * Get schema of paritialPath
   *
   * @param fullPath (may be ParitialPath or VectorPartialPath)
   * @return MeasurementSchema or VectorMeasurementSchema
   */
  public IMeasurementSchema getSeriesSchema(PartialPath fullPath) throws MetadataException {
    MeasurementMNode leaf = (MeasurementMNode) mtree.getNodeByPath(fullPath);
    return getSeriesSchema(fullPath, leaf);
  }

  protected IMeasurementSchema getSeriesSchema(PartialPath fullPath, MeasurementMNode leaf) {
    IMeasurementSchema schema = leaf.getSchema();

    if (schema == null || schema.getType() != TSDataType.VECTOR) {
      return schema;
    }
    List<String> measurementsInLeaf = schema.getValueMeasurementIdList();
    List<PartialPath> measurements = ((VectorPartialPath) fullPath).getSubSensorsPathList();
    TSDataType[] types = new TSDataType[measurements.size()];
    TSEncoding[] encodings = new TSEncoding[measurements.size()];

    for (int i = 0; i < measurements.size(); i++) {
      int index = measurementsInLeaf.indexOf(measurements.get(i).getMeasurement());
      types[i] = schema.getValueTSDataTypeList().get(index);
      encodings[i] = schema.getValueTSEncodingList().get(index);
    }
    String[] array = new String[measurements.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = measurements.get(i).getMeasurement();
    }
    return new VectorMeasurementSchema(
        schema.getMeasurementId(), array, types, encodings, schema.getCompressor());
  }

  /**
   * Transform the PartialPath to VectorPartialPath if it is a sub sensor of one vector. otherwise,
   * we don't change it.
   */
  public PartialPath transformPath(PartialPath partialPath) throws MetadataException {
    MeasurementMNode node = (MeasurementMNode) getNodeByPath(partialPath);
    if (node.getSchema() instanceof MeasurementSchema) {
      return partialPath;
    } else {
      return toVectorPath(partialPath);
    }
  }

  /** Convert the PartialPath to VectorPartialPath. */
  protected VectorPartialPath toVectorPath(PartialPath partialPath) throws MetadataException {
    List<PartialPath> subSensorsPathList = new ArrayList<>();
    subSensorsPathList.add(partialPath);
    return new VectorPartialPath(partialPath.getDevice(), subSensorsPathList);
  }

  /**
   * Get schema of partialPaths, in which aligned timeseries should only organized to one schema.
   * This method should be called when logical plan converts to physical plan.
   *
   * @param fullPaths full path list without pointing out which timeseries are aligned. For example,
   *     maybe (s1,s2) are aligned, but the input could be [root.sg1.d1.s1, root.sg1.d1.s2]
   * @return Size of partial path list could NOT equal to the input list size. For example, the
   *     VectorMeasurementSchema (s1,s2) would be returned once; Size of integer list must equal to
   *     the input list size. It indicates the index of elements of original list in the result list
   */
  public Pair<List<PartialPath>, Map<String, Integer>> getSeriesSchemas(List<PartialPath> fullPaths)
      throws MetadataException {
    Map<MNode, PartialPath> nodeToPartialPath = new LinkedHashMap<>();
    Map<MNode, List<Integer>> nodeToIndex = new LinkedHashMap<>();
    for (int i = 0; i < fullPaths.size(); i++) {
      PartialPath path = fullPaths.get(i);
      // use dfs to collect paths
      MeasurementMNode node = (MeasurementMNode) getNodeByPath(path);
      getNodeToPartialPath(node, nodeToPartialPath, nodeToIndex, path, i);
    }
    return getPair(fullPaths, nodeToPartialPath, nodeToIndex);
  }

  protected void getNodeToPartialPath(
      MeasurementMNode node,
      Map<MNode, PartialPath> nodeToPartialPath,
      Map<MNode, List<Integer>> nodeToIndex,
      PartialPath path,
      int index)
      throws MetadataException {
    if (!nodeToPartialPath.containsKey(node)) {
      if (node.getSchema() instanceof MeasurementSchema) {
        nodeToPartialPath.put(node, path);
      } else {
        List<PartialPath> subSensorsPathList = new ArrayList<>();
        subSensorsPathList.add(path);
        nodeToPartialPath.put(node, new VectorPartialPath(node.getFullPath(), subSensorsPathList));
      }
      nodeToIndex.computeIfAbsent(node, k -> new ArrayList<>()).add(index);
    } else {
      // if nodeToPartialPath contains node
      String existPath = nodeToPartialPath.get(node).getFullPath();
      if (existPath.equals(path.getFullPath())) {
        // could be the same path in different aggregate functions
        nodeToIndex.get(node).add(index);
      } else {
        // could be VectorPartialPath
        ((VectorPartialPath) nodeToPartialPath.get(node)).addSubSensor(path);
        nodeToIndex.get(node).add(index);
      }
    }
  }

  protected Pair<List<PartialPath>, Map<String, Integer>> getPair(
      List<PartialPath> fullPaths,
      Map<MNode, PartialPath> nodeToPartialPath,
      Map<MNode, List<Integer>> nodeToIndex)
      throws MetadataException {
    Map<String, Integer> indexMap = new HashMap<>();
    int i = 0;
    for (List<Integer> indexList : nodeToIndex.values()) {
      for (int index : indexList) {
        PartialPath partialPath = fullPaths.get(i);
        if (indexMap.containsKey(partialPath.getFullPath())) {
          throw new MetadataException(
              "Query for measurement and its alias at the same time!", true);
        }
        indexMap.put(partialPath.getFullPath(), index);
        if (partialPath.isMeasurementAliasExists()) {
          indexMap.put(partialPath.getFullPathWithAlias(), index);
        }
        i++;
      }
    }
    return new Pair<>(new ArrayList<>(nodeToPartialPath.values()), indexMap);
  }

  /**
   * Get child node path in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @param path The given path
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  public Set<String> getChildNodePathInNextLevel(PartialPath path) throws MetadataException {
    return mtree.getChildNodePathInNextLevel(path);
  }

  /**
   * Get child node in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [d1, d2] given path = root.sg.d1 return [s1,s2]
   *
   * @return All child nodes of given seriesPath.
   */
  public Set<String> getChildNodeInNextLevel(PartialPath path) throws MetadataException {
    return mtree.getChildNodeInNextLevel(path);
  }

  /**
   * Check whether the path exists.
   *
   * @param path a full path or a prefix path
   */
  public boolean isPathExist(PartialPath path) {
    return mtree.isPathExist(path);
  }

  /** Get node by path */
  public MNode getNodeByPath(PartialPath path) throws MetadataException {
    return mtree.getNodeByPath(path);
  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node by path. If storage group is not
   * set, StorageGroupNotSetException will be thrown
   */
  public StorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    return mtree.getStorageGroupNodeByStorageGroupPath(path);
  }

  /** Get storage group node by path. the give path don't need to be storage group path. */
  public StorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    return mtree.getStorageGroupNodeByPath(path);
  }

  /**
   * get device node, if the storage group is not set, create it when autoCreateSchema is true
   *
   * <p>(we develop this method as we need to get the node's lock after we get the lock.writeLock())
   *
   * @param path path
   * @param allowCreateSg The stand-alone version can create an sg at will, but the cluster version
   *     needs to make the Meta group aware of the creation of an SG, so an exception needs to be
   *     thrown here
   */
  public Pair<MNode, Template> getDeviceNodeWithAutoCreate(
      PartialPath path, boolean autoCreateSchema, boolean allowCreateSg, int sgLevel)
      throws IOException, MetadataException {
    Pair<MNode, Template> node;
    boolean shouldSetStorageGroup;
    try {
      node = mNodeCache.get(path);
      return node;
    } catch (CacheException e) {
      if (!autoCreateSchema) {
        throw new PathNotExistException(path.getFullPath());
      }
      shouldSetStorageGroup = e.getCause() instanceof StorageGroupNotSetException;
    }

    try {
      if (shouldSetStorageGroup) {
        if (allowCreateSg) {
          PartialPath storageGroupPath = MetaUtils.getStorageGroupPathByLevel(path, sgLevel);
          setStorageGroup(storageGroupPath);
        } else {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
      }
      node = mtree.getDeviceNodeWithAutoCreating(path, sgLevel);
      if (!(node.left instanceof StorageGroupMNode)) {
        logWriter.autoCreateDeviceMNode(new AutoCreateDeviceMNodePlan(node.left.getPartialPath()));
      }
      return node;
    } catch (StorageGroupAlreadySetException e) {
      // ignore set storage group concurrently
      node = mtree.getDeviceNodeWithAutoCreating(path, sgLevel);
      if (!(node.left instanceof StorageGroupMNode)) {
        logWriter.autoCreateDeviceMNode(new AutoCreateDeviceMNodePlan(node.left.getPartialPath()));
      }
      return node;
    }
  }

  /** !!!!!!Attention!!!!! must call the return node's readUnlock() if you call this method. */
  public Pair<MNode, Template> getDeviceNodeWithAutoCreate(PartialPath path)
      throws MetadataException, IOException {
    return getDeviceNodeWithAutoCreate(
        path, config.isAutoCreateSchemaEnabled(), true, config.getDefaultStorageGroupLevel());
  }

  // attention: this path must be a device node
  public List<IMeasurementSchema> getAllMeasurementByDevicePath(PartialPath path)
      throws PathNotExistException {
    Set<IMeasurementSchema> res = new HashSet<>();
    try {
      Pair<MNode, Template> mNodeTemplatePair = mNodeCache.get(path);
      if (mNodeTemplatePair.left.getDeviceTemplate() != null) {
        mNodeTemplatePair.right = mNodeTemplatePair.left.getDeviceTemplate();
      }

      for (MNode mNode : mNodeTemplatePair.left.getChildren().values()) {
        MeasurementMNode measurementMNode = (MeasurementMNode) mNode;
        res.add(measurementMNode.getSchema());
      }

      // template
      if (mNodeTemplatePair.left.isUseTemplate() && mNodeTemplatePair.right != null) {
        res.addAll(mNodeTemplatePair.right.getSchemaMap().values());
      }
    } catch (CacheException e) {
      throw new PathNotExistException(path.getFullPath());
    }

    return new ArrayList<>(res);
  }

  public MNode getDeviceNode(PartialPath path) throws MetadataException {
    MNode node;
    try {
      node = mNodeCache.get(path).left;
      return node;
    } catch (CacheException e) {
      throw new PathNotExistException(path.getFullPath());
    }
  }

  /**
   * To reduce the String number in memory, use the deviceId from MManager instead of the deviceId
   * read from disk
   *
   * @param path read from disk
   * @return deviceId
   */
  public String getDeviceId(PartialPath path) {
    String device = null;
    try {
      MNode deviceNode = getDeviceNode(path);
      device = deviceNode.getFullPath();
    } catch (MetadataException | NullPointerException e) {
      // Cannot get deviceId from MManager, return the input deviceId
    }
    return device;
  }

  /** Get metadata in string */
  public String getMetadataInString() {
    return TIME_SERIES_TREE_HEADER + mtree;
  }

  public void setTTL(PartialPath storageGroup, long dataTTL) throws MetadataException, IOException {
    getStorageGroupNodeByStorageGroupPath(storageGroup).setDataTTL(dataTTL);
    if (!isRecovering) {
      logWriter.setTTL(storageGroup, dataTTL);
    }
  }

  /**
   * get all storageGroups ttl
   *
   * @return key-> storageGroupPath, value->ttl
   */
  public Map<PartialPath, Long> getStorageGroupsTTL() {
    Map<PartialPath, Long> storageGroupsTTL = new HashMap<>();
    try {
      List<PartialPath> storageGroups = this.getAllStorageGroupPaths();
      for (PartialPath storageGroup : storageGroups) {
        long ttl = getStorageGroupNodeByStorageGroupPath(storageGroup).getDataTTL();
        storageGroupsTTL.put(storageGroup, ttl);
      }
    } catch (MetadataException e) {
      logger.error("get storage groups ttl failed.", e);
    }
    return storageGroupsTTL;
  }

  /**
   * Check whether the given path contains a storage group change or set the new offset of a
   * timeseries
   *
   * @param path timeseries
   * @param offset offset in the tag file
   */
  public void changeOffset(PartialPath path, long offset) throws MetadataException {
    ((MeasurementMNode) mtree.getNodeByPath(path)).setOffset(offset);
  }

  public void changeAlias(PartialPath path, String alias) throws MetadataException {
    MeasurementMNode leafMNode = (MeasurementMNode) mtree.getNodeByPath(path);
    if (leafMNode.getAlias() != null) {
      leafMNode.getParent().deleteAliasChild(leafMNode.getAlias());
    }
    leafMNode.getParent().addAlias(alias, leafMNode);
    leafMNode.setAlias(alias);
  }

  /**
   * upsert tags and attributes key-value for the timeseries if the key has existed, just use the
   * new value to update it.
   *
   * @param alias newly added alias
   * @param tagsMap newly added tags map
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void upsertTagsAndAttributes(
      String alias,
      Map<String, String> tagsMap,
      Map<String, String> attributesMap,
      PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    // upsert alias
    if (alias != null && !alias.equals(leafMNode.getAlias())) {
      if (!leafMNode.getParent().addAlias(alias, leafMNode)) {
        throw new MetadataException("The alias already exists.");
      }

      if (leafMNode.getAlias() != null) {
        leafMNode.getParent().deleteAliasChild(leafMNode.getAlias());
      }

      leafMNode.setAlias(alias);
      // persist to WAL
      logWriter.changeAlias(fullPath, alias);
    }

    if (tagsMap == null && attributesMap == null) {
      return;
    }
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagLogFile.write(tagsMap, attributesMap);
      logWriter.changeOffset(fullPath, offset);
      leafMNode.setOffset(offset);
      // update inverted Index map
      if (tagsMap != null) {
        for (Entry<String, String> entry : tagsMap.entrySet()) {
          tagIndex
              .computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
              .computeIfAbsent(entry.getValue(), v -> new CopyOnWriteArraySet<>())
              .add(leafMNode);
        }
      }
      return;
    }

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    if (tagsMap != null) {
      for (Entry<String, String> entry : tagsMap.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        String beforeValue = pair.left.get(key);
        pair.left.put(key, value);
        // if the key has existed and the value is not equal to the new one
        // we should remove before key-value from inverted index map
        if (beforeValue != null && !beforeValue.equals(value)) {

          if (tagIndex.containsKey(key) && tagIndex.get(key).containsKey(beforeValue)) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  String.format(
                      String.format(DEBUG_MSG, "Upsert" + TAG_FORMAT, leafMNode.getFullPath()),
                      key,
                      beforeValue,
                      leafMNode.getOffset()));
            }

            tagIndex.get(key).get(beforeValue).remove(leafMNode);
            if (tagIndex.get(key).get(beforeValue).isEmpty()) {
              tagIndex.get(key).remove(beforeValue);
            }
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  String.format(
                      String.format(
                          DEBUG_MSG_1, "Upsert" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                      key,
                      beforeValue,
                      leafMNode.getOffset(),
                      tagIndex.containsKey(key)));
            }
          }
        }

        // if the key doesn't exist or the value is not equal to the new one
        // we should add a new key-value to inverted index map
        if (beforeValue == null || !beforeValue.equals(value)) {
          tagIndex
              .computeIfAbsent(key, k -> new ConcurrentHashMap<>())
              .computeIfAbsent(value, v -> new CopyOnWriteArraySet<>())
              .add(leafMNode);
        }
      }
    }

    if (attributesMap != null) {
      pair.right.putAll(attributesMap);
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
  }

  /**
   * add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   * @param fullPath timeseries
   */
  public void addAttributes(Map<String, String> attributesMap, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagLogFile.write(Collections.emptyMap(), attributesMap);
      logWriter.changeOffset(fullPath, offset);
      leafMNode.setOffset(offset);
      return;
    }

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    for (Entry<String, String> entry : attributesMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (pair.right.containsKey(key)) {
        throw new MetadataException(
            String.format("TimeSeries [%s] already has the attribute [%s].", fullPath, key));
      }
      pair.right.put(key, value);
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
  }

  /**
   * add new tags key-value for the timeseries
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   */
  public void addTags(Map<String, String> tagsMap, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    // no tag or attribute, we need to add a new record in log
    if (leafMNode.getOffset() < 0) {
      long offset = tagLogFile.write(tagsMap, Collections.emptyMap());
      logWriter.changeOffset(fullPath, offset);
      leafMNode.setOffset(offset);
      // update inverted Index map
      for (Entry<String, String> entry : tagsMap.entrySet()) {
        tagIndex
            .computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
            .computeIfAbsent(entry.getValue(), v -> new CopyOnWriteArraySet<>())
            .add(leafMNode);
      }
      return;
    }

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    for (Entry<String, String> entry : tagsMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (pair.left.containsKey(key)) {
        throw new MetadataException(
            String.format("TimeSeries [%s] already has the tag [%s].", fullPath, key));
      }
      pair.left.put(key, value);
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    // update tag inverted map
    tagsMap.forEach(
        (key, value) ->
            tagIndex
                .computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(value, v -> new CopyOnWriteArraySet<>())
                .add(leafMNode));
  }

  /**
   * drop tags or attributes of the timeseries
   *
   * @param keySet tags key or attributes key
   * @param fullPath timeseries path
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void dropTagsOrAttributes(Set<String> keySet, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    // no tag or attribute, just do nothing.
    if (leafMNode.getOffset() < 0) {
      return;
    }
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    Map<String, String> deleteTag = new HashMap<>();
    for (String key : keySet) {
      // check tag map
      // check attribute map
      String removeVal = pair.left.remove(key);
      if (removeVal != null) {
        deleteTag.put(key, removeVal);
      } else {
        removeVal = pair.right.remove(key);
        if (removeVal == null) {
          logger.warn("TimeSeries [{}] does not have tag/attribute [{}]", fullPath, key);
        }
      }
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    Map<String, Set<MeasurementMNode>> tagVal2LeafMNodeSet;
    Set<MeasurementMNode> MMNodes;
    for (Entry<String, String> entry : deleteTag.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      // change the tag inverted index map
      tagVal2LeafMNodeSet = tagIndex.get(key);
      if (tagVal2LeafMNodeSet != null) {
        MMNodes = tagVal2LeafMNodeSet.get(value);
        if (MMNodes != null) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                String.format(
                    String.format(DEBUG_MSG, "Drop" + TAG_FORMAT, leafMNode.getFullPath()),
                    entry.getKey(),
                    entry.getValue(),
                    leafMNode.getOffset()));
          }

          MMNodes.remove(leafMNode);
          if (MMNodes.isEmpty()) {
            tagVal2LeafMNodeSet.remove(value);
            if (tagVal2LeafMNodeSet.isEmpty()) {
              tagIndex.remove(key);
            }
          }
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(DEBUG_MSG_1, "Drop" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                  key,
                  value,
                  leafMNode.getOffset(),
                  tagIndex.containsKey(key)));
        }
      }
    }
  }

  /**
   * set/change the values of tags or attributes
   *
   * @param alterMap the new tags or attributes key-value
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void setTagsOrAttributesValue(Map<String, String> alterMap, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    if (leafMNode.getOffset() < 0) {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have any tag/attribute.", fullPath));
    }

    // tags, attributes
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());
    Map<String, String> oldTagValue = new HashMap<>();
    Map<String, String> newTagValue = new HashMap<>();

    for (Entry<String, String> entry : alterMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      // check tag map
      if (pair.left.containsKey(key)) {
        oldTagValue.put(key, pair.left.get(key));
        newTagValue.put(key, value);
        pair.left.put(key, value);
      } else if (pair.right.containsKey(key)) {
        // check attribute map
        pair.right.put(key, value);
      } else {
        throw new MetadataException(
            String.format("TimeSeries [%s] does not have tag/attribute [%s].", fullPath, key),
            true);
      }
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    for (Entry<String, String> entry : oldTagValue.entrySet()) {
      String key = entry.getKey();
      String beforeValue = entry.getValue();
      String currentValue = newTagValue.get(key);
      // change the tag inverted index map
      if (tagIndex.containsKey(key) && tagIndex.get(key).containsKey(beforeValue)) {

        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(DEBUG_MSG, "Set" + TAG_FORMAT, leafMNode.getFullPath()),
                  entry.getKey(),
                  beforeValue,
                  leafMNode.getOffset()));
        }

        tagIndex.get(key).get(beforeValue).remove(leafMNode);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(DEBUG_MSG_1, "Set" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                  key,
                  beforeValue,
                  leafMNode.getOffset(),
                  tagIndex.containsKey(key)));
        }
      }
      tagIndex
          .computeIfAbsent(key, k -> new ConcurrentHashMap<>())
          .computeIfAbsent(currentValue, k -> new CopyOnWriteArraySet<>())
          .add(leafMNode);
    }
  }

  /**
   * rename the tag or attribute's key of the timeseries
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   * @param fullPath timeseries
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void renameTagOrAttributeKey(String oldKey, String newKey, PartialPath fullPath)
      throws MetadataException, IOException {
    MNode mNode = mtree.getNodeByPath(fullPath);
    if (!(mNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(fullPath.getFullPath());
    }
    MeasurementMNode leafMNode = (MeasurementMNode) mNode;
    if (leafMNode.getOffset() < 0) {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have [%s] tag/attribute.", fullPath, oldKey),
          true);
    }
    // tags, attributes
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

    // current name has existed
    if (pair.left.containsKey(newKey) || pair.right.containsKey(newKey)) {
      throw new MetadataException(
          String.format(
              "TimeSeries [%s] already has a tag/attribute named [%s].", fullPath, newKey),
          true);
    }

    // check tag map
    if (pair.left.containsKey(oldKey)) {
      String value = pair.left.remove(oldKey);
      pair.left.put(newKey, value);
      // persist the change to disk
      tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
      // change the tag inverted index map
      if (tagIndex.containsKey(oldKey) && tagIndex.get(oldKey).containsKey(value)) {

        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(DEBUG_MSG, "Rename" + TAG_FORMAT, leafMNode.getFullPath()),
                  oldKey,
                  value,
                  leafMNode.getOffset()));
        }

        tagIndex.get(oldKey).get(value).remove(leafMNode);

      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(
                      DEBUG_MSG_1, "Rename" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                  oldKey,
                  value,
                  leafMNode.getOffset(),
                  tagIndex.containsKey(oldKey)));
        }
      }
      tagIndex
          .computeIfAbsent(newKey, k -> new ConcurrentHashMap<>())
          .computeIfAbsent(value, k -> new CopyOnWriteArraySet<>())
          .add(leafMNode);
    } else if (pair.right.containsKey(oldKey)) {
      // check attribute map
      pair.right.put(newKey, pair.right.remove(oldKey));
      // persist the change to disk
      tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
    } else {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have tag/attribute [%s].", fullPath, oldKey),
          true);
    }
  }

  /** Check whether the given path contains a storage group */
  boolean checkStorageGroupByPath(PartialPath path) {
    return mtree.checkStorageGroupByPath(path);
  }

  /**
   * Get all storage groups under the given path
   *
   * @return List of String represented all storage group names
   * @apiNote :for cluster
   */
  List<String> getStorageGroupByPath(PartialPath path) throws MetadataException {
    try {
      return mtree.getStorageGroupByPath(path);
    } catch (MetadataException e) {
      throw new MetadataException(e);
    }
  }

  public void collectTimeseriesSchema(
      MNode startingNode, Collection<TimeseriesSchema> timeseriesSchemas) {
    Deque<MNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(startingNode);
    while (!nodeDeque.isEmpty()) {
      MNode node = nodeDeque.removeFirst();
      if (node instanceof MeasurementMNode) {
        IMeasurementSchema nodeSchema = ((MeasurementMNode) node).getSchema();
        timeseriesSchemas.add(
            new TimeseriesSchema(
                node.getFullPath(),
                nodeSchema.getType(),
                nodeSchema.getEncodingType(),
                nodeSchema.getCompressor()));
      } else if (!node.getChildren().isEmpty()) {
        nodeDeque.addAll(node.getChildren().values());
      }
    }
  }

  public void collectTimeseriesSchema(
      String prefixPath, Collection<TimeseriesSchema> timeseriesSchemas) throws MetadataException {
    collectTimeseriesSchema(getNodeByPath(new PartialPath(prefixPath)), timeseriesSchemas);
  }

  public void collectMeasurementSchema(
      MNode startingNode, Collection<IMeasurementSchema> measurementSchemas) {
    Deque<MNode> nodeDeque = new ArrayDeque<>();
    nodeDeque.addLast(startingNode);
    while (!nodeDeque.isEmpty()) {
      MNode node = nodeDeque.removeFirst();
      if (node instanceof MeasurementMNode) {
        IMeasurementSchema nodeSchema = ((MeasurementMNode) node).getSchema();
        measurementSchemas.add(nodeSchema);
      } else if (!node.getChildren().isEmpty()) {
        nodeDeque.addAll(node.getChildren().values());
      }
    }
  }

  /** Collect the timeseries schemas under "startingPath". */
  public void collectSeries(PartialPath startingPath, List<IMeasurementSchema> measurementSchemas) {
    MNode mNode;
    try {
      mNode = getNodeByPath(startingPath);
    } catch (MetadataException e) {
      return;
    }
    collectMeasurementSchema(mNode, measurementSchemas);
  }

  /**
   * For a path, infer all storage groups it may belong to. The path can have wildcards.
   *
   * <p>Consider the path into two parts: (1) the sub path which can not contain a storage group
   * name and (2) the sub path which is substring that begin after the storage group name.
   *
   * <p>(1) Suppose the part of the path can not contain a storage group name (e.g.,
   * "root".contains("root.sg") == false), then: If the wildcard is not at the tail, then for each
   * wildcard, only one level will be inferred and the wildcard will be removed. If the wildcard is
   * at the tail, then the inference will go on until the storage groups are found and the wildcard
   * will be kept. (2) Suppose the part of the path is a substring that begin after the storage
   * group name. (e.g., For "root.*.sg1.a.*.b.*" and "root.x.sg1" is a storage group, then this part
   * is "a.*.b.*"). For this part, keep what it is.
   *
   * <p>Assuming we have three SGs: root.group1, root.group2, root.area1.group3 Eg1: for input
   * "root.*", returns ("root.group1", "root.group1.*"), ("root.group2", "root.group2.*")
   * ("root.area1.group3", "root.area1.group3.*") Eg2: for input "root.*.s1", returns
   * ("root.group1", "root.group1.s1"), ("root.group2", "root.group2.s1")
   *
   * <p>Eg3: for input "root.area1.*", returns ("root.area1.group3", "root.area1.group3.*")
   *
   * @param path can be a prefix or a full path.
   * @return StorageGroupName-FullPath pairs
   */
  public Map<String, String> determineStorageGroup(PartialPath path) throws IllegalPathException {
    Map<String, String> sgPathMap = mtree.determineStorageGroup(path);
    if (logger.isDebugEnabled()) {
      logger.debug("The storage groups of path {} are {}", path, sgPathMap.keySet());
    }
    return sgPathMap;
  }

  /**
   * if the path is in local mtree, nothing needed to do (because mtree is in the memory); Otherwise
   * cache the path to mRemoteSchemaCache
   */
  public void cacheMeta(
      PartialPath path, MeasurementMNode measurementMNode, boolean needSetFullPath) {
    // do nothing
  }

  public void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime,
      MeasurementMNode node) {
    if (node != null) {
      node.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
    } else {
      try {
        MeasurementMNode node1 = (MeasurementMNode) mtree.getNodeByPath(seriesPath);
        node1.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
      } catch (MetadataException e) {
        logger.warn("failed to update last cache for the {}, err:{}", seriesPath, e.getMessage());
      }
    }
  }

  public TimeValuePair getLastCache(PartialPath seriesPath) {
    try {
      MeasurementMNode node = (MeasurementMNode) mtree.getNodeByPath(seriesPath);
      return node.getCachedLast();
    } catch (MetadataException e) {
      logger.warn("failed to get last cache for the {}, err:{}", seriesPath, e.getMessage());
    }
    return null;
  }

  @TestOnly
  public void flushAllMlogForTest() throws IOException {
    logWriter.close();
  }

  private void checkMTreeModified() {
    if (logWriter == null || logFile == null) {
      // the logWriter is not initialized now, we skip the check once.
      return;
    }
    if (System.currentTimeMillis() - logFile.lastModified() < mtreeSnapshotThresholdTime) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "MTree snapshot need not be created. Time from last modification: {} ms.",
            System.currentTimeMillis() - logFile.lastModified());
      }
    } else if (logWriter.getLogNum() < mtreeSnapshotInterval) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "MTree snapshot need not be created. New mlog line number: {}.", logWriter.getLogNum());
      }
    } else {
      logger.info(
          "New mlog line number: {}, time from last modification: {} ms",
          logWriter.getLogNum(),
          System.currentTimeMillis() - logFile.lastModified());
      createMTreeSnapshot();
    }
  }

  public void createMTreeSnapshot() {
    long time = System.currentTimeMillis();
    logger.info("Start creating MTree snapshot to {}", mtreeSnapshotPath);
    try {
      mtree.serializeTo(mtreeSnapshotTmpPath);
      File tmpFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath);
      File snapshotFile = SystemFileFactory.INSTANCE.getFile(mtreeSnapshotPath);
      if (snapshotFile.exists()) {
        Files.delete(snapshotFile.toPath());
      }
      if (tmpFile.renameTo(snapshotFile)) {
        logger.info(
            "Finish creating MTree snapshot to {}, spend {} ms.",
            mtreeSnapshotPath,
            System.currentTimeMillis() - time);
      }
      logWriter.clear();
    } catch (IOException e) {
      logger.warn("Failed to create MTree snapshot to {}", mtreeSnapshotPath, e);
      if (SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath).exists()) {
        try {
          Files.delete(SystemFileFactory.INSTANCE.getFile(mtreeSnapshotTmpPath).toPath());
        } catch (IOException e1) {
          logger.warn("delete file {} failed: {}", mtreeSnapshotTmpPath, e1.getMessage());
        }
      }
    }
  }

  /** get schema for device. Attention!!! Only support insertPlan */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public MNode getSeriesSchemasAndReadLockDevice(InsertPlan plan)
      throws MetadataException, IOException {
    PartialPath prefixPath = plan.getPrefixPath();
    PartialPath deviceId = prefixPath;
    String vectorId = null;
    if (plan.isAligned()) {
      deviceId = prefixPath.getDevicePath();
      vectorId = prefixPath.getMeasurement();
    }
    String[] measurementList = plan.getMeasurements();
    MeasurementMNode[] measurementMNodes = plan.getMeasurementMNodes();

    // 1. get device node
    Pair<MNode, Template> deviceMNode = getDeviceNodeWithAutoCreate(deviceId);
    if (deviceMNode.left.getDeviceTemplate() != null) {
      deviceMNode.right = deviceMNode.left.getDeviceTemplate();
    }

    // check insert non-aligned InsertPlan for aligned timeseries
    if (deviceMNode.left instanceof MeasurementMNode
        && ((MeasurementMNode) deviceMNode.left).getSchema() instanceof VectorMeasurementSchema
        && !plan.isAligned()) {
      throw new MetadataException(
          String.format(
              "Path [%s] is an aligned timeseries, please set InsertPlan.isAligned() = true",
              prefixPath));
    }
    // check insert aligned InsertPlan for non-aligned timeseries
    else if (plan.isAligned()
        && deviceMNode.left.getChild(vectorId) != null
        && !(deviceMNode.left.getChild(vectorId) instanceof MeasurementMNode)) {
      throw new MetadataException(
          String.format(
              "Path [%s] is not an aligned timeseries, please set InsertPlan.isAligned() = false",
              prefixPath));
    }

    // 2. get schema of each measurement
    // if do not have measurement
    MeasurementMNode measurementMNode;
    for (int i = 0; i < measurementList.length; i++) {
      try {
        String measurement = measurementList[i];
        MNode child = getMNode(deviceMNode.left, plan.isAligned() ? vectorId : measurement);
        if (child instanceof MeasurementMNode) {
          measurementMNode = (MeasurementMNode) child;
        } else if (child instanceof StorageGroupMNode) {
          throw new PathAlreadyExistException(deviceId + PATH_SEPARATOR + measurement);
        } else if ((measurementMNode = findTemplate(deviceMNode, measurement, vectorId)) != null) {
          // empty
        } else {
          if (!config.isAutoCreateSchemaEnabled()) {
            throw new PathNotExistException(deviceId + PATH_SEPARATOR + measurement);
          } else {
            if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
              if (!plan.isAligned()) {
                internalCreateTimeseries(
                    prefixPath.concatNode(measurement), plan.getDataTypes()[i]);
                measurementMNode = (MeasurementMNode) deviceMNode.left.getChild(measurement);
              } else {
                internalAlignedCreateTimeseries(
                    prefixPath, Arrays.asList(measurementList), Arrays.asList(plan.getDataTypes()));
                measurementMNode = (MeasurementMNode) deviceMNode.left.getChild(vectorId);
              }
            } else {
              throw new MetadataException(
                  String.format(
                      "Only support insertRow and insertTablet, plan is [%s]",
                      plan.getOperatorType()));
            }
          }
        }

        // check type is match
        TSDataType insertDataType;
        if (plan instanceof InsertRowPlan || plan instanceof InsertTabletPlan) {
          if (plan.isAligned()) {
            TSDataType dataTypeInNode =
                measurementMNode.getSchema().getValueTSDataTypeList().get(i);
            insertDataType = plan.getDataTypes()[i];
            if (insertDataType == null) {
              insertDataType = dataTypeInNode;
            }
            if (dataTypeInNode != insertDataType) {
              logger.warn(
                  "DataType mismatch, Insert measurement {} in {} type {}, metadata tree type {}",
                  measurementMNode.getSchema().getValueMeasurementIdList().get(i),
                  measurementList[i],
                  insertDataType,
                  dataTypeInNode);
              DataTypeMismatchException mismatchException =
                  new DataTypeMismatchException(measurementList[i], insertDataType, dataTypeInNode);
              if (!config.isEnablePartialInsert()) {
                throw mismatchException;
              } else {
                // mark failed measurement
                plan.markFailedMeasurementAlignedInsertion(mismatchException);
                for (int j = 0; j < i; j++) {
                  // all the measurementMNodes should be null
                  measurementMNodes[j] = null;
                }
                break;
              }
            }
            measurementMNodes[i] = measurementMNode;
          } else {
            if (plan instanceof InsertRowPlan) {
              if (!((InsertRowPlan) plan).isNeedInferType()) {
                // only when InsertRowPlan's values is object[], we should check type
                insertDataType = getTypeInLoc(plan, i);
              } else {
                insertDataType = measurementMNode.getSchema().getType();
              }
            } else {
              insertDataType = getTypeInLoc(plan, i);
            }
            if (measurementMNode.getSchema().getType() != insertDataType) {
              logger.warn(
                  "DataType mismatch, Insert measurement {} type {}, metadata tree type {}",
                  measurementList[i],
                  insertDataType,
                  measurementMNode.getSchema().getType());
              DataTypeMismatchException mismatchException =
                  new DataTypeMismatchException(
                      measurementList[i], insertDataType, measurementMNode.getSchema().getType());
              if (!config.isEnablePartialInsert()) {
                throw mismatchException;
              } else {
                // mark failed measurement
                plan.markFailedMeasurementInsertion(i, mismatchException);
                continue;
              }
            }
            measurementMNodes[i] = measurementMNode;
            // set measurementName instead of alias
            measurementList[i] = measurementMNode.getName();
          }
        }
      } catch (MetadataException e) {
        logger.warn(
            "meet error when check {}.{}, message: {}",
            deviceId,
            measurementList[i],
            e.getMessage());
        if (config.isEnablePartialInsert()) {
          // mark failed measurement
          plan.markFailedMeasurementInsertion(i, e);
        } else {
          throw e;
        }
      }
    }

    return deviceMNode.left;
  }

  /** get dataType of plan, in loc measurements only support InsertRowPlan and InsertTabletPlan */
  private TSDataType getTypeInLoc(InsertPlan plan, int loc) throws MetadataException {
    TSDataType dataType;
    if (plan instanceof InsertRowPlan) {
      InsertRowPlan tPlan = (InsertRowPlan) plan;
      dataType =
          TypeInferenceUtils.getPredictedDataType(tPlan.getValues()[loc], tPlan.isNeedInferType());
    } else if (plan instanceof InsertTabletPlan) {
      dataType = (plan).getDataTypes()[loc];
    } else {
      throw new MetadataException(
          String.format(
              "Only support insert and insertTablet, plan is [%s]", plan.getOperatorType()));
    }
    return dataType;
  }

  public MNode getMNode(MNode deviceMNode, String measurementName) {
    return deviceMNode.getChild(measurementName);
  }

  private MeasurementMNode findTemplate(
      Pair<MNode, Template> deviceMNode, String measurement, String vectorId)
      throws MetadataException {
    if (deviceMNode.right != null) {
      Map<String, IMeasurementSchema> curTemplateMap = deviceMNode.right.getSchemaMap();

      String schemaName = vectorId != null ? vectorId : measurement;
      IMeasurementSchema schema = curTemplateMap.get(schemaName);
      if (!deviceMNode.left.isUseTemplate()) {
        deviceMNode.left.setUseTemplate(true);
        try {
          logWriter.setUsingDeviceTemplate(deviceMNode.left.getPartialPath());
        } catch (IOException e) {
          throw new MetadataException(e);
        }
      }

      if (schema != null) {
        if (schema instanceof MeasurementSchema) {
          return new MeasurementMNode(deviceMNode.left, measurement, schema, null);
        } else if (schema instanceof VectorMeasurementSchema) {
          return new MeasurementMNode(deviceMNode.left, vectorId, schema, null);
        }
      }
      return null;
    }
    return null;
  }

  /** create timeseries ignoring PathAlreadyExistException */
  private void internalCreateTimeseries(PartialPath path, TSDataType dataType)
      throws MetadataException {
    createTimeseries(
        path,
        dataType,
        getDefaultEncoding(dataType),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  /** create aligned timeseries ignoring PathAlreadyExistException */
  private void internalAlignedCreateTimeseries(
      PartialPath prefixPath, List<String> measurements, List<TSDataType> dataTypes)
      throws MetadataException {
    List<TSEncoding> encodings = new ArrayList<>();
    for (TSDataType dataType : dataTypes) {
      encodings.add(getDefaultEncoding(dataType));
    }
    createAlignedTimeSeries(
        prefixPath,
        measurements,
        dataTypes,
        encodings,
        TSFileDescriptor.getInstance().getConfig().getCompressor());
  }

  /**
   * StorageGroupFilter filters unsatisfied storage groups in metadata queries to speed up and
   * deduplicate.
   */
  @FunctionalInterface
  public interface StorageGroupFilter {

    boolean satisfy(String storageGroup);
  }

  public void createDeviceTemplate(CreateTemplatePlan plan) throws MetadataException {
    try {
      Template template = new Template(plan);
      if (templateMap.putIfAbsent(plan.getName(), template) != null) {
        // already have template
        throw new MetadataException("Duplicated template name: " + plan.getName());
      }

      // write wal
      if (!isRecovering) {
        logWriter.createDeviceTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public void setDeviceTemplate(SetDeviceTemplatePlan plan) throws MetadataException {
    try {
      Template template = templateMap.get(plan.getTemplateName());

      if (template == null) {
        throw new UndefinedTemplateException(plan.getTemplateName());
      }

      // get mnode and update template should be atomic
      synchronized (this) {
        Pair<MNode, Template> node =
            getDeviceNodeWithAutoCreate(new PartialPath(plan.getPrefixPath()));

        if (node.left.getDeviceTemplate() != null) {
          if (node.left.getDeviceTemplate().equals(template)) {
            throw new DuplicatedTemplateException(template.getName());
          } else {
            throw new MetadataException("Specified node already has template");
          }
        }

        if (!isTemplateCompatible(node.right, template)) {
          throw new MetadataException("Incompatible template");
        }

        node.left.setDeviceTemplate(template);
      }

      // write wal
      if (!isRecovering) {
        logWriter.setDeviceTemplate(plan);
      }
    } catch (IOException e) {
      throw new MetadataException(e);
    }
  }

  public boolean isTemplateCompatible(Template upper, Template current) {
    if (upper == null) {
      return true;
    }

    Map<String, IMeasurementSchema> upperMap = new HashMap<>(upper.getSchemaMap());
    Map<String, IMeasurementSchema> currentMap = new HashMap<>(current.getSchemaMap());

    // for identical vector schema, we should just compare once
    Map<IMeasurementSchema, IMeasurementSchema> sameSchema = new HashMap<>();

    for (String name : currentMap.keySet()) {
      IMeasurementSchema upperSchema = upperMap.remove(name);
      if (upperSchema != null) {
        IMeasurementSchema currentSchema = currentMap.get(name);
        // use "==" to compare actual address space
        if (upperSchema == sameSchema.get(currentSchema)) {
          continue;
        }

        if (!upperSchema.equals(currentSchema)) {
          return false;
        }

        sameSchema.put(currentSchema, upperSchema);
      }
    }

    // current template must contains all measurements of upper template
    return upperMap.isEmpty();
  }

  public void autoCreateDeviceMNode(AutoCreateDeviceMNodePlan plan) throws MetadataException {
    mtree.getDeviceNodeWithAutoCreating(plan.getPath(), config.getDefaultStorageGroupLevel());
  }

  private void setUsingDeviceTemplate(SetUsingDeviceTemplatePlan plan) throws MetadataException {
    getDeviceNode(plan.getPrefixPath()).setUseTemplate(true);
  }

  public long getTotalSeriesNumber() {
    return totalSeriesNumber.get();
  }
}
