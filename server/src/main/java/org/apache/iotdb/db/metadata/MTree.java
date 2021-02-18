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

import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_SEPARATOR;
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.LOSS;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT_COMP_DEV;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT_COMP_MAX_TIME;
import static org.apache.iotdb.db.conf.IoTDBConstant.SDT_COMP_MIN_TIME;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.collections4.MapUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.metadata.AliasAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.IllegalParameterOfPathException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager.StorageGroupFilter;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ShowDevicesPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ShowDevicesResult;
import org.apache.iotdb.db.query.executor.fill.LastPointReader;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The hierarchical struct of the Metadata Tree is implemented in this class.
 */
public class MTree implements Serializable {

  public static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final long serialVersionUID = -4200394435237291964L;
  private static final Logger logger = LoggerFactory.getLogger(MTree.class);
  private static final String NO_CHILDNODE_MSG = " does not have the child node ";
  private static transient ThreadLocal<Integer> limit = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> offset = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> count = new ThreadLocal<>();
  private static transient ThreadLocal<Integer> curOffset = new ThreadLocal<>();
  private MNode root;

  MTree() {
    this.root = new MNode(null, IoTDBConstant.PATH_ROOT);
  }

  private MTree(MNode root) {
    this.root = root;
  }

  static long getLastTimeStamp(MeasurementMNode node, QueryContext queryContext) {
    TimeValuePair last = node.getCachedLast();
    if (last != null) {
      return node.getCachedLast().getTimestamp();
    } else {
      try {
        QueryDataSource dataSource = QueryResourceManager.getInstance().
            getQueryDataSource(node.getPartialPath(), queryContext, null);
        Set<String> measurementSet = new HashSet<>();
        measurementSet.add(node.getPartialPath().getFullPath());
        LastPointReader lastReader = new LastPointReader(node.getPartialPath(),
            node.getSchema().getType(), measurementSet, queryContext,
            dataSource, Long.MAX_VALUE, null);
        last = lastReader.readLastPoint();
        return (last != null ? last.getTimestamp() : Long.MIN_VALUE);
      } catch (Exception e) {
        logger.error("Something wrong happened while trying to get last time value pair of {}",
            node.getFullPath(), e);
        return Long.MIN_VALUE;
      }
    }
  }

  private static String jsonToString(JsonObject jsonObject) {
    return GSON.toJson(jsonObject);
  }

  /**
   * combine multiple metadata in string format
   */
  @TestOnly
  static JsonObject combineMetadataInStrings(String[] metadataStrs) {
    JsonObject[] jsonObjects = new JsonObject[metadataStrs.length];
    for (int i = 0; i < jsonObjects.length; i++) {
      jsonObjects[i] = GSON.fromJson(metadataStrs[i], JsonObject.class);
    }

    JsonObject root = jsonObjects[0];
    for (int i = 1; i < jsonObjects.length; i++) {
      root = combineJsonObjects(root, jsonObjects[i]);
    }

    return root;
  }

  private static JsonObject combineJsonObjects(JsonObject a, JsonObject b) {
    JsonObject res = new JsonObject();

    Set<String> retainSet = new HashSet<>(a.keySet());
    retainSet.retainAll(b.keySet());
    Set<String> aCha = new HashSet<>(a.keySet());
    Set<String> bCha = new HashSet<>(b.keySet());
    aCha.removeAll(retainSet);
    bCha.removeAll(retainSet);

    for (String key : aCha) {
      res.add(key, a.get(key));
    }

    for (String key : bCha) {
      res.add(key, b.get(key));
    }
    for (String key : retainSet) {
      JsonElement v1 = a.get(key);
      JsonElement v2 = b.get(key);
      if (v1 instanceof JsonObject && v2 instanceof JsonObject) {
        res.add(key, combineJsonObjects((JsonObject) v1, (JsonObject) v2));
      } else {
        res.add(v1.getAsString(), v2);
      }
    }
    return res;
  }

  /**
   * Create a timeseries with a full path from root to leaf node Before creating a timeseries, the
   * storage group should be set first, throw exception otherwise
   *
   * @param path       timeseries path
   * @param dataType   data type
   * @param encoding   encoding
   * @param compressor compressor
   * @param props      props
   * @param alias      alias of measurement
   */
  MeasurementMNode createTimeseries(PartialPath path, TSDataType dataType, TSEncoding encoding,
      CompressionType compressor, Map<String, String> props, String alias) throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 2 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    checkTimeseries(path.getFullPath());
    MNode cur = root;
    boolean hasSetStorageGroup = false;
    // e.g, path = root.sg.d1.s1,  create internal nodes and set cur to d1 node
    for (int i = 1; i < nodeNames.length - 1; i++) {
      String nodeName = nodeNames[i];
      if (cur instanceof StorageGroupMNode) {
        hasSetStorageGroup = true;
      }
      if (!cur.hasChild(nodeName)) {
        if (!hasSetStorageGroup) {
          throw new StorageGroupNotSetException("Storage group should be created first");
        }
        cur.addChild(nodeName, new MNode(cur, nodeName));
      }
      cur = cur.getChild(nodeName);
    }

    if (props != null && props.containsKey(LOSS) && props.get(LOSS).equals(SDT)) {
      checkSDTFormat(path.getFullPath(), props);
    }

    String leafName = nodeNames[nodeNames.length - 1];

    // synchronize check and add, we need addChild and add Alias become atomic operation
    // only write on mtree will be synchronized
    synchronized (this) {
      MNode child = cur.getChild(leafName);
      if (child instanceof MeasurementMNode) {
          throw new PathAlreadyExistException(path.getFullPath());
      }
      if (alias != null) {
        MNode childByAlias = cur.getChild(alias);
        if (childByAlias instanceof MeasurementMNode) {
          throw new AliasAlreadyExistException(path.getFullPath(), alias);
        }
      }

      MeasurementMNode leaf = new MeasurementMNode(cur, leafName, alias, dataType, encoding,
          compressor, props);
      if (child != null) {
        cur.replaceChild(leaf.getName(), leaf);
      } else {
        cur.addChild(leafName, leaf);
      }

      // link alias to LeafMNode
      if (alias != null) {
        cur.addAlias(alias, leaf);
      }

      return leaf;
    }
  }

  private void checkTimeseries(String timeseries) throws IllegalPathException {
    if (!IoTDBConfig.NODE_PATTERN.matcher(timeseries).matches()) {
      throw new IllegalPathException(String
          .format("The timeseries name contains unsupported character. %s",
              timeseries));
    }
  }

  protected void checkPartialPath(PartialPath partialPath) throws MetadataException {
    String[] nodes = partialPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(partialPath.getFullPath());
    }
  }

  //check if sdt parameters are valid
  private void checkSDTFormat(String path, Map<String, String> props) throws IllegalParameterOfPathException {
    if (!props.containsKey(SDT_COMP_DEV)) {
      throw new IllegalParameterOfPathException("SDT compression deviation is required", path);
    }

    try {
      double d = Double.parseDouble(props.get(SDT_COMP_DEV));
      if (d < 0) {
        throw new IllegalParameterOfPathException("SDT compression deviation cannot be negative", path);
      }
    } catch (NumberFormatException e) {
      throw new IllegalParameterOfPathException("SDT compression deviation formatting error", path);
    }

    long compMinTime = sdtCompressionTimeFormat(SDT_COMP_MIN_TIME, props, path);
    long compMaxTime = sdtCompressionTimeFormat(SDT_COMP_MAX_TIME, props, path);

    if (compMaxTime <= compMinTime) {
      throw new IllegalParameterOfPathException(
          "SDT compression maximum time needs to be greater than compression minimum time", path);
    }
  }

  private long sdtCompressionTimeFormat(String compTime, Map<String, String> props, String path)
      throws IllegalParameterOfPathException {
    boolean isCompMaxTime = compTime.equals(SDT_COMP_MAX_TIME);
    long time = isCompMaxTime ? Long.MAX_VALUE : 0;
    String s = isCompMaxTime ? "maximum" : "minimum";
    if (props.containsKey(compTime)) {
      try {
        time = Long.parseLong(props.get(compTime));
        if (time < 0) {
          throw new IllegalParameterOfPathException(String.format("SDT compression %s time cannot be negative", s), path);
        }
      } catch (IllegalParameterOfPathException e) {
        throw new IllegalParameterOfPathException(String.format("SDT compression %s time formatting error", s), path);
      }
    } else {
      logger.info("{} enabled SDT but did not set compression {} time", path, s);
    }
    return time;
  }

  /**
   * Add an interval path to MTree. This is only used for automatically creating schema
   *
   * <p>e.g., get root.sg.d1, get or create all internal nodes and return the node of d1
   */
  MNode getDeviceNodeWithAutoCreating(PartialPath deviceId, int sgLevel) throws MetadataException {
    String[] nodeNames = deviceId.getNodes();
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(deviceId.getFullPath());
    }
    MNode cur = root;
    for (int i = 1; i < nodeNames.length; i++) {
      if (!cur.hasChild(nodeNames[i])) {
        if (i == sgLevel) {
          cur.addChild(nodeNames[i], new StorageGroupMNode(cur, nodeNames[i],
              IoTDBDescriptor.getInstance().getConfig().getDefaultTTL()));
        } else {
          cur.addChild(nodeNames[i], new MNode(cur, nodeNames[i]));
        }
      }
      cur = cur.getChild(nodeNames[i]);
    }
    return cur;
  }

  /**
   * Check whether the given path exists.
   *
   * @param path a full path or a prefix path
   */
  boolean isPathExist(PartialPath path) {
    Object[] res = judgePathExistAndGetLastNode(path);
    return (boolean) res[0];
  }

  /**
   * Check whether the given timeseries exists.
   *
   * @param path a timeseries path
   */
  boolean isTimeSeriesExist(PartialPath path) {
    Object[] res = judgePathExistAndGetLastNode(path);
    boolean pathExist = (boolean) res[0];
    if (pathExist) {
      return res[1] instanceof MeasurementMNode;
    }

    return false;
  }

  /**
   * @param path a full path or a prefix path
   *
   * @return res[0] refers to flag indicating if path exists; res[1] refers to lastNode
   */
  Object[] judgePathExistAndGetLastNode(PartialPath path) {
    Object[] res = {false, null};
    String[] nodeNames = path.getNodes();
    MNode cur = root;
    if (!nodeNames[0].equals(root.getName())) {
      return res;
    }
    for (int i = 1; i < nodeNames.length; i++) {
      String childName = nodeNames[i];
      cur = cur.getChild(childName);
      if (cur == null) {
        return res;
      }
    }
    res[0] = true;
    res[1] = cur;
    return res;
  }

  /**
   * Set storage group. Make sure check seriesPath before setting storage group
   *
   * @param path path
   */
  void setStorageGroup(PartialPath path) throws MetadataException {
    String[] nodeNames = path.getNodes();
    checkStorageGroup(path.getFullPath());
    MNode cur = root;
    if (nodeNames.length <= 1 || !nodeNames[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    int i = 1;
    // e.g., path = root.a.b.sg, create internal nodes for a, b
    while (i < nodeNames.length - 1) {
      MNode temp = cur.getChild(nodeNames[i]);
      if (temp == null) {
        cur.addChild(nodeNames[i], new MNode(cur, nodeNames[i]));
      } else if (temp instanceof StorageGroupMNode) {
        // before set storage group, check whether the exists or not
        throw new StorageGroupAlreadySetException(temp.getFullPath());
      }
      cur = cur.getChild(nodeNames[i]);
      i++;
    }
    if (cur.hasChild(nodeNames[i])) {
      // node b has child sg
      throw new StorageGroupAlreadySetException(path.getFullPath());
    } else {
      StorageGroupMNode storageGroupMNode =
          new StorageGroupMNode(
              cur, nodeNames[i], IoTDBDescriptor.getInstance().getConfig().getDefaultTTL());
      cur.addChild(nodeNames[i], storageGroupMNode);
    }
  }

  private void checkStorageGroup(String storageGroup) throws IllegalPathException {
    if (!IoTDBConfig.STORAGE_GROUP_PATTERN.matcher(storageGroup).matches()) {
      throw new IllegalPathException(String
          .format("The storage group name can only be characters, numbers and underscores. %s",
              storageGroup));
    }
  }

  /**
   * Delete a storage group
   */
  List<MeasurementMNode> deleteStorageGroup(PartialPath path) throws MetadataException {
    MNode cur = getNodeByPath(path);
    if (!(cur instanceof StorageGroupMNode)) {
      throw new StorageGroupNotSetException(path.getFullPath());
    }
    // Suppose current system has root.a.b.sg1, root.a.sg2, and delete root.a.b.sg1
    // delete the storage group node sg1
    cur.getParent().deleteChild(cur.getName());

    // collect all the LeafMNode in this storage group
    List<MeasurementMNode> leafMNodes = new LinkedList<>();
    Queue<MNode> queue = new LinkedList<>();
    queue.add(cur);
    while (!queue.isEmpty()) {
      MNode node = queue.poll();
      for (MNode child : node.getChildren().values()) {
        if (child instanceof MeasurementMNode) {
          leafMNodes.add((MeasurementMNode) child);
        } else {
          queue.add(child);
        }
      }
    }

    cur = cur.getParent();
    // delete node b while retain root.a.sg2
    while (!IoTDBConstant.PATH_ROOT.equals(cur.getName()) && cur.getChildren().size() == 0) {
      cur.getParent().deleteChild(cur.getName());
      cur = cur.getParent();
    }
    return leafMNodes;
  }

  /**
   * Check whether path is storage group or not
   *
   * <p>e.g., path = root.a.b.sg. if nor a and b is StorageGroupMNode and sg is a StorageGroupMNode
   * path is a storage group
   *
   * @param path path
   * @apiNote :for cluster
   */
  boolean isStorageGroup(PartialPath path) {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= 1 || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      return false;
    }
    MNode cur = root;
    int i = 1;
    while (i < nodeNames.length - 1) {
      cur = cur.getChild(nodeNames[i]);
      if (cur == null || cur instanceof StorageGroupMNode) {
        return false;
      }
      i++;
    }
    cur = cur.getChild(nodeNames[i]);
    return cur instanceof StorageGroupMNode;
  }

  /**
   * Delete path. The path should be a full path from root to leaf node
   *
   * @param path Format: root.node(.node)+
   */
  Pair<PartialPath, MeasurementMNode> deleteTimeseriesAndReturnEmptyStorageGroup(PartialPath path)
      throws MetadataException {
    MNode curNode = getNodeByPath(path);
    if (!(curNode instanceof MeasurementMNode)) {
      throw new PathNotExistException(path.getFullPath());
    }
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !IoTDBConstant.PATH_ROOT.equals(nodes[0])) {
      throw new IllegalPathException(path.getFullPath());
    }

    // judge whether it has children
    if (MapUtils.isNotEmpty(curNode.getChildren())) {
      // it has children, then itself should be converted into MNode type.
      MeasurementMNode cur = (MeasurementMNode) curNode;
      MNode mNode = new MNode(cur.getParent(), cur.getName());
      cur.getParent().replaceChild(cur.getName(), mNode);
      return new Pair<>(null, cur);
    }

    // it has no children, then delete itself directly.
    curNode.getParent().deleteChild(curNode.getName());
    MeasurementMNode deletedNode = (MeasurementMNode) curNode;
    if (deletedNode.getAlias() != null) {
      curNode.getParent().deleteAliasChild(((MeasurementMNode) curNode).getAlias());
    }
    curNode = curNode.getParent();
    // delete all empty ancestors except storage group and MeasurementMNode
    while (!IoTDBConstant.PATH_ROOT.equals(curNode.getName())
        && !(curNode instanceof MeasurementMNode) && curNode.getChildren().size() == 0) {
      // if current storage group has no time series, return the storage group name
      if (curNode instanceof StorageGroupMNode) {
        return new Pair<>(curNode.getPartialPath(), deletedNode);
      }
      curNode.getParent().deleteChild(curNode.getName());
      curNode = curNode.getParent();
    }
    return new Pair<>(null, deletedNode);
  }

  /**
   * Get measurement schema for a given path. Path must be a complete Path from root to leaf node.
   */
  MeasurementSchema getSchema(PartialPath path) throws MetadataException {
    MeasurementMNode node = (MeasurementMNode) getNodeByPath(path);
    return node.getSchema();
  }

  /**
   * Get node by path with storage group check If storage group is not set,
   * StorageGroupNotSetException will be thrown
   */
  MNode getNodeByPathWithStorageGroupCheck(PartialPath path) throws MetadataException {
    boolean storageGroupChecked = false;
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }

    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        // not find
        if (!storageGroupChecked) {
          throw new StorageGroupNotSetException(path.getFullPath());
        }
        throw new PathNotExistException(path.getFullPath());
      }

      if (cur instanceof StorageGroupMNode) {
        storageGroupChecked = true;
      }
    }

    if (!storageGroupChecked) {
      throw new StorageGroupNotSetException(path.getFullPath());
    }
    return cur;
  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], throw exception Get storage group node, if the give path is not a storage group, throw
   * exception
   */
  StorageGroupMNode getStorageGroupNodeByStorageGroupPath(PartialPath path)
      throws MetadataException {
    MNode node = getNodeByPath(path);
    if (node instanceof StorageGroupMNode) {
      return (StorageGroupMNode) node;
    } else {
      throw new StorageGroupNotSetException(path.getFullPath(), true);
    }
  }

  /**
   * E.g., root.sg is storage group given [root, sg], return the MNode of root.sg given [root, sg,
   * device], return the MNode of root.sg Get storage group node, the give path don't need to be
   * storage group path.
   */
  StorageGroupMNode getStorageGroupNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur instanceof StorageGroupMNode) {
        return (StorageGroupMNode) cur;
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  /**
   * Get node by the path
   *
   * @return last node in given seriesPath
   */
  MNode getNodeByPath(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        throw new PathNotExistException(path.getFullPath(), true);
      }
    }
    return cur;
  }

  /**
   * Get all storage groups under the given path
   *
   * @return storage group list
   * @apiNote :for cluster
   */
  List<String> getStorageGroupByPath(PartialPath path) throws MetadataException {
    List<String> storageGroups = new ArrayList<>();
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    findStorageGroup(root, nodes, 1, "", storageGroups);
    return storageGroups;
  }

  /**
   * Recursively find all storage group according to a specific path
   *
   * @apiNote :for cluster
   */
  private void findStorageGroup(
      MNode node, String[] nodes, int idx, String parent, List<String> storageGroupNames) {
    if (node instanceof StorageGroupMNode) {
      storageGroupNames.add(node.getFullPath());
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      MNode next = node.getChild(nodeReg);
      if (next != null) {
        findStorageGroup(
            next,
            nodes,
            idx + 1,
            parent + node.getName() + PATH_SEPARATOR,
            storageGroupNames);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findStorageGroup(
            child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, storageGroupNames);
      }
    }
  }

  /**
   * Get all storage group names
   *
   * @return a list contains all distinct storage groups
   */
  List<PartialPath> getAllStorageGroupPaths() {
    List<PartialPath> res = new ArrayList<>();
    Deque<MNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      MNode current = nodeStack.pop();
      if (current instanceof StorageGroupMNode) {
        res.add(current.getPartialPath());
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return res;
  }

  /**
   * Get the storage group that given path belonged to or under given path
   * All related storage groups refer two cases:
   * 1. Storage groups with a prefix that is identical to path, e.g. given path "root.sg1",
   *    storage group "root.sg1.sg2" and "root.sg1.sg3" will be added into result list.
   * 2. Storage group that this path belongs to, e.g. given path "root.sg1.d1", and it is in
   *    storage group "root.sg1". Then we adds "root.sg1" into result list.
   *
   * @return a list contains all storage groups related to given path
   */
  List<PartialPath> searchAllRelatedStorageGroups(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    List<PartialPath> storageGroupPaths = new ArrayList<>();
    findStorageGroupPaths(root, nodes, 1, "", storageGroupPaths, false);
    return storageGroupPaths;
  }

  /**
   * Get all storage group under given path
   *
   * @return a list contains all storage group names under give path
   */
  List<PartialPath> getStorageGroupPaths(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    List<PartialPath> storageGroupPaths = new ArrayList<>();
    findStorageGroupPaths(root, nodes, 1, "", storageGroupPaths, true);
    return storageGroupPaths;
  }

  /**
   * Traverse the MTree to match all storage group with prefix path.
   * When trying to find storage groups via a path, we divide into two cases:
   * 1. This path is only regarded as a prefix, in other words, this path is part of the result
   *    storage groups.
   * 2. This path is a full path and we use this method to find its belonged storage group.
   * When prefixOnly is set to true, storage group paths in 1 is only added into result,
   * otherwise, both 1 and 2 are returned.
   *
   * @param node              the current traversing node
   * @param nodes             split the prefix path with '.'
   * @param idx               the current index of array nodes
   * @param parent            current parent path
   * @param storageGroupPaths store all matched storage group names
   * @param prefixOnly        only return storage groups that start with this prefix path
   */
  private void findStorageGroupPaths(MNode node, String[] nodes, int idx, String parent,
      List<PartialPath> storageGroupPaths, boolean prefixOnly) {
    if (node instanceof StorageGroupMNode && (!prefixOnly || idx >= nodes.length)) {
      storageGroupPaths.add(node.getPartialPath());
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      MNode next = node.getChild(nodeReg);
      if (next != null) {
        findStorageGroupPaths(node.getChild(nodeReg), nodes, idx + 1,
            parent + node.getName() + PATH_SEPARATOR, storageGroupPaths, prefixOnly);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        findStorageGroupPaths(
            child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, storageGroupPaths,
            prefixOnly);
      }
    }
  }

  /**
   * Get all storage group MNodes
   */
  List<StorageGroupMNode> getAllStorageGroupNodes() {
    List<StorageGroupMNode> ret = new ArrayList<>();
    Deque<MNode> nodeStack = new ArrayDeque<>();
    nodeStack.add(root);
    while (!nodeStack.isEmpty()) {
      MNode current = nodeStack.pop();
      if (current instanceof StorageGroupMNode) {
        ret.add((StorageGroupMNode) current);
      } else {
        nodeStack.addAll(current.getChildren().values());
      }
    }
    return ret;
  }

  /**
   * Get storage group path by path
   *
   * <p>e.g., root.sg1 is storage group, path is root.sg1.d1, return root.sg1
   *
   * @return storage group in the given path
   */
  PartialPath getStorageGroupPath(PartialPath path) throws StorageGroupNotSetException {
    String[] nodes = path.getNodes();
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur instanceof StorageGroupMNode) {
        return cur.getPartialPath();
      } else if (cur == null) {
        throw new StorageGroupNotSetException(path.getFullPath());
      }
    }
    throw new StorageGroupNotSetException(path.getFullPath());
  }

  /**
   * Check whether the given path contains a storage group
   */
  boolean checkStorageGroupByPath(PartialPath path) {
    String[] nodes = path.getNodes();
    MNode cur = root;
    for (int i = 1; i < nodes.length; i++) {
      cur = cur.getChild(nodes[i]);
      if (cur == null) {
        return false;
      } else if (cur instanceof StorageGroupMNode) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get all timeseries under the given path
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  List<PartialPath> getAllTimeseriesPath(PartialPath prefixPath) throws MetadataException {
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(prefixPath);
    List<Pair<PartialPath, String[]>> res = getAllMeasurementSchema(plan);
    List<PartialPath> paths = new ArrayList<>();
    for (Pair<PartialPath, String[]> p : res) {
      paths.add(p.left);
    }
    return paths;
  }

  /**
   * Get all timeseries paths under the given path
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   *
   * @return Pair.left  contains all the satisfied paths
   *         Pair.right means the current offset or zero if we don't set offset.
   */
  Pair<List<PartialPath>, Integer> getAllTimeseriesPathWithAlias(PartialPath prefixPath, int limit,
      int offset) throws MetadataException {
    PartialPath prePath = new PartialPath(prefixPath.getNodes());
    ShowTimeSeriesPlan plan = new ShowTimeSeriesPlan(prefixPath);
    plan.setLimit(limit);
    plan.setOffset(offset);
    List<Pair<PartialPath, String[]>> res = getAllMeasurementSchema(plan, false);
    List<PartialPath> paths = new ArrayList<>();
    for (Pair<PartialPath, String[]> p : res) {
      if (prePath.getMeasurement().equals(p.right[0])) {
        p.left.setMeasurementAlias(p.right[0]);
      }
      paths.add(p.left);
    }
    if (curOffset.get() == null) {
      offset = 0;
    } else {
      offset = curOffset.get() + 1;
    }
    curOffset.remove();
    return new Pair<>(paths, offset);
  }

  /**
   * Get the count of timeseries under the given prefix path.
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  int getAllTimeseriesCount(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    try {
      return getCount(root, nodes, 1);
    } catch (PathNotExistException e) {
      throw new PathNotExistException(prefixPath.getFullPath());
    }
  }

  /**
   * Get the count of devices under the given prefix path.
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  int getDevicesNum(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    return getDevicesCount(root, nodes, 1);
  }

  /**
   * Get the count of storage group under the given prefix path.
   *
   * @param prefixPath a prefix path or a full path, may contain '*'.
   */
  int getStorageGroupNum(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    return getStorageGroupCount(root, nodes, 1, "");
  }

  /**
   * Get the count of nodes in the given level under the given prefix path.
   */
  int getNodesCountInGivenLevel(PartialPath prefixPath, int level) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    MNode node = root;
    int i;
    for (i = 1; i < nodes.length; i++) {
      if (nodes[i].equals("*")) {
        break;
      }
      if (node.getChild(nodes[i]) != null) {
        node = node.getChild(nodes[i]);
      } else {
        throw new MetadataException(nodes[i - 1] + NO_CHILDNODE_MSG + nodes[i]);
      }
    }
    return getCountInGivenLevel(node, level - (i - 1));
  }

  /**
   * Traverse the MTree to get the count of timeseries.
   */
  private int getCount(MNode node, String[] nodes, int idx) throws PathNotExistException {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      MNode next = node.getChild(nodeReg);
      if (next != null) {
        if (next instanceof MeasurementMNode) {
          return 1;
        } else {
          return getCount(next, nodes, idx + 1);
        }
      } else {
        throw new PathNotExistException(node.getName() + NO_CHILDNODE_MSG + nodeReg);
      }
    } else {
      int cnt = 0;
      for (MNode child : node.getChildren().values()) {
        if (child instanceof MeasurementMNode) {
          cnt++;
        }
        cnt += getCount(child, nodes, idx + 1);
      }
      return cnt;
    }
  }

  /**
   * Traverse the MTree to get the count of devices.
   */
  private int getDevicesCount(MNode node, String[] nodes, int idx) throws MetadataException {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    int cnt = 0;
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      MNode next = node.getChild(nodeReg);
      if (next != null) {
        if (next instanceof MeasurementMNode && idx >= nodes.length) {
          cnt++;
        } else {
          cnt += getDevicesCount(node.getChild(nodeReg), nodes, idx + 1);
        }
      }
    } else {
      boolean deviceAdded = false;
      for (MNode child : node.getChildren().values()) {
        if (child instanceof MeasurementMNode && !deviceAdded && idx >= nodes.length) {
          cnt++;
          deviceAdded = true;
        }
        cnt += getDevicesCount(child, nodes, idx + 1);
      }
    }
    return cnt;
  }

  /**
   * Traverse the MTree to get the count of storage group.
   */
  private int getStorageGroupCount(
      MNode node, String[] nodes, int idx, String parent) throws MetadataException {
    int cnt = 0;
    if (node instanceof StorageGroupMNode && idx >= nodes.length) {
      cnt++;
      return cnt;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!(PATH_WILDCARD).equals(nodeReg)) {
      MNode next = node.getChild(nodeReg);
      if (next != null) {
        cnt += getStorageGroupCount(next,
            nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        cnt += getStorageGroupCount(
            child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR);
      }
    }
    return cnt;
  }

  /**
   * Traverse the MTree to get the count of timeseries in the given level.
   *
   * @param targetLevel Record the distance to the target level, 0 means the target level.
   */
  private int getCountInGivenLevel(MNode node, int targetLevel) {
    if (targetLevel == 0) {
      return 1;
    }
    int cnt = 0;
    for (MNode child : node.getChildren().values()) {
      cnt += getCountInGivenLevel(child, targetLevel - 1);
    }
    return cnt;
  }

  /**
   * Get all time series schema under the given path order by insert frequency
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  List<Pair<PartialPath, String[]>> getAllMeasurementSchemaByHeatOrder(ShowTimeSeriesPlan plan,
      QueryContext queryContext) throws MetadataException {
    String[] nodes = plan.getPath().getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    List<Pair<PartialPath, String[]>> allMatchedNodes = new ArrayList<>();

    findPath(root, nodes, 1, allMatchedNodes, false, true, queryContext);

    Stream<Pair<PartialPath, String[]>> sortedStream = allMatchedNodes.stream().sorted(
        Comparator.comparingLong((Pair<PartialPath, String[]> p) -> Long.parseLong(p.right[6]))
            .reversed()
            .thenComparing((Pair<PartialPath, String[]> p) -> p.left));

    // no limit
    if (plan.getLimit() == 0) {
      return sortedStream.collect(toList());
    } else {
      return sortedStream.skip(plan.getOffset()).limit(plan.getLimit()).collect(toList());
    }
  }

  /**
   * Get all time series schema under the given path
   *
   * <p>result: [name, alias, storage group, dataType, encoding, compression, offset]
   */
  List<Pair<PartialPath, String[]>> getAllMeasurementSchema(ShowTimeSeriesPlan plan)
      throws MetadataException {
    return getAllMeasurementSchema(plan, true);
  }


  List<Pair<PartialPath, String[]>> getAllMeasurementSchema(ShowTimeSeriesPlan plan,
      boolean removeCurrentOffset)
      throws MetadataException {
    List<Pair<PartialPath, String[]>> res = new LinkedList<>();
    String[] nodes = plan.getPath().getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    findPath(root, nodes, 1, res, offset.get() != 0 || limit.get() != 0, false, null);
    // avoid memory leaks
    limit.remove();
    offset.remove();
    if (removeCurrentOffset) {
      curOffset.remove();
    }
    count.remove();
    return res;
  }

  /**
   * Iterate through MTree to fetch metadata info of all leaf nodes under the given seriesPath
   *
   * @param needLast             if false, lastTimeStamp in timeseriesSchemaList will be null
   * @param timeseriesSchemaList List<timeseriesSchema> result: [name, alias, storage group,
   *                             dataType, encoding, compression, offset, lastTimeStamp]
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void findPath(MNode node, String[] nodes, int idx,
      List<Pair<PartialPath, String[]>> timeseriesSchemaList,
      boolean hasLimit, boolean needLast, QueryContext queryContext) throws MetadataException {
    if (node instanceof MeasurementMNode && nodes.length <= idx) {
      if (hasLimit) {
        curOffset.set(curOffset.get() + 1);
        if (curOffset.get() < offset.get() || count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }

      PartialPath nodePath = node.getPartialPath();
      nodePath.setMeasurementAlias(((MeasurementMNode) node).getAlias());
      String[] tsRow = new String[7];
      tsRow[0] = ((MeasurementMNode) node).getAlias();
      MeasurementSchema measurementSchema = ((MeasurementMNode) node).getSchema();
      tsRow[1] = getStorageGroupPath(nodePath).getFullPath();
      tsRow[2] = measurementSchema.getType().toString();
      tsRow[3] = measurementSchema.getEncodingType().toString();
      tsRow[4] = measurementSchema.getCompressor().toString();
      tsRow[5] = String.valueOf(((MeasurementMNode) node).getOffset());
      tsRow[6] =
          needLast ? String.valueOf(getLastTimeStamp((MeasurementMNode) node, queryContext)) : null;
      Pair<PartialPath, String[]> temp = new Pair<>(nodePath, tsRow);
      timeseriesSchemaList.add(temp);

      if (hasLimit) {
        count.set(count.get() + 1);
      }
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      MNode next = node.getChild(nodeReg);
      if (next != null) {
        findPath(next, nodes, idx + 1, timeseriesSchemaList, hasLimit, needLast,
            queryContext);
      }
    } else {
      for (MNode child : node.getChildren().values()) {
        if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        findPath(child, nodes, idx + 1, timeseriesSchemaList, hasLimit, needLast, queryContext);
        if (hasLimit && count.get().intValue() == limit.get().intValue()) {
          return;
        }
      }
    }
  }

  /**
   * Get child node path in the next level of the given path.
   *
   * <p>e.g., MTree has [root.sg1.d1.s1, root.sg1.d1.s2, root.sg1.d2.s1] given path = root.sg1,
   * return [root.sg1.d1, root.sg1.d2]
   *
   * @return All child nodes' seriesPath(s) of given seriesPath.
   */
  Set<String> getChildNodePathInNextLevel(PartialPath path) throws MetadataException {
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    Set<String> childNodePaths = new TreeSet<>();
    findChildNodePathInNextLevel(root, nodes, 1, "", childNodePaths, nodes.length + 1);
    return childNodePaths;
  }

  /**
   * Traverse the MTree to match all child node path in next level
   *
   * @param node   the current traversing node
   * @param nodes  split the prefix path with '.'
   * @param idx    the current index of array nodes
   * @param parent store the node string having traversed
   * @param res    store all matched device names
   * @param length expected length of path
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void findChildNodePathInNextLevel(
      MNode node, String[] nodes, int idx, String parent, Set<String> res, int length) {
    if (node == null) {
      return;
    }
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    if (!nodeReg.contains(PATH_WILDCARD)) {
      if (idx == length) {
        res.add(parent + node.getName());
      } else {
        findChildNodePathInNextLevel(node.getChild(nodeReg), nodes, idx + 1,
            parent + node.getName() + PATH_SEPARATOR, res, length);
      }
    } else {
      if (node.getChildren().size() > 0) {
        for (MNode child : node.getChildren().values()) {
          if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
            continue;
          }
          if (idx == length) {
            res.add(parent + node.getName());
          } else {
            findChildNodePathInNextLevel(
                child, nodes, idx + 1, parent + node.getName() + PATH_SEPARATOR, res, length);
          }
        }
      } else if (idx == length) {
        String nodeName = node.getName();
        res.add(parent + nodeName);
      }
    }
  }

  /**
   * Get all devices under give path
   *
   * @return a list contains all distinct devices names
   */
  Set<PartialPath> getDevices(PartialPath prefixPath) throws MetadataException {
    String[] nodes = prefixPath.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(prefixPath.getFullPath());
    }
    Set<PartialPath> devices = new TreeSet<>();
    findDevices(root, nodes, 1, devices, false);
    return devices;
  }

  List<ShowDevicesResult> getDevices(ShowDevicesPlan plan) throws MetadataException {
    String[] nodes = plan.getPath().getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(plan.getPath().getFullPath());
    }
    Set<PartialPath> devices = new TreeSet<>();
    limit.set(plan.getLimit());
    offset.set(plan.getOffset());
    curOffset.set(-1);
    count.set(0);
    findDevices(root, nodes, 1, devices, offset.get() != 0 || limit.get() != 0);
    // avoid memory leaks
    limit.remove();
    offset.remove();
    curOffset.remove();
    count.remove();
    List<ShowDevicesResult> res = new ArrayList<>();
    for (PartialPath device : devices) {
      if (plan.hasSgCol()) {
        res.add(new ShowDevicesResult(device.getFullPath(), getStorageGroupPath(device).getFullPath()));
      } else {
        res.add(new ShowDevicesResult(device.getFullPath()));
      }
    }
    return res;
  }

  /**
   * Traverse the MTree to match all devices with prefix path.
   *
   * @param node  the current traversing node
   * @param nodes split the prefix path with '.'
   * @param idx   the current index of array nodes
   * @param res   store all matched device names
   */
  @SuppressWarnings("squid:S3776")
  private void findDevices(MNode node, String[] nodes, int idx, Set<PartialPath> res, boolean hasLimit) {
    String nodeReg = MetaUtils.getNodeRegByIdx(idx, nodes);
    // the node path doesn't contains '*'
    if (!nodeReg.contains(PATH_WILDCARD)) {
      MNode next = node.getChild(nodeReg);
      if (next != null) {
        if (next instanceof MeasurementMNode && idx >= nodes.length) {
          if (hasLimit) {
            curOffset.set(curOffset.get() + 1);
            if (curOffset.get() < offset.get() || count.get().intValue() == limit.get()
                .intValue()) {
              return;
            }
            count.set(count.get() + 1);
          }
          res.add(node.getPartialPath());
        } else {
          findDevices(next, nodes, idx + 1, res, hasLimit);
        }
      }
    } else { // the node path contains '*'
      boolean deviceAdded = false;
      for (MNode child : node.getChildren().values()) {
        // use '.*' to replace '*' to form a regex to match
        // if the match failed, skip it.
        if (!Pattern.matches(nodeReg.replace("*", ".*"), child.getName())) {
          continue;
        }
        if (child instanceof MeasurementMNode && !deviceAdded && idx >= nodes.length) {
          if (hasLimit) {
            curOffset.set(curOffset.get() + 1);
            if (curOffset.get() < offset.get() || count.get().intValue() == limit.get()
                .intValue()) {
              return;
            }
            count.set(count.get() + 1);
          }
          res.add(node.getPartialPath());
          deviceAdded = true;
        }
        findDevices(child, nodes, idx + 1, res, hasLimit);
      }
    }
  }

  /**
   * Get all paths from root to the given level.
   */
  List<PartialPath> getNodesList(PartialPath path, int nodeLevel) throws MetadataException {
    return getNodesList(path, nodeLevel, null);
  }

  /**
   * Get all paths from root to the given level
   */
  List<PartialPath> getNodesList(PartialPath path, int nodeLevel, StorageGroupFilter filter)
      throws MetadataException {
    String[] nodes = path.getNodes();
    if (!nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }
    List<PartialPath> res = new ArrayList<>();
    MNode node = root;
    for (int i = 1; i < nodes.length; i++) {
      if (node.getChild(nodes[i]) != null) {
        node = node.getChild(nodes[i]);
        if (node instanceof StorageGroupMNode && filter != null && !filter
            .satisfy(node.getFullPath())) {
          return res;
        }
      } else {
        throw new MetadataException(nodes[i - 1] + NO_CHILDNODE_MSG + nodes[i]);
      }
    }
    findNodes(node, path, res, nodeLevel - (nodes.length - 1), filter);
    return res;
  }

  /**
   * Get all paths under the given level.
   *
   * @param targetLevel Record the distance to the target level, 0 means the target level.
   */
  private void findNodes(MNode node, PartialPath path, List<PartialPath> res, int targetLevel,
      StorageGroupFilter filter) {
    if (node == null || node instanceof StorageGroupMNode && filter != null && !filter
        .satisfy(node.getFullPath())) {
      return;
    }
    if (targetLevel == 0) {
      res.add(path);
      return;
    }
    for (MNode child : node.getChildren().values()) {
      findNodes(child, path.concatNode(child.toString()), res, targetLevel - 1, filter);
    }
  }

  public void serializeTo(String snapshotPath) throws IOException {
    try (MLogWriter mLogWriter = new MLogWriter(snapshotPath)) {
      root.serializeTo(mLogWriter);
    }
  }

  public static MTree deserializeFrom(File mtreeSnapshot) {
    try (MLogReader mLogReader = new MLogReader(mtreeSnapshot)) {
      return deserializeFromReader(mLogReader);
    } catch (IOException e) {
      logger.warn("Failed to deserialize from {}. Use a new MTree.", mtreeSnapshot.getPath());
      return new MTree();
    } finally {
      limit = new ThreadLocal<>();
      offset = new ThreadLocal<>();
      count = new ThreadLocal<>();
      curOffset = new ThreadLocal<>();
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private static MTree deserializeFromReader(MLogReader mLogReader) {
    Deque<MNode> nodeStack = new ArrayDeque<>();
    MNode node = null;
    while (mLogReader.hasNext()) {
      PhysicalPlan plan = null;
      try {
        plan = mLogReader.next();
        if (plan == null) {
          continue;
        }
        int childrenSize = 0;
        if (plan instanceof StorageGroupMNodePlan) {
          node = StorageGroupMNode.deserializeFrom((StorageGroupMNodePlan) plan);
          childrenSize = ((StorageGroupMNodePlan) plan).getChildSize();
        } else if (plan instanceof MeasurementMNodePlan) {
          node = MeasurementMNode.deserializeFrom((MeasurementMNodePlan) plan);
          childrenSize = ((MeasurementMNodePlan) plan).getChildSize();
        } else if (plan instanceof MNodePlan) {
          node = new MNode(null, ((MNodePlan) plan).getName());
          childrenSize = ((MNodePlan) plan).getChildSize();
        }

        if (childrenSize != 0) {
          ConcurrentHashMap<String, MNode> childrenMap = new ConcurrentHashMap<>();
          for (int i = 0; i < childrenSize; i++) {
            MNode child = nodeStack.removeFirst();
            child.setParent(node);
            childrenMap.put(child.getName(), child);
            if (child instanceof MeasurementMNode) {
              String alias = ((MeasurementMNode) child).getAlias();
              if (alias != null) {
                node.addAlias(alias, child);
              }
            }
          }
          node.setChildren(childrenMap);
        }
        nodeStack.push(node);
      } catch (Exception e) {
        logger.error("Can not operate cmd {} for err:", plan == null ? "" : plan.getOperatorType(),
            e);
      }
    }

    return new MTree(node);
  }

  @Override
  public String toString() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.add(root.getName(), mNodeToJSON(root, null));
    return jsonToString(jsonObject);
  }

  private JsonObject mNodeToJSON(MNode node, String storageGroupName) {
    JsonObject jsonObject = new JsonObject();
    if (node.getChildren().size() > 0) {
      if (node instanceof StorageGroupMNode) {
        storageGroupName = node.getFullPath();
      }
      for (MNode child : node.getChildren().values()) {
        jsonObject.add(child.getName(), mNodeToJSON(child, storageGroupName));
      }
    } else if (node instanceof MeasurementMNode) {
      MeasurementMNode leafMNode = (MeasurementMNode) node;
      jsonObject.add("DataType", GSON.toJsonTree(leafMNode.getSchema().getType()));
      jsonObject.add("Encoding", GSON.toJsonTree(leafMNode.getSchema().getEncodingType()));
      jsonObject.add("Compressor", GSON.toJsonTree(leafMNode.getSchema().getCompressor()));
      if (leafMNode.getSchema().getProps() != null) {
        jsonObject.addProperty("args", leafMNode.getSchema().getProps().toString());
      }
      jsonObject.addProperty("StorageGroup", storageGroupName);
    }
    return jsonObject;
  }

  Map<String, String> determineStorageGroup(PartialPath path) throws IllegalPathException {
    Map<String, String> paths = new HashMap<>();
    String[] nodes = path.getNodes();
    if (nodes.length == 0 || !nodes[0].equals(root.getName())) {
      throw new IllegalPathException(path.getFullPath());
    }

    Deque<MNode> nodeStack = new ArrayDeque<>();
    Deque<Integer> depthStack = new ArrayDeque<>();
    if (!root.getChildren().isEmpty()) {
      nodeStack.push(root);
      depthStack.push(0);
    }

    while (!nodeStack.isEmpty()) {
      MNode mNode = nodeStack.removeFirst();
      int depth = depthStack.removeFirst();

      determineStorageGroup(depth + 1, nodes, mNode, paths, nodeStack, depthStack);
    }
    return paths;
  }

  /**
   * Try determining the storage group using the children of a mNode. If one child is a storage
   * group node, put a storageGroupName-fullPath pair into paths. Otherwise put the children that
   * match the path into the queue and discard other children.
   */
  private void determineStorageGroup(
      int depth,
      String[] nodes,
      MNode mNode,
      Map<String, String> paths,
      Deque<MNode> nodeStack,
      Deque<Integer> depthStack) {
    String currNode = depth >= nodes.length ? PATH_WILDCARD : nodes[depth];
    for (Entry<String, MNode> entry : mNode.getChildren().entrySet()) {
      if (!currNode.equals(PATH_WILDCARD) && !currNode.equals(entry.getKey())) {
        continue;
      }
      // this child is desired
      MNode child = entry.getValue();
      if (child instanceof StorageGroupMNode) {
        // we have found one storage group, record it
        String sgName = child.getFullPath();
        // concat the remaining path with the storage group name
        StringBuilder pathWithKnownSG = new StringBuilder(sgName);
        for (int i = depth + 1; i < nodes.length; i++) {
          pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(nodes[i]);
        }
        if (depth >= nodes.length - 1 && currNode.equals(PATH_WILDCARD)) {
          // the we find the sg at the last node and the last node is a wildcard (find "root
          // .group1", for "root.*"), also append the wildcard (to make "root.group1.*")
          pathWithKnownSG.append(IoTDBConstant.PATH_SEPARATOR).append(PATH_WILDCARD);
        }
        paths.put(sgName, pathWithKnownSG.toString());
      } else if (!child.getChildren().isEmpty()) {
        // push it back so we can traver its children later
        nodeStack.push(child);
        depthStack.push(depth);
      }
    }
  }
}
