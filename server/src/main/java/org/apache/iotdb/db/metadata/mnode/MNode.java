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
package org.apache.iotdb.db.metadata.mnode;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.rescon.CachedStringPool;

/**
 * This class is the implementation of Metadata Node. One MNode instance represents one node in the
 * Metadata Tree
 */
public class MNode implements Serializable {

  private static final long serialVersionUID = -770028375899514063L;
  private static Map<String, String> cachedPathPool = CachedStringPool.getInstance()
      .getCachedPool();

  /**
   * Name of the MNode
   */
  protected String name;
  protected MNode parent;

  /**
   * from root to this node, only be set when used once for InternalMNode
   */
  protected String fullPath;

  /**
   * use in Measurement Node so it's protected
   * suppress warnings reason: volatile for double synchronized check
   *
   * This will be a ConcurrentHashMap instance
   */
  @SuppressWarnings("squid:S3077")
  protected transient volatile Map<String, MNode> children = null;

  /**
   * suppress warnings reason: volatile for double synchronized check
   */
  @SuppressWarnings("squid:S3077")
  private transient volatile ConcurrentMap<String, MNode> aliasChildren = null;

  /**
   * Constructor of MNode.
   */
  public MNode(MNode parent, String name) {
    this.parent = parent;
    this.name = name;
  }

  /**
   * check whether the MNode has a child with the name
   */
  public boolean hasChild(String name) {
    return (children != null && children.containsKey(name)) ||
        (aliasChildren != null && aliasChildren.containsKey(name));
  }

  /**
   * add a child to current mnode
   *
   * @param name  child's name
   * @param child child's node
   */
  public void addChild(String name, MNode child) {
    /* use cpu time to exchange memory
     * measurementNode's children should be null to save memory
     * add child method will only be called when writing MTree, which is not a frequent operation
     */
    if (children == null) {
      // double check, children is volatile
      synchronized (this) {
        if (children == null) {
          children = new ConcurrentHashMap<>();
        }
      }
    }

    children.putIfAbsent(name, child);
  }

  /**
   * delete a child
   */
  public void deleteChild(String name) {
    if (children != null) {
      children.remove(name);
    }
  }

  /**
   * delete the alias of a child
   */
  public void deleteAliasChild(String alias) {
    if (aliasChildren != null) {
      aliasChildren.remove(alias);
    }
  }

  /**
   * get the child with the name
   */
  public MNode getChild(String name) {
    MNode child = null;
    if (children != null) {
      child = children.get(name);
    }
    if (child != null) {
      return child;
    }
    return aliasChildren == null ? null : aliasChildren.get(name);
  }

  /**
   * get the count of all MeasurementMNode whose ancestor is current node
   */
  public int getMeasurementMNodeCount() {
    if (children == null) {
      return 1;
    }
    int measurementMNodeCount = 0;
    if (this instanceof MeasurementMNode) {
      measurementMNodeCount += 1; // current node itself may be MeasurementMNode
    }
    for (MNode child : children.values()) {
      measurementMNodeCount += child.getMeasurementMNodeCount();
    }
    return measurementMNodeCount;
  }

  /**
   * add an alias
   */
  public boolean addAlias(String alias, MNode child) {
    if (aliasChildren == null) {
      // double check, alias children volatile
      synchronized (this) {
        if (aliasChildren == null) {
          aliasChildren = new ConcurrentHashMap<>();
        }
      }
    }

    return aliasChildren.computeIfAbsent(alias, aliasName -> child) == child;
  }

  /**
   * get full path
   */
  public String getFullPath() {
    if (fullPath == null) {
      fullPath = concatFullPath();
      String cachedFullPath = cachedPathPool.get(fullPath);
      if (cachedFullPath == null) {
        cachedPathPool.put(fullPath, fullPath);
      } else {
        fullPath = cachedFullPath;
      }
    }
    return fullPath;
  }

  public PartialPath getPartialPath() {
    List<String> detachedPath = new ArrayList<>();
    MNode temp = this;
    detachedPath.add(temp.getName());
    while (temp.getParent() != null) {
      temp = temp.getParent();
      detachedPath.add(0, temp.getName());
    }
    return new PartialPath(detachedPath.toArray(new String[0]));
  }

  String concatFullPath() {
    StringBuilder builder = new StringBuilder(name);
    MNode curr = this;
    while (curr.getParent() != null) {
      curr = curr.getParent();
      builder.insert(0, IoTDBConstant.PATH_SEPARATOR).insert(0, curr.name);
    }
    return builder.toString();
  }

  @Override
  public String toString() {
    return this.getName();
  }

  public MNode getParent() {
    return parent;
  }

  public void setParent(MNode parent) {
    this.parent = parent;
  }

  public Map<String, MNode> getChildren() {
    if (children == null) {
      return Collections.emptyMap();
    }
    return children;
  }

  public void setChildren(Map<String, MNode> children) {
    this.children = children;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void serializeTo(MLogWriter logWriter) throws IOException {
    serializeChildren(logWriter);

    logWriter.serializeMNode(this);
  }

  void serializeChildren(MLogWriter logWriter) throws IOException {
    if (children == null) {
      return;
    }
    for (Entry<String, MNode> entry : children.entrySet()) {
      entry.getValue().serializeTo(logWriter);
    }
  }

  public void replaceChild(String measurement, MNode newChildNode) {
    MNode oldChildNode = this.getChild(measurement);
    if (oldChildNode == null) {
      return;
    }
    Map<String, MNode> grandChildren = oldChildNode.getChildren();
    //newChildNode builds parent-child relationship
    newChildNode.setChildren(grandChildren);
    grandChildren.forEach((grandChildName, grandChildNode) -> grandChildNode.setParent(newChildNode));
    newChildNode.setParent(this);

    this.deleteChild(measurement);
    this.addChild(newChildNode.getName(), newChildNode);
  }

}
