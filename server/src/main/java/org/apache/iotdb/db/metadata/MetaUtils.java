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

import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.mnode.MNode;

public class MetaUtils {

  private MetaUtils() {

  }

  static String getNodeRegByIdx(int idx, String[] nodes) {
    return idx >= nodes.length ? PATH_WILDCARD : nodes[idx];
  }

  /**
   * @param path the path will split. ex, root.ln.
   * @return string array. ex, [root, ln]
   * @throws IllegalPathException if path isn't correct, the exception will throw
   */
  public static String[] splitPathToDetachedPath(String path) throws IllegalPathException {
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    for (int i = 0; i < path.length(); i++) {
      if (path.charAt(i) == IoTDBConstant.PATH_SEPARATOR) {
        String node = path.substring(startIndex, i);
        if(node.isEmpty()) {
          throw new IllegalPathException(path);
        }
        nodes.add(node);
        startIndex = i + 1;
      } else if (path.charAt(i) == '"') {
        int endIndex = path.indexOf('"', i + 1);
        // if a double quotes with escape character
        while (endIndex != -1 && path.charAt(endIndex - 1) == '\\') {
          endIndex = path.indexOf('"', endIndex + 1);
        }
        if (endIndex != -1 && (endIndex == path.length() - 1 || path.charAt(endIndex + 1) == '.')) {
          String node = path.substring(startIndex, endIndex + 1);
          if(node.isEmpty()) {
            throw new IllegalPathException(path);
          }
          nodes.add(node);
          i = endIndex + 1;
          startIndex = endIndex + 2;
        } else {
          throw new IllegalPathException(path);
        }
      } else if (path.charAt(i) == '\'') {
        throw new IllegalPathException(path);
      }
    }
    if (startIndex <= path.length() - 1) {
      String node = path.substring(startIndex);
      if(node.isEmpty()) {
        throw new IllegalPathException(path);
      }
      nodes.add(node);
    }
    return nodes.toArray(new String[0]);
  }

  /**
   * Get storage group path when creating schema automatically is enable
   *
   * e.g., path = root.a.b.c and level = 1, return root.a
   *
   * @param path path
   * @param level level
   */
  public static PartialPath getStorageGroupPathByLevel(PartialPath path, int level) throws MetadataException {
    String[] nodeNames = path.getNodes();
    if (nodeNames.length <= level || !nodeNames[0].equals(IoTDBConstant.PATH_ROOT)) {
      throw new IllegalPathException(path.getFullPath());
    }
    String[] storageGroupNodes = new String[level + 1];
    System.arraycopy(nodeNames, 0, storageGroupNodes, 0, level + 1);
    return new PartialPath(storageGroupNodes);
  }

  public static List<String> getMultiFullPaths(MNode node) {
    if (node == null) {
      return Collections.emptyList();
    }

    List<MNode> lastNodeList = new ArrayList<>();
    collectLastNode(node, lastNodeList);

    List<String> result = new ArrayList<>();
    for (MNode mNode : lastNodeList) {
      result.add(mNode.getFullPath());
    }

    return result;
  }

  public static void collectLastNode(MNode node, List<MNode> lastNodeList) {
    if (node != null) {
      Map<String, MNode> children = node.getChildren();
      if (children.isEmpty()) {
        lastNodeList.add(node);
      }

      for (Entry<String, MNode> entry : children.entrySet()) {
        MNode childNode = entry.getValue();
        collectLastNode(childNode, lastNodeList);
      }
    }
  }
}
