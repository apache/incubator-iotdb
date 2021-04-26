/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.common;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.NotImplementedException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class IndexUtils {

  /**
   * justify whether the fullPath matches the pathWithStar. The two paths must have the same node
   * lengths. Refer to {@code org.apache.iotdb.db.metadata.MManager#match}
   *
   * @param pathWithStar a path with wildcard characters.
   * @param fullPath a full path without wildcard characters.
   * @return true if fullPath matches pathWithStar.
   */
  public static boolean match(PartialPath pathWithStar, PartialPath fullPath) {
    String[] fullNodes = fullPath.getNodes();
    String[] starNodes = pathWithStar.getNodes();
    if (starNodes.length != fullNodes.length) {
      return false;
    }
    for (int i = 0; i < fullNodes.length; i++) {
      if (!"*".equals(starNodes[i]) && !fullNodes[i].equals(starNodes[i])) {
        return false;
      }
    }
    return true;
  }

  public static String removeQuotation(String v) {
    int start = 0;
    int end = v.length();
    if (v.startsWith("'") || v.startsWith("\"")) {
      start = 1;
    }
    if (v.endsWith("'") || v.endsWith("\"")) {
      end = v.length() - 1;
    }
    return v.substring(start, end);
  }

  public static File getIndexFile(String filePath) {
    return SystemFileFactory.INSTANCE.getFile(filePath);
  }

  private IndexUtils() {}

  public static Map<String, Object> toUpperCaseProps(Map<String, Object> props) {
    Map<String, Object> uppercase = new HashMap<>(props.size());
    for (Entry<String, Object> entry : props.entrySet()) {
      String k = entry.getKey();
      Object v = entry.getValue();
      uppercase.put(k.toUpperCase(), v);
    }
    return uppercase;
  }

  public static Object getValue(TVList srcData, int idx) {
    switch (srcData.getDataType()) {
      case INT32:
        return srcData.getInt(idx);
      case INT64:
        return srcData.getLong(idx);
      case FLOAT:
        return srcData.getFloat(idx);
      case DOUBLE:
        return (float) srcData.getDouble(idx);
      default:
        throw new NotImplementedException(srcData.getDataType().toString());
    }
  }

  /**
   * "*" is illegal in Windows directory path. Replace it with "#"
   *
   * @param previousDir path which may contains "*"
   * @return path replacing "*" with "#"
   */
  public static String removeIllegalStarInDir(String previousDir) {
    return previousDir.replace('*', '#');
  }
}
