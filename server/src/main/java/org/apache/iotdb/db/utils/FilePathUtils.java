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
package org.apache.iotdb.db.utils;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class FilePathUtils {

  private static final String PATH_SPLIT_STRING = File.separator.equals("\\") ? "\\\\" : "/";

  private FilePathUtils() {
    // forbidding instantiation
  }

  /**
   * Format file path to end with File.separator
   * @param filePath origin file path
   * @return Regularized Path
   */
  public static String regularizePath(String filePath){
    if (filePath.length() > 0
        && filePath.charAt(filePath.length() - 1) != File.separatorChar) {
      filePath = filePath + File.separatorChar;
    }
    return filePath;
  }

  public static String[] splitTsFilePath(TsFileResource resource) {
    return resource.getTsFile().getAbsolutePath().split(PATH_SPLIT_STRING);
  }

  public static String[] splitTsFilePath(String tsFileAbsolutePath) {
    return tsFileAbsolutePath.split(PATH_SPLIT_STRING);
  }

  /**
   * get paths from group by level, like root.sg1.d2.s0, root.sg1.d1.s0
   * level=1, return [root.sg1.*.s0, 0] and pathIndex turns to be [[0, root.sg1.*.s0],
   * [1, root.sg1.*.s0]]
   * @param plan the original Aggregation Plan
   * @param pathIndex the mapping from index of aggregations to the result path name
   * @return
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static Map<String, AggregateResult> getPathByLevel(AggregationPlan plan,
          Map<Integer, String> pathIndex) throws QueryProcessException {
    // pathGroupByLevel -> count
    Map<String, AggregateResult> finalPaths = new TreeMap<>();

    List<PartialPath> seriesPaths = plan.getDeduplicatedPaths();
    List<TSDataType> dataTypes = plan.getDeduplicatedDataTypes();
    for (int i = 0; i < seriesPaths.size(); i++) {
      String[] tmpPath;
      try {
        tmpPath = MetaUtils.splitPathToDetachedPath(seriesPaths.get(i).getFullPath());
      } catch (IllegalPathException e) {
        throw new QueryProcessException(e.getMessage());
      }

      String key;
      if (tmpPath.length <= plan.getLevel()) {
        key = seriesPaths.get(i).getFullPath();
      } else {
        StringBuilder path = new StringBuilder();
        path.append(tmpPath[0]);
        for (int k = 1; k < tmpPath.length - 1; k++) {
          if (k <= plan.getLevel()) {
            path.append(TsFileConstant.PATH_SEPARATOR).append(tmpPath[k]);
          } else {
            path.append(TsFileConstant.PATH_SEPARATOR).append(IoTDBConstant.PATH_WILDCARD);
          }
        }
        path.append(TsFileConstant.PATH_SEPARATOR).append(seriesPaths.get(i).getMeasurement());
        key = path.toString();
      }
      AggregateResult aggRet = AggregateResultFactory
              .getAggrResultByName(plan.getAggregations().get(i), dataTypes.get(i));
      finalPaths.putIfAbsent(key, aggRet);
      pathIndex.put(i, key);
    }

    return finalPaths;
  }

  /**
   * mergeUnseq the raw record by level, for example
   * raw record [timestamp, root.sg1.d1.s0, root.sg1.d1.s1, root.sg1.d2.s2], level=1
   * and newRecord data is [100, 1, 1, 1]
   * return [100, 3]
   *
   * @param newRecord
   * @param finalPaths
   * @param pathIndex
   * @return
   */
  public static List<AggregateResult> mergeRecordByPath(
          AggregationPlan plan, RowRecord newRecord, Map<String, AggregateResult> finalPaths,
          Map<Integer, String> pathIndex) {
    if (newRecord.getFields().size() < finalPaths.size()) {
      return Collections.emptyList();
    }
    List<AggregateResult> aggregateResultList = new ArrayList<>();
    for (int i = 0; i < newRecord.getFields().size(); i++) {
      if (newRecord.getFields().get(i) == null) {
        aggregateResultList.add(AggregateResultFactory.getAggrResultByName(
            plan.getAggregations().get(i), plan.getDeduplicatedDataTypes().get(i)));
      } else {
        TSDataType dataType = newRecord.getFields().get(i).getDataType();
        AggregateResult aggRet = AggregateResultFactory.getAggrResultByName(
            plan.getAggregations().get(i), dataType);
        switch (dataType) {
          case TEXT:
            aggRet.setBinaryValue(newRecord.getFields().get(i).getBinaryV());
            break;
          case INT32:
            aggRet.setIntValue(newRecord.getFields().get(i).getIntV());
            break;
          case INT64:
            aggRet.setLongValue(newRecord.getFields().get(i).getLongV());
            break;
          case FLOAT:
            aggRet.setFloatValue(newRecord.getFields().get(i).getFloatV());
            break;
          case DOUBLE:
            aggRet.setDoubleValue(newRecord.getFields().get(i).getDoubleV());
            break;
          case BOOLEAN:
            aggRet.setBooleanValue(newRecord.getFields().get(i).getBoolV());
            break;
          default:
            throw new UnSupportedDataTypeException(dataType.toString());
        }
        aggregateResultList.add(aggRet);
      }
    }
    return mergeRecordByPath(aggregateResultList, finalPaths, pathIndex);
  }

  public static List<AggregateResult> mergeRecordByPath(
          List<AggregateResult> aggResults, Map<String, AggregateResult> finalPaths,
          Map<Integer, String> pathIndex) {
    if (aggResults.size() < finalPaths.size()) {
      return Collections.emptyList();
    }
    for (Map.Entry<String, AggregateResult> entry : finalPaths.entrySet()) {
      finalPaths.put(entry.getKey(), null);
    }

    List<AggregateResult> resultSet = new ArrayList<>();
    for (int i = 0; i < aggResults.size(); i++) {
      if (aggResults.get(i) != null) {
        AggregateResult tempAggResult = finalPaths.get(pathIndex.get(i));
        if (tempAggResult == null) {
          finalPaths.put(pathIndex.get(i), aggResults.get(i));
        } else {
          tempAggResult.merge(aggResults.get(i));
          finalPaths.put(pathIndex.get(i), tempAggResult);
        }
      }
    }

    for (Map.Entry<String, AggregateResult> entry : finalPaths.entrySet()) {
      resultSet.add(entry.getValue());
    }
    return resultSet;
  }

  public static long splitAndGetTsFileVersion(String tsFileName) {
    String[] names = tsFileName.split(FILE_NAME_SEPARATOR);
    if (names.length != 3) {
      return 0;
    }
    return Long.parseLong(names[1]);
  }
}
