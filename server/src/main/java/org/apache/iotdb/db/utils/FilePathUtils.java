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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.aggregation.impl.AvgAggrResult;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

public class FilePathUtils {

  private static final String PATH_SPLIT_STRING = File.separator.equals("\\") ? "\\\\" : "/";

  private FilePathUtils() {
    // forbidding instantiation
  }

  /**
   * Format file path to end with File.separator
   *
   * @param filePath origin file path
   * @return Regularized Path
   */
  public static String regularizePath(String filePath) {
    if (filePath.length() > 0 && filePath.charAt(filePath.length() - 1) != File.separatorChar) {
      filePath = filePath + File.separatorChar;
    }
    return filePath;
  }

  /**
   * IMPORTANT, when the path of TsFile changes, the following methods should be changed
   * accordingly. The sequence TsFile is located at ${IOTDB_DATA_DIR}/data/sequence/. The unsequence
   * TsFile is located at ${IOTDB_DATA_DIR}/data/unsequence/. Where different storage group's TsFile
   * is located at <fullStorageGroupName>/<virtualStorageGroupName>/<timePartitionId>/<fileName>.
   * For example, one sequence TsFile may locate at
   * /data/data/sequence/root.group_9/0/0/1611199237113-4-0.tsfile
   *
   * @param resource the tsFileResource
   */
  public static String[] splitTsFilePath(TsFileResource resource) {
    return resource.getTsFile().getAbsolutePath().split(PATH_SPLIT_STRING);
  }

  public static String getLogicalStorageGroupName(TsFileResource resource) {
    String[] pathSegments = splitTsFilePath(resource);
    return pathSegments[pathSegments.length - 4];
  }

  public static String getVirtualStorageGroupId(TsFileResource resource) {
    String[] pathSegments = splitTsFilePath(resource);
    return pathSegments[pathSegments.length - 3];
  }

  public static long getTimePartitionId(TsFileResource resource) {
    String[] pathSegments = splitTsFilePath(resource);
    return Long.parseLong(pathSegments[pathSegments.length - 2]);
  }

  /**
   * @param resource the RemoteTsFileResource
   * @return the file in the snapshot is a hardlink, remove the hardlink suffix
   */
  public static String getTsFileNameWithoutHardLink(TsFileResource resource) {
    String[] pathSegments = splitTsFilePath(resource);
    return pathSegments[pathSegments.length - 1].substring(
        0, pathSegments[pathSegments.length - 1].lastIndexOf(TsFileConstant.PATH_SEPARATOR));
  }

  public static String getTsFilePrefixPath(TsFileResource resource) {
    String[] pathSegments = splitTsFilePath(resource);
    int pathLength = pathSegments.length;
    return pathSegments[pathLength - 4]
        + File.separator
        + pathSegments[pathLength - 3]
        + File.separator
        + pathSegments[pathLength - 2];
  }

  public static Pair<String, Long> getLogicalSgNameAndTimePartitionIdPair(TsFileResource resource) {
    String[] pathSegments = splitTsFilePath(resource);
    return new Pair<>(
        pathSegments[pathSegments.length - 4],
        Long.parseLong(pathSegments[pathSegments.length - 2]));
  }

  public static String[] splitTsFilePath(String tsFileAbsolutePath) {
    return tsFileAbsolutePath.split(PATH_SPLIT_STRING);
  }

  /**
   * Transform an originalPath to a partial path that satisfies given level. Path nodes exceed the
   * given level will be replaced by "*", e.g. generatePartialPathByLevel("root.sg.dh.d1.s1", 2)
   * will return "root.sg.dh.*.s1"
   *
   * @param originalPath the original timeseries path
   * @param pathLevel the expected path level
   * @return result partial path
   */
  public static String generatePartialPathByLevel(String originalPath, int pathLevel)
      throws IllegalPathException {
    String[] tmpPath = MetaUtils.splitPathToDetachedPath(originalPath);
    if (tmpPath.length <= pathLevel) {
      return originalPath;
    }
    StringBuilder transformedPath = new StringBuilder();
    transformedPath.append(tmpPath[0]);
    for (int k = 1; k < tmpPath.length - 1; k++) {
      if (k <= pathLevel) {
        transformedPath.append(TsFileConstant.PATH_SEPARATOR).append(tmpPath[k]);
      } else {
        transformedPath.append(TsFileConstant.PATH_SEPARATOR).append(IoTDBConstant.PATH_WILDCARD);
      }
    }
    transformedPath.append(TsFileConstant.PATH_SEPARATOR).append(tmpPath[tmpPath.length - 1]);
    return transformedPath.toString();
  }

  /**
   * merge the raw record by level, for example raw record [timestamp, root.sg1.d1.s0,
   * root.sg1.d1.s1, root.sg1.d2.s2], level=1 and newRecord data is [100, 1, 1, 1] return [100, 3]
   *
   * @param newRecord
   * @param finalPaths
   * @return
   */
  public static List<AggregateResult> mergeRecordByPath(
      AggregationPlan plan, RowRecord newRecord, Map<String, AggregateResult> finalPaths)
      throws QueryProcessException {
    if (newRecord.getFields().size() < finalPaths.size()) {
      return Collections.emptyList();
    }
    List<AggregateResult> aggregateResultList = new ArrayList<>();
    for (int i = 0; i < newRecord.getFields().size(); i++) {
      if (newRecord.getFields().get(i) == null) {
        aggregateResultList.add(
            AggregateResultFactory.getAggrResultByName(
                plan.getDeduplicatedAggregations().get(i), plan.getDeduplicatedDataTypes().get(i)));
      } else {
        TSDataType dataType = newRecord.getFields().get(i).getDataType();
        AggregateResult aggRet =
            AggregateResultFactory.getAggrResultByName(
                plan.getDeduplicatedAggregations().get(i), dataType);
        if (aggRet.getAggregationType().equals(AggregationType.AVG)) {
          ((AvgAggrResult) aggRet)
              .setAvgResult(dataType, newRecord.getFields().get(i).getDoubleV());
        } else {
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
        }
        aggregateResultList.add(aggRet);
      }
    }
    return mergeRecordByPath(plan, aggregateResultList, finalPaths);
  }

  public static List<AggregateResult> mergeRecordByPath(
      AggregationPlan plan,
      List<AggregateResult> aggResults,
      Map<String, AggregateResult> finalPaths)
      throws QueryProcessException {
    if (aggResults.size() < finalPaths.size()) {
      return Collections.emptyList();
    }
    for (Map.Entry<String, AggregateResult> entry : finalPaths.entrySet()) {
      finalPaths.put(entry.getKey(), null);
    }

    List<AggregateResult> resultSet = new ArrayList<>();
    List<PartialPath> dupPaths = plan.getDeduplicatedPaths();
    try {
      for (int i = 0; i < aggResults.size(); i++) {
        if (aggResults.get(i) != null) {
          String transformedPath =
              generatePartialPathByLevel(dupPaths.get(i).getFullPath(), plan.getLevel());
          String key = plan.getDeduplicatedAggregations().get(i) + "(" + transformedPath + ")";
          AggregateResult tempAggResult = finalPaths.get(key);
          if (tempAggResult == null) {
            finalPaths.put(key, aggResults.get(i));
          } else {
            tempAggResult.merge(aggResults.get(i));
            finalPaths.put(key, tempAggResult);
          }
        }
      }
    } catch (IllegalPathException e) {
      throw new QueryProcessException(e.getMessage());
    }

    for (Map.Entry<String, AggregateResult> entry : finalPaths.entrySet()) {
      resultSet.add(entry.getValue());
    }
    return resultSet;
  }

  public static long splitAndGetTsFileVersion(String tsFileName) {
    String[] names = tsFileName.split(FILE_NAME_SEPARATOR);
    if (names.length != 4) {
      return 0;
    }
    return Long.parseLong(names[1]);
  }
}
