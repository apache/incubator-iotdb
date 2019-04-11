/**
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * This class stores all the metadata info for every deviceId and every timeseries.
 */
public class Metadata {

  private Map<String, List<MeasurementSchema>> seriesMap;
  private Map<String, List<String>> deviceIdMap;

  public Metadata(Map<String, List<MeasurementSchema>> seriesMap,
      Map<String, List<String>> deviceIdMap) {
    this.seriesMap = seriesMap;
    this.deviceIdMap = deviceIdMap;
  }

  /**
   * function for getting series for one type.
   */
  public List<MeasurementSchema> getSeriesForOneType(String type) throws PathErrorException {
    if (this.seriesMap.containsKey(type)) {
      return seriesMap.get(type);
    } else {
      throw new PathErrorException("Input deviceIdType is not exist. " + type);
    }
  }

  /**
   * function for getting devices for one type.
   */
  public List<String> getDevicesForOneType(String type) throws PathErrorException {
    if (this.seriesMap.containsKey(type)) {
      return deviceIdMap.get(type);
    } else {
      throw new PathErrorException("Input deviceIdType is not exist. " + type);
    }
  }

  public Map<String, List<MeasurementSchema>> getSeriesMap() {
    return seriesMap;
  }

  public Map<String, List<String>> getDeviceMap() {
    return deviceIdMap;
  }

  /**
   * combine multiple metadatas
   */
  public static Metadata combineMetadatas(Metadata[] metadatas) {
    Map<String, List<MeasurementSchema>> seriesMap = new HashMap<>();
    Map<String, List<String>> deviceIdMap = new HashMap<>();
    Map<String, Map<String, MeasurementSchema>> typeSchemaMap = new HashMap<>();

    if (metadatas == null || metadatas.length == 0) {
      return new Metadata(seriesMap, deviceIdMap);
    }

    for (int i = 0; i < metadatas.length; i++) {
      Map<String, List<MeasurementSchema>> subSeriesMap = metadatas[i].seriesMap;
      for (Entry<String, List<MeasurementSchema>> entry : subSeriesMap.entrySet()) {
        Map<String, MeasurementSchema> map;
        if (typeSchemaMap.containsKey(entry.getKey())) {
          map = typeSchemaMap.get(entry.getKey());
        } else {
          map = new HashMap<>();
        }
        entry.getValue().forEach(schema -> map.put(schema.getMeasurementId(), schema));
        if (!typeSchemaMap.containsKey(entry.getKey())) {
          typeSchemaMap.put(entry.getKey(), map);
        }
      }

      Map<String, List<String>> subDeviceIdMap = metadatas[i].deviceIdMap;
      for (Entry<String, List<String>> entry : subDeviceIdMap.entrySet()) {
        List<String> list;
        if (deviceIdMap.containsKey(entry.getKey())) {
          list = deviceIdMap.get(entry.getKey());
        } else {
          list = new ArrayList<>();
        }
        list.addAll(entry.getValue());
        if (!deviceIdMap.containsKey(entry.getKey())) {
          deviceIdMap.put(entry.getKey(), list);
        }
      }
    }

    for (Entry<String, Map<String, MeasurementSchema>> entry : typeSchemaMap.entrySet()) {
      List<MeasurementSchema> list = new ArrayList<>();
      list.addAll(entry.getValue().values());
      seriesMap.put(entry.getKey(), list);
    }

    return new Metadata(seriesMap, deviceIdMap);
  }

  @Override
  public String toString() {
    return seriesMap.toString() + "\n" + deviceIdMap.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if(this == obj){
      return true;
    }
    if(obj == null){
      return false;
    }
    if(this.getClass() != obj.getClass()){
      return false;
    }

    Metadata metadata = (Metadata) obj;
    return seriesMapEquals(seriesMap, metadata.seriesMap) && deviceIdMapEquals(deviceIdMap, metadata.deviceIdMap);
  }

  /**
   * only used to check if seriesMap is equal to another seriesMap
   */
  private boolean seriesMapEquals(Map<String, List<MeasurementSchema>> map1, Map<String, List<MeasurementSchema>> map2) {
    if (!map1.keySet().equals(map2.keySet())) {
      return false;
    }

    for (Entry<String, List<MeasurementSchema>> entry : map1.entrySet()) {
      List list1 = entry.getValue();
      List list2 = map2.get(entry.getKey());

      if (!listEquals(list1, list2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * only used to check if deviceIdMap is equal to another deviceIdMap
   */
  private boolean deviceIdMapEquals(Map<String, List<String>> map1, Map<String, List<String>> map2) {
    if (!map1.keySet().equals(map2.keySet())) {
      return false;
    }

    for (Entry<String, List<String>> entry : map1.entrySet()) {
      List list1 = entry.getValue();
      List list2 = map2.get(entry.getKey());

      if (!listEquals(list1, list2)) {
        return false;
      }
    }
    return true;
  }

  private boolean listEquals(List list1, List list2) {
    Set set1 = new HashSet();
    set1.addAll(list1);
    Set set2 = new HashSet();
    set2.addAll(list2);

    return set1.equals(set2);
  }
}
