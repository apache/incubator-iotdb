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
package org.apache.iotdb.db.metadata.template;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Template {
  private String name;

  private Map<String, IMeasurementSchema> schemaMap = new HashMap<>();

  /**
   * build a template from a createTemplatePlan
   *
   * @param plan createTemplatePlan
   */
  public Template(CreateTemplatePlan plan) {
    name = plan.getName();

    // put measurement into a map
    for (int i = 0; i < plan.getMeasurements().size(); i++) {
      IMeasurementSchema curSchema;
      // vector
      int size = plan.getMeasurements().get(i).size();
      if (size > 1) {
        String[] measurementsArray = new String[size];
        TSDataType[] typeArray = new TSDataType[size];
        TSEncoding[] encodingArray = new TSEncoding[size];

        for (int j = 0; j < size; j++) {
          measurementsArray[j] = plan.getMeasurements().get(i).get(j);
          typeArray[j] = plan.getDataTypes().get(i).get(j);
          encodingArray[j] = plan.getEncodings().get(i).get(j);
        }

        curSchema =
            new VectorMeasurementSchema(
                plan.getSchemaNames().get(i),
                measurementsArray,
                typeArray,
                encodingArray,
                plan.getCompressors().get(i));
      }
      // normal measurement
      else {
        curSchema =
            new MeasurementSchema(
                plan.getMeasurements().get(i).get(0),
                plan.getDataTypes().get(i).get(0),
                plan.getEncodings().get(i).get(0),
                plan.getCompressors().get(i));
      }

      String path = plan.getSchemaNames().get(i);
      if (schemaMap.containsKey(path)) {
        throw new IllegalArgumentException(
            "Duplicate measurement name in create template plan. Name is :" + path);
      }

      schemaMap.put(path, curSchema);
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, IMeasurementSchema> getSchemaMap() {
    return schemaMap;
  }

  public void setSchemaMap(Map<String, IMeasurementSchema> schemaMap) {
    this.schemaMap = schemaMap;
  }

  /**
   * check whether a timeseries path is compatible with this template
   *
   * @param path timeseries path
   * @return whether we can create this new timeseries (whether it's compatible with this template)
   */
  public boolean isCompatible(PartialPath path) {
    return !(schemaMap.containsKey(path.getMeasurement())
        || schemaMap.containsKey(path.getDevicePath().getMeasurement()));
  }

  public boolean hasSchema(String measurementId) {
    return schemaMap.containsKey(measurementId);
  }

  public List<MeasurementMNode> getMeasurementMNode() {
    Set<IMeasurementSchema> deduplicateSchema = new HashSet<>();
    List<MeasurementMNode> res = new ArrayList<>();

    for (IMeasurementSchema measurementSchema : schemaMap.values()) {
      if (deduplicateSchema.add(measurementSchema)) {
        MeasurementMNode measurementMNode = null;
        if (measurementSchema instanceof MeasurementSchema) {
          measurementMNode =
              new MeasurementMNode(
                  null, measurementSchema.getMeasurementId(), measurementSchema, null);

        } else if (measurementSchema instanceof VectorMeasurementSchema) {
          measurementMNode =
              new MeasurementMNode(
                  null,
                  getMeasurementNodeName(measurementSchema.getMeasurementId()),
                  measurementSchema,
                  null);
        }

        res.add(measurementMNode);
      }
    }

    return res;
  }

  public String getMeasurementNodeName(String measurementName) {
    return schemaMap.get(measurementName).getMeasurementId();
  }

  /**
   * get all path in this template (to support aligned by device query)
   *
   * @return a hash map looks like below {vector -> [s1, s2, s3] normal_timeseries -> []}
   */
  public HashMap<String, List<String>> getAllPath() {
    HashMap<String, List<String>> res = new HashMap<>();
    for (Map.Entry<String, IMeasurementSchema> schemaEntry : schemaMap.entrySet()) {
      if (schemaEntry.getValue() instanceof VectorMeasurementSchema) {
        VectorMeasurementSchema vectorMeasurementSchema =
            (VectorMeasurementSchema) schemaEntry.getValue();
        res.put(schemaEntry.getKey(), vectorMeasurementSchema.getValueMeasurementIdList());
      } else {
        res.put(schemaEntry.getKey(), new ArrayList<>());
      }
    }

    return res;
  }

  @Override
  public boolean equals(Object t) {
    if (this == t) {
      return true;
    }
    if (t == null || getClass() != t.getClass()) {
      return false;
    }
    Template that = (Template) t;
    return this.name.equals(that.name) && this.schemaMap.equals(that.schemaMap);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(name).append(schemaMap).toHashCode();
  }
}
