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
package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class VectorSessionExample {

  private static Session session;
  private static final String ROOT_SG1_D1 = "root.sg_0.d1";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6668, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    createTemplate();
    insertTabletWithAlignedTimeseries();
    //    selectTest();
    session.close();
  }

  private static void selectTest() throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select s1 from root.sg_0.d1 where time >= 0 and time < 10000000");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();

    dataSet =
        session.executeQueryStatement(
            "select * from root.sg_0.d1 where time >= 0 and time < 10000000");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet = session.executeQueryStatement("select last d1 from root.sg_0");
    System.out.println(dataSet.getColumnNames());
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }

    dataSet.closeOperationHandle();
  }

  // be sure template is coordinate with tablet
  private static void createTemplate()
      throws StatementExecutionException, IoTDBConnectionException {
    List<List<String>> measurementList = new ArrayList<>();
    List<String> measurements = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      measurements.add("s" + i);
    }
    measurementList.add(measurements);

    List<List<TSDataType>> dataTypeList = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.INT32);
    dataTypeList.add(dataTypes);

    List<List<TSEncoding>> encodingList = new ArrayList<>();
    List<TSEncoding> encodings = new ArrayList<>();
    for (int i = 1; i <= 2; i++) {
      encodings.add(TSEncoding.RLE);
    }
    encodingList.add(encodings);

    List<CompressionType> compressionTypes = new ArrayList<>();
    compressionTypes.add(CompressionType.SNAPPY);

    session.createDeviceTemplate(
        "template1", measurementList, dataTypeList, encodingList, compressionTypes);
    session.setDeviceTemplate("template1", "root.sg_0");
  }

  private static void insertTabletWithAlignedTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(
        new VectorMeasurementSchema(
            new String[] {"s1", "s2"}, new TSDataType[] {TSDataType.INT64, TSDataType.INT32}));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList);

    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;

      long[] sensor = (long[]) values[0];
      sensor[row] = new Random().nextLong();

      int[] sensors = (int[]) values[1];
      sensors[row] = new Random().nextInt();

      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet, true);
      tablet.reset();
    }

    session.executeNonQueryStatement("flush");
  }
}
