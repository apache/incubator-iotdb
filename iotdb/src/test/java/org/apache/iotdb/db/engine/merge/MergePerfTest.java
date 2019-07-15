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

package org.apache.iotdb.db.engine.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.junit.Before;

public class MergePerfTest extends MergeTaskTest{

  private int seqFileNum;
  private int unseqFileNum;
  private double unseqRatio;

  private Random random = new Random(System.currentTimeMillis());

  private long timeConsumption;

  @Before
  @Override
  public void setUp() throws IOException, WriteProcessException {
    tempSGDir = new File("tempSG");
    tempSGDir.mkdirs();
    prepareSeries();
    prepareFiles();
  }

  private void prepareFiles() throws IOException, WriteProcessException {
    for (int i = 0; i < seqFileNum; i++) {
      File file = new File(i + "seq.tsfile");
      TsFileResource tsFileResource = new TsFileResource(file);
      seqResources.add(tsFileResource);
      prepareFile(tsFileResource, i * ptNum, ptNum, 0);
    }
    long timeRange = seqFileNum * ptNum;
    long unseqLength = (long) (timeRange * unseqRatio);
    for (int i = 0; i < unseqFileNum; i++) {
      long unseqOffset = (long) ((1.0 - unseqRatio) * random.nextDouble() * timeRange);
      System.out.println(unseqOffset + "  " + unseqLength);
      File file = new File(i + "unseq.tsfile");
      TsFileResource tsFileResource = new TsFileResource(file);
      unseqResources.add(tsFileResource);
      prepareFile(tsFileResource, unseqOffset, unseqLength, 10000);
    }
  }

  public void test() throws Exception {
    setUp();
    timeConsumption = System.currentTimeMillis();
    MergeTask mergeTask =
        new MergeTask(seqResources, unseqResources, tempSGDir.getPath(), (k, v, l) -> {}, "test");
    mergeTask.call();
    timeConsumption = System.currentTimeMillis() - timeConsumption;
    tearDown();
  }

  public static void main(String[] args) throws Exception {

    List<Long> timeConsumptions = new ArrayList<>();
    MergePerfTest perfTest = new MergePerfTest();

    perfTest.seqFileNum = 5;
    perfTest.unseqFileNum = 5;
    perfTest.measurementNum = 100;
    perfTest.deviceNum = 10;
    perfTest.unseqRatio = 0.2;
    perfTest.ptNum = 10000;
    perfTest.flushInterval = 1000;

    for (int i = 0; i < 3; i++) {
      // cache warm-up
      perfTest.test();
    }

//    int[] intParameters = new int[10];
//    for (int i = 1; i <= 10; i++) {
//      intParameters[i-1] = i;
//    }
//    for (int param : intParameters) {
//      perfTest.seqFileNum = param;
//      perfTest.test();
//      timeConsumptions.add(perfTest.timeConsumption);
//    }
//    long[] longParameters = new long[10];
//    for (int i = 1; i <= 10; i++) {
//      longParameters[i-1] = i * 2000;
//    }
//    for (long param : longParameters) {
//      perfTest.ptNum = param;
//      perfTest.test();
//      timeConsumptions.add(perfTest.timeConsumption);
//    }
    double[] doubleParameters = new double[10];
    for (int i = 1; i <= 10; i++) {
      doubleParameters[i-1] = 0.1 * i;
    }
    for (double param : doubleParameters) {
      perfTest.unseqRatio = param;
      perfTest.test();
      timeConsumptions.add(perfTest.timeConsumption);
    }

    System.out.println(timeConsumptions);
  }
}
