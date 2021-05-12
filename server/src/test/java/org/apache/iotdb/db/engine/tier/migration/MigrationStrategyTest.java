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

package org.apache.iotdb.db.engine.tier.migration;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class MigrationStrategyTest {
  private static final FSFactory fsFactory =
      FSFactoryProducer.getFSFactory(TestConstant.DEFAULT_TEST_FS);

  @Test
  public void testParseSuccess() {
    IMigrationStrategy pinnedStrategy = IMigrationStrategy.parse(PinnedStrategy.class.getName());
    Assert.assertTrue(pinnedStrategy instanceof PinnedStrategy);

    IMigrationStrategy ttlStrategy =
        IMigrationStrategy.parse(Time2LiveStrategy.class.getName() + "(1000)");
    Assert.assertTrue(ttlStrategy instanceof Time2LiveStrategy);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseWithInvalidClass() {
    String className = "XxxStrategy";
    IMigrationStrategy.parse(className);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseWithWrongFormat() {
    String className = Time2LiveStrategy.class.getName() + "()1000";
    IMigrationStrategy.parse(className);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testParseWithWrongParams() {
    String className = Time2LiveStrategy.class.getName() + "(1000, 1000)";
    IMigrationStrategy.parse(className);
  }

  @Test
  public void testPinnedStrategy() {
    IMigrationStrategy strategy = PinnedStrategy.getInstance();
    TsFileResource resource = mockFile(System.currentTimeMillis());
    Assert.assertFalse(strategy.shouldMigrate(resource));
  }

  @Test
  public void testTime2LiveStrategy1() {
    IMigrationStrategy strategy = new Time2LiveStrategy(60000);
    TsFileResource resource = mockFile(System.currentTimeMillis());
    Assert.assertFalse(strategy.shouldMigrate(resource));
  }

  @Test
  public void testTime2LiveStrategy2() throws InterruptedException {
    IMigrationStrategy strategy = new Time2LiveStrategy(500);
    TsFileResource resource = mockFile(System.currentTimeMillis());
    Thread.sleep(1000);
    Assert.assertTrue(strategy.shouldMigrate(resource));
  }

  private TsFileResource mockFile(long createdTimeStamp) {
    File file =
        fsFactory.getFile(
            TestConstant.BASE_OUTPUT_PATH.concat(
                createdTimeStamp
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + IoTDBConstant.FILE_NAME_SEPARATOR
                    + 0
                    + ".tsfile"));
    TsFileResource tsFileResource = new TsFileResource(file);
    tsFileResource.setClosed(true);
    tsFileResource.updatePlanIndexes(0);
    tsFileResource.updateStartTime("root.sg.d1", createdTimeStamp);
    tsFileResource.updateEndTime("root.sg.d1", createdTimeStamp);
    return tsFileResource;
  }
}
