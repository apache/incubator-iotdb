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

import static org.junit.Assert.assertEquals;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.junit.Before;
import org.junit.Test;

public class MNodeTest{
  private static ExecutorService service;

  @Before
  public void setUp() throws Exception {
    service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("replaceChild-%d").build());
  }

  @Test
  public void testReplaceChild() throws InterruptedException {
    // after replacing a with c, the timeseries root.a.b becomes root.c.b
    MNode rootNode = new MNode(null, "root");

    MNode aNode = new MNode(rootNode, "a");
    rootNode.addChild(aNode.getName(), aNode);

    MNode bNode = new MNode(aNode, "b");
    aNode.addChild(bNode.getName(), bNode);

    for (int i = 0; i < 500; i++) {
      service.submit(new Thread(() -> rootNode.replaceChild(aNode.getName(), new MNode(null, "c"))));
    }

    if (!service.isShutdown()) {
      service.shutdown();
      service.awaitTermination(30, TimeUnit.SECONDS);
    }

    List<String> multiFullPaths = MetaUtils.getMultiFullPaths(rootNode);
    assertEquals("root.c.b", multiFullPaths.get(0));
  }

}