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

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.junit.Assert;
import org.junit.Test;

public class MetaUtilsTest {

  @Test
  public void testSplitPathToNodes() throws IllegalPathException {
    assertArrayEquals(Arrays.asList("root", "sg", "d1", "s1").toArray(),
        MetaUtils.splitPathToDetachedPath("root.sg.d1.s1"));

    assertArrayEquals(Arrays.asList("root", "sg", "d1", "\"s.1\"").toArray(),
        MetaUtils.splitPathToDetachedPath("root.sg.d1.\"s.1\""));

    assertArrayEquals(Arrays.asList("root", "sg", "d1", "\"s\\\".1\"").toArray(),
        MetaUtils.splitPathToDetachedPath("root.sg.d1.\"s\\\".1\""));

    assertArrayEquals(Arrays.asList("root", "\"s g\"", "d1", "\"s.1\"").toArray(),
        MetaUtils.splitPathToDetachedPath("root.\"s g\".d1.\"s.1\""));

    assertArrayEquals(Arrays.asList("root", "\"s g\"", "\"d_.1\"", "\"s.1.1\"").toArray(),
        MetaUtils.splitPathToDetachedPath("root.\"s g\".\"d_.1\".\"s.1.1\""));

    assertArrayEquals(Arrays.asList("root", "1").toArray(), MetaUtils.splitPathToDetachedPath("root.1"));

    assertArrayEquals(Arrays.asList("root", "sg", "d1", "s", "1").toArray(),
        MetaUtils.splitPathToDetachedPath("root.sg.d1.s.1"));

    try {
      MetaUtils.splitPathToDetachedPath("root.sg.\"d.1\"\"s.1\"");
    } catch (IllegalPathException e) {
      Assert.assertEquals("root.sg.\"d.1\"\"s.1\" is not a legal path", e.getMessage());
    }

    try {
      MetaUtils.splitPathToDetachedPath("root..a");
    } catch (IllegalPathException e) {
      Assert.assertEquals("root..a is not a legal path", e.getMessage());
    }

    try {
      MetaUtils.splitPathToDetachedPath("root.sg.d1.'s1'");
    } catch (IllegalPathException e) {
      Assert.assertEquals("root.sg.d1.'s1' is not a legal path", e.getMessage());
    }
  }

  @Test
  public void testGetMultiFullPaths() {
    MNode rootNode = new MNode(null, "root");

    // builds the relationship of root.a and root.aa
    MNode aNode = new MNode(rootNode, "a");
    rootNode.addChild(aNode.getName(), aNode);
    MNode aaNode = new MNode(rootNode, "aa");
    rootNode.addChild(aaNode.getName(), aaNode);

    // builds the relationship of root.a.b and root.aa.bb
    MNode bNode = new MNode(aNode, "b");
    aNode.addChild(bNode.getName(), bNode);
    MNode bbNode = new MNode(aaNode, "bb");
    aaNode.addChild(bbNode.getName(), bbNode);

    // builds the relationship of root.aa.bb.cc
    MNode ccNode = new MNode(bbNode, "cc");
    bbNode.addChild(ccNode.getName(), ccNode);

    List<String> multiFullPaths = MetaUtils.getMultiFullPaths(rootNode);
    Assert.assertSame(2, multiFullPaths.size());

    multiFullPaths.forEach(fullPath -> {
      if (fullPath.contains("aa")) {
        Assert.assertEquals("root.aa.bb.cc", fullPath);
      } else {
        Assert.assertEquals("root.a.b", fullPath);
      }
    });
  }
}