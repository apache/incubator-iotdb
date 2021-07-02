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

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.compaction.cross.inplace.selector.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MaxSeriesMergeFileSelectorTest extends MergeTest {

  @Test
  public void testFullSelection() throws MergeException, IOException {
    CrossSpaceMergeResource resource = new CrossSpaceMergeResource(seqResources, unseqResources);
    MaxSeriesMergeFileSelector mergeFileSelector =
        new MaxSeriesMergeFileSelector(resource, Long.MAX_VALUE);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources, seqSelected);
    assertEquals(unseqResources, unseqSelected);
    assertEquals(
        MaxSeriesMergeFileSelector.MAX_SERIES_NUM, mergeFileSelector.getConcurrentMergeNum());
    resource.clear();

    resource = new CrossSpaceMergeResource(seqResources.subList(0, 1), unseqResources);
    mergeFileSelector = new MaxSeriesMergeFileSelector(resource, Long.MAX_VALUE);
    result = mergeFileSelector.select();
    seqSelected = result[0];
    unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources, unseqSelected);
    assertEquals(
        MaxSeriesMergeFileSelector.MAX_SERIES_NUM, mergeFileSelector.getConcurrentMergeNum());
    resource.clear();

    resource = new CrossSpaceMergeResource(seqResources, unseqResources.subList(0, 1));
    mergeFileSelector = new MaxSeriesMergeFileSelector(resource, Long.MAX_VALUE);
    result = mergeFileSelector.select();
    seqSelected = result[0];
    unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 1), seqSelected);
    assertEquals(unseqResources.subList(0, 1), unseqSelected);
    assertEquals(
        MaxSeriesMergeFileSelector.MAX_SERIES_NUM, mergeFileSelector.getConcurrentMergeNum());
    resource.clear();
  }

  @Test
  public void testNonSelection() throws MergeException, IOException {
    CrossSpaceMergeResource resource = new CrossSpaceMergeResource(seqResources, unseqResources);
    MaxSeriesMergeFileSelector mergeFileSelector = new MaxSeriesMergeFileSelector(resource, 1);
    List[] result = mergeFileSelector.select();
    assertEquals(0, result.length);
    assertEquals(0, mergeFileSelector.getConcurrentMergeNum());
    resource.clear();
  }

  @Test
  public void testRestrictedSelection() throws MergeException, IOException {
    CrossSpaceMergeResource resource = new CrossSpaceMergeResource(seqResources, unseqResources);
    MaxSeriesMergeFileSelector mergeFileSelector = new MaxSeriesMergeFileSelector(resource, 400000);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 4), seqSelected);
    assertEquals(unseqResources.subList(0, 4), unseqSelected);
    assertEquals(
        MaxSeriesMergeFileSelector.MAX_SERIES_NUM, mergeFileSelector.getConcurrentMergeNum());
    resource.clear();
  }

  @Test
  public void testRestrictedSelection2() throws MergeException, IOException {
    CrossSpaceMergeResource resource = new CrossSpaceMergeResource(seqResources, unseqResources);
    MaxSeriesMergeFileSelector mergeFileSelector = new MaxSeriesMergeFileSelector(resource, 100000);
    List[] result = mergeFileSelector.select();
    List<TsFileResource> seqSelected = result[0];
    List<TsFileResource> unseqSelected = result[1];
    assertEquals(seqResources.subList(0, 2), seqSelected);
    assertEquals(unseqResources.subList(0, 2), unseqSelected);
    resource.clear();
  }
}
