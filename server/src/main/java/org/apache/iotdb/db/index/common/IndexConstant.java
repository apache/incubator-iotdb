/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.common;

public class IndexConstant {

  // SQL show
  public static final String ID = "ID";
  public static final String NON_IMPLEMENTED_MSG = "Not implemented yet";

  public static final String ROUTER_DIR = "router";
  public static final String META_DIR_NAME = "index_meta";
  public static final String INDEX_DATA_DIR_NAME = "index_data";
  public static final String STORAGE_GROUP_INDEXING_SUFFIX = ".sg_indexing";
  public static final String STORAGE_GROUP_INDEXED_SUFFIX = ".sg_index";

  public static final String INDEXING_SUFFIX = ".indexing";
  public static final String INDEXED_SUFFIX = ".index";

  // whole matching
  public static final int NON_SET_TOP_K = -1;
  public static final String TOP_K = "TOP_K";

  // subsequence matching: sliding window
  public static final String INDEX_WINDOW_RANGE = "INDEX_WINDOW_RANGE";
  public static final String INDEX_RANGE_STRATEGY = "INDEX_RANGE_STRATEGY";
  public static final String INDEX_SLIDE_STEP = "INDEX_SLIDE_STEP";

  public static final String INDEX_MAGIC = "IoTDBIndex";
  public static final String DEFAULT_PROP_NAME = "DEFAULT";

  public static final int INDEX_MAP_INIT_RESERVE_SIZE = 5;

  public static final String PATTERN = "PATTERN";
  public static final String THRESHOLD = "THRESHOLD";
  public static final String BORDER = "BORDER";

  // MBR Index parameters
  public static final String FEATURE_DIM = "FEATURE_DIM";
  public static final String DEFAULT_FEATURE_DIM = "4";
  public static final String SEED_PICKER = "SEED_PICKER";
  public static final String MAX_ENTRIES = "MAX_ENTRIES";
  public static final String MIN_ENTRIES = "MIN_ENTRIES";

  // RTree PAA parameters
  public static final String PAA_DIM = "PAA_DIM";
  public static final String SERIES_LENGTH = "SERIES_LENGTH";
  public static final String DEFAULT_SERIES_LENGTH = "16";
  public static final String DEFAULT_RTREE_PAA_DISTANCE = "2";

  // Distance
  public static final String DISTANCE = "DISTANCE";
  public static final String L_INFINITY = "L_INFINITY";
  public static final String DEFAULT_DISTANCE = "2";

  // ELB Type
  public static final String ELB_TYPE = "ELB_TYPE";
  public static final String ELB_TYPE_ELE = "ELE";
  public static final String ELB_TYPE_SEQ = "SEQ";
  public static final String DEFAULT_ELB_TYPE = "SEQ";
  public static final int DEFAULT_BLOCK_SIZE = 20;

  // ELB: calc param
  public static final String BLOCK_SIZE = "BLOCK_SIZE";
  public static final String ELB_CALC_PARAM = "ELB_CALC_PARAM";
  public static final String DEFAULT_ELB_CALC_PARAM = "SINGLE";
  public static final String ELB_CALC_PARAM_SINGLE = "SINGLE";
  public static final String ELB_THRESHOLD_BASE = "ELB_THRESHOLD_BASE";
  public static final String ELB_THRESHOLD_RATIO = "ELB_THRESHOLD_RATIO";
  public static final double ELB_DEFAULT_THRESHOLD_RATIO = 0.1;

  public static final String MISSING_PARAM_ERROR_MESSAGE = "missing parameter: %s";

  private IndexConstant() {}
}
