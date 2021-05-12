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

package org.apache.iotdb.tsfile.constant;

import org.apache.iotdb.tsfile.fileSystem.FSType;

import java.io.File;
import java.util.Random;

public class TestConstant {
  public static final String BASE_OUTPUT_PATH = "target".concat(File.separator);
  public static final float float_min_delta = 0.00001f;
  public static final double double_min_delta = 0.00001d;
  public static final Random random = new Random(System.currentTimeMillis());
  public static final FSType DEFAULT_TEST_FS = FSType.LOCAL;
}
