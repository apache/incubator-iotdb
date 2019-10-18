/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.fileSystem;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.reader.DefaultTsFileInput;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum FileInputFactory {

  INSTANCE;

  private static FSType fsType = TSFileDescriptor.getInstance().getConfig().getTSFileStorageFs();
  private static final Logger logger = LoggerFactory.getLogger(FileInputFactory.class);

  public TsFileInput getTsFileInput(String filePath) {
    try {
      if (fsType.equals(FSType.HDFS)) {
        Class<?> clazz = Class.forName("org.apache.iotdb.fileSystem.HDFSInput");
        return (TsFileInput) clazz.getConstructor(String.class).newInstance(filePath);
      } else {
        return new DefaultTsFileInput(Paths.get(filePath));
      }
    } catch (IOException e) {
      logger.error("Failed to get TsFile input of file: {}, ", filePath, e);
      return null;
    } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
      logger.error(
          "Failed to get TsFile input of file: {}. Please check your dependency of Hadoop module.",
          filePath, e);
      return null;
    }
  }
}
