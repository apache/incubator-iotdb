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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.FSUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * This tool can analyze the tsfile.resource files from a folder, or analyze a single
 * tsfile.resource file.
 */
public class TsFileResourcePrinter {

  @SuppressWarnings("squid:S106")
  public static void main(String[] args) throws IOException {

    String folder = "data/data/sequence/root.group_1/0";
    if (args.length >= 1) {
      folder = args[0];
    }
    File folderFile = SystemFileFactory.INSTANCE.getFile(folder);
    if (folderFile.isDirectory()) {
      // analyze the tsfile.resource files from a folder
      File[] files =
          FSFactoryProducer.getFSFactory(FSUtils.getFSType(folderFile))
              .listFilesBySuffix(folderFile.getAbsolutePath(), ".tsfile.resource");
      Arrays.sort(files, Comparator.comparingLong(x -> Long.valueOf(x.getName().split("-")[0])));

      for (File file : files) {
        printResource(file.getAbsolutePath());
      }
      System.out.println("Analyzing the resource file folder " + folder + " finished.");
    } else {
      // analyze a tsfile.resource file
      printResource(folderFile.getAbsolutePath());
      System.out.println("Analyzing the resource file " + folder + " finished.");
    }
  }

  @SuppressWarnings("squid:S106")
  public static void printResource(String filename) throws IOException {
    filename = filename.substring(0, filename.length() - 9);
    TsFileResource resource = new TsFileResource(SystemFileFactory.INSTANCE.getFile(filename));
    System.out.printf("Analyzing %s ...%n", filename);
    System.out.println();
    resource.deserialize();

    System.out.printf(
        "Resource plan index range [%d, %d]%n",
        resource.getMinPlanIndex(), resource.getMaxPlanIndex());

    for (String device : resource.getDevices()) {
      System.out.printf(
          "device %s, start time %d (%s), end time %d (%s)%n",
          device,
          resource.getStartTime(device),
          DatetimeUtils.convertMillsecondToZonedDateTime(resource.getStartTime(device)),
          resource.getEndTime(device),
          DatetimeUtils.convertMillsecondToZonedDateTime(resource.getEndTime(device)));
    }
    System.out.println();
  }
}
