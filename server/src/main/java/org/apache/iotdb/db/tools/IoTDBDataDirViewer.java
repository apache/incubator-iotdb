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
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class IoTDBDataDirViewer {

  public static void main(String[] args) throws IOException {
    String[] data_dir;
    String outFile = "IoTDB_data_dir_overview.txt";
    if (args.length == 0) {
      String path = "data/data";
      data_dir = path.split(","); // multiple data dirs separated by comma
    } else if (args.length == 1) {
      data_dir = args[0].split(",");
    } else if (args.length == 2) {
      data_dir = args[0].split(",");
      outFile = args[1];
    } else {
      throw new IOException("Invalid parameters. Please check the user guide.");
    }
    System.out.println("output save path:" + outFile);
    System.out.println("data dir num:" + data_dir.length);
    try (PrintWriter pw = new PrintWriter(new FileWriter(outFile))) {
      for (String dir : data_dir) {
        File dirFile = FSPath.parse(dir).toFile();
        File[] seqAndUnseqDirs = dirFile.listFiles();
        if (seqAndUnseqDirs == null || seqAndUnseqDirs.length != 2) {
          throw new IOException(
              "Irregular data dir structure.There should be a sequence and unsequence directory "
                  + "under the data directory "
                  + dirFile.getName());
        }
        List<File> fileList = Arrays.asList(seqAndUnseqDirs);
        fileList.sort((Comparator.comparing(File::getName)));
        if (!seqAndUnseqDirs[0].getName().equals("sequence")
            || !seqAndUnseqDirs[1].getName().equals("unsequence")) {
          throw new IOException(
              "Irregular data dir structure.There should be a sequence and unsequence directory "
                  + "under the data directory "
                  + dirFile.getName());
        }

        printlnBoth(pw, "|==============================================================");
        printlnBoth(pw, "|" + dir);
        printlnBoth(pw, "|--sequence");
        printFilesInSeqOrUnseqDir(seqAndUnseqDirs[0], pw);
        printlnBoth(pw, "|--unsequence");
        printFilesInSeqOrUnseqDir(seqAndUnseqDirs[1], pw);
      }
      printlnBoth(pw, "|==============================================================");
    }
  }

  private static void printFilesInSeqOrUnseqDir(File seqOrUnseqDir, PrintWriter pw)
      throws IOException {
    File[] storageGroupDirs = seqOrUnseqDir.listFiles();
    if (storageGroupDirs == null) {
      throw new IOException(
          "Irregular data dir structure.There should be storage group directories under "
              + "the sequence/unsequence directory "
              + seqOrUnseqDir.getName());
    }
    List<File> fileList = Arrays.asList(storageGroupDirs);
    fileList.sort((Comparator.comparing(File::getName)));
    for (File storageGroup : storageGroupDirs) {
      printlnBoth(pw, "|  |--" + storageGroup.getName());
      printFilesInStorageGroupDir(storageGroup, pw);
    }
  }

  private static void printFilesInStorageGroupDir(File storageGroup, PrintWriter pw)
      throws IOException {
    File[] files = storageGroup.listFiles();
    if (files == null) {
      throw new IOException(
          "Irregular data dir structure.There should be timeInterval directories under "
              + "the storage group directory "
              + storageGroup.getName());
    }
    List<File> fileList = Arrays.asList(files);
    fileList.sort((Comparator.comparing(File::getName)));
    for (File file : files) {
      printlnBoth(pw, "|  |  |--" + file.getName());
      printFilesInTimeInterval(file, pw);
    }
  }

  private static void printFilesInTimeInterval(File timeInterval, PrintWriter pw)
      throws IOException {
    File[] files = timeInterval.listFiles();
    if (files == null) {
      throw new IOException(
          "Irregular data dir structure.There should be tsfiles under "
              + "the timeInterval directories directory "
              + timeInterval.getName());
    }
    List<File> fileList = Arrays.asList(files);
    fileList.sort((Comparator.comparing(File::getName)));
    for (File file : files) {
      printlnBoth(pw, "|  |  |  |--" + file.getName());

      // To print the content if it is a tsfile.resource
      if (file.getName().endsWith(".tsfile.resource")) {
        printResource(file.getAbsolutePath(), pw);
      }
    }
  }

  private static void printResource(String filename, PrintWriter pw) throws IOException {
    filename = filename.substring(0, filename.length() - 9);
    TsFileResource resource = new TsFileResource(SystemFileFactory.INSTANCE.getFile(filename));
    resource.deserialize();
    // sort device strings
    SortedSet<String> keys = new TreeSet<>(resource.getDevices());
    for (String device : keys) {
      printlnBoth(
          pw,
          String.format(
              "|  |  |  |  |--device %s, start time %d (%s), end time %d (%s)",
              device,
              resource.getStartTime(device),
              DatetimeUtils.convertMillsecondToZonedDateTime(resource.getStartTime(device)),
              resource.getEndTime(device),
              DatetimeUtils.convertMillsecondToZonedDateTime(resource.getEndTime(device))));
    }
  }

  private static void printlnBoth(PrintWriter pw, String str) {
    System.out.println(str);
    pw.println(str);
  }
}
