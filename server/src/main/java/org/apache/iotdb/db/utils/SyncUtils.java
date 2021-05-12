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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.sync.conf.SyncSenderDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SyncUtils {

  private static final String IP_SEPARATOR = "\\.";

  private SyncUtils() {}

  /**
   * This method is to get a snapshot file seriesPath according to a tsfile seriesPath. Due to
   * multiple directories, it's necessary to make a snapshot in the same disk. It's used by sync
   * sender.
   */
  public static File getSnapshotFile(File file) {
    String relativeFilePath =
        file.getParentFile().getParentFile().getParentFile().getName()
            + File.separator
            + file.getParentFile().getParentFile().getName()
            + File.separator
            + file.getParentFile().getName()
            + File.separator
            + file.getName();
    FSPath snapshotDir = SyncSenderDescriptor.getInstance().getConfig().getSnapshotPath();
    if (!snapshotDir.toFile().exists()) {
      snapshotDir.toFile().mkdirs();
    }
    return snapshotDir.getChildFile(relativeFilePath);
  }

  /** Verify sending list is empty or not It's used by sync sender. */
  public static boolean isEmpty(Map<String, Map<Long, Map<Long, Set<File>>>> sendingFileList) {
    for (Entry<String, Map<Long, Map<Long, Set<File>>>> entry : sendingFileList.entrySet()) {
      for (Entry<Long, Map<Long, Set<File>>> vgEntry : entry.getValue().entrySet()) {
        for (Entry<Long, Set<File>> innerEntry : vgEntry.getValue().entrySet()) {
          if (!innerEntry.getValue().isEmpty()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   * Verify IP address with IP white list which contains more than one IP segment. It's used by sync
   * sender.
   */
  public static boolean verifyIPSegment(String ipWhiteList, String ipAddress) {
    String[] ipSegments = ipWhiteList.split(",");
    for (String IPsegment : ipSegments) {
      int subnetMask = Integer.parseInt(IPsegment.substring(IPsegment.indexOf('/') + 1));
      IPsegment = IPsegment.substring(0, IPsegment.indexOf('/'));
      if (verifyIP(IPsegment, ipAddress, subnetMask)) {
        return true;
      }
    }
    return false;
  }

  /** Verify IP address with IP segment. */
  private static boolean verifyIP(String ipSegment, String ipAddress, int subnetMark) {
    String ipSegmentBinary;
    String ipAddressBinary;
    String[] ipSplits = ipSegment.split(IP_SEPARATOR);
    DecimalFormat df = new DecimalFormat("00000000");
    StringBuilder ipSegmentBuilder = new StringBuilder();
    for (String IPsplit : ipSplits) {
      ipSegmentBuilder.append(
          df.format(Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
    }
    ipSegmentBinary = ipSegmentBuilder.toString();
    ipSegmentBinary = ipSegmentBinary.substring(0, subnetMark);
    ipSplits = ipAddress.split(IP_SEPARATOR);
    StringBuilder ipAddressBuilder = new StringBuilder();
    for (String IPsplit : ipSplits) {
      ipAddressBuilder.append(
          df.format(Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
    }
    ipAddressBinary = ipAddressBuilder.toString();
    ipAddressBinary = ipAddressBinary.substring(0, subnetMark);
    return ipAddressBinary.equals(ipSegmentBinary);
  }
}
