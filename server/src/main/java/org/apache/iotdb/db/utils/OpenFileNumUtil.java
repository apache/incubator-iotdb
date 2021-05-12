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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.tier.TierManager;
import org.apache.iotdb.tsfile.fileSystem.FSPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;

// Notice : statistics in this class may not be accurate because of limited user authority.
public class OpenFileNumUtil {

  private static final Logger logger = LoggerFactory.getLogger(OpenFileNumUtil.class);
  private static final int PID_ERROR_CODE = -1;
  private static final int UNSUPPORTED_OS_ERROR_CODE = -2;
  private static final int UNKNOWN_STATISTICS_ERROR_CODE = -3;
  private static final String IOTDB_PROCESS_KEY_WORD = IoTDBConstant.GLOBAL_DB_NAME;
  private static final String LINUX_OS_NAME = "linux";
  private static final String MAC_OS_NAME = "mac";
  private static final String SEARCH_PID_LINUX = "ps -aux | grep -i %s | grep -v grep";
  private static final String SEARCH_PID_MAC = "ps aux | grep -i %s | grep -v grep";
  // command 'lsof -p' is available on most Linux distro except CentOS.
  private static final String SEARCH_OPEN_DATA_FILE_BY_PID = "lsof -p %d";

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final TierManager tierManager = TierManager.getInstance();
  private static final String[] COMMAND_TEMPLATE = {"/bin/bash", "-c", ""};
  private static boolean isOutputValid = false;
  private int pid;

  /** constructor, process key word is defined by IOTDB_PROCESS_KEY_WORD. */
  private OpenFileNumUtil() {
    pid = getIotdbPid();
  }

  /**
   * singleton instance.
   *
   * @return instance
   */
  public static OpenFileNumUtil getInstance() {
    return OpenFileNumUtilHolder.INSTANCE;
  }

  /**
   * check if the string is numeric.
   *
   * @param str string need to be checked
   * @return whether the string is a number
   */
  private static boolean isNumeric(String str) {
    if (str == null || "".equals(str)) {
      return false;
    } else {
      for (int i = str.length(); --i >= 0; ) {
        if (!Character.isDigit(str.charAt(i))) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * get IoTDB server process ID by executing command.
   *
   * @return pid of IoTDB server process
   */
  private static int getIotdbPid() {
    int iotdbPid = -1;
    Process pro1;
    Runtime r = Runtime.getRuntime();
    // System.getProperty("os.name") can detect which type of OS is using now.
    // this code can detect Windows, Mac, Unix and Solaris.
    String os = System.getProperty("os.name");
    String osName = os.toLowerCase();
    if (osName.startsWith(LINUX_OS_NAME) || osName.startsWith(MAC_OS_NAME)) {
      try {
        String command;
        if (osName.startsWith(LINUX_OS_NAME)) {
          command = String.format(SEARCH_PID_LINUX, IOTDB_PROCESS_KEY_WORD);
        } else {
          command = String.format(SEARCH_PID_MAC, IOTDB_PROCESS_KEY_WORD);
        }
        COMMAND_TEMPLATE[2] = command;
        pro1 = r.exec(COMMAND_TEMPLATE);
        BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
        String line;
        while ((line = in1.readLine()) != null) {
          line = line.trim();
          String[] temp = line.split("\\s+");
          if (temp.length > 1 && isNumeric(temp[1])) {
            iotdbPid = Integer.parseInt(temp[1]);
            break;
          }
        }
        in1.close();
        pro1.destroy();
      } catch (IOException e) {
        logger.error("Cannot get PID of IoTDB process because ", e);
      }
    } else {
      logger.warn("Unsupported OS {} for OpenFileNumUtil to get the PID of IoTDB.", os);
    }
    return iotdbPid;
  }

  /**
   * set pid.
   *
   * @param pid is the process ID of IoTDB service process
   */
  void setPid(int pid) {
    this.pid = pid;
  }

  /**
   * return statistic Map, whose key belongs to enum OpenFileNumStatistics: TOTAL_OPEN_FILE_NUM is
   * the current total open file number of IoTDB service process; SEQUENCE_FILE_OPEN_NUM is the
   * current open file number under data directory; DELTA_OPEN_FILE_NUM is the current open file
   * number of TsFile; UNSEQUENCE_FILE_OPEN_NUM is the current open file number of unsequence file;
   * WAL_OPEN_FILE_NUM is the current open file number of WAL file; METADATA_OPEN_FILE_NUM is the
   * current open file number of metadata; DIGEST_OPEN_FILE_NUM is the current open file number of
   * fileNodeDir; SOCKET_OPEN_FILE_NUM is the current open socket connection of IoTDB service
   * process.
   *
   * @param pid : IoTDB service pid
   * @return list : statistics list
   */
  private static EnumMap<OpenFileNumStatistics, Integer> getOpenFile(int pid) {
    EnumMap<OpenFileNumStatistics, Integer> resultMap = new EnumMap<>(OpenFileNumStatistics.class);
    // initialize resultMap
    for (OpenFileNumStatistics openFileNumStatistics : OpenFileNumStatistics.values()) {
      resultMap.put(openFileNumStatistics, 0);
    }
    Process pro;
    int lineCount = 0;
    Runtime r = Runtime.getRuntime();
    try {
      String command = String.format(SEARCH_OPEN_DATA_FILE_BY_PID, pid);
      COMMAND_TEMPLATE[2] = command;
      pro = r.exec(COMMAND_TEMPLATE);
      String line;

      try (BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()))) {
        while ((line = in.readLine()) != null) {
          lineCount++;
          countOneFile(line, pid, resultMap);
        }
      }
      if (lineCount < OpenFileNumStatistics.values().length) {
        isOutputValid = false;
        for (OpenFileNumStatistics statistics : OpenFileNumStatistics.values()) {
          resultMap.put(statistics, UNSUPPORTED_OS_ERROR_CODE);
        }
      } else {
        isOutputValid = true;
      }
      pro.destroy();
    } catch (Exception e) {
      logger.error("Cannot get open file number of IoTDB process because ", e);
    }
    return resultMap;
  }

  private static void countOneFile(
      String line, int pid, EnumMap<OpenFileNumStatistics, Integer> resultMap) {
    String[] temp = line.split("\\s+");
    if (!line.contains(Integer.toString(pid)) || temp.length <= 8) {
      return;
    }
    int oldValue = resultMap.get(OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
    resultMap.put(OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM, oldValue + 1);
    for (OpenFileNumStatistics openFileNumStatistics : OpenFileNumStatistics.values()) {
      if (openFileNumStatistics.path == null) {
        continue;
      }
      for (FSPath path : openFileNumStatistics.path) {
        if (temp[8].contains(path.getPath())) {
          oldValue = resultMap.get(openFileNumStatistics);
          resultMap.put(openFileNumStatistics, oldValue + 1);
        }
      }
    }
    if (temp[7].contains("TCP") || temp[7].contains("UDP")) {
      oldValue = resultMap.get(OpenFileNumStatistics.SOCKET_OPEN_FILE_NUM);
      resultMap.put(OpenFileNumStatistics.SOCKET_OPEN_FILE_NUM, oldValue + 1);
    }
  }

  /**
   * Check if runtime OS is supported then return the result list. If pid is abnormal then all
   * statistics returns -1, if OS is not supported then all statistics returns -2
   *
   * @return map
   */
  private EnumMap<OpenFileNumStatistics, Integer> getStatisticMap() {
    EnumMap<OpenFileNumStatistics, Integer> resultMap = new EnumMap<>(OpenFileNumStatistics.class);
    String osName = System.getProperty("os.name").toLowerCase();
    // get runtime OS name, currently only support Linux and MacOS
    if (osName.startsWith(LINUX_OS_NAME) || osName.startsWith(MAC_OS_NAME)) {
      // if pid is normal, then get statistics
      if (pid > 0) {
        resultMap = getOpenFile(pid);
      } else {
        // pid is abnormal, give all statistics abnormal value -1
        for (OpenFileNumStatistics statistics : OpenFileNumStatistics.values()) {
          resultMap.put(statistics, PID_ERROR_CODE);
        }
      }
    } else {
      // operation system not supported, give all statistics abnormal value -2
      for (OpenFileNumStatistics statistics : OpenFileNumStatistics.values()) {
        resultMap.put(statistics, UNSUPPORTED_OS_ERROR_CODE);
      }
    }
    return resultMap;
  }

  /**
   * get statistics.
   *
   * @param statistics get what statistics of open file number
   * @return open file number
   */
  public int get(OpenFileNumStatistics statistics) {
    EnumMap<OpenFileNumStatistics, Integer> statisticsMap = getStatisticMap();
    return statisticsMap.getOrDefault(statistics, UNKNOWN_STATISTICS_ERROR_CODE);
  }

  boolean isCommandValid() {
    return isOutputValid;
  }

  public enum OpenFileNumStatistics {
    TOTAL_OPEN_FILE_NUM(null),
    SEQUENCE_FILE_OPEN_NUM(tierManager.getAllSequenceFileFolders()),
    UNSEQUENCE_FILE_OPEN_NUM(tierManager.getAllUnSequenceFileFolders()),
    WAL_OPEN_FILE_NUM(
        Collections.singletonList(new FSPath(config.getSystemFileStorageFs(), config.getWalDir()))),
    DIGEST_OPEN_FILE_NUM(
        Collections.singletonList(
            new FSPath(config.getSystemFileStorageFs(), config.getSystemDir()))),
    SOCKET_OPEN_FILE_NUM(null);

    // path is a list of directory corresponding to the OpenFileNumStatistics enum element,
    // e.g. data/data/ for SEQUENCE_FILE_OPEN_NUM
    private List<FSPath> path;

    OpenFileNumStatistics(List<FSPath> path) {
      this.path = path;
    }

    public List<FSPath> getPath() {
      return path;
    }
  }

  private static class OpenFileNumUtilHolder {

    private OpenFileNumUtilHolder() {}

    private static final OpenFileNumUtil INSTANCE = new OpenFileNumUtil();
  }
}
