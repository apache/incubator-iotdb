/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.rpc;

import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.lang.reflect.Proxy;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RpcUtils {

  /** How big should the default read and write buffers be? Defaults to 1KB */
  public static final int THRIFT_DEFAULT_BUF_CAPACITY = 1024;
  /**
   * It is used to prevent the size of the parsing package from being too large and allocating the
   * buffer will cause oom. Therefore, the maximum length of the requested memory is limited when
   * reading. Thrift max frame size (16384000 bytes by default), we change it to 512MB.
   */
  public static final int THRIFT_FRAME_MAX_SIZE = 536870912;

  /**
   * if resizeIfNecessary is called continuously with a small size for more than
   * MAX_BUFFER_OVERSIZE_TIME times, we will shrink the buffer to reclaim space.
   */
  public static final int MAX_BUFFER_OVERSIZE_TIME = 5;

  public static final long MIN_SHRINK_INTERVAL = 60_000L;

  private RpcUtils() {
    // util class
  }

  public static final TSStatus SUCCESS_STATUS =
      new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

  public static TSIService.Iface newSynchronizedClient(TSIService.Iface client) {
    return (TSIService.Iface)
        Proxy.newProxyInstance(
            RpcUtils.class.getClassLoader(),
            new Class[] {TSIService.Iface.class},
            new SynchronizedHandler(client));
  }

  /**
   * verify success.
   *
   * @param status -status
   */
  public static void verifySuccess(TSStatus status) throws StatementExecutionException {
    if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      verifySuccess(status.getSubStatus());
      return;
    }
    if (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
      return;
    }
    if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new StatementExecutionException(status);
    }
  }

  public static void verifySuccessWithRedirection(TSStatus status)
      throws StatementExecutionException, RedirectException {
    verifySuccess(status);
    if (status.isSetRedirectNode()) {
      throw new RedirectException(status.getRedirectNode());
    }
  }

  public static void verifySuccessWithRedirectionForMultiDevices(
      TSStatus status, List<String> devices) throws StatementExecutionException, RedirectException {
    verifySuccess(status);
    if (status.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      Map<String, EndPoint> deviceEndPointMap = new HashMap<>();
      List<TSStatus> statusSubStatus = status.getSubStatus();
      for (int i = 0; i < statusSubStatus.size(); i++) {
        TSStatus subStatus = statusSubStatus.get(i);
        if (subStatus.isSetRedirectNode()) {
          deviceEndPointMap.put(devices.get(i), subStatus.getRedirectNode());
        }
      }
      throw new RedirectException(deviceEndPointMap);
    }
  }

  public static void verifySuccess(List<TSStatus> statuses) throws BatchExecutionException {
    StringBuilder errMsgs = new StringBuilder();
    for (TSStatus status : statuses) {
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && status.getCode() != TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
        errMsgs.append(status.getMessage()).append(";");
      }
    }
    if (errMsgs.length() > 0) {
      throw new BatchExecutionException(statuses, errMsgs.toString());
    }
  }

  /** convert from TSStatusCode to TSStatus according to status code and status message */
  public static TSStatus getStatus(TSStatusCode tsStatusCode) {
    return new TSStatus(tsStatusCode.getStatusCode());
  }

  public static TSStatus getStatus(List<TSStatus> statusList) {
    TSStatus status = new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode());
    status.setSubStatus(statusList);
    return status;
  }

  /**
   * convert from TSStatusCode to TSStatus, which has message appending with existed status message
   *
   * @param tsStatusCode status type
   * @param message appending message
   */
  public static TSStatus getStatus(TSStatusCode tsStatusCode, String message) {
    TSStatus status = new TSStatus(tsStatusCode.getStatusCode());
    status.setMessage(message);
    return status;
  }

  public static TSStatus getStatus(int code, String message) {
    TSStatus status = new TSStatus(code);
    status.setMessage(message);
    return status;
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSExecuteStatementResp(status);
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(
      TSStatusCode tsStatusCode, String message) {
    TSStatus status = getStatus(tsStatusCode, message);
    return getTSExecuteStatementResp(status);
  }

  public static TSExecuteStatementResp getTSExecuteStatementResp(TSStatus status) {
    TSExecuteStatementResp resp = new TSExecuteStatementResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    return resp;
  }

  public static TSFetchResultsResp getTSFetchResultsResp(TSStatusCode tsStatusCode) {
    TSStatus status = getStatus(tsStatusCode);
    return getTSFetchResultsResp(status);
  }

  public static TSFetchResultsResp getTSFetchResultsResp(
      TSStatusCode tsStatusCode, String appendMessage) {
    TSStatus status = getStatus(tsStatusCode, appendMessage);
    return getTSFetchResultsResp(status);
  }

  public static TSFetchResultsResp getTSFetchResultsResp(TSStatus status) {
    TSFetchResultsResp resp = new TSFetchResultsResp();
    TSStatus tsStatus = new TSStatus(status);
    resp.setStatus(tsStatus);
    return resp;
  }

  public static final String DEFAULT_TIME_FORMAT = "default";
  public static final String DEFAULT_TIMESTAMP_PRECISION = "ms";

  public static String setTimeFormat(String newTimeFormat) {
    String timeFormat;
    switch (newTimeFormat.trim().toLowerCase()) {
      case "long":
      case "number":
      case DEFAULT_TIME_FORMAT:
      case "iso8601":
        timeFormat = newTimeFormat.trim().toLowerCase();
        break;
      default:
        // use java default SimpleDateFormat to check whether input time format is legal
        // if illegal, it will throw an exception
        new SimpleDateFormat(newTimeFormat.trim());
        timeFormat = newTimeFormat;
        break;
    }
    return timeFormat;
  }

  public static String formatDatetime(
      String timeFormat, String timePrecision, long timestamp, ZoneId zoneId) {
    ZonedDateTime dateTime;
    switch (timeFormat) {
      case "long":
      case "number":
        return Long.toString(timestamp);
      case DEFAULT_TIME_FORMAT:
      case "iso8601":
        return parseLongToDateWithPrecision(
            DateTimeFormatter.ISO_OFFSET_DATE_TIME, timestamp, zoneId, timePrecision);
      default:
        dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId);
        return dateTime.format(DateTimeFormatter.ofPattern(timeFormat));
    }
  }

  public static String formateDatetimeStr(String datetime, StringBuilder digits) {
    if (datetime.contains("+")) {
      String timeZoneStr = datetime.substring(datetime.length() - 6);
      return datetime.substring(0, datetime.length() - 6) + "." + digits + timeZoneStr;
    } else if (datetime.contains("Z")) {
      String timeZoneStr = datetime.substring(datetime.length() - 1);
      return datetime.substring(0, datetime.length() - 1) + "." + digits + timeZoneStr;
    } else {
      String timeZoneStr = datetime.substring(datetime.length());
      return datetime + "." + digits + timeZoneStr;
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static String parseLongToDateWithPrecision(
      DateTimeFormatter formatter, long timestamp, ZoneId zoneid, String timestampPrecision) {
    if (timestampPrecision.equals("ms")) {
      long integerofDate = timestamp / 1000;
      StringBuilder digits = new StringBuilder(Long.toString(timestamp % 1000));
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 3) {
        for (int i = 0; i < 3 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return formateDatetimeStr(datetime, digits);
    } else if (timestampPrecision.equals("us")) {
      long integerofDate = timestamp / 1000_000;
      StringBuilder digits = new StringBuilder(Long.toString(timestamp % 1000_000));
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 6) {
        for (int i = 0; i < 6 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return formateDatetimeStr(datetime, digits);
    } else {
      long integerofDate = timestamp / 1000_000_000L;
      StringBuilder digits = new StringBuilder(Long.toString(timestamp % 1000_000_000L));
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
      String datetime = dateTime.format(formatter);
      int length = digits.length();
      if (length != 9) {
        for (int i = 0; i < 9 - length; i++) {
          digits.insert(0, "0");
        }
      }
      return formateDatetimeStr(datetime, digits);
    }
  }
}
