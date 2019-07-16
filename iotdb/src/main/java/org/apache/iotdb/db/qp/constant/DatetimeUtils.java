/**
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
package org.apache.iotdb.db.qp.constant;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;

public class DatetimeUtils {

  private DatetimeUtils() {
    // forbidding instantiation
  }

  public static final DateTimeFormatter ISO_LOCAL_DATE_WIDTH_1_2;

  static {
    ISO_LOCAL_DATE_WIDTH_1_2 = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD).appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('-')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER).toFormatter();
  }

  /**
   * such as '2011/12/03'.
   */
  public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_SLASH;

  static {
    ISO_LOCAL_DATE_WITH_SLASH = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD).appendLiteral('/')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('/')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER).toFormatter();
  }

  /**
   * such as '2011.12.03'.
   */
  public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_DOT;

  static {
    ISO_LOCAL_DATE_WITH_DOT = new DateTimeFormatterBuilder()
            .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD).appendLiteral('.')
            .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER).appendLiteral('.')
            .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER).toFormatter();
  }

  /**
   * such as '10:15:30' or '10:15:30.123'.
   */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_MS;

  static {
    ISO_LOCAL_TIME_WITH_MS = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendLiteral('.')
            .appendValue(ChronoField.MILLI_OF_SECOND, 3).optionalEnd().toFormatter();
  }

  //wmx
  /**
   *  such as '10:15:30' or '10:15:30.123456'.
   */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_US;

  static {
    ISO_LOCAL_TIME_WITH_US  = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendLiteral('.')
            .appendValue(ChronoField.MICRO_OF_SECOND, 6).optionalEnd().toFormatter();
  }

  //wmx
  /**
   *  such as '10:15:30' or '10:15:30.123456789'.
   */
  public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_NS;

  static {
    ISO_LOCAL_TIME_WITH_NS  = new DateTimeFormatterBuilder().appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2).optionalStart().appendLiteral('.')
            .appendValue(ChronoField.NANO_OF_SECOND, 9).optionalEnd().toFormatter();
  }

  /**
   * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_MS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_MS = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WIDTH_1_2).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WIDTH_1_2).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WIDTH_1_2).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /**
   * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /**
   * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral('T').append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /**
   * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId().toFormatter();
  }

  //wmx
  /**
   * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId().toFormatter();
  }

  //wmx
  /**
   * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SPACE_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId().toFormatter();
  }

  /**
   * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_SLASH).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  /**
   * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_MS)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_US)
            .appendOffsetId()
            .toFormatter();
  }

  //wmx
  /**
   * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456789+01:00'.
   */
  public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS;

  static {
    ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS = new DateTimeFormatterBuilder().parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_WITH_DOT).appendLiteral(' ').append(ISO_LOCAL_TIME_WITH_NS)
            .appendOffsetId()
            .toFormatter();
  }

  public static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
          /**
           * The ISO date-time formatter that formats or parses a date-time with an offset, such as
           * '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_MS)

          //wmx
          /**
           * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_US)

          //wmx
          /**
           * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123456789+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_NS)

          /**
           * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH)

          //wmx
          /**
           * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_US)

          //wmx
          /**
           * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123456789+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_NS)

          /**
           * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT)

          //wmx
          /**
           * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_US)

          //wmx
          /**
           * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123456789+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_NS)

          /**
           * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE)

          //wmx
          /**
           * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE_US)

          //wmx
          /**
           * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123456789+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE_NS)

          /**
           * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE)

          //wmx
          /**
           * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_US)

          //wmx
          /**
           * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123456789+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE_NS)

          /**
           * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE)

          //wmx
          /**
           * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_US)

          //wmx
          /**
           * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123456789+01:00'.
           */
          .appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE_NS).toFormatter();

  public static long convertDatetimeStrToMillisecond(String str, ZoneId zoneId)
          throws LogicalOperatorException {
    return convertDatetimeStrToMillisecond(str, toZoneOffset(zoneId), 0);
  }

  //wmx
  public static long getInstantWithPrecision(String str, String timestampPrecision){
    /* str have dot */
    if (str.substring(20) != "") {
      char[] st = str.substring(20).toCharArray();
      for (char s : st) {
        if(s == '.') {
          if ((str.substring(20).length() != 3 && timestampPrecision == "ms")
                  || (str.substring(20).length() != 6 && timestampPrecision == "us")
                  || (str.substring(20).length() != 9 && timestampPrecision == "ns")) {
            System.out.println(str.substring(20));
            System.exit(-1);
          }
        }
      }
    }

    ZonedDateTime zonedDateTime = ZonedDateTime.parse(str, formatter);
    Instant instant = zonedDateTime.toInstant();

    if(timestampPrecision == "us"){
      if (instant.getEpochSecond() < 0 && instant.getNano() > 0) {
        /* adjustment can reduce loss of division */
        long micros = Math.multiplyExact(instant.getEpochSecond() + 1, 1000_000);
        long adjustment = instant.getNano() / 1000 - 1;
        return Math.addExact(micros, adjustment);
      } else {
        long micros = Math.multiplyExact(instant.getEpochSecond(), 1000_000);
        return Math.addExact(micros, instant.getNano() / 1000);
      }
    }
    else if(timestampPrecision == "ns"){
      long nanos = Math.multiplyExact(instant.getEpochSecond(), 1000_000_000);
      return Math.addExact(nanos, instant.getNano());
    }
    return instant.toEpochMilli();
  }

  /**
   * //wmx
   * convert date time string to millisecond, microsecond or nanosecond.
   */
  public static long convertDatetimeStrToMillisecond(String str, ZoneOffset offset, int depth)
          throws LogicalOperatorException {

    //wmx
    String timestampPrecision = IoTDBDescriptor.getInstance().getConfig().getTimestampPrecision();

    if (depth >= 2){
      throw new DateTimeException(
              String.format("Failed to convert %s to millisecond, zone offset is %s, "
                      + "please input like 2011-12-03T10:15:30 or 2011-12-03T10:15:30+01:00", str, offset));
    }
    if (str.contains("Z")){
      return convertDatetimeStrToMillisecond(str.substring(0, str.indexOf('Z')) + "+00:00", offset, depth);
    } else if (str.length() - str.lastIndexOf('+') != 6 && str.length() - str.lastIndexOf('-') != 6) {
      return convertDatetimeStrToMillisecond(str + offset, offset, depth + 1);
    } else if (str.contains("[")  || str.contains("]")) {
      throw new DateTimeException(
              String.format("%s with [time-region] at end is not supported now, "
                      + "please input like 2011-12-03T10:15:30 or 2011-12-03T10:15:30+01:00", str));
    }
    //wmx
    return getInstantWithPrecision(str, timestampPrecision);
  }

  public static ZoneOffset toZoneOffset(ZoneId zoneId) {
    return zoneId.getRules().getOffset(Instant.now());
  }

  public static ZonedDateTime convertMillsecondToZonedDateTime(long millisecond) {
    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisecond),
            IoTDBDescriptor.getInstance().getConfig().getZoneID());
  }
}
