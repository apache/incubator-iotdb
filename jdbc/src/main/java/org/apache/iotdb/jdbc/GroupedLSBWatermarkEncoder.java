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
package org.apache.iotdb.jdbc;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.thrift.EncodingUtils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class GroupedLSBWatermarkEncoder implements WatermarkEncoder {
  private String secretKey;
  private String bitString;
  private int markRate = 2;
  private int groupNumber;
  private int maxBitPosition = 5;
  private int minBitPosition = 0;

  public GroupedLSBWatermarkEncoder(
      String secretKey, String bitString, int markRate, int minBitPosition) {
    this.secretKey = secretKey;
    this.bitString = bitString;
    this.groupNumber = bitString.length();
    this.markRate = markRate;
    this.maxBitPosition = minBitPosition;
  }

  public static int hashMod(String val, Integer base) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("ERROR: Cannot find MD5 algorithm!");
    }
    md.update(val.getBytes());
    BigInteger resultInteger = new BigInteger(1, md.digest());
    return resultInteger.mod(new BigInteger(base.toString())).intValue();
  }

  public boolean needEncode(long timestamp) {
    return hashMod(String.format("%s%d", secretKey, timestamp), markRate) == 0;
  }

  private int getGroupId(long timestamp) {
    return hashMod(String.format("%d%s", timestamp, secretKey), groupNumber);
  }

  private int getBitPosition(long timestamp) {
    if (maxBitPosition <= minBitPosition) {
      throw new RuntimeException("Error: minBitPosition is bigger than maxBitPosition");
    }
    int range = maxBitPosition - minBitPosition;
    return minBitPosition
        + hashMod(String.format("%s%d%s", secretKey, timestamp, secretKey), range);
  }

  private boolean getBitValue(long timestamp) {
    int groupId = getGroupId(timestamp);
    int bitIndex = groupId % bitString.length();
    return bitString.charAt(bitIndex) == '1';
  }

  public int encodeInt(int value, long timestamp) {
    int targetBitPosition = getBitPosition(timestamp);
    boolean targetBitValue = getBitValue(timestamp);
    return EncodingUtils.setBit(value, targetBitPosition, targetBitValue);
  }

  public long encodeLong(long value, long timestamp) {
    int targetBitPosition = getBitPosition(timestamp);
    boolean targetBitValue = getBitValue(timestamp);
    return EncodingUtils.setBit(value, targetBitPosition, targetBitValue);
  }

  public float encodeFloat(float value, long timestamp) {
    int intBits = Float.floatToIntBits(value);
    return Float.intBitsToFloat(encodeInt(intBits, timestamp));
  }

  public double encodeDouble(double value, long timestamp) {
    long longBits = Double.doubleToLongBits(value);
    return Double.longBitsToDouble(encodeLong(longBits, timestamp));
  }

  public RowRecord encodeRecord(RowRecord record) {
    long timestamp = record.getTimestamp();
    if (!needEncode(timestamp)) {
      return record;
    }
    List<Field> fields = record.getFields();
    for (Field field : fields) {
      if (field == null || field.getDataType() == null) {
        continue;
      }
      TSDataType dataType = field.getDataType();
      switch (dataType) {
        case INT32:
          int originIntValue = field.getIntV();
          field.setIntV(encodeInt(originIntValue, timestamp));
          break;
        case INT64:
          long originLongValue = field.getLongV();
          field.setLongV(encodeLong(originLongValue, timestamp));
          break;
        case FLOAT:
          float originFloatValue = field.getFloatV();
          field.setFloatV(encodeFloat(originFloatValue, timestamp));
          break;
        case DOUBLE:
          double originDoubleValue = field.getDoubleV();
          field.setDoubleV(encodeDouble(originDoubleValue, timestamp));
          break;
        default:
      }
    }
    return record;
  }
}
