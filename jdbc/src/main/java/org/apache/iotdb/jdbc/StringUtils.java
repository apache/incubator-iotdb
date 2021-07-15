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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;

public class StringUtils {

  private static byte[] allBytes = new byte[256];

  private static char[] byteToChars = new char[256];

  private static Method toPlainStringMethod;

  static final int WILD_COMPARE_MATCH_NO_WILD = 0;

  static final int WILD_COMPARE_MATCH_WITH_WILD = 1;

  static final int WILD_COMPARE_NO_MATCH = -1;

  static {
    for (int i = -128; i <= 127; i++) allBytes[i - -128] = (byte) i;
    String allBytesString = new String(allBytes, 0, 255);
    int allBytesStringLen = allBytesString.length();
    int j = 0;
    for (; j < 255 && j < allBytesStringLen; j++) byteToChars[j] = allBytesString.charAt(j);
    try {
      toPlainStringMethod = BigDecimal.class.getMethod("toPlainString", new Class[0]);
    } catch (NoSuchMethodException nsme) {
    }
  }

  public static String consistentToString(BigDecimal decimal) {
    if (decimal == null) return null;
    if (toPlainStringMethod != null)
      try {
        return (String) toPlainStringMethod.invoke(decimal, null);
      } catch (InvocationTargetException invokeEx) {

      } catch (IllegalAccessException accessEx) {
      }
    return decimal.toString();
  }

  public static final String fixDecimalExponent(String dString) {
    int ePos = dString.indexOf("E");
    if (ePos == -1) ePos = dString.indexOf("e");
    if (ePos != -1 && dString.length() > ePos + 1) {
      char maybeMinusChar = dString.charAt(ePos + 1);
      if (maybeMinusChar != '-' && maybeMinusChar != '+') {
        StringBuffer buf = new StringBuffer(dString.length() + 1);
        buf.append(dString.substring(0, ePos + 1));
        buf.append('+');
        buf.append(dString.substring(ePos + 1, dString.length()));
        dString = buf.toString();
      }
    }
    return dString;
  }
}
