package org.apache.iotdb.db.index.utils;


import com.clearspring.analytics.util.Pair;
import org.apache.iotdb.db.index.storage.Config;

public class DigestUtil {

  // if left point, serialNo is the result of getRootCodeBySerialNum().
  // else, serialNo is the result of getRootCodeBySerialNum() of its left bother - 1.
  public static long serialToCode(long serialNumber) {
    if (serialNumber % 2 == 1) {
      return getRootCodeBySerialNum(serialNumber);
    } else {
      return getRootCodeBySerialNum(serialNumber - 1) + 1;
    }
  }

  /**
   * get the right leaf serial number of the code number
   */
  public static long codeToSerial(long code) {
    long begin = code / 2;
    for (long i = begin; ; ++i) {
      if (getRootCodeBySerialNum(i) == code) {
        return i;
      }
    }
  }

  public static long timeToCode(long time) {
    long serial = time / Config.timeWindow + 1;
    return DigestUtil.serialToCode(serial);
  }


  public static long regularTime(long time) {
    return time - time % Config.timeWindow;
  }

  public static long timeToSerial(long time) {
    return time / Config.timeWindow + 1;
  }

  public static void main(String[] args) {
    System.out.println(getRootCodeBySerialNum(13));
  }

  // get root code number by serial number
  public static long getRootCodeBySerialNum(long serialNumber) {
    long count = 0;
    long serialNum = serialNumber;
    for (; serialNum != 0; ++count) {
      serialNum &= (serialNum -  1);
    }
    return (serialNumber << 1) - count;
  }

  /**
   * depth=depth of tree - 1
   */
  public static long getDistanceBetweenLeftestAndRoot(long depth) {
    //2^(depth+1)
    long nodesOfTree = 2 << depth;
    long distance = nodesOfTree - 2L;
    return distance;
  }

  public static long getLeftestCode(long root, long rightestCode) {
    long depth = root - rightestCode;
    long leftestCode = root - getDistanceBetweenLeftestAndRoot(depth);
    return leftestCode;
  }

  private static long[] table2 = new long[129];

  static {
    for (int i = 0; i <= 128; i++) {
      table2[i] = (long) Math.pow(2, i) - 1;
    }
  }

  public static Pair<Long, Long> getChildrenCode(long parentCode) {
    int i = 0;
    long root = parentCode;
    int skew = 0;
    while (true) {
      i = 0;
      while (i <= 128) {
        if (root == 1) {
          return new Pair<Long, Long>(skew + 1L, skew + 1L);
        } else if (root == 2) {
          return new Pair<Long, Long>(skew + 2L, skew + 2L);
        }
        if (root == table2[i]) {
          return new Pair<Long, Long>(root / 2 + skew, parentCode - 1);
        }
        if (root < table2[i]) {
          i--;
          break;
        } else {
          i++;
        }
      }
      root -= table2[i];
      skew += table2[i];
    }
  }
}