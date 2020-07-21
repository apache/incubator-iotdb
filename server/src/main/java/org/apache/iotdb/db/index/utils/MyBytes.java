package org.apache.iotdb.db.index.utils;

import java.io.UnsupportedEncodingException;
import java.util.List;


//TODO Kangrong comment by English
public class MyBytes {

  /**
   * byte[] convert to int
   */
  public static byte[] intToBytes(int i) {
    byte[] result = new byte[4];
    result[0] = (byte) ((i >> 24) & 0xFF);
    result[1] = (byte) ((i >> 16) & 0xFF);
    result[2] = (byte) ((i >> 8) & 0xFF);
    result[3] = (byte) (i & 0xFF);
    return result;
  }

  /**
   * byte[] convert to int
   */
  public static int bytesToInt(byte[] bytes) {
    int value = 0;
    // high bit to low
    for (int i = 0; i < 4; i++) {
      int shift = (4 - 1 - i) * 8;
      value += (bytes[i] & 0x000000FF) << shift;
    }
    return value;
  }

  /**
   * float杞崲byte
   */
  public static byte[] floatToBytes(float x) {
    byte[] b = new byte[4];
    int l = Float.floatToIntBits(x);
    for (int i = 0; i < 4; i++) {
      b[i] = new Integer(l).byteValue();
      l = l >> 8;
    }
    return b;
  }

  public static int floatSize() {
    return 4;
  }

  /**
   * fill byte array b by x from offset position.
   *
   * @param b result
   */
  public static void floatToBytes(float x, byte[] b, int offset) {
    assert b.length - offset >= 4;
    int l = Float.floatToIntBits(x);
    for (int i = offset; i < 4 + offset; i++) {
      b[i] = new Integer(l).byteValue();
      l = l >> 8;
    }
  }


  /**
   * 閫氳繃byte鏁扮粍鍙栧緱float
   */
  public static float bytesToFloat(byte[] b) {
    int l;
    l = b[0];
    l &= 0xff;
    l |= ((long) b[1] << 8);
    l &= 0xffff;
    l |= ((long) b[2] << 16);
    l &= 0xffffff;
    l |= ((long) b[3] << 24);
    return Float.intBitsToFloat(l);
  }

  /**
   * double杞崲byte
   *
   * @return byte[]
   */
  public static byte[] doubleToBytes(double data) {
    byte[] bytes = new byte[8];
    long value = Double.doubleToLongBits(data);
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = new Long(value).byteValue();
      value = value >> 8;
    }
    return bytes;
  }

  public static int doubleSize() {
    return 8;
  }

  /**
   * 閫氳繃byte鏁扮粍鍙栧緱double
   */
  public static double bytesToDouble(byte[] bytes) {
    long value = bytes[0];
    value &= 0xff;
    value |= ((long) bytes[1] << 8);
    value &= 0xffff;
    value |= ((long) bytes[2] << 16);
    value &= 0xffffff;
    value |= ((long) bytes[3] << 24);
    value &= 0xffffffffL;
    value |= ((long) bytes[4] << 32);
    value &= 0xffffffffffL;
    value |= ((long) bytes[5] << 40);
    value &= 0xffffffffffffL;
    value |= ((long) bytes[6] << 48);
    value &= 0xffffffffffffffL;
    value |= ((long) bytes[7] << 56);

    return Double.longBitsToDouble(value);
  }

  /**
   * float杞崲byte
   */
  public static byte[] boolToBytes(boolean x) {
    byte[] b = new byte[1];
    if (x) {
      b[0] = 1;
    } else {
      b[0] = 0;
    }
    return b;
  }

  public static int boolSize() {
    return 1;
  }

  /**
   * float杞崲byte
   */
  public static byte[] boolToBytes(boolean x, byte[] b, int offset) {
    if (x) {
      b[offset] = 1;
    } else {
      b[offset] = 0;
    }
    return b;
  }

  /**
   * 閫氳繃byte鏁扮粍鍙栧緱float
   */
  public static boolean bytesToBool(byte[] b) {
    if (b.length != 1) {
      return false;
    }
    if (b[0] == 0) {
      return false;
    } else {
      return true;
    }
  }

  public static byte[] longToBytes(long num) {
    byte[] byteNum = new byte[8];
    for (int ix = 0; ix < 8; ++ix) {
      int offset = 64 - (ix + 1) * 8;
      byteNum[ix] = (byte) ((num >> offset) & 0xff);
    }
    return byteNum;
  }

  public static int longSize() {
    return 8;
  }

  public static byte[] longToBytes(long num, byte[] byteNum, int offset_) {
    for (int ix = 0; ix < 8; ++ix) {
      int offset = 64 - (ix + 1) * 8;
      byteNum[ix + offset_] = (byte) ((num >> offset) & 0xff);
    }
    return byteNum;
  }

  public static long bytesToLong(byte[] byteNum) {
    long num = 0;
    for (int ix = 0; ix < 8; ++ix) {
      num <<= 8;
      num |= (byteNum[ix] & 0xff);
    }
    return num;
  }

  public static byte[] StringToBytes(String str) {
    try {
      return str.getBytes("UTF8");
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  public static String bytesToString(byte[] byteStr) {
    try {
      return new String(byteStr, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  // concat two byteArray
  public static byte[] concatByteArray(byte[] a, byte[] b) {
    byte[] c = new byte[a.length + b.length];
    System.arraycopy(a, 0, c, 0, a.length);
    System.arraycopy(b, 0, c, a.length, b.length);
    return c;
  }

  // concat two byteArray
  public static byte[] concatByteArrayList(List<byte[]> list) {
    int size = list.size();
    int len = 0;
    for (byte[] cs : list) {
      len += cs.length;
    }
    byte[] result = new byte[len];
    int pos = 0;
    for (int i = 0; i < size; i++) {
      int l = list.get(i).length;
      System.arraycopy(list.get(i), 0, result, pos, l);
      pos += l;
    }
    return result;
  }

  public static byte[] subBytes(byte[] src, int start, int length) {
    if ((start + length) > src.length) {
      return null;
    }
    if (length <= 0) {
      return null;
    }
    byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = src[start + i];
    }
    return result;
  }
}
