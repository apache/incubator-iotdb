package org.apache.iotdb.db.index.utils;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.Vector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iotdb.db.index.FloatDigest;

public class DataDigestUtil {

  public static FloatDigest aggregate(String key, long startTime, FloatDigest[] dataDigests) {
    if (dataDigests.length == 0) {
      return null;
    }
    return floatAggregate(key, startTime, dataDigests);
  }

  public static FloatDigest getDigest(String key, long startTime, long timeWindow,
      Collection<Object> datas) {
    float[] floatDatas = ArrayUtils.toPrimitive(datas.toArray(new Float[]{}));
    return floatDigest(key, startTime, timeWindow, floatDatas);
  }

  public static FloatDigest getDigest(String key, long startTime, long timeWindow,
      SortedMap<Long, Object> dataPoints) {
    return DataDigestUtil.partFloatDigest(key, startTime, timeWindow,
        dataPoints);
  }


  public static FloatDigest floatDigest(String key, long startTime, long timeWindow,
      float[] data) {
    float max = Float.MIN_VALUE;
    float min = Float.MAX_VALUE;
    long count = 0;
    double sum = 0;
    Vector<Float> container = new Vector<>();
    for (float aData : data) {
      if (aData != Float.MAX_VALUE) {
        if (max < aData) {
          max = aData;
        }
        if (min > aData) {
          min = aData;
        }
        ++count;
        sum += aData;
        container.add(aData);
      }
    }
    float avg = (float) (sum / count);

    BigDecimal squareSum = new BigDecimal(0);
    for (Float float1 : container) {
      Double squaredif = Math.pow(float1 - avg, 2.0);
      squareSum = squareSum.add(new BigDecimal(squaredif));
    }
    return new FloatDigest(key, startTime, timeWindow, max, min, count, avg,
        squareSum);
  }

  public static FloatDigest partFloatDigest(String key, long startTime, long timeWindow,
      SortedMap<Long, Object> dataPoints) {
    float max = Float.MIN_VALUE;
    float min = Float.MAX_VALUE;
    long count = 0;
    double sum = 0;
    BigDecimal squareSum = new BigDecimal(0);
    Vector<Float> container = new Vector<>();
    for (Entry<Long, Object> dataPoint : dataPoints.entrySet()) {
      float value = (float) dataPoint.getValue();
      if (value != Float.MAX_VALUE) {
        if (max < value) {
          max = value;
        }
        if (min > value) {
          min = value;
        }
        count++;
        sum += value;
        container.add(value);
      }
    }
    float avg = (float) (sum / count);

    for (Float aFloat : container) {
      double squareDif = Math.pow(aFloat - avg, 2.0);
      squareSum = squareSum.add(new BigDecimal(squareDif));
    }
    return new FloatDigest(key, startTime, timeWindow, max, min, count, avg,
        squareSum);
  }

  public static FloatDigest floatAggregate(String key, long startTime,
      FloatDigest[] dataDigests) {
    long timeWindow = 0L;

    float max = Float.MIN_VALUE;
    float min = Float.MAX_VALUE;
    long count = 0;
    double sum = 0;
    BigDecimal squareSum = new BigDecimal(0);
    for (FloatDigest dataDigest : dataDigests) {
      FloatDigest floatDigest = (FloatDigest) dataDigest;
      timeWindow = timeWindow + dataDigest.getTimeWindow();

      if (max < floatDigest.getMax()) {
        max = floatDigest.getMax();
      }
      if (min > floatDigest.getMin()) {
        min = floatDigest.getMin();
      }
      sum += (floatDigest.getAvg() * floatDigest.getCount());
      count += floatDigest.getCount();
      squareSum.add(floatDigest.getSquareSum());
    }
    float avg = (float) (sum / count);

    return new FloatDigest(key, startTime, timeWindow, max, min, count, avg,
        squareSum);
  }

}
