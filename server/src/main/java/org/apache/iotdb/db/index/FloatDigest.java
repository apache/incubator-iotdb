package org.apache.iotdb.db.index;

import java.math.BigDecimal;

/**
 * Float's digest has five property: amx,min.average,square of average and starttime
 *
 * @author zhangjinrui
 */
public class FloatDigest {

  private String key = "";
  private long startTime = -1L;  // -code
  private long timeWindow = -1L;
  private long code = -1L;
  private long serial = -1L;

  public long parent = -1L;

  private float max = 0;
  private float min = 0;
  private float avg = 0;
  private long count = 0;
  private BigDecimal squareSum = new BigDecimal(0.0);
  private boolean isEmpty = false;

  public void setEmpty(boolean empty) {
    isEmpty = empty;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Float.floatToIntBits(avg);
    result = prime * result + (int) (count ^ (count >>> 32));
    result = prime * result + Float.floatToIntBits(max);
    result = prime * result + Float.floatToIntBits(min);
    result = prime * result + ((squareSum == null) ? 0 : squareSum.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "FloatDigest [key=" + key + ", startTime=" + startTime + ", timeWindow=" + timeWindow
        + ", code=" + code + ", parent=" + parent
        + ", serial=" + serial + ", max=" + max + ", min=" + min + ", avg=" + avg + ", count="
        + count
        + ", squareSum=" + squareSum + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    FloatDigest other = (FloatDigest) obj;
    if (Float.floatToIntBits(avg) != Float.floatToIntBits(other.avg)) {
      return false;
    }
    if (count != other.count) {
      return false;
    }
    if (Float.floatToIntBits(max) != Float.floatToIntBits(other.max)) {
      return false;
    }
    if (Float.floatToIntBits(min) != Float.floatToIntBits(other.min)) {
      return false;
    }
    if (squareSum == null) {
      if (other.squareSum != null) {
        return false;
      }
    } else if (!squareSum.equals(other.squareSum)) {
      return false;
    }
    return true;
  }

  public float getMax() {
    return max;
  }

  public void setMax(float max) {
    this.max = max;
  }

  public float getMin() {
    return min;
  }

  public void setMin(float min) {
    this.min = min;
  }

  public float getAvg() {
    return avg;
  }

  public void setAvg(float avg) {
    this.avg = avg;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public BigDecimal getSquareSum() {
    return squareSum;
  }

  public void setSquareSum(BigDecimal squareSum) {
    this.squareSum = squareSum;
  }

  public FloatDigest() {
    super();
  }

  public FloatDigest(boolean isEmpty) {
    this.isEmpty = isEmpty;
  }

  public FloatDigest(String key, long startTime, long timeWindow, long code, long serial) {
    this.key = key;
    this.startTime = startTime;
    this.timeWindow = timeWindow;
    this.code = code;
    this.serial = serial;
  }

  /**
   * 经过merge产生的中间节点的  startTime 都是 -code
   */
  public FloatDigest(FloatDigest left, FloatDigest right) {
    this.key = left.key;
    this.code = right.code + 1;
    if (left.isEmpty && right.isEmpty) {
      isEmpty = true;
      return;
    }
    this.startTime = -this.code;
    this.timeWindow = left.timeWindow + right.timeWindow;

    max = (left.max > right.max) ? left.max : right.max;
    min = (left.min < right.min) ? left.min : right.min;
    count = left.count + right.count;
    avg = (left.avg / (left.count + right.count)) * left.count
        + (right.avg / (left.count + right.count))
        * right.count;
    squareSum = left.squareSum.add(right.squareSum);

    // mark for MPISA
    left.parent = this.code;
    right.parent = this.code;
  }

  public FloatDigest(String key, long startTime, long timeWindow, float max, float min,
      long count, float avg, BigDecimal squareSum) {
    this.key = key;
    this.startTime = startTime;
    this.timeWindow = timeWindow;
    this.max = max;
    this.min = min;
    this.count = count;
    this.avg = avg;
    this.squareSum = squareSum;
  }

  public FloatDigest generateParent(FloatDigest a, FloatDigest b) {
    return new FloatDigest(a, b);
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getTimeWindow() {
    return timeWindow;
  }

  public long getEndTime() {
    return startTime + timeWindow;
  }

  public void setTimeWindow(long timeWindow) {
    this.timeWindow = timeWindow;
  }

  public long getCode() {
    return code;
  }

  public void setCode(long code) {
    this.code = code;
  }

  public long getSerial() {
    return serial;
  }

  public void setSerial(long serial) {
    this.serial = serial;
  }

}
