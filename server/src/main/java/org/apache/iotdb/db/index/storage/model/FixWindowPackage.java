package org.apache.iotdb.db.index.storage.model;

import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.iotdb.db.index.FloatDigest;
import org.apache.iotdb.db.index.utils.DataDigestUtil;
import org.apache.iotdb.tsfile.utils.Pair;

public class FixWindowPackage {

  protected String key = null;
  private Pair<Long, Long> timeWindow;
  private TreeMap<Long, Object> treeMap;

  public FixWindowPackage(String key, Pair<Long, Long> timeWindow) {
    this.key = key;
    this.timeWindow = timeWindow;
    treeMap = new TreeMap<>();
  }

  public void add(Pair<Long, Float> data) {
    treeMap.put(data.left, data.right);
  }

  public void add(long time, float value) {
    treeMap.put(time, value);
  }

  public boolean isEmpty() {
    return treeMap.isEmpty();
  }

  public boolean cover(Pair<Long, Float> data) {
    return data.left >= timeWindow.left && data.left < timeWindow.right;
  }

  public boolean cover(long time) {
    return time >= timeWindow.left && time < timeWindow.right;
  }

  public long getStartTime() {
    return timeWindow.left;
  }

  public long getEndTime() {
    return timeWindow.right;
  }

  public int size() {
    return treeMap.size();
  }

  public long getTimeWindow() {
    return timeWindow.right - timeWindow.left;
  }

  public TreeMap<Long, Object> getData() {
    return treeMap;
  }

  public FloatDigest getDigest(long startTime, long endTime) {
    Pair<Long, Long> regularRange = FixWindowPackage
        .rangeRegular(startTime, endTime, timeWindow.left, timeWindow.right);
    long actStartTime = regularRange.left;
    long actEndTime = regularRange.right;

    SortedMap<Long, Object> dataPoints = treeMap.subMap(actStartTime, actEndTime + 1);
    if (dataPoints.size() < 1) {
      return new FloatDigest(true);
    }

    return DataDigestUtil.getDigest(key, timeWindow.left, getTimeWindow(), dataPoints);
  }

  public static Pair<Long, Long> rangeRegular(long queryStartTime, long queryEndTime,
      long startTime, long endTime) {
    long actStartTime = queryStartTime;
    long actEndTime = queryEndTime;

    if (queryStartTime > endTime) {
      actStartTime = endTime;
      actEndTime = endTime;
    } else if (queryEndTime < startTime) {
      actStartTime = startTime;
      actEndTime = startTime;
    } else {
      if (queryStartTime == -1 || queryStartTime < startTime) {
        actStartTime = startTime;
      }
      if (queryEndTime == -1 || queryEndTime > endTime) {
        actEndTime = endTime;
      }

      if (actEndTime < actStartTime) {
        actEndTime = actStartTime;
      }
    }

    return new Pair<>(actStartTime, actEndTime);
  }


  public FloatDigest getDigest() {
    return getDigest(timeWindow.left, timeWindow.right);
  }

  @Override
  public String toString() {
    return treeMap.toString();
  }

}
