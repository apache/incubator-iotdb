package org.apache.iotdb.db.index.storage.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.iotdb.db.index.FloatDigest;
import org.apache.iotdb.db.index.storage.interfaces.IBackendModelCreator;
import org.apache.iotdb.db.index.storage.interfaces.IBackendReader;
import org.apache.iotdb.db.index.storage.interfaces.IBackendWriter;
import org.apache.iotdb.db.index.storage.model.FixWindowPackage;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * this fake store has only one cf and one
 */
public class FakeStore implements IBackendModelCreator, IBackendWriter, IBackendReader {

  private Map<String, TreeMap<Long, FixWindowPackage>> data = new HashMap<String, TreeMap<Long, FixWindowPackage>>();
  private Map<String, TreeMap<Long, FloatDigest>> digests = new HashMap<String, TreeMap<Long, FloatDigest>>();

  @Override
  public void initialize(String ks, int replicaFactor, List<String> columnfamilies)
      throws Exception {
  }

  @Override
  public void addColumnFamily(String ks, String cf) throws Exception {
  }

  @Override
  public void addColumnFamilies(String ks, List<String> cfs) throws Exception {
  }

  @Override
  public FloatDigest[] getDigests(String key, Long[] timeStamps) {
    List<FloatDigest> list = new ArrayList<>();
    Map<Long, FloatDigest> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    FloatDigest pkg = null;
    for (long time : timeStamps) {
      if ((pkg = map.get(time)) != null) {
        list.add(pkg);
      }
    }
    return list.toArray(new FloatDigest[]{});
  }

  @Override
  public FloatDigest getBeforeOrEqualDigest(String key, long timestamp) {
    TreeMap<Long, FloatDigest> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    if (map.floorEntry(timestamp) == null) {
      return null;
    }
    return map.floorEntry(timestamp).getValue();
  }

  @Override
  public FixWindowPackage getBeforeOrEqualPackage(String key, long timestamp) {
    TreeMap<Long, FixWindowPackage> map = data.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    if (map.floorEntry(timestamp) == null) {
      return null;
    }
    return map.floorEntry(timestamp).getValue();
  }

  @Override
  public FloatDigest getAfterOrEqualDigest(String key, long timestamp) {
    TreeMap<Long, FloatDigest> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    if (map.ceilingKey(timestamp) == null) {
      return null;
    }
    return map.ceilingEntry(timestamp).getValue();
  }

  @Override
  public FloatDigest getLatestDigest(String key) throws Exception {
    List<Pair<Long, FloatDigest>> list = new ArrayList<>();
    TreeMap<Long, FloatDigest> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    return map.lastEntry().getValue();
  }

  @Override
  public void write(String key, String cf, long startTimestamp, FixWindowPackage dp)
      throws Exception {
    TreeMap<Long, FixWindowPackage> map = data.computeIfAbsent(key, k -> new TreeMap<>());
    map.put(startTimestamp, dp);
  }

  @Override
  public void write(String key, String cf, long startTimestamp, FloatDigest digest)
      throws Exception {
    TreeMap<Long, FloatDigest> map = this.digests.computeIfAbsent(key, k -> new TreeMap<>());
    map.put(startTimestamp, digest);
  }

  @Override
  public void write(String key, String cf, long startTimestamp, byte[] digest) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void addFloatColumnFamily(String ks, String cf) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public byte[] getBytes(String key, String cf, long startTime) {
    // TODO Auto-generated method stub
    return null;
  }
}
