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
import org.apache.iotdb.db.index.storage.model.serializer.FixWindowPackageSerializer;
import org.apache.iotdb.db.index.storage.model.serializer.FloatDigestSerializer;
import org.apache.iotdb.tsfile.utils.Pair;

/**
 * this fake store has only one cf and one
 */

public class FakeByteStore implements IBackendModelCreator, IBackendWriter, IBackendReader {

  private Map<String, TreeMap<Long, byte[]>> data = new HashMap<>();
  private Map<String, TreeMap<Long, byte[]>> digests = new HashMap<>();

  private FixWindowPackageSerializer dataSerializer = FixWindowPackageSerializer.getInstance();
  private FloatDigestSerializer digestSerializer = FloatDigestSerializer.getInstance();

  @Override
  public void initialize(String ks, int replicaFactor, List<String> columnFamilies)
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
    Map<Long, byte[]> map = digests.get(key);
    if (map == null) {
      return null;
    }
    byte[] pkg;
    for (long time : timeStamps) {
      if ((pkg = map.get(time)) != null) {
        list.add(digestSerializer.deserialize(key, time, pkg));
      }
    }
    return list.toArray(new FloatDigest[]{});
  }

  @Override
  public FloatDigest getBeforeOrEqualDigest(String key, long timestamp) {
    TreeMap<Long, byte[]> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    Map.Entry<Long, byte[]> entry = map.floorEntry(timestamp);
    if (entry == null) {
      return null;
    }
    return digestSerializer.deserialize(key, entry.getKey(), entry.getValue());
  }

  @Override
  public FixWindowPackage getBeforeOrEqualPackage(String key, long timestamp) {
    TreeMap<Long, byte[]> map = data.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    Map.Entry<Long, byte[]> entry = map.floorEntry(timestamp);
    if (entry == null) {
      return null;
    }
    return dataSerializer.deserialize(key, entry.getKey(), entry.getValue());
  }

  @Override
  public FloatDigest getAfterOrEqualDigest(String key, long timestamp) {
    TreeMap<Long, byte[]> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    Map.Entry<Long, byte[]> entry = map.ceilingEntry(timestamp);
    if (entry == null) {
      return null;
    }
    return digestSerializer.deserialize(key, entry.getKey(), entry.getValue());
  }

  @Override
  public FloatDigest getLatestDigest(String key) throws Exception {
    List<Pair<Long, FloatDigest>> list = new ArrayList<>();
    TreeMap<Long, byte[]> map = digests.get(key);
    if (map == null || map.size() == 0) {
      return null;
    }
    return digestSerializer.deserialize(key, map.lastKey(), map.lastEntry().getValue());
  }

  @Override
  public void write(String key, String cf, long startTimestamp, FixWindowPackage dp)
      throws Exception {
    TreeMap<Long, byte[]> map = data.computeIfAbsent(key, k -> new TreeMap<>());
    map.put(startTimestamp, dataSerializer.serialize(dp));
  }

  @Override
  public void write(String key, String cf, long startTimestamp, FloatDigest digest)
      throws Exception {
    TreeMap<Long, byte[]> map = this.digests.computeIfAbsent(key, k -> new TreeMap<>());
    map.put(startTimestamp, digestSerializer.serialize(digest));
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
