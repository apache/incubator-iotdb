package org.apache.iotdb.db.index.storage.model.serializer;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iotdb.db.index.storage.model.FixWindowPackage;
import org.apache.iotdb.db.index.utils.MyBytes;
import org.apache.iotdb.tsfile.utils.Pair;

public class FixWindowPackageSerializer {

  protected static FixWindowPackageSerializer instance = new FixWindowPackageSerializer();

  private FixWindowPackageSerializer() {
  }

  public static FixWindowPackageSerializer getInstance() {
    return instance;
  }

  public byte[] serialize(FixWindowPackage dataPackage) {

    ArrayList<byte[]> byteList = new ArrayList<>();
    byte[] aBytes;

    //8byte: time window
    aBytes = MyBytes.longToBytes(dataPackage.getTimeWindow());
    byteList.add(aBytes);
    //4byte: size
    aBytes = MyBytes.intToBytes(dataPackage.size());
    byteList.add(aBytes);

    //size * (8byte long + 4byte float): data
    TreeMap<Long, Object> data = dataPackage.getData();
    for (Map.Entry<Long, Object> entry : data.entrySet()) {
      aBytes = MyBytes.longToBytes(entry.getKey());
      byteList.add(aBytes);
      aBytes = MyBytes.floatToBytes((float) entry.getValue());
      byteList.add(aBytes);
    }

    return MyBytes.concatByteArrayList(byteList);
  }

  public FixWindowPackage deserialize(String key, long startTime, byte[] bytes) {

    int position = 0;

    //8byte: time window
    byte[] aBytes = MyBytes.subBytes(bytes, position, 8);
    long timeWindow = MyBytes.bytesToLong(aBytes);
    position += 8;

    //4byte: size,
    aBytes = MyBytes.subBytes(bytes, position, 4);
    int size = MyBytes.bytesToInt(aBytes);
    position += 4;

    //size * (8byte long + 4byte float): data
    FixWindowPackage pkg = new FixWindowPackage(key,
        new Pair<Long, Long>(startTime, startTime + timeWindow));
    long time;
    float value;
    for (int i = 0; i < size; ++i) {
      aBytes = MyBytes.subBytes(bytes, position, 8);
      time = MyBytes.bytesToLong(aBytes);
      position += 8;
      aBytes = MyBytes.subBytes(bytes, position, 4);
      value = MyBytes.bytesToFloat(aBytes);
      position += 4;
      pkg.add(time, value);
    }

    return pkg;
  }

}
