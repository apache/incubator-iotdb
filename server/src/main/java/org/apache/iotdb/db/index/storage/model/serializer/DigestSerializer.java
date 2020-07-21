package org.apache.iotdb.db.index.storage.model.serializer;

import java.util.ArrayList;
import org.apache.iotdb.db.index.FloatDigest;
import org.apache.iotdb.db.index.utils.MyBytes;


public class DigestSerializer {

  public byte[] serialize(FloatDigest FloatDigest) {
    ArrayList<byte[]> byteList = new ArrayList<>();

    byte[] aBytes = MyBytes.longToBytes(FloatDigest.getTimeWindow());
    byteList.add(aBytes);

    aBytes = MyBytes.longToBytes(FloatDigest.getCode());
    byteList.add(aBytes);

    aBytes = MyBytes.longToBytes(FloatDigest.getSerial());
    byteList.add(aBytes);

    return MyBytes.concatByteArrayList(byteList);
  }

  public FloatDigest deserialize(String key, long startTime, byte[] bytes) {

    int position = 1;
    byte[] aBytes = MyBytes.subBytes(bytes, position, 8);
    long timeWindow = MyBytes.bytesToLong(aBytes);

    position += 8;
    aBytes = MyBytes.subBytes(bytes, position, 8);
    long code = MyBytes.bytesToLong(aBytes);

    position += 8;
    aBytes = MyBytes.subBytes(bytes, position, 8);
    long serial = MyBytes.bytesToLong(aBytes);

    return new FloatDigest(key, startTime, timeWindow, code, serial);
  }
}
