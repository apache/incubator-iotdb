package org.apache.iotdb.db.index.storage.model.serializer;

import org.apache.iotdb.db.index.FloatDigest;

public class DigestSerializeFactory {

  private final String FLOAT = "FloatDigest";

  private final String BTREENODE = "BtreeNode";

  private static DigestSerializeFactory instance = new DigestSerializeFactory();

  private FloatDigestSerializer floatSerializer = FloatDigestSerializer.getInstance();

  private DigestSerializeFactory() {
  }

  public static DigestSerializeFactory getInstance() {
    return instance;
  }

  public byte[] serialize(FloatDigest dataDigest) {
    String className = dataDigest.getClass().getSimpleName();
    switch (className) {
      case FLOAT:
        return floatSerializer.serialize((FloatDigest) dataDigest);
      default:
        return null;
    }
  }

  public FloatDigest deserializeFloatDigest(String key, long timestamp, byte[] bytes) {
    return floatSerializer.deserialize(key, timestamp, bytes);
  }
}