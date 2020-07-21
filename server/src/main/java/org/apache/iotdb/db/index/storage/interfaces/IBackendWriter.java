package org.apache.iotdb.db.index.storage.interfaces;


import org.apache.iotdb.db.index.FloatDigest;
import org.apache.iotdb.db.index.storage.model.FixWindowPackage;

public interface IBackendWriter {

  /**
   * @param startTimestamp an align time.
   */
  void write(String key, String cf, long startTimestamp, FixWindowPackage dp)
      throws Exception;

  /**
   * @param startTimestamp an align time.
   */
  void write(String key, String cf, long startTimestamp, FloatDigest digest)
      throws Exception;

  void write(String key, String cf, long startTimestamp, byte[] digest)
      throws Exception;
}
