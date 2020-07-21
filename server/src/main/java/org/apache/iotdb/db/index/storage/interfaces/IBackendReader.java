package org.apache.iotdb.db.index.storage.interfaces;


import org.apache.iotdb.db.index.FloatDigest;
import org.apache.iotdb.db.index.storage.model.FixWindowPackage;

public interface IBackendReader {

  byte[] getBytes(String key, String cf, long startTime);

  /**
   * giving some DataPackage's startTime,return these digests from Cassandra in one command
   */
  FloatDigest[] getDigests(String key, Long[] timeStamps);

  FloatDigest getBeforeOrEqualDigest(String key, long timestamp);

  FixWindowPackage getBeforeOrEqualPackage(String key, long timestamp);

  FloatDigest getAfterOrEqualDigest(String key, long timestamp);

  /**
   * get latest one data digest after the timestamp if the timestamp belongs to a datapackage (not
   * the starttime of a datapackage), the method will return the datapackage. SO user NEED to decide
   * whether allign the timestamp
   */
  FloatDigest getLatestDigest(String key) throws Exception;

}
