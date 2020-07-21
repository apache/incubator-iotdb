package org.apache.iotdb.db.index.storage.interfaces;

import java.util.List;


public interface IBackendModelCreator {

  /**
   * initialize the schema. create the keyspace and a group of columnfamilies.
   */
  public void initialize(String ks, int replicaFactor, List<String> columnfamilies)
      throws Exception;

  /**
   * add a column Family into StorageSystem.
   *
   * @param ks database name
   * @param cf column family name
   */
  public void addColumnFamily(String ks, String cf) throws Exception;

  public void addFloatColumnFamily(String ks, String cf) throws Exception;

  /**
   * add column families in to StorageSystem. <br> this method is recommended when you want to
   * create many cfs in a short time (When the storageSystem is Cassandra, because Cassandra has
   * some performance problem if u create cf one by one).
   */
  public void addColumnFamilies(String ks, List<String> cfs) throws Exception;
}
