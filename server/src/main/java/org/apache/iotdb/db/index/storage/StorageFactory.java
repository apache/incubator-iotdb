package org.apache.iotdb.db.index.storage;

import org.apache.iotdb.db.index.storage.interfaces.IBackendModelCreator;
import org.apache.iotdb.db.index.storage.interfaces.IBackendReader;
import org.apache.iotdb.db.index.storage.interfaces.IBackendWriter;
import org.apache.iotdb.db.index.storage.memory.FakeByteStore;
import org.apache.iotdb.db.index.storage.memory.FakeStore;

public class StorageFactory {

  private static FakeStore fakeStore = new FakeStore();
  private static FakeByteStore fakeByteStore = new FakeByteStore();

  public static IBackendReader getBackaBackendReader() {
    String storageClass = Config.storage_engine;
    switch (storageClass) {
      case "local":
        return fakeStore;
      default:
        return fakeByteStore;
    }
  }

  public static IBackendWriter getBackaBackendWriter() {
    String storageClass = Config.storage_engine;
    switch (storageClass) {
      case "local":
        return fakeStore;
      default:
        return fakeByteStore;
    }
  }

  public static IBackendModelCreator getBackendModelCreator() {
    String storageClass = Config.storage_engine;
    switch (storageClass) {
      case "local":
        return fakeStore;
      default:
        return fakeByteStore;
    }
  }
}
