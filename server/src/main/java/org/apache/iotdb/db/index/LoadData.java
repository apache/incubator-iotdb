package org.apache.iotdb.db.index;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.iotdb.db.index.storage.Config;
import org.apache.iotdb.db.index.storage.StorageFactory;
import org.apache.iotdb.db.index.storage.interfaces.IBackendModelCreator;
import org.apache.iotdb.tsfile.utils.Pair;

public class LoadData {

  public static void main(String[] args) throws Exception {
    List<String> cfs = new ArrayList<>();
    cfs.add(Config.digest_cf);
    cfs.add(Config.data_cf);
    IBackendModelCreator schemaCreator = StorageFactory.getBackendModelCreator();
    schemaCreator.initialize("pisa", 1, cfs);

    loadPisa(Config.timeSeriesName, (long) Math.pow(2, Integer.valueOf(Config.totals)));
    System.exit(1);

  }

  /**
   * @param key time series name
   * @param total total points
   */
  private static void loadPisa(String key, long total) throws Exception {
    PisaIndex<FloatDigest> pisaIndex = new PisaIndex<>();
    //time window in ms
    Random random = new Random();
    long currentTime = 0;
    for (long i = 0; i < total; i++) {
      if (i % 100 == 0) {
        System.out.println("current progress:" + i);
      }
      Pair<Long, Float> data = new Pair<>(currentTime++, random.nextFloat());
      pisaIndex.insertPoint(data);

    }

    pisaIndex.close();
    System.out.println("total data points:" + total);
  }
}
