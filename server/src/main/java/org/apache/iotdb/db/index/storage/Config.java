package org.apache.iotdb.db.index.storage;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * A class that contains configuration properties for the cassandra node it runs within.
 *
 * Properties declared as volatile can be mutated via JMX.
 */
public class Config {

  public static String storage_engine = "local";
  public static String digest_cf = "digest";
  public static String data_cf = "data";
  public static long timeWindow = 100L;

  //time series name
  public static String timeSeriesName = "key";

  //total number of points: Math.pow(2,total)
  public static String totals = "10";

  public static boolean writePackages = true;


  public static void load(String path) {
    if (path == null) {
      return;
    }

    Properties properties = new Properties();

    try {
      FileInputStream fileInputStream = new FileInputStream(path);
      properties.load(fileInputStream);
      properties.putAll(System.getenv());
    } catch (Exception e) {
      e.printStackTrace();
    }

    storage_engine = properties.getOrDefault("storage_engine", storage_engine).toString();
    digest_cf = properties.getOrDefault("digest_cf", digest_cf).toString();
    data_cf = properties.getOrDefault("data_cf", data_cf).toString();
    timeSeriesName = properties.getOrDefault("timeSeriesName", timeSeriesName).toString();

    timeWindow = Long
        .parseLong(properties.getOrDefault("timeWindow", String.valueOf(timeWindow)).toString());

    writePackages = Boolean
        .parseBoolean(properties.getOrDefault("writePackages", writePackages).toString());
  }

}
