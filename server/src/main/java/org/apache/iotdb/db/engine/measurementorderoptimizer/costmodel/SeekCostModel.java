package org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel;

import com.csvreader.CsvReader;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SeekCostModel {
  private static List<Pair<Long, Float>> empiricalData = new ArrayList<>();
  private static  File EMPIRICAL_SEEK_FILE = null;
  private static Logger logger = LoggerFactory.getLogger(SeekCostModel.class);
  private static boolean init = false;

  /**
   * Read the empirical data from local file
   */
  public static synchronized boolean readEmpiricalData() {
    if (empiricalData.size() > 0) {
      return true;
    }
    String systemDir = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    String filename = systemDir + File.separator + "experiment" + File.separator +  "seek_time.csv";
    EMPIRICAL_SEEK_FILE = new File(filename);
    logger.info("Loading seek empirical data from " + EMPIRICAL_SEEK_FILE.getAbsolutePath());
    try{
      if (!EMPIRICAL_SEEK_FILE.exists()) {
        logger.error("Seeking empirical data file " + EMPIRICAL_SEEK_FILE.getAbsolutePath() + " does not exists");
        return false;
      }
      CsvReader csvReader = new CsvReader(EMPIRICAL_SEEK_FILE.getAbsolutePath());
      csvReader.readHeaders();
      while(csvReader.readRecord()) {
        String blockSize = csvReader.get("BlockSize");
        String averageTime = csvReader.get("AverageTime");
        empiricalData.add(new Pair<Long, Float>(stringDataToBytes(blockSize), Float.valueOf(averageTime)));
      }
      csvReader.close();
      return true;
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  public static long stringDataToBytes(String data) {
    String[] suffixes = {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
    long result = 0;
    for(int i = suffixes.length - 1; i >= 0; --i) {
      if (data.endsWith(suffixes[i])) {
        int pos = data.indexOf(suffixes[i]);
        float base = Float.valueOf(data.substring(0, pos));
        for(int j = 0; j < i; ++j) {
          base *= 1024.0;
        }
        result = (long)base;
        break;
      }
    }
    return result;
  }

  /**
   * Approximate the seek cost during the queries.
   * @Param queries is the workload.
   * @Param measurements
   * @Param blockSize is a list of integer, which is the size of per block in byte.
  */
  public static float approximate(List<QueryRecord> queries, List<String> measurements, List<Long> chunkSize, int chunkGroupNum) {
    if (!init) {
      readEmpiricalData();
      init = true;
    }
    float totalCost = 0;

    for(QueryRecord query: queries) {
      List<String> queryMeasurements = query.getSensors();
      List<String> sortedAndDeduplicatedQueryMeasurements = CostModel.sortInSameOrderAndDeduplicate(queryMeasurements, measurements);

      int k = 0;
      for(int i = 0; i < sortedAndDeduplicatedQueryMeasurements.size(); ++i) {
        long curSeekDistance = 0;
        while (true) {
          if (!sortedAndDeduplicatedQueryMeasurements.get(i).equals(measurements.get(k))) {
            curSeekDistance += chunkSize.get(k++);
          } else {
            totalCost += getSeekCost(curSeekDistance);
            k++;
            curSeekDistance = 0;
            break;
          }
        }
      }
    }

    return totalCost * chunkGroupNum;
  }


  /**
   * Return the cost of seeking a specified distance according to the empirical data
   */
  public static float getSeekCost(long distance) {
    if (!init) {
      readEmpiricalData();
      init = true;
    }
    float seekCost = 0;
    for(int i = 0; i < empiricalData.size() - 1; ++i) {
      if (distance >= empiricalData.get(i).left && distance < empiricalData.get(i + 1).left) {
        seekCost = (float)(distance - empiricalData.get(i).left) / (float)(empiricalData.get(i+1).left - empiricalData.get(i).left);
        seekCost = seekCost * (empiricalData.get(i+1).right - empiricalData.get(i).right) + empiricalData.get(i).right;
        break;
      }
    }
    return seekCost;
  }
}
