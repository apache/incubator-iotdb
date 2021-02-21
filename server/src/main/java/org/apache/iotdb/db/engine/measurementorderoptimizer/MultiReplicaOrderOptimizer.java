package org.apache.iotdb.db.engine.measurementorderoptimizer;

import org.apache.iotdb.db.engine.divergentdesign.Replica;
import org.apache.iotdb.db.query.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MultiReplicaOrderOptimizer {
  private int replicaNum = 3;
  private int maxIter = 1000;
  private float breakPoint = 1e-2f;
  private List<QueryRecord> queryRecords;
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiReplicaOrderOptimizer.class);
  private String deviceID;
  private Replica[] replicas;
  private List<String> measurementOrder;
  private List<QueryRecord> records;
  private List<Long> chunkSize;
  private final float SA_INIT_TEMPERATURE = 100.0f;
  private final float COOLING_RATE = 0.95f;

  public MultiReplicaOrderOptimizer(String deviceID) {
    this.deviceID = deviceID;
    measurementOrder = new ArrayList<>(MeasurementOrderOptimizer.
            getInstance().getMeasurementsOrder(deviceID));
    replicas = new Replica[replicaNum];
    for (int i = 0; i < replicaNum; ++i) {
      replicas[i] = new Replica(deviceID, measurementOrder,
              MeasurementOrderOptimizer.getInstance().getAverageChunkSize(deviceID));
    }
    records = new ArrayList<>(WorkloadManager.getInstance().getRecord(deviceID));
    chunkSize = new ArrayList<>(MeasurementOrderOptimizer.getInstance().getChunkSize(deviceID));
  }

  public MultiReplicaOrderOptimizer(String deviceID, int replicaNum) {
    this.deviceID = deviceID;
    measurementOrder = new ArrayList<>(MeasurementOrderOptimizer.
            getInstance().getMeasurementsOrder(deviceID));
    this.replicaNum = replicaNum;
    replicas = new Replica[replicaNum];
    for (int i = 0; i < replicaNum; ++i) {
      replicas[i] = new Replica(deviceID, measurementOrder,
              MeasurementOrderOptimizer.getInstance().getAverageChunkSize(deviceID));
    }
    records = new ArrayList<>(WorkloadManager.getInstance().getRecord(deviceID));
    chunkSize = new ArrayList<>(MeasurementOrderOptimizer.getInstance().getChunkSize(deviceID));
  }

  public Replica[] optimizeBySA() {
    float curCost = getCostForCurReplicas(records, replicas);
    float temperature = SA_INIT_TEMPERATURE;
    Random r = new Random();
    for (int k = 0; k < maxIter; ++k) {
      temperature = temperature * COOLING_RATE;

      int swapReplica = r.nextInt(replicaNum);
      int swapLeft = r.nextInt(measurementOrder.size());
      int swapRight = r.nextInt(measurementOrder.size());
      while (swapLeft == swapRight) {
        swapLeft = r.nextInt(measurementOrder.size());
        swapRight = r.nextInt(measurementOrder.size());
      }
      replicas[swapReplica].swapMeasurementPos(swapLeft, swapRight);
      float newCost = getCostForCurReplicas(records, replicas);
      float probability = r.nextFloat();
      probability = probability < 0 ? -probability : probability;
      probability %= 1.0f;
      if (newCost < curCost ||
              Math.exp((curCost - newCost) / temperature) > probability) {
        curCost = newCost;
      } else {
        replicas[swapReplica].swapMeasurementPos(swapLeft, swapRight);
      }
    }
    return replicas;
  }

  private float getCostForCurReplicas(List<QueryRecord> records, Replica[] replicas) {
    return 0.0f;
  }
}
