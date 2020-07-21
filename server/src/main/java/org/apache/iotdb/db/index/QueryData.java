package org.apache.iotdb.db.index;

import org.apache.iotdb.tsfile.utils.Pair;

// query data in cassandra or mysql using pisa
public class QueryData {

  public static void main(String[] args) {
    args = new String[]{"key", "0", "16384000000"};
    if (args.length != 3) {
      System.out.println("key starttime, endtime");
      return;
    }
    String key = args[0];
    long start = Long.valueOf(args[1]);
    long end = Long.valueOf(args[2]);
    PisaIndex<FloatDigest> pisaIndex = new PisaIndex<>();
    System.out.println(String.format("will search %d nodes...",
        pisaIndex.queryPlan(new Pair<Long, Long>(start, end))));

    long time = System.currentTimeMillis();
//		FloatDigest digest= pisaIndex.queryV2(new Pair<Long,Long>(start,end));
    FloatDigest digest = pisaIndex.query(new Pair<Long, Long>(start, end));
    time = System.currentTimeMillis() - time;

    System.out.println("digest time cost:" + time);
    System.out.println("count value:" + digest.getCount());
    System.exit(1);
  }

}
