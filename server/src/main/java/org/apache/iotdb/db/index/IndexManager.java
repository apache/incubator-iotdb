package org.apache.iotdb.db.index;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.exception.index.UnSupportedIndexTypeException;

public class IndexManager {

  private static Map<IndexType, PisaIndex> indexMap = new HashMap<>();


  static {
    indexMap.put(IndexType.PISAIndex, PisaIndex.getInstance());
  }

  public static PisaIndex getIndexInstance(IndexType indexType) {
    return indexMap.get(indexType);
  }

  public enum IndexType {
    PISAIndex;

    public static IndexType getIndexType(String indexType) throws UnSupportedIndexTypeException {
      String normalized = indexType.toLowerCase();
      switch (normalized) {
        case "pisa":
          return PISAIndex;
        default:
          throw new UnSupportedIndexTypeException(indexType);
      }
    }
  }
}
