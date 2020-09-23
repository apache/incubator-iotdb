package org.apache.iotdb.db.query.aggregation.impl;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;

public class FirstValueDescAggrResult extends FirstValueAggrResult {

  public FirstValueDescAggrResult(TSDataType dataType) {
    super(dataType);
  }

  @Override
  public void updateResultFromStatistics(Statistics statistics) {
    Object firstVal = statistics.getFirstValue();
    setValue(firstVal);
    timestamp = statistics.getStartTime();
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage) {
    if (dataInThisPage.hasCurrent()) {
      setValue(dataInThisPage.currentValue());
      timestamp = dataInThisPage.currentTime();
    }
  }

  @Override
  public void updateResultFromPageData(BatchData dataInThisPage, long minBound, long maxBound)
      throws IOException {
    while (dataInThisPage.hasCurrent()
        && dataInThisPage.currentTime() < maxBound
        && dataInThisPage.currentTime() >= minBound) {
      setValue(dataInThisPage.currentValue());
      timestamp = dataInThisPage.currentTime();
      dataInThisPage.next();
    }
  }

  @Override
  public void updateResultUsingTimestamps(long[] timestamps, int length,
      IReaderByTimestamp dataReader) throws IOException {
    for (int i = 0; i < length; i++) {
      Object value = dataReader.getValueInTimestamp(timestamps[i]);
      if (value != null) {
        setValue(value);
        timestamp = timestamps[i];
      }
    }
  }

  @Override
  public boolean isCalculatedAggregationResult() {
    return false;
  }

  @Override
  public boolean needAscReader() {
    return false;
  }
}
