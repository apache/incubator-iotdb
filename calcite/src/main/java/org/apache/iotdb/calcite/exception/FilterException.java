package org.apache.iotdb.calcite.exception;

public class FilterException extends CalciteException {

  public FilterException(String message) {
    super(message);
  }

  public FilterException(String message, Throwable cause) {
    super(message, cause);
  }
}
