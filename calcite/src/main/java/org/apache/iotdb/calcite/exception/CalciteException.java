package org.apache.iotdb.calcite.exception;

public class CalciteException extends Exception{

  public CalciteException(String message) {
    super(message);
  }

  public CalciteException(String message, Throwable cause) {
    super(message, cause);
  }
}
