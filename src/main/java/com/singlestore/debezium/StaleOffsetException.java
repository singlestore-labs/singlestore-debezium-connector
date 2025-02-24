package com.singlestore.debezium;

import java.sql.SQLException;

public class StaleOffsetException extends SQLException {

  public StaleOffsetException(Throwable t) {
    super(t);
  }

  public static boolean isStaleOffsetException(SQLException error) {
    return error.getMessage().contains(
        "The requested Offset is too stale")
        &&
        error.getErrorCode() == 2851 &&
        error.getSQLState().equals("HY000");
  }
}
