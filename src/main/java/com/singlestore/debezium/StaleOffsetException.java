package com.singlestore.debezium;

import java.sql.SQLException;

public class StaleOffsetException extends SQLException {

  public StaleOffsetException(Throwable t) {
    super(t);
  }
}
