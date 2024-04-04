package com.singlestore.debezium.exception;

import io.debezium.DebeziumException;

public class WrongOffsetException extends DebeziumException {

  public WrongOffsetException(String message) {
    super(message);
  }

  public WrongOffsetException(Throwable cause) {
    super(cause);
  }

  public WrongOffsetException(String message, Throwable cause) {
    super(message, cause);
  }
}
