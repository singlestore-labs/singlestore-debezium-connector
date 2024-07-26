package com.singlestore.debezium;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

import com.singlestore.debezium.exception.WrongOffsetException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

public class SingleStoreErrorHandler extends ErrorHandler {

  public SingleStoreErrorHandler(SingleStoreConnectorConfig connectorConfig,
      ChangeEventQueue<?> queue) {
    super(SingleStoreConnector.class, connectorConfig, queue, null);
  }

  @Override
  protected Set<Class<? extends Exception>> communicationExceptions() {
    return Collect.unmodifiableSet(IOException.class, SQLException.class,
        WrongOffsetException.class);
  }

  protected boolean isRetriable(Throwable throwable) {
    if (throwable instanceof SQLException) {
      SQLException e = (SQLException) throwable;
      if (e.getMessage().contains(
          "The requested Offset is too stale. Please re-start the OBSERVE query from the latest snapshot.")
          &&
          e.getErrorCode() == 2851 &&
          e.getSQLState().equals("HY000")
      ) {
        return false;
      }
    }
    
    return super.isRetriable(throwable);
  }
}
