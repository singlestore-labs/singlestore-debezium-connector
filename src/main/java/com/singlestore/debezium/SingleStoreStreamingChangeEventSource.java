package com.singlestore.debezium;

import com.singlestore.debezium.events.ObserveStreamingStartedEvent;
import com.singlestore.debezium.util.ObserveResultSetUtils;
import com.singlestore.jdbc.Connection;
import io.debezium.DebeziumException;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleStoreStreamingChangeEventSource implements
    StreamingChangeEventSource<SingleStorePartition, SingleStoreOffsetContext> {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      SingleStoreStreamingChangeEventSource.class);

  SingleStoreConnectorConfig connectorConfig;
  SingleStoreConnection connection;
  EventDispatcher<SingleStorePartition, TableId> dispatcher;
  ErrorHandler errorHandler;
  SingleStoreDatabaseSchema schema;
  Clock clock;

  public SingleStoreStreamingChangeEventSource(SingleStoreConnectorConfig connectorConfig,
      SingleStoreConnection connection,
      EventDispatcher<SingleStorePartition, TableId> dispatcher,
      ErrorHandler errorHandler,
      SingleStoreDatabaseSchema schema,
      Clock clock) {
    this.connectorConfig = connectorConfig;
    this.connection = connection;
    this.dispatcher = dispatcher;
    this.errorHandler = errorHandler;
    this.schema = schema;
    this.clock = clock;
  }

  @Override
  public void execute(ChangeEventSourceContext context, SingleStorePartition partition,
      SingleStoreOffsetContext offsetContext) throws InterruptedException {
    if (!connectorConfig.getSnapshotMode().shouldStream()) {
      LOGGER.info("Streaming is disabled for snapshot mode {}", connectorConfig.getSnapshotMode());
      return;
    }

    Set<TableId> tables = schema.tableIds();
    // We must have only one table
    assert (tables.size() == 1);
    TableId table = tables.iterator().next();

    try (Connection conn = (com.singlestore.jdbc.Connection) connection.connection(true)) {
      Thread t = new Thread(() -> {
        do {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignored) {
          }
        } while (context.isRunning());
        try {
          conn.cancelCurrentQuery();
        } catch (SQLException ex) {
          // TODO handle exception
        }
      });
      t.start();

      dispatcher.dispatchConnectorEvent(partition, ObserveStreamingStartedEvent.INSTANCE);
      String query = connection.generateObserveQuery(table, offsetContext.offsets());
      try (
          Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery(query)
      ) {
        List<Integer> columnPositions =
            ObserveResultSetUtils.columnPositions(rs, schema.tableFor(table).columns(),
                connectorConfig.populateInternalId());
        try {
          while (rs.next() && context.isRunning()) {
            LOGGER.trace(
                "Streaming record, type: {}, internalId: {}, partitionId: {}, offset: {} values: {}",
                ObserveResultSetUtils.snapshotType(rs),
                ObserveResultSetUtils.internalId(rs),
                ObserveResultSetUtils.partitionId(rs),
                ObserveResultSetUtils.offset(rs),
                ObserveResultSetUtils.rowToArray(rs, columnPositions));
            Operation operation;
            switch (rs.getString("Type")) {
              case "Insert":
                operation = Operation.CREATE;
                break;
              case "Update":
                operation = Operation.UPDATE;
                break;
              case "Delete":
                operation = Operation.DELETE;
                break;
              default:
                continue;
            }

            String offset = ObserveResultSetUtils.offset(rs);
            Integer partitionId = ObserveResultSetUtils.partitionId(rs);
            String txId = ObserveResultSetUtils.txId(rs);
            Long internalId = ObserveResultSetUtils.internalId(rs);

            offsetContext.event(table, Instant.now());
            offsetContext.update(partitionId, txId, offset);

            Object[] after = ObserveResultSetUtils.rowToArray(rs, columnPositions);

            dispatcher.dispatchDataChangeEvent(partition, table,
                new SingleStoreChangeRecordEmitter(
                    partition,
                    offsetContext,
                    clock,
                    operation,
                    null,
                    after,
                    internalId,
                    connectorConfig,
                    schema.tableFor(table)));
          }
        } finally {
          t.interrupt();
        }
      }
    } catch (SQLException e) {
      // TODO: handle schema change event
      if (!(!context.isRunning() &&
          e.getMessage().contains("Query execution was interrupted") &&
          e.getErrorCode() == 1317 &&
          e.getSQLState().equals("70100"))) {
        String msg;
        if (e.getMessage().contains(
            "The requested Offset is too stale. Please re-start the OBSERVE query from the latest snapshot.")
            &&
            e.getErrorCode() == 2851 &&
            e.getSQLState().equals("HY000")
        ) {
          msg = "Offset the connector is trying to resume from is considered stale.\n"
              + "Therefore, the connector cannot resume streaming.\n"
              + "You can use either of the following options to recover from the failure:\n"
              + " * Delete the failed connector, and create a new connector with the same configuration but with a different connector name.\n"
              + " * Pause the connector and then remove offsets, or change the offset topic.\n"
              + "To help prevent failures related to stale offsets, you can increase the value of the following engine variables in SingleStore:\n"
              + " * 'snapshots_to_keep' - Defines the number of snapshots to keep for backup and replication.\n"
              + " * 'snapshot_trigger_size' - Defines the size of transaction logs in bytes, which, when reached, triggers a snapshot that is written to disk.";
        } else {
          msg =
              e.getMessage() + " Error code: " + e.getErrorCode() + "; SQLSTATE: " + e.getSQLState()
                  + ".";
        }

        LOGGER.error(msg);

        errorHandler.setProducerThrowable(new DebeziumException(msg, e));
      }
    }
  }
}
