package com.singlestore.debezium;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.singlestore.debezium.util.ObserveResultSetUtils;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope.Operation;
import io.debezium.jdbc.JdbcConnection.ResultSetConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class SingleStoreDBStreamingChangeEventSource implements StreamingChangeEventSource<SingleStoreDBPartition, SingleStoreDBOffsetContext>{

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleStoreDBStreamingChangeEventSource.class);

    SingleStoreDBConnectorConfig connectorConfig;
    SingleStoreDBConnection connection;
    EventDispatcher<SingleStoreDBPartition, TableId> dispatcher;
    ErrorHandler errorHandler;
    SingleStoreDBDatabaseSchema schema;
    Clock clock;

    public SingleStoreDBStreamingChangeEventSource(SingleStoreDBConnectorConfig connectorConfig,
        SingleStoreDBConnection connection, 
        EventDispatcher<SingleStoreDBPartition, TableId> dispatcher,
        ErrorHandler errorHandler,
        SingleStoreDBDatabaseSchema schema,
        Clock clock) {
        this.connectorConfig = connectorConfig;    
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.schema = schema;
        this.clock = clock;
    }

    @Override
    public void execute(ChangeEventSourceContext context, SingleStoreDBPartition partition,
            SingleStoreDBOffsetContext offsetContext) throws InterruptedException {
        if (!connectorConfig.getSnapshotMode().shouldStream()) {
            LOGGER.info("Streaming is disabled for snapshot mode {}", connectorConfig.getSnapshotMode());
            return;
        }
        
        Set<TableId> tables = schema.tableIds();
        // We must have only one table
        assert(tables.size() == 1);
        TableId table = tables.iterator().next();
        List<String> offsets = offsetContext.offsets()
            .stream()
            .map(o -> o == null ? "NULL" : "'" + o + "'")
            .collect(Collectors.toList());
        Optional<String> offset;
        if (offsets == null) {
            offset = Optional.empty();
        } else {
            offset = Optional.of("(" + String.join(",", offsets) + ")");
        }

        InterruptedException interrupted[] = new InterruptedException[1];
        try {
            // TODO: add field filter when it will be supported
            connection.observe(null, tables, Optional.empty(), Optional.empty(), offset, Optional.empty(), new ResultSetConsumer() {
                @Override
                public void accept(ResultSet rs) throws SQLException {
                    Thread t = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            while (context.isRunning()) {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                }
                            }
                            try {
                                ((com.singlestore.jdbc.Connection)rs.getStatement().getConnection()).cancelCurrentQuery();
                            } catch (SQLException ex) {
                                // TODO handle exception
                            }
                        }
                    });
                    t.start();

                    List<Integer> columnPositions = 
                        ObserveResultSetUtils.columnPositions(rs, schema.schemaFor(table).valueSchema().fields(), connectorConfig.populateInternalId());
                    try {
                        while (rs.next() && context.isRunning()) {
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

                            try {
                                dispatcher.dispatchDataChangeEvent(partition, table, 
                                    new SingleStoreDBChangeRecordEmitter(
                                        partition, 
                                        offsetContext, 
                                        clock, 
                                        operation, 
                                        null,
                                        after, 
                                        internalId,
                                        connectorConfig));
                            } catch (InterruptedException e) {
                                interrupted[0] = e;
                                break;
                            }
                        }
                    } finally {
                        t.interrupt();
                    }
                }
            });
        } catch (SQLException e) {
            // TODO: handle schema change event
            if (!(!context.isRunning() && 
                e.getMessage().contains("Query execution was interrupted") &&
                e.getErrorCode() == 1317 && 
                e.getSQLState().equals("70100"))) {
                String msg = e.getMessage() + " Error code: " + e.getErrorCode() + "; SQLSTATE: " + e.getSQLState() + ".";
                errorHandler.setProducerThrowable(new DebeziumException(msg, e));
            }
        }

        if (interrupted[0] != null) {
            throw interrupted[0];
        }
    }
}
