package com.singlestore.debezium;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

public class SingleStoreDBSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<SingleStoreDBPartition, SingleStoreDBOffsetContext> {

    public SingleStoreDBSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig,
            MainConnectionProvidingConnectionFactory<? extends JdbcConnection> jdbcConnectionFactory,
            RelationalDatabaseSchema schema, EventDispatcher<SingleStoreDBPartition, TableId> dispatcher, Clock clock,
            SnapshotProgressListener<SingleStoreDBPartition> snapshotProgressListener) {
        super(connectorConfig, jdbcConnectionFactory, schema, dispatcher, clock, snapshotProgressListener);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected Set<TableId> getAllTableIds(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext)
            throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAllTableIds'");
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext)
            throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'lockTablesForSchemaSnapshot'");
    }

    @Override
    protected void determineSnapshotOffset(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext,
            SingleStoreDBOffsetContext previousOffset) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'determineSnapshotOffset'");
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext,
            SingleStoreDBOffsetContext offsetContext) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'readTableStructure'");
    }

    @Override
    protected void releaseSchemaSnapshotLocks(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext)
            throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'releaseSchemaSnapshotLocks'");
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext, Table table)
            throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getCreateTableEvent'");
    }

    @Override
    protected SingleStoreDBOffsetContext copyOffset(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'copyOffset'");
    }

    @Override
    protected Optional<String> getSnapshotSelect(
            RelationalSnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> snapshotContext,
            TableId tableId, List<String> columns) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSnapshotSelect'");
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(SingleStoreDBPartition partition,
            SingleStoreDBOffsetContext previousOffset) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSnapshottingTask'");
    }

    @Override
    protected SnapshotContext<SingleStoreDBPartition, SingleStoreDBOffsetContext> prepare(
            SingleStoreDBPartition partition) throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'prepare'");
    }
    
}
