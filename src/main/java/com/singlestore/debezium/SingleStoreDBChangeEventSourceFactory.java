package com.singlestore.debezium;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class SingleStoreDBChangeEventSourceFactory implements ChangeEventSourceFactory<SingleStoreDBPartition, SingleStoreDBOffsetContext> {

    SingleStoreDBConnectorConfig connectorConfig;
    MainConnectionProvidingConnectionFactory<SingleStoreDBConnection> connectionFactory;
    SingleStoreDBDatabaseSchema schema;
    EventDispatcher<SingleStoreDBPartition, TableId> dispatcher;
    Clock clock;

    public SingleStoreDBChangeEventSourceFactory(SingleStoreDBConnectorConfig connectorConfig, 
    MainConnectionProvidingConnectionFactory<SingleStoreDBConnection> connectionFactory,
    SingleStoreDBDatabaseSchema schema,
    EventDispatcher<SingleStoreDBPartition, TableId> dispatcher,
    Clock clock) {
        this.connectorConfig = connectorConfig;
        this.connectionFactory = connectionFactory;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
    }

    @Override
    public SnapshotChangeEventSource<SingleStoreDBPartition, SingleStoreDBOffsetContext> getSnapshotChangeEventSource(
            SnapshotProgressListener<SingleStoreDBPartition> snapshotProgressListener) {
        return new SingleStoreDBSnapshotChangeEventSource(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<SingleStoreDBPartition, SingleStoreDBOffsetContext> getStreamingChangeEventSource() {
        return new SingleStoreDBStreamingChangeEventSource();
    }
    
    // TODO incremental snapshot
}
