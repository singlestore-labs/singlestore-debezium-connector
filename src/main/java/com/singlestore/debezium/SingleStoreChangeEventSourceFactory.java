package com.singlestore.debezium;

import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

public class SingleStoreChangeEventSourceFactory implements ChangeEventSourceFactory<SingleStorePartition, SingleStoreOffsetContext> {

    SingleStoreConnectorConfig connectorConfig;
    MainConnectionProvidingConnectionFactory<SingleStoreConnection> connectionFactory;
    SingleStoreDatabaseSchema schema;
    EventDispatcher<SingleStorePartition, TableId> dispatcher;
    ErrorHandler errorHandler;
    Clock clock;

    public SingleStoreChangeEventSourceFactory(SingleStoreConnectorConfig connectorConfig, 
    MainConnectionProvidingConnectionFactory<SingleStoreConnection> connectionFactory,
    SingleStoreDatabaseSchema schema,
    EventDispatcher<SingleStorePartition, TableId> dispatcher,
    ErrorHandler errorHandler,
    Clock clock) {
        this.connectorConfig = connectorConfig;
        this.connectionFactory = connectionFactory;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
    }

    @Override
    public SnapshotChangeEventSource<SingleStorePartition, SingleStoreOffsetContext> getSnapshotChangeEventSource(
            SnapshotProgressListener<SingleStorePartition> snapshotProgressListener, 
            NotificationService<SingleStorePartition, SingleStoreOffsetContext> notificationService) {
        return new SingleStoreSnapshotChangeEventSource(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService);
    }

    @Override
    public StreamingChangeEventSource<SingleStorePartition, SingleStoreOffsetContext> getStreamingChangeEventSource() {
        return new SingleStoreStreamingChangeEventSource(connectorConfig, connectionFactory.mainConnection(), dispatcher, errorHandler, schema, clock);
    }
    
    // TODO incremental snapshot
}
