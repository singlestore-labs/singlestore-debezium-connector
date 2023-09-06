package com.singlestore.debezium;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

public class SingleStoreDBConnectorTask extends BaseSourceTask<SingleStoreDBPartition, SingleStoreDBOffsetContext> {
    
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile SingleStoreDBDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SingleStoreDBConnectorConfig.ALL_FIELDS;
    }


    @Override
    public ChangeEventSourceCoordinator<SingleStoreDBPartition, SingleStoreDBOffsetContext> start(Configuration config) {
        final SingleStoreDBConnectorConfig connectorConfig = new SingleStoreDBConnectorConfig(config);
        final SchemaNameAdjuster schemaNameAdjuster =  connectorConfig.schemaNameAdjuster();
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(SingleStoreDBConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SingleStoreDBValueConverter valueConverter = new SingleStoreDBValueConverter();
        final SingleStoreDBDefaultValueConverter defaultValueConverter = new SingleStoreDBDefaultValueConverter();

        this.schema = new SingleStoreDBDatabaseSchema(connectorConfig,
        defaultValueConverter, 
        topicNamingStrategy, 
        schemaNameAdjuster, 
        false, 
        valueConverter);

        SingleStoreDBTaskContext taskContext = new SingleStoreDBTaskContext(connectorConfig, schema);
        SingleStoreDBEventMetadataProvider metadataProvider = new SingleStoreDBEventMetadataProvider();
        SingleStoreDBErrorHandler errorHandler = new SingleStoreDBErrorHandler(connectorConfig, queue);

        final EventDispatcher<SingleStoreDBPartition, TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicNamingStrategy,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    metadataProvider,
                    // TODO: add heartbeat
                    schemaNameAdjuster,
                    signalProcessor);

        // TODO
        // change this.taskName
        // change this.taskContext
        // change this.schema

        // schema = new SingleStoreDBDatabaseSchema(connectorConfig, null, null, schemaNameAdjuster, false, null)

        // offset
        // errorHandler
        // ChangeEventSourceFactory
        // ChangeEventSourceMetricsFactory
        // dispatcher
        // schema
        // signalProcessor
        // notificationService
        // taskContext
        // metadataProvider

        ChangeEventSourceCoordinator<SingleStoreDBPartition, SingleStoreDBOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
            previousOffsets,
            errorHandler,
            SingleStoreDBConnector.class,
            connectorConfig,
            new SingleStoreDBChangeEventSourceFactory(),
            new SingleStoreDBChangeEventSourceMetricsFactory(),
            dispatcher,
            schema,
            signalProcessor,
            notificationService);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

        @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }


    @Override
    protected void doStop() {
        // TODO
    }
}
