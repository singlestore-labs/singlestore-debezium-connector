package com.singlestore.debezium;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.document.DocumentReader;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing streaming from SingleStoreDB.
 * Responsible for lifecycle management of the streaming code.
 */
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
        final Clock clock = Clock.system();
        final SingleStoreDBConnectorConfig connectorConfig = new SingleStoreDBConnectorConfig(config);
        final SchemaNameAdjuster schemaNameAdjuster =  connectorConfig.schemaNameAdjuster();
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(SingleStoreDBConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SingleStoreDBValueConverters valueConverter = new SingleStoreDBValueConverters(connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.binaryHandlingMode());
        final SingleStoreDBDefaultValueConverter defaultValueConverter = new SingleStoreDBDefaultValueConverter(valueConverter);

        MainConnectionProvidingConnectionFactory<SingleStoreDBConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new SingleStoreDBConnection(new SingleStoreDBConnection.SingleStoreDBConnectionConfiguration(config)));

        this.schema = new SingleStoreDBDatabaseSchema(connectorConfig, valueConverter,
                defaultValueConverter,
                topicNamingStrategy,
                false);

        SingleStoreDBTaskContext taskContext = new SingleStoreDBTaskContext(connectorConfig, schema);
        SingleStoreDBEventMetadataProvider metadataProvider = new SingleStoreDBEventMetadataProvider();
        SingleStoreDBErrorHandler errorHandler = new SingleStoreDBErrorHandler(connectorConfig, queue);

        Offsets<SingleStoreDBPartition, SingleStoreDBOffsetContext> previousOffsets = getPreviousOffsets(
            new SingleStoreDBPartition.Provider(connectorConfig, config),
            new SingleStoreDBOffsetContext.Loader(connectorConfig));

        SignalProcessor<SingleStoreDBPartition, SingleStoreDBOffsetContext> signalProcessor = new SignalProcessor<>(
            SingleStoreDBConnector.class, connectorConfig, Map.of(),
                    getAvailableSignalChannels(),
                    DocumentReader.defaultReader(),
                    previousOffsets);


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
        
        NotificationService<SingleStoreDBPartition, SingleStoreDBOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                    connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<SingleStoreDBPartition, SingleStoreDBOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
            previousOffsets,
            errorHandler,
            SingleStoreDBConnector.class,
            connectorConfig,
            new SingleStoreDBChangeEventSourceFactory(connectorConfig, connectionFactory, schema, dispatcher, clock),
            // TODO create custom metrics
            new DefaultChangeEventSourceMetricsFactory<>(),            
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
        schema.close();
        // TODO
    }
}
