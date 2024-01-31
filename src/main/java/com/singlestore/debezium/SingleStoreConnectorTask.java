package com.singlestore.debezium;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;
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
 * The main task executing streaming from SingleStore.
 * Responsible for lifecycle management of the streaming code.
 */
public class SingleStoreConnectorTask extends BaseSourceTask<SingleStorePartition, SingleStoreOffsetContext> {

    private static final String CONTEXT_NAME = "singlestore-connector-task";
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile SingleStoreDatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return SingleStoreConnectorConfig.ALL_FIELDS;
    }

    @Override
    public ChangeEventSourceCoordinator<SingleStorePartition, SingleStoreOffsetContext> start(Configuration config) {
        final Clock clock = Clock.system();
        final SingleStoreConnectorConfig connectorConfig = new SingleStoreConnectorConfig(config);
        final SchemaNameAdjuster schemaNameAdjuster =  connectorConfig.schemaNameAdjuster();
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(SingleStoreConnectorConfig.TOPIC_NAMING_STRATEGY);
        final SingleStoreValueConverters valueConverter = new SingleStoreValueConverters(connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode(),
                connectorConfig.binaryHandlingMode());
        final SingleStoreDefaultValueConverter defaultValueConverter = new SingleStoreDefaultValueConverter(valueConverter);

        MainConnectionProvidingConnectionFactory<SingleStoreConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new SingleStoreConnection(new SingleStoreConnection.SingleStoreConnectionConfiguration(config)));

        this.schema = new SingleStoreDatabaseSchema(connectorConfig, valueConverter,
                defaultValueConverter,
                topicNamingStrategy,
                false);

        SingleStoreTaskContext taskContext = new SingleStoreTaskContext(connectorConfig, schema);
        SingleStoreEventMetadataProvider metadataProvider = new SingleStoreEventMetadataProvider();
        SingleStoreErrorHandler errorHandler = new SingleStoreErrorHandler(connectorConfig, queue);

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
            .pollInterval(connectorConfig.getPollInterval())
            .maxBatchSize(connectorConfig.getMaxBatchSize())
            .maxQueueSize(connectorConfig.getMaxQueueSize())
            .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
            .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
            .build();

        Offsets<SingleStorePartition, SingleStoreOffsetContext> previousOffsets = getPreviousOffsets(
            new SingleStorePartition.Provider(connectorConfig, config),
            new SingleStoreOffsetContext.Loader(connectorConfig));

        SignalProcessor<SingleStorePartition, SingleStoreOffsetContext> signalProcessor = new SignalProcessor<>(
            SingleStoreConnector.class, connectorConfig, Map.of(),
                    getAvailableSignalChannels(),
                    DocumentReader.defaultReader(),
                    previousOffsets);

        final Configuration heartbeatConfig = config;
        final EventDispatcher<SingleStorePartition, TableId> dispatcher = new EventDispatcher<>(
            connectorConfig,
            topicNamingStrategy,
            schema,
            queue,
            connectorConfig.getTableFilters().dataCollectionFilter(),
            DataChangeEvent::new,
                metadataProvider,
                connectorConfig.createHeartbeat(
                        topicNamingStrategy,
                        schemaNameAdjuster,
                        () -> new SingleStoreConnection(new SingleStoreConnection.SingleStoreConnectionConfiguration(heartbeatConfig)),
                        exception -> {
                            final String sqlErrorId = exception.getMessage();
                            throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                        }
                ),
            schemaNameAdjuster,
            signalProcessor);
        
        NotificationService<SingleStorePartition, SingleStoreOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                    connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        ChangeEventSourceCoordinator<SingleStorePartition, SingleStoreOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
            previousOffsets,
            errorHandler,
            SingleStoreConnector.class,
            connectorConfig,
            new SingleStoreChangeEventSourceFactory(connectorConfig, connectionFactory, schema, dispatcher, errorHandler, clock),
            // TODO create custom metrics PLAT-6970
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