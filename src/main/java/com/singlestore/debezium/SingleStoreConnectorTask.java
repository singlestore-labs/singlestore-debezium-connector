package com.singlestore.debezium;

import com.singlestore.debezium.metrics.SingleStoreChangeEventSourceMetricsFactory;
import io.debezium.DebeziumException;
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
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The main task executing streaming from SingleStore. Responsible for lifecycle management of the
 * streaming code.
 */
public class SingleStoreConnectorTask extends
    BaseSourceTask<SingleStorePartition, SingleStoreOffsetContext> {

  private static final Logger LOGGER = LoggerFactory.getLogger(
          SingleStoreConnectorTask.class);
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
  public ChangeEventSourceCoordinator<SingleStorePartition, SingleStoreOffsetContext> start(
      Configuration config) {
    final Clock clock = Clock.system();
    final SingleStoreConnectorConfig connectorConfig = new SingleStoreConnectorConfig(config);
    final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
    final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(
        SingleStoreConnectorConfig.TOPIC_NAMING_STRATEGY);
    final SingleStoreValueConverters valueConverter = new SingleStoreValueConverters(
        connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode(),
        connectorConfig.binaryHandlingMode());
    final SingleStoreDefaultValueConverter defaultValueConverter = new SingleStoreDefaultValueConverter(
        valueConverter);

    MainConnectionProvidingConnectionFactory<SingleStoreConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
        () -> new SingleStoreConnection(
            new SingleStoreConnection.SingleStoreConnectionConfiguration(config)));

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
    validateOffset(connectionFactory, previousOffsets, connectorConfig.getSnapshotMode());

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
            () -> new SingleStoreConnection(
                new SingleStoreConnection.SingleStoreConnectionConfiguration(heartbeatConfig)),
            exception -> {
              final String sqlErrorId = exception.getMessage();
              throw new DebeziumException(
                  "Could not execute heartbeat action query (Error: " + sqlErrorId + ")",
                  exception);
            }
        ),
        schemaNameAdjuster,
        signalProcessor);

    NotificationService<SingleStorePartition, SingleStoreOffsetContext> notificationService = new NotificationService<>(
        getNotificationChannels(),
        connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

    SingleStoreChangeEventSourceCoordinator coordinator = new SingleStoreChangeEventSourceCoordinator(
        previousOffsets,
        errorHandler,
        SingleStoreConnector.class,
        connectorConfig,
        new SingleStoreChangeEventSourceFactory(connectorConfig, connectionFactory, schema,
            dispatcher, errorHandler, clock),
        new SingleStoreChangeEventSourceMetricsFactory(),
        dispatcher,
        schema,
        signalProcessor,
        notificationService);

    coordinator.start(taskContext, this.queue, metadataProvider);

    return coordinator;
  }

    private void validateOffset(MainConnectionProvidingConnectionFactory<SingleStoreConnection> connectionFactory,
                                Offsets<SingleStorePartition, SingleStoreOffsetContext> previousOffsets,
                                SingleStoreConnectorConfig.SnapshotMode snapshotMode) {
        if (previousOffsets.getOffsets() != null && snapshotMode == SingleStoreConnectorConfig.SnapshotMode.WHEN_NEEDED) {
            try (SingleStoreConnection connection = connectionFactory.newConnection()) {
                schema.refresh(connection);
                Set<TableId> tableIds = schema.tableIds();
                if (tableIds == null || tableIds.isEmpty()) {
                    return;
                }
                assert (tableIds.size() == 1);
                for (Map.Entry<SingleStorePartition, SingleStoreOffsetContext> previousOffset : previousOffsets) {
                    SingleStorePartition partition = previousOffset.getKey();
                    SingleStoreOffsetContext offset = previousOffset.getValue();
                    if (offset != null && !connection.validateOffset(tableIds, partition, offset)) {
                        LOGGER.info("The last recorded offset is no longer available but we are in {} snapshot mode. "
                                + "Attempting to snapshot data to fill the gap.", snapshotMode.name());
                        previousOffsets.resetOffset(previousOffsets.getTheOnlyPartition());
                        return;
                    }
                }
            } catch (SQLException e) {
                LOGGER.error("Failed to validate offset");
                throw new RuntimeException(e);
            }
        }
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
