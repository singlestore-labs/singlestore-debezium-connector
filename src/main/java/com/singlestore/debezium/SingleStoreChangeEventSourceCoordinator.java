package com.singlestore.debezium;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.schema.DataCollectionId;
import java.util.Optional;
import org.apache.kafka.connect.source.SourceConnector;

public class SingleStoreChangeEventSourceCoordinator extends
    ChangeEventSourceCoordinator<SingleStorePartition, SingleStoreOffsetContext> {

  public SingleStoreChangeEventSourceCoordinator(
      Offsets<SingleStorePartition, SingleStoreOffsetContext> previousOffsets,
      ErrorHandler errorHandler,
      Class<? extends SourceConnector> connectorType,
      CommonConnectorConfig connectorConfig,
      ChangeEventSourceFactory<SingleStorePartition, SingleStoreOffsetContext> changeEventSourceFactory,
      ChangeEventSourceMetricsFactory<SingleStorePartition> changeEventSourceMetricsFactory,
      EventDispatcher<SingleStorePartition, ?> eventDispatcher,
      DatabaseSchema<?> schema,
      SignalProcessor<SingleStorePartition, SingleStoreOffsetContext> signalProcessor,
      NotificationService<SingleStorePartition, SingleStoreOffsetContext> notificationService) {
    super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
        changeEventSourceMetricsFactory, eventDispatcher, schema, signalProcessor,
        notificationService);
  }

  @Override
  protected void initStreamEvents(SingleStorePartition partition,
      SingleStoreOffsetContext offsetContext) throws InterruptedException {

    streamingSource = changeEventSourceFactory.getStreamingChangeEventSource();
    eventDispatcher.setEventListener(streamingMetrics);
    streamingSource.init(offsetContext);

    getSignalProcessor(previousOffsets)
        .ifPresent(s -> s.setContext(streamingSource.getOffsetContext()));

    final Optional<IncrementalSnapshotChangeEventSource<SingleStorePartition, ? extends DataCollectionId>> incrementalSnapshotChangeEventSource = changeEventSourceFactory
        .getIncrementalSnapshotChangeEventSource(offsetContext, snapshotMetrics, snapshotMetrics,
            notificationService);
    eventDispatcher.setIncrementalSnapshotChangeEventSource(incrementalSnapshotChangeEventSource);
    incrementalSnapshotChangeEventSource.ifPresent(x -> x.init(partition, offsetContext));
  }
}
